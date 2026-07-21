//! CPU task pool (PROTOCOL §5.1), two modes:
//!
//! **Fork-server mode (default):** ONE parent process
//! (`{python} -m cauli._exec --app {spec} --fork-server --connect {sock}
//! --child-threads {M}`) imports the app once, calls `gc.freeze()`, and forks
//! a child per `{"cmd":"fork"}` control line (stdin/stdout). Each forked
//! child connects back to the worker's unix socket listener, sends
//! `{"ready": true, "pid": N, "concurrency": M}`, then serves the line
//! protocol on that connection with up to M requests in flight (responses
//! matched by `id`, possibly out of order). Hard timeout: SIGKILL the child
//! by pid and request a replacement fork (cheap: no re-import). Child death
//! fails its in-flight requests as WorkerLost (retryable). If the parent's
//! control channel breaks mid-run the parent is respawned; if fork-server
//! startup fails outright the pool falls back to stdio mode.
//!
//! **Stdio mode (fallback, `--no-fork-server`):** each child is spawned
//! directly (`{python} -m cauli._exec --app {spec}`), speaks the protocol on
//! its own stdin/stdout, one request in flight, kill+respawn on hard timeout
//! or death. This is the pre-fork-server behavior, preserved verbatim.
//!
//! Test hook (documented, M5): if env var `CAULI_EXEC_CMD` is set, it is
//! split on whitespace and used verbatim as the child argv instead of
//! `{python} -m cauli._exec --app {spec}` (fork-server mode appends its
//! `--fork-server --connect ... --child-threads ...` flags to the override
//! argv too). This lets the e2e suite run a standalone stand-in child
//! (tests/fixtures/fake_exec.py) without the real cauli Python package
//! installed. Compiled in only under `cfg(test)` or the `test-hooks`
//! feature -- a plain `cargo build --release` has no code path that reads
//! this env var at all, so `cargo test --features test-hooks` is required to
//! exercise it (see worker/Cargo.toml `[features]`).

use crate::stats::Counters;
use anyhow::Context;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixListener;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, timeout, Instant};
use tracing::{info, warn};

/// Handshake budget for the parent's server-ready line, a forked child's
/// ready line, and a control-channel fork reply.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a pool slot waits for a requested fork to connect before asking
/// again (covers a lost fork request across a parent respawn).
const FORK_WAIT: Duration = Duration::from_secs(60);

pub enum CpuOutcome {
    /// Raw response line from the child.
    Resp(String),
    /// Hard timeout: child was SIGKILLed and respawned.
    Timeout,
    /// Child died / pipe broke: respawned.
    Lost,
}

pub struct CpuJob {
    /// Wire correlation id (unique per request; §5.1 fork-server mode matches
    /// responses to requests by this).
    pub id: String,
    pub req_line: String,
    pub timeout_ms: u64,
    pub resp: oneshot::Sender<CpuOutcome>,
}

#[derive(Clone)]
pub struct CpuPool {
    pub tx: async_channel::Sender<CpuJob>,
    /// Number of dispatch tasks currently blocked on a full backlog; the fetch
    /// loop pauses while > 0 so the in-worker cpu backlog stays bounded
    /// without starving io fetch indefinitely (children always make progress
    /// thanks to the hard-timeout SIGKILL).
    pub overflow: Arc<AtomicUsize>,
    /// Live executor pids: fork-server parent + serving children (fork mode)
    /// or the spawned children (stdio mode). Killed on worker exit paths.
    pub child_pids: Arc<Mutex<Vec<u32>>>,
    /// Unix listener path (fork-server mode), removed on shutdown.
    pub sock_path: Option<Arc<PathBuf>>,
}

pub fn child_argv(python: &str, app_spec: &str) -> (String, Vec<String>) {
    // M5: the CAULI_EXEC_CMD override is a test-only hook (e2e uses it to run
    // tests/fixtures/fake_exec.py without the real cauli package). Compiled
    // out entirely for a normal `cargo build --release` so a production
    // binary has no env-driven way to replace the cpu child command; only
    // `cargo test` / `--features test-hooks` builds honor it.
    #[cfg(any(test, feature = "test-hooks"))]
    if let Ok(cmd) = std::env::var("CAULI_EXEC_CMD") {
        tracing::warn!(
            "CAULI_EXEC_CMD test hook active: overriding cpu child command with {cmd:?}"
        );
        let mut parts = cmd.split_whitespace().map(str::to_string);
        if let Some(prog) = parts.next() {
            return (prog, parts.collect());
        }
    }
    (
        python.to_string(),
        vec![
            "-m".into(),
            "cauli._exec".into(),
            "--app".into(),
            app_spec.into(),
        ],
    )
}

/// Start the cpu pool. Tries fork-server mode unless `no_fork_server`; any
/// startup failure there (listener bind, parent spawn, no server-ready line)
/// logs a warning and falls back to stdio mode.
pub async fn start(
    workers: usize,
    child_threads: usize,
    python: &str,
    app_spec: &str,
    no_fork_server: bool,
    counters: Arc<Counters>,
) -> CpuPool {
    let child_threads = child_threads.max(1);
    // Backlog bound: 2 in-flight-capacities worth of pending items.
    let cap = (2 * workers * child_threads).max(1);
    let (tx, rx) = async_channel::bounded::<CpuJob>(cap);
    let overflow = Arc::new(AtomicUsize::new(0));
    let pids = Arc::new(Mutex::new(Vec::new()));
    let (prog, argv) = child_argv(python, app_spec);

    if !no_fork_server {
        match start_fork_server(
            workers,
            child_threads,
            prog.clone(),
            argv.clone(),
            rx.clone(),
            counters.clone(),
            pids.clone(),
        )
        .await
        {
            Ok(sock_path) => {
                return CpuPool {
                    tx,
                    overflow,
                    child_pids: pids,
                    sock_path: Some(Arc::new(sock_path)),
                };
            }
            Err(e) => warn!("cpu: fork-server startup failed ({e:#}); falling back to stdio mode"),
        }
    }

    for i in 0..workers {
        tokio::spawn(stdio_child_loop(
            i,
            prog.clone(),
            argv.clone(),
            rx.clone(),
            counters.clone(),
            pids.clone(),
        ));
    }
    CpuPool {
        tx,
        overflow,
        child_pids: pids,
        sock_path: None,
    }
}

/// SIGKILL every live executor pid (children + fork-server parent) and clean
/// up the listener socket. Used on process exit paths; every executor also
/// carries PR_SET_PDEATHSIG (parent via pre_exec, forked children re-arm it
/// themselves) so a SIGKILLed worker cannot leak them either.
pub fn kill_children(pool: &CpuPool) {
    let pids = pool.child_pids.lock().unwrap().clone();
    for pid in pids {
        kill_pid(pid);
    }
    if let Some(p) = &pool.sock_path {
        let _ = std::fs::remove_file(p.as_path());
    }
}

fn kill_pid(pid: u32) {
    // L1: pid 0 is never a real executor (Command::id() is Some right after a
    // successful spawn, and ready lines with pid <= 1 are rejected);
    // `kill(0, SIGKILL)` signals this process's ENTIRE process group
    // (self-SIGKILL) rather than one child, so it must never reach libc::kill
    // even if tracking ever regresses.
    if pid <= 1 {
        return;
    }
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
}

fn track_pid(pids: &Mutex<Vec<u32>>, pid: u32, add: bool) {
    let mut g = pids.lock().unwrap();
    if add {
        g.push(pid);
    } else {
        g.retain(|p| *p != pid);
    }
}

// ---------------------------------------------------------------------------
// fork-server mode
// ---------------------------------------------------------------------------

struct ParentProc {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    pid: u32,
}

struct ChildConn {
    lines: Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
    pid: u32,
    concurrency: usize,
}

fn fork_sock_path() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("cauli-cpu-{}-{nanos:x}.sock", std::process::id()))
}

fn spawn_parent(
    prog: &str,
    argv: &[String],
    sock_path: &PathBuf,
    child_threads: usize,
) -> anyhow::Result<ParentProc> {
    let mut cmd = Command::new(prog);
    cmd.args(argv)
        .arg("--fork-server")
        .arg("--connect")
        .arg(sock_path)
        .arg("--child-threads")
        .arg(child_threads.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .env("PYTHONUNBUFFERED", "1")
        .kill_on_drop(true);
    unsafe {
        cmd.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            Ok(())
        });
    }
    let mut child = cmd.spawn().context("spawn fork-server parent")?;
    let pid = child.id().unwrap_or(0);
    let stdin = child.stdin.take().expect("parent stdin piped");
    let lines = BufReader::new(child.stdout.take().expect("parent stdout piped")).lines();
    Ok(ParentProc {
        child,
        stdin,
        lines,
        pid,
    })
}

/// Read the parent's `{"server": true, "pid": N}` line (its app import and
/// gc.freeze happen before it, so allow the full handshake budget).
async fn wait_server_ready(parent: &mut ParentProc) -> anyhow::Result<()> {
    let line = timeout(READY_TIMEOUT, parent.lines.next_line())
        .await
        .context("timed out waiting for fork-server ready line")?
        .context("read fork-server ready line")?
        .context("fork-server parent closed stdout before ready line")?;
    let v: serde_json::Value =
        serde_json::from_str(&line).context("unparseable fork-server ready line")?;
    if v["server"] != serde_json::Value::Bool(true) {
        anyhow::bail!("bad fork-server ready line: {line}");
    }
    Ok(())
}

async fn start_fork_server(
    workers: usize,
    child_threads: usize,
    prog: String,
    argv: Vec<String>,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) -> anyhow::Result<PathBuf> {
    let sock_path = fork_sock_path();
    let _ = std::fs::remove_file(&sock_path);
    let listener = UnixListener::bind(&sock_path)
        .with_context(|| format!("bind unix listener at {}", sock_path.display()))?;

    let mut parent = spawn_parent(&prog, &argv, &sock_path, child_threads)?;
    if let Err(e) = wait_server_ready(&mut parent).await {
        let _ = parent.child.start_kill();
        let _ = parent.child.wait().await;
        return Err(e);
    }
    info!(
        "cpu: fork-server parent ready pid={} (children: {workers} x {child_threads} threads)",
        parent.pid
    );
    track_pid(&pids, parent.pid, true);

    let (fork_tx, fork_rx) = mpsc::channel::<()>(workers * 2 + 4);
    let (conn_tx, conn_rx) = async_channel::unbounded::<ChildConn>();

    tokio::spawn(parent_control_loop(
        parent,
        fork_rx,
        prog,
        argv,
        sock_path.clone(),
        child_threads,
        pids.clone(),
    ));
    tokio::spawn(acceptor_loop(listener, conn_tx));
    for i in 0..workers {
        tokio::spawn(slot_loop(
            i,
            fork_tx.clone(),
            conn_rx.clone(),
            rx.clone(),
            counters.clone(),
            pids.clone(),
        ));
    }
    Ok(sock_path)
}

/// Owns the fork-server parent: writes one `{"cmd":"fork"}` per request from
/// the pool slots and reads the `{"forked": pid}` reply. A control-channel
/// failure mid-run (parent crashed/killed) respawns the parent; its children
/// died with it (PDEATHSIG), so the slots' connection EOFs re-request forks
/// which the fresh parent then serves. The failed request itself is dropped:
/// the requesting slot re-asks after FORK_WAIT.
async fn parent_control_loop(
    mut parent: ParentProc,
    mut fork_rx: mpsc::Receiver<()>,
    prog: String,
    argv: Vec<String>,
    sock_path: PathBuf,
    child_threads: usize,
    pids: Arc<Mutex<Vec<u32>>>,
) {
    loop {
        if fork_rx.recv().await.is_none() {
            // all slots gone (worker exiting): shut the parent down
            let _ = parent.child.start_kill();
            let _ = parent.child.wait().await;
            track_pid(&pids, parent.pid, false);
            return;
        }
        match request_fork(&mut parent).await {
            Ok(pid) => info!("cpu: fork-server forked child pid={pid}"),
            Err(e) => {
                warn!("cpu: fork-server control channel failed ({e:#}); respawning parent");
                let _ = parent.child.start_kill();
                let _ = parent.child.wait().await;
                track_pid(&pids, parent.pid, false);
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    match spawn_parent(&prog, &argv, &sock_path, child_threads) {
                        Ok(mut p) => {
                            match wait_server_ready(&mut p).await {
                                Ok(()) => {
                                    info!("cpu: fork-server parent respawned pid={}", p.pid);
                                    track_pid(&pids, p.pid, true);
                                    parent = p;
                                    break;
                                }
                                Err(e) => {
                                    warn!("cpu: fork-server parent respawn not ready ({e:#}); retrying");
                                    let _ = p.child.start_kill();
                                    let _ = p.child.wait().await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("cpu: fork-server parent respawn failed ({e:#}); retrying")
                        }
                    }
                }
            }
        }
    }
}

async fn request_fork(parent: &mut ParentProc) -> anyhow::Result<u32> {
    parent
        .stdin
        .write_all(b"{\"cmd\":\"fork\"}\n")
        .await
        .context("write fork command")?;
    let line = timeout(READY_TIMEOUT, parent.lines.next_line())
        .await
        .context("timed out waiting for fork reply")?
        .context("read fork reply")?
        .context("fork-server parent closed stdout")?;
    let v: serde_json::Value = serde_json::from_str(&line).context("unparseable fork reply")?;
    match v["forked"].as_u64() {
        Some(pid) if pid > 1 => Ok(pid as u32),
        _ => anyhow::bail!("fork request refused: {line}"),
    }
}

/// Accept forked-child connections and complete their ready handshake off the
/// accept path (a child that never sends its ready line must not stall other
/// children's handshakes).
async fn acceptor_loop(listener: UnixListener, conn_tx: async_channel::Sender<ChildConn>) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tx = conn_tx.clone();
                tokio::spawn(async move {
                    match read_child_ready(stream).await {
                        Ok(conn) => {
                            let _ = tx.send(conn).await;
                        }
                        Err(e) => warn!("cpu: forked child handshake failed: {e:#}"),
                    }
                });
            }
            Err(e) => {
                warn!("cpu: unix accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Read `{"ready": true, "pid": N, "concurrency": M}` from a fresh child
/// connection. The same buffered reader is kept for the serving loop so no
/// bytes can be lost between handshake and first response.
async fn read_child_ready(stream: tokio::net::UnixStream) -> anyhow::Result<ChildConn> {
    let (read, write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    let line = timeout(READY_TIMEOUT, lines.next_line())
        .await
        .context("timed out waiting for child ready line")?
        .context("read child ready line")?
        .context("child closed connection before ready line")?;
    let v: serde_json::Value = serde_json::from_str(&line).context("unparseable ready line")?;
    if v["ready"] != serde_json::Value::Bool(true) {
        anyhow::bail!("bad ready line: {line}");
    }
    let pid = match v["pid"].as_u64() {
        Some(p) if p > 1 => p as u32,
        _ => anyhow::bail!("ready line without a usable pid: {line}"),
    };
    let concurrency = v["concurrency"].as_u64().unwrap_or(1).max(1) as usize;
    Ok(ChildConn {
        lines,
        write,
        pid,
        concurrency,
    })
}

/// One pool slot: keep exactly one serving child alive, requesting a
/// replacement fork whenever the current child is gone (death, hard-timeout
/// SIGKILL). Respawns are cheap: the parent forks its warmed, frozen image.
async fn slot_loop(
    idx: usize,
    fork_tx: mpsc::Sender<()>,
    conn_rx: async_channel::Receiver<ChildConn>,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) {
    loop {
        if fork_tx.send(()).await.is_err() {
            return; // control loop gone: worker exiting
        }
        let conn = match timeout(FORK_WAIT, conn_rx.recv()).await {
            Ok(Ok(c)) => c,
            Ok(Err(_)) => return, // acceptor gone: worker exiting
            Err(_) => {
                warn!("cpu[{idx}]: no forked child connected within {FORK_WAIT:?}; re-requesting");
                continue;
            }
        };
        info!(
            "cpu[{idx}]: fork child serving pid={} concurrency={}",
            conn.pid, conn.concurrency
        );
        if !serve_child(idx, conn, &rx, &counters, &pids).await {
            return; // job channel closed: worker exiting
        }
    }
}

struct Pending {
    resp: oneshot::Sender<CpuOutcome>,
    deadline: Instant,
}

enum ChildGone {
    /// EOF / read or write error: the child process died.
    Died,
    /// A request exceeded its hard timeout: SIGKILL the child.
    HardTimeout,
    /// The job channel closed: worker shutdown.
    Shutdown,
}

/// Serve one child connection, multiplexing up to `concurrency` requests in
/// flight (pending map keyed by request id). Returns false when the worker is
/// shutting down (job channel closed), true when the slot should fork a
/// replacement child.
async fn serve_child(
    idx: usize,
    conn: ChildConn,
    rx: &async_channel::Receiver<CpuJob>,
    counters: &Arc<Counters>,
    pids: &Arc<Mutex<Vec<u32>>>,
) -> bool {
    let ChildConn {
        mut lines,
        mut write,
        pid,
        concurrency,
    } = conn;
    track_pid(pids, pid, true);
    let mut pending: HashMap<String, Pending> = HashMap::new();

    let gone = loop {
        let next_deadline = pending.values().map(|p| p.deadline).min();
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(l)) => {
                        let rid = serde_json::from_str::<serde_json::Value>(&l)
                            .ok()
                            .and_then(|v| v.get("id").and_then(|x| x.as_str()).map(str::to_string));
                        match rid.and_then(|id| pending.remove(&id)) {
                            Some(p) => {
                                let _ = p.resp.send(CpuOutcome::Resp(l));
                                counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
                            }
                            None => warn!(
                                "cpu[{idx}] pid={pid}: response with unknown or missing id: {}",
                                crate::envelope::safe_truncate(&l, 256)
                            ),
                        }
                    }
                    _ => {
                        warn!(
                            "cpu[{idx}] pid={pid}: child connection closed ({} in flight -> WorkerLost)",
                            pending.len()
                        );
                        break ChildGone::Died;
                    }
                }
            }
            job = rx.recv(), if pending.len() < concurrency => {
                match job {
                    Ok(job) => {
                        let mut l = job.req_line;
                        l.push('\n');
                        if let Err(e) = write.write_all(l.as_bytes()).await {
                            warn!("cpu[{idx}] pid={pid}: write failed: {e}");
                            let _ = job.resp.send(CpuOutcome::Lost);
                            break ChildGone::Died;
                        }
                        counters.inflight_cpu.fetch_add(1, Ordering::Relaxed);
                        // H3-style saturation: a crafted/huge timeout_ms must
                        // not overflow Instant math into a panic or a
                        // near-zero deadline.
                        let deadline = Instant::now()
                            .checked_add(Duration::from_millis(job.timeout_ms))
                            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400 * 365));
                        pending.insert(job.id, Pending { resp: job.resp, deadline });
                    }
                    Err(_) => break ChildGone::Shutdown,
                }
            }
            // Some(_) pattern guard: the branch is disabled when nothing is
            // pending, so unwrap-by-pattern here is safe.
            _ = sleep_until(next_deadline.unwrap_or_else(Instant::now)), if next_deadline.is_some() => {
                warn!(
                    "cpu[{idx}] pid={pid}: hard timeout with {} in flight; SIGKILL + replacement fork",
                    pending.len()
                );
                break ChildGone::HardTimeout;
            }
        }
    };

    // The child is gone (or being disposed of): SIGKILL is idempotent and the
    // fork-server parent reaps it. Resolve every in-flight request: expired
    // ones as Timeout, the rest as Lost (retryable WorkerLost).
    kill_pid(pid);
    let now = Instant::now();
    for (_, p) in pending.drain() {
        let out = match gone {
            ChildGone::HardTimeout if p.deadline <= now => CpuOutcome::Timeout,
            _ => CpuOutcome::Lost,
        };
        let _ = p.resp.send(out);
        counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
    }
    track_pid(pids, pid, false);
    !matches!(gone, ChildGone::Shutdown)
}

// ---------------------------------------------------------------------------
// stdio mode (fallback): spawn-per-child, one request in flight
// ---------------------------------------------------------------------------

async fn stdio_child_loop(
    idx: usize,
    prog: String,
    argv: Vec<String>,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) {
    loop {
        let mut cmd = Command::new(&prog);
        cmd.args(&argv)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .env("PYTHONUNBUFFERED", "1")
            .kill_on_drop(true);
        unsafe {
            cmd.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                Ok(())
            });
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                warn!("cpu[{idx}]: failed to spawn {prog}: {e}; retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let pid = child.id().unwrap_or(0);
        track_pid(&pids, pid, true);

        let mut stdin = child.stdin.take().expect("child stdin piped");
        let mut lines = BufReader::new(child.stdout.take().expect("child stdout piped")).lines();

        // Ready line: {"ready": true, "pid": N}
        let ready_ok = match timeout(READY_TIMEOUT, lines.next_line()).await {
            Ok(Ok(Some(line))) => {
                let ok = serde_json::from_str::<serde_json::Value>(&line)
                    .map(|v| v["ready"] == serde_json::Value::Bool(true))
                    .unwrap_or(false);
                if !ok {
                    warn!("cpu[{idx}] pid={pid}: bad ready line: {line}");
                }
                ok
            }
            other => {
                warn!("cpu[{idx}] pid={pid}: no ready line ({other:?})");
                false
            }
        };
        if !ready_ok {
            let _ = child.start_kill();
            let _ = child.wait().await;
            track_pid(&pids, pid, false);
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        info!("cpu[{idx}]: child ready pid={pid}");

        let mut respawn = false;
        while !respawn {
            let job = match rx.recv().await {
                Ok(j) => j,
                Err(_) => {
                    // channel closed: shut this child down
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    track_pid(&pids, pid, false);
                    return;
                }
            };
            counters.inflight_cpu.fetch_add(1, Ordering::Relaxed);

            let mut line = job.req_line;
            line.push('\n');
            if let Err(e) = stdin.write_all(line.as_bytes()).await {
                warn!("cpu[{idx}] pid={pid}: write failed: {e}");
                let _ = job.resp.send(CpuOutcome::Lost);
                counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
                break; // exits while: child gets killed + respawned below
            }

            match timeout(Duration::from_millis(job.timeout_ms), lines.next_line()).await {
                Ok(Ok(Some(resp))) => {
                    let _ = job.resp.send(CpuOutcome::Resp(resp));
                }
                Ok(_) => {
                    // EOF or read error: child died mid-task
                    warn!("cpu[{idx}] pid={pid}: child died mid-task (WorkerLost)");
                    let _ = job.resp.send(CpuOutcome::Lost);
                    respawn = true;
                }
                Err(_) => {
                    warn!(
                        "cpu[{idx}] pid={pid}: hard timeout after {}ms; SIGKILL + respawn",
                        job.timeout_ms
                    );
                    let _ = job.resp.send(CpuOutcome::Timeout);
                    respawn = true;
                }
            }
            counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
        }

        let _ = child.start_kill();
        let _ = child.wait().await;
        track_pid(&pids, pid, false);
    }
}
