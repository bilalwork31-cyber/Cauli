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
//! control channel breaks mid-run the parent is respawned and the fork
//! request that was in flight is retried against the fresh parent, rather
//! than dropped for the requesting slot's `FORK_WAIT` backstop to notice; a
//! *healthy* parent's transient fork refusal (EAGAIN/ENOMEM) retries with
//! backoff and never touches the parent. If fork-server startup fails
//! outright the pool falls back to stdio mode. The listener socket lives in
//! a private (0700) directory and every accepted connection is checked
//! against our own uid (SO_PEERCRED) before it is trusted as a real child.
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
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixListener;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::{sleep_until, timeout, Instant};
use tracing::{info, warn};

/// Handshake budget for the parent's server-ready line, a forked child's
/// ready line, and a control-channel fork reply.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a pool slot waits for a requested fork to connect before asking
/// again. `parent_control_loop` already retries a request internally until
/// it succeeds (FS-3/FS-6), so this is a backstop for the rarer case where
/// the fork itself succeeded but the resulting child never completed its
/// ready handshake (e.g. it crashed between connect() and its ready line).
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
    /// Admission slots for cpu work: `workers * (child_threads + prefetch)`,
    /// the number of requests the pool can hold without a new one queueing
    /// behind work that has not started.
    ///
    /// `run_cpu_task` never took an io slot, so `io_sem` sat at full and the
    /// fetch loop's bound did nothing for cpu work: entries piled into the
    /// backlog channel, into blocked dispatch tasks and into each child's
    /// staged queue, all with their PEL idle clocks running from delivery.
    /// Past `max(visibility_timeout, timeout_ms + grace)` the recovery loop
    /// reclaimed them, and four reclaims dead lettered a task as
    /// `RedeliveryLimitExceeded` that had executed zero instructions. This
    /// gives the cpu lane an admission gate of its own, and a dispatch
    /// waiting on it counts into `overflow`, which is what the fetch loop
    /// and the recovery loop's admission gate already watch.
    pub admission: Arc<Semaphore>,
    /// Number of dispatch tasks currently blocked on a full backlog or
    /// waiting for an admission slot; the fetch loop pauses while > 0 so the
    /// in-worker cpu backlog stays bounded without starving io fetch
    /// indefinitely (children always make progress thanks to the
    /// hard-timeout SIGKILL).
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

/// A pool with no executors at all, for an app that registers no
/// `kind = "cpu"` task.
///
/// Routing is by REGISTRY kind, not envelope kind (dispatch.rs: "registry
/// authoritative over envelope kind"), so when no registered task is cpu-kind
/// `exec::run_cpu_task` is unreachable by construction -- no fork-server
/// parent, no children, and none of their RAM. The channel is created with
/// its receiver dropped so the defensive path (if this ever did get reached)
/// fails fast and loudly as `WorkerLost` rather than parking forever on a
/// send nobody will service.
pub fn disabled() -> CpuPool {
    let (tx, rx) = async_channel::bounded::<CpuJob>(1);
    drop(rx);
    CpuPool {
        tx,
        admission: Arc::new(Semaphore::new(1)),
        overflow: Arc::new(AtomicUsize::new(0)),
        child_pids: Arc::new(Mutex::new(Vec::new())),
        sock_path: None,
    }
}

/// Everything needed to start the pool. Held by Ctx so the pool can start
/// lazily: forked on the first cpu task rather than at boot, so an io heavy
/// deployment that registers a rarely used cpu task does not pay resident
/// children for work that has not arrived (`--eager-cpu` restores warmup).
#[derive(Clone)]
pub struct StartCfg {
    pub workers: usize,
    pub child_threads: usize,
    pub prefetch: usize,
    /// Recycle a child after this many completed tasks (0 = never). The
    /// backstop for leaky C extensions and CoW pages dirtied over hours,
    /// same role as Celery's maxtasksperchild.
    pub recycle: usize,
    pub python: String,
    pub app_spec: String,
    pub no_fork_server: bool,
}

/// Start the cpu pool. Tries fork-server mode unless `no_fork_server`; any
/// startup failure there (listener bind, parent spawn, no server-ready line)
/// logs a warning and falls back to stdio mode.
pub async fn start(cfg: StartCfg, counters: Arc<Counters>) -> CpuPool {
    let StartCfg {
        workers,
        child_threads,
        prefetch,
        recycle,
        python,
        app_spec,
        no_fork_server,
    } = cfg;
    let child_threads = child_threads.max(1);
    // Backlog bound: 2 in-flight-capacities worth of pending items. Prefetched
    // requests live in the children, not this channel, so they are counted in
    // the per-child queue depth rather than here.
    let cap = (2 * workers * child_threads).max(1);
    let (tx, rx) = async_channel::bounded::<CpuJob>(cap);
    // One admission slot per request the pool can actually hold: the
    // executing ones plus the deliberately staged prefetch depth. Anything
    // beyond that would be parking with its idle clock running (see the
    // `admission` field on CpuPool).
    let admission = Arc::new(Semaphore::new(
        workers
            .saturating_mul(child_threads.saturating_add(prefetch))
            .max(1),
    ));
    let overflow = Arc::new(AtomicUsize::new(0));
    let pids = Arc::new(Mutex::new(Vec::new()));
    let (prog, argv) = child_argv(&python, &app_spec);

    if !no_fork_server {
        match start_fork_server(
            PoolCfg {
                workers,
                child_threads,
                prefetch,
                recycle,
                prog: prog.clone(),
                argv: argv.clone(),
            },
            rx.clone(),
            counters.clone(),
            pids.clone(),
        )
        .await
        {
            Ok(sock_path) => {
                return CpuPool {
                    tx,
                    admission,
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
            recycle,
            rx.clone(),
            counters.clone(),
            pids.clone(),
        ));
    }
    CpuPool {
        tx,
        admission,
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
        cleanup_sock_path(p);
    }
}

/// Remove the fork-server socket file and its private containing directory
/// (FS-8). Called on every path that can leave a bound socket behind: clean
/// shutdown, forced double-signal exit, and fork-server startup failure
/// after a successful bind. `remove_dir` only succeeds once the directory is
/// empty, so this is safe to call from a partial-startup path too.
fn cleanup_sock_path(p: &std::path::Path) {
    let _ = std::fs::remove_file(p);
    if let Some(dir) = p.parent() {
        let _ = std::fs::remove_dir(dir);
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

/// Create a private (0700) directory under the system temp dir and return a
/// socket path inside it (FS-1). Connecting to a unix-domain socket on Linux
/// also requires search/write permission on its containing directory, so a
/// 0700 directory alone already stops any other local uid from reaching the
/// socket; the SO_PEERCRED check in `read_child_ready` is defense in depth
/// on top of that (belt-and-suspenders against a umask/platform surprise).
fn fork_sock_path() -> anyhow::Result<PathBuf> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("cauli-cpu-{}-{nanos:x}", std::process::id()));
    std::fs::create_dir(&dir).with_context(|| format!("create {}", dir.display()))?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod 0700 {}", dir.display()))?;
    Ok(dir.join("cpu.sock"))
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

/// Pool shape: how many children, how each one is launched, and how deeply
/// each is fed. Grouped rather than passed as a parameter run, which both
/// reads better and keeps the fork-server entry point under clippy's argument
/// limit as the knobs grow.
pub struct PoolCfg {
    pub workers: usize,
    pub child_threads: usize,
    pub prefetch: usize,
    pub recycle: usize,
    pub prog: String,
    pub argv: Vec<String>,
}

async fn start_fork_server(
    cfg: PoolCfg,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) -> anyhow::Result<PathBuf> {
    let PoolCfg {
        workers,
        child_threads,
        prefetch,
        recycle,
        prog,
        argv,
    } = cfg;
    let sock_path = fork_sock_path()?;
    let listener = UnixListener::bind(&sock_path)
        .with_context(|| format!("bind unix listener at {}", sock_path.display()))?;
    // FS-1 defense in depth: explicitly restrict the socket file itself too,
    // rather than relying solely on whatever the platform's default bind()
    // mode (subject to umask) happens to produce.
    std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", sock_path.display()))?;

    let mut parent = spawn_parent(&prog, &argv, &sock_path, child_threads)?;
    if let Err(e) = wait_server_ready(&mut parent).await {
        let _ = parent.child.start_kill();
        let _ = parent.child.wait().await;
        cleanup_sock_path(&sock_path); // FS-8: don't abandon a bound socket on startup failure
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
    tokio::spawn(acceptor_loop(listener, conn_tx, child_threads));
    for i in 0..workers {
        tokio::spawn(slot_loop(
            i,
            fork_tx.clone(),
            conn_rx.clone(),
            ServeCfg { prefetch, recycle },
            rx.clone(),
            counters.clone(),
            pids.clone(),
        ));
    }
    Ok(sock_path)
}

/// Owns the fork-server parent: writes one `{"cmd":"fork"}` per request from
/// the pool slots and reads the reply. FS-3/FS-6: a request is retried until
/// it succeeds instead of being dropped on the first setback. A parseable
/// `{"error": ...}` reply from a HEALTHY parent (e.g. transient
/// EAGAIN/ENOMEM) retries with backoff and never touches the parent process;
/// a genuine control-channel failure (parent crashed/killed) respawns the
/// parent and then retries the SAME request against the fresh one, so the
/// requesting slot is served promptly instead of waiting out its FORK_WAIT
/// backstop.
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
        let mut backoff = Duration::from_millis(100);
        loop {
            match request_fork(&mut parent).await {
                Ok(ForkResult::Forked(pid)) => {
                    info!("cpu: fork-server forked child pid={pid}");
                    break;
                }
                Ok(ForkResult::Refused(err)) => {
                    warn!("cpu: fork refused by a healthy parent ({err}); retrying in {backoff:?}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                }
                Err(e) => {
                    warn!("cpu: fork-server control channel failed ({e:#}); respawning parent");
                    let _ = parent.child.start_kill();
                    let _ = parent.child.wait().await;
                    track_pid(&pids, parent.pid, false);
                    parent = respawn_parent(&prog, &argv, &sock_path, child_threads, &pids).await;
                    // loop back and retry request_fork against the fresh parent
                }
            }
        }
    }
}

/// Block until a fresh fork-server parent is spawned and ready, retrying
/// once a second. Used after the control channel breaks.
async fn respawn_parent(
    prog: &str,
    argv: &[String],
    sock_path: &PathBuf,
    child_threads: usize,
    pids: &Arc<Mutex<Vec<u32>>>,
) -> ParentProc {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        match spawn_parent(prog, argv, sock_path, child_threads) {
            Ok(mut p) => match wait_server_ready(&mut p).await {
                Ok(()) => {
                    info!("cpu: fork-server parent respawned pid={}", p.pid);
                    track_pid(pids, p.pid, true);
                    return p;
                }
                Err(e) => {
                    warn!("cpu: fork-server parent respawn not ready ({e:#}); retrying");
                    let _ = p.child.start_kill();
                    let _ = p.child.wait().await;
                }
            },
            Err(e) => warn!("cpu: fork-server parent respawn failed ({e:#}); retrying"),
        }
    }
}

enum ForkResult {
    /// The parent forked a child successfully.
    Forked(u32),
    /// The parent is healthy and replied with a parseable `{"error": ...}`
    /// (e.g. transient EAGAIN/ENOMEM) -- FS-3: this must NOT be treated the
    /// same as a dead/unreachable parent (no kill, no respawn).
    Refused(String),
}

async fn request_fork(parent: &mut ParentProc) -> anyhow::Result<ForkResult> {
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
    if let Some(pid) = v["forked"].as_u64().filter(|&p| p > 1) {
        return Ok(ForkResult::Forked(pid as u32));
    }
    if let Some(err) = v.get("error") {
        let msg = err
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| err.to_string());
        return Ok(ForkResult::Refused(msg));
    }
    anyhow::bail!("unexpected fork reply: {line}")
}

/// Accept forked-child connections and complete their ready handshake off the
/// accept path (a child that never sends its ready line must not stall other
/// children's handshakes).
async fn acceptor_loop(
    listener: UnixListener,
    conn_tx: async_channel::Sender<ChildConn>,
    child_threads: usize,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tx = conn_tx.clone();
                tokio::spawn(async move {
                    match read_child_ready(stream, child_threads).await {
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
/// connection. FS-1: the private 0700 socket directory already keeps other
/// uids out; this additionally verifies the connecting process's
/// kernel-reported credentials (SO_PEERCRED) match our own uid before
/// trusting anything it says, and prefers the kernel-reported peer pid over
/// the JSON-claimed one (which is otherwise attacker-controlled) for
/// tracking/kill decisions. FS-2: concurrency is clamped to the configured
/// `child_threads` so a hostile/buggy value can't defeat the pool's backlog
/// bound. The same buffered reader is kept for the serving loop so no bytes
/// can be lost between handshake and first response.
async fn read_child_ready(
    stream: tokio::net::UnixStream,
    child_threads: usize,
) -> anyhow::Result<ChildConn> {
    let peer = stream.peer_cred().context("read peer credentials")?;
    let our_uid = unsafe { libc::getuid() };
    if peer.uid() != our_uid {
        anyhow::bail!(
            "rejected connection from uid {} (expected {our_uid})",
            peer.uid()
        );
    }
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
    let claimed_pid = v["pid"].as_u64().filter(|&p| p > 1).map(|p| p as u32);
    let pid = peer
        .pid()
        .and_then(|p| u32::try_from(p).ok())
        .filter(|&p| p > 1)
        .or(claimed_pid)
        .ok_or_else(|| anyhow::anyhow!("ready line without a usable pid: {line}"))?;
    if let (Some(kernel_pid), Some(json_pid)) = (peer.pid(), claimed_pid) {
        if kernel_pid as u64 != json_pid as u64 {
            warn!(
                "cpu: forked child claimed pid={json_pid} but kernel reports pid={kernel_pid}; \
                 using the kernel-verified value"
            );
        }
    }
    let concurrency = v["concurrency"].as_u64().unwrap_or(1).max(1) as usize;
    let concurrency = concurrency.min(child_threads);
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
/// Per-child serving knobs, fixed for the pool's lifetime.
#[derive(Clone, Copy)]
struct ServeCfg {
    prefetch: usize,
    recycle: usize,
}

async fn slot_loop(
    idx: usize,
    fork_tx: mpsc::Sender<()>,
    conn_rx: async_channel::Receiver<ChildConn>,
    cfg: ServeCfg,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) {
    // Mirrors parent_control_loop's Refused backoff (FS-3): a child that
    // forks successfully and then dies immediately, over and over, must
    // not be forked again at full OS speed with no delay. Reset on a
    // planned recycle, which can legitimately be this frequent if the
    // operator configured it that way.
    let mut backoff = Duration::from_millis(100);
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
        match serve_child(idx, conn, cfg, &rx, &counters, &pids).await {
            ServeExit::Shutdown => return, // job channel closed: worker exiting
            ServeExit::Recycled => backoff = Duration::from_millis(100),
            ServeExit::Died => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
}

struct Pending {
    resp: oneshot::Sender<CpuOutcome>,
    timeout_ms: u64,
    /// `None` while this request is still sitting unread in the child's socket
    /// buffer behind earlier work (a prefetched request), `Some` once the
    /// child is believed to have actually started executing it.
    ///
    /// A prefetched request must NOT have its hard-timeout clock running while
    /// an earlier request is still computing -- otherwise a queued task could
    /// be declared timed out having never executed a single instruction.
    deadline: Option<Instant>,
}

/// A request accepted from `rx` but not yet fully handed to the child: at
/// most one of these at a time (see the `job = rx.recv()` branch's guard
/// below), tracked outside `pending`/`order` until the write actually
/// finishes so those two only ever describe requests the child has
/// definitely received. Written a few bytes at a time via plain `write`
/// (never `write_all`) so a stalled write never stops this same select! loop
/// from also polling `lines.next_line()` -- otherwise the child's own
/// response, sent right back over the same socket, could fill its buffer and
/// block on us reading it while we are blocked writing to it, a deadlock
/// neither side can break.
struct PendingWrite {
    id: String,
    buf: Vec<u8>,
    sent: usize,
    resp: oneshot::Sender<CpuOutcome>,
    timeout_ms: u64,
    /// How long this write may stall once nothing else in flight excuses it
    /// (`next_deadline` is `None`): `min(timeout_ms, 5s)`, the same shape as
    /// the old flat write budget. Applied from `write_stall_since` in
    /// `serve_child`, NOT from when this write started -- see its comment.
    budget: Duration,
}

impl PendingWrite {
    fn remaining(&self) -> &[u8] {
        &self.buf[self.sent..]
    }
}

/// H3-style saturation: a crafted/huge `timeout_ms` must not overflow `Instant`
/// math into a panic or wrap to a near-zero deadline.
fn deadline_for(timeout_ms: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_millis(timeout_ms))
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400 * 365))
}

/// Start the hard-timeout clock on the oldest not-yet-started requests until
/// `concurrency` of them are running. Called after every send and after every
/// completion: a prefetched request's clock starts when the request ahead of
/// it finishes, which is the moment the child actually picks it up.
fn arm_started(
    pending: &mut HashMap<String, Pending>,
    order: &std::collections::VecDeque<String>,
    concurrency: usize,
) {
    let mut armed = pending.values().filter(|p| p.deadline.is_some()).count();
    for id in order.iter() {
        if armed >= concurrency {
            return;
        }
        if let Some(p) = pending.get_mut(id) {
            if p.deadline.is_none() {
                p.deadline = Some(deadline_for(p.timeout_ms));
                armed += 1;
            }
        }
    }
}

/// Extracts just the `id` from a child response line, borrowing it out of the
/// source string. serde skips every other field (including the result) without
/// building or allocating it -- `parse_pyresp` does the real parse afterwards,
/// and only for a line we've matched to a pending request.
#[derive(serde::Deserialize)]
struct IdOnly<'a> {
    #[serde(borrow, default)]
    id: Option<&'a str>,
}

enum ChildGone {
    /// EOF observed on read, or a write that completed with an error (or
    /// returned 0 for a non empty buffer): either way the kernel is telling
    /// us the child process has already gone, one way reaped by the
    /// fork-server parent's SIGCHLD handler (FS-7) and the other simply
    /// refusing to receive: its pid may already be reused by an unrelated
    /// process, so it must NOT be SIGKILLed again.
    Exited,
    /// A request exceeded its hard timeout: SIGKILL the child.
    HardTimeout,
    /// A write to the child's socket stalled past its budget (FS-4) with
    /// nothing already in flight to excuse it: the child is presumed
    /// wedged, not confirmed exited. SIGKILL it.
    Wedged,
    /// `--cpu-max-tasks-per-child` reached with nothing in flight: SIGKILL and
    /// fork a replacement. Only ever chosen with an empty pending map, so no
    /// task can be lost to it.
    Recycled,
    /// The job channel closed: worker shutdown.
    Shutdown,
}

/// What `slot_loop` should do once `serve_child` returns: told apart from a
/// plain bool so a child that dies (or is killed) gets backed off before the
/// next fork request, while a `--cpu-max-tasks-per-child` recycle, which is
/// planned and can legitimately be just as frequent as the operator
/// configured it to be, does not.
enum ServeExit {
    /// The job channel closed: worker shutdown, stop looping.
    Shutdown,
    /// Recycled: fork a replacement immediately, no backoff.
    Recycled,
    /// Gone for an unplanned reason (death, hard timeout, wedged): fork a
    /// replacement, but back off first.
    Died,
}

/// Serve one child connection, keeping up to `concurrency` requests EXECUTING
/// and up to `prefetch` more pre-staged in the child's socket buffer (pending
/// map keyed by request id, FIFO order tracked separately). Returns what the
/// slot should do next (`ServeExit`): stop (worker shutting down), or fork a
/// replacement child, immediately or after a backoff.
///
/// Prefetch is what keeps a child busy back to back. Without it, a child that
/// finishes a task writes its response and then sits idle for a full round
/// trip -- socket write, tokio wakeup, select-loop iteration, channel recv,
/// socket write, child wakeup -- before the next request even reaches it. That
/// dead time is pure lost throughput on cpu-bound work, and at small task
/// sizes it can rival the task itself. With a request already queued behind
/// the current one, the child's next read returns immediately.
async fn serve_child(
    idx: usize,
    conn: ChildConn,
    cfg: ServeCfg,
    rx: &async_channel::Receiver<CpuJob>,
    counters: &Arc<Counters>,
    pids: &Arc<Mutex<Vec<u32>>>,
) -> ServeExit {
    let ServeCfg { prefetch, recycle } = cfg;
    let ChildConn {
        mut lines,
        mut write,
        pid,
        concurrency,
    } = conn;
    track_pid(pids, pid, true);
    let mut pending: HashMap<String, Pending> = HashMap::new();
    // Send order, so we can tell which pending requests the child has actually
    // reached (it reads its socket in order) and which are still queued.
    let mut order: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let queue_depth = concurrency.saturating_add(prefetch);
    // --cpu-max-tasks-per-child accounting. The intake gate below stops
    // ADMITTING once completed + in flight reaches the budget, so staged
    // prefetch work always drains before the recycle fires.
    let mut completed: usize = 0;
    // At most one request being written to the child at a time -- see the
    // `job = rx.recv()` branch's guard below.
    let mut pending_write: Option<PendingWrite> = None;
    // The instant `pending_write` most recently became stalled with nothing
    // in `pending` to excuse it (`next_deadline` is `None`). Recomputed every
    // iteration below, NOT set once when the write started: a write that
    // stalls first because the child is legitimately busy with something
    // already in flight, and only later runs out of that excuse, must be
    // judged against a clock that starts at the moment it ran out, not one
    // that has been running since before it had anything to excuse.
    let mut write_stall_since: Option<Instant> = None;

    let gone = loop {
        let next_deadline = pending.values().filter_map(|p| p.deadline).min();
        if pending_write.is_some() && next_deadline.is_none() {
            write_stall_since.get_or_insert_with(Instant::now);
        } else {
            write_stall_since = None;
        }
        // `pending_write`'s own budget (`min(timeout_ms, 5s)`), counted from
        // `write_stall_since` above -- `None` whenever there is nothing to
        // apply it to, or something else in flight already excuses the wait.
        let write_fallback_at = write_stall_since
            .zip(pending_write.as_ref())
            .map(|(since, pw)| since + pw.budget);
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(l)) => {
                        // Read ONLY the correlation id. The previous
                        // `from_str::<Value>` built a full tree for the entire
                        // response -- including the task's whole result -- and
                        // threw it away, then `parse_pyresp` parsed the same
                        // line a second time. `IdOnly` borrows the id out of
                        // `l` and skips every other field without allocating.
                        let rid = serde_json::from_str::<IdOnly>(&l)
                            .ok()
                            .and_then(|v| v.id);
                        match rid.and_then(|id| pending.remove_entry(id)) {
                            Some((done_id, p)) => {
                                let _ = p.resp.send(CpuOutcome::Resp(l));
                                counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
                                order.retain(|x| *x != done_id);
                                // The child just freed a slot: whatever it was
                                // holding prefetched starts executing now, so
                                // start that request's clock now too.
                                arm_started(&mut pending, &order, concurrency);
                                completed += 1;
                                if recycle > 0 && completed >= recycle && pending.is_empty() {
                                    info!(
                                        "cpu[{idx}] pid={pid}: recycled after {completed} tasks \
                                         (--cpu-max-tasks-per-child {recycle})"
                                    );
                                    break ChildGone::Recycled;
                                }
                            }
                            // rid alone, plus a length: never the response
                            // line itself, which carries the task's own
                            // result / error.message content (audit: the
                            // last log site in the worker that still leaked
                            // task data).
                            None => warn!(
                                "cpu[{idx}] pid={pid}: response with unknown or missing id (rid={rid:?} len={})",
                                l.len()
                            ),
                        }
                    }
                    _ => {
                        warn!(
                            "cpu[{idx}] pid={pid}: child connection closed ({} in flight -> WorkerLost)",
                            pending.len()
                        );
                        break ChildGone::Exited;
                    }
                }
            }
            // Only ever admit a NEW request once the last one is fully
            // handed to the child: the socket has one write side to share.
            job = rx.recv(), if pending_write.is_none() && pending.len() < queue_depth
                && (recycle == 0 || completed + pending.len() < recycle) => {
                match job {
                    Ok(job) => {
                        let mut buf = job.req_line.into_bytes();
                        buf.push(b'\n');
                        let budget =
                            Duration::from_millis(job.timeout_ms).min(Duration::from_secs(5));
                        pending_write = Some(PendingWrite {
                            id: job.id,
                            buf,
                            sent: 0,
                            resp: job.resp,
                            timeout_ms: job.timeout_ms,
                            budget,
                        });
                    }
                    Err(_) => break ChildGone::Shutdown,
                }
            }
            // Drive an in flight write forward a `write()` call at a time
            // (never `write_all`, which would run to completion or timeout
            // outside this select! -- see `PendingWrite`'s doc comment for
            // why that already deadlocked once). A blocked write is NOT by
            // itself evidence the child is wedged: prefetch means the
            // worker stages more requests than a busy child has drained
            // yet, and a busy child legitimately does not read again until
            // it finishes what it is already executing -- that is
            // backpressure the worker itself created, not a wedge. So a
            // stall here only counts against the child once nothing already
            // in flight is still within ITS OWN deadline either: once
            // `next_deadline` passes with still no write and no response,
            // the child has stopped both reading and responding, a real
            // hard timeout (handled by the `sleep_until(next_deadline)` arm
            // below, which this same stall leaves free to fire). With
            // nothing armed to excuse it, `write_fallback_at` above is this
            // write's only bound -- nothing legitimate can excuse a stall
            // with nothing else in flight either.
            //
            // The `write()` call is wrapped in an async block, not called
            // directly as the branch expression: `tokio::select!` only skips
            // POLLING a disabled branch, it still constructs every branch's
            // future up front, and `write.write(buf)` needs `buf` (borrowed
            // from `pending_write`) at construction time. An async block
            // defers everything in it, including that borrow, to first
            // poll -- which the `if pending_write.is_some()` guard below
            // then ensures never happens while it is `None`.
            r = async {
                write.write(pending_write.as_ref().expect("guarded by is_some() below").remaining()).await
            }, if pending_write.is_some() => {
                match r {
                    Ok(n) if n > 0 => {
                        let pw = pending_write.as_mut().expect("just matched Some");
                        pw.sent += n;
                        if pw.sent >= pw.buf.len() {
                            let pw = pending_write.take().expect("just matched Some");
                            counters.inflight_cpu.fetch_add(1, Ordering::Relaxed);
                            order.push_back(pw.id.clone());
                            pending.insert(pw.id, Pending {
                                resp: pw.resp,
                                timeout_ms: pw.timeout_ms,
                                // Armed by arm_started below only if the
                                // child has capacity to run it right now;
                                // otherwise it is prefetched and its clock
                                // starts when the request ahead completes.
                                deadline: None,
                            });
                            arm_started(&mut pending, &order, concurrency);
                        }
                    }
                    // A write that completes with an error (EPIPE,
                    // ECONNRESET, ...), or a stream write() returning 0 for
                    // a non empty buffer, is the kernel telling us the same
                    // "already gone" thing a read EOF does: the child is not
                    // there to receive this, so its pid may just as well
                    // already be reused by an unrelated process (see
                    // `ChildGone::Exited`'s doc comment). This is not a
                    // write that merely stalled while the child might still
                    // be alive (the arms below), so it does not repeat the
                    // kill: it takes the `Exited` path, not `Wedged`.
                    Ok(_zero) => {
                        let pw = pending_write.take().expect("just matched Some");
                        warn!("cpu[{idx}] pid={pid}: write returned 0 bytes, treating as closed");
                        let _ = pw.resp.send(CpuOutcome::Lost);
                        break ChildGone::Exited;
                    }
                    Err(e) => {
                        let pw = pending_write.take().expect("just matched Some");
                        warn!("cpu[{idx}] pid={pid}: write failed: {e}");
                        let _ = pw.resp.send(CpuOutcome::Lost);
                        break ChildGone::Exited;
                    }
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
            _ = sleep_until(write_fallback_at.unwrap_or_else(Instant::now)), if write_fallback_at.is_some() => {
                warn!(
                    "cpu[{idx}] pid={pid}: write stalled past budget with nothing else in \
                     flight ({} in flight); SIGKILL + replacement fork",
                    pending.len()
                );
                break ChildGone::Wedged;
            }
        }
    };

    // Resolve every in-flight request: expired ones as Timeout, the rest as
    // Lost (retryable WorkerLost). FS-7: only SIGKILL when the child is not
    // confirmed already exited -- on a read-EOF (`Exited`) the fork-server
    // parent has already reaped this pid via SIGCHLD, so it may already be
    // reused by an unrelated process; SIGKILLing it again would be wrong.
    if !matches!(gone, ChildGone::Exited) {
        kill_pid(pid);
    }
    if let Some(pw) = pending_write {
        let _ = pw.resp.send(CpuOutcome::Lost);
    }
    let now = Instant::now();
    for (_, p) in pending.drain() {
        let out = match gone {
            // Only the request that actually blew its deadline is a Timeout;
            // a prefetched sibling that never started (deadline None) is
            // collateral damage and must stay retryable WorkerLost.
            ChildGone::HardTimeout if p.deadline.is_some_and(|d| d <= now) => CpuOutcome::Timeout,
            _ => CpuOutcome::Lost,
        };
        let _ = p.resp.send(out);
        counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
    }
    track_pid(pids, pid, false);
    match gone {
        ChildGone::Shutdown => ServeExit::Shutdown,
        ChildGone::Recycled => ServeExit::Recycled,
        ChildGone::Exited | ChildGone::HardTimeout | ChildGone::Wedged => ServeExit::Died,
    }
}

// ---------------------------------------------------------------------------
// stdio mode (fallback): spawn-per-child, one request in flight
// ---------------------------------------------------------------------------

async fn stdio_child_loop(
    idx: usize,
    prog: String,
    argv: Vec<String>,
    recycle: usize,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) {
    // Mirrors parent_control_loop's Refused backoff (FS-3) and slot_loop's
    // fork-server equivalent: a child that dies instantly and repeatedly
    // must not respawn at full OS speed. Reset on a planned recycle.
    let mut backoff = Duration::from_millis(100);
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
        // Set on every respawn EXCEPT a planned recycle, so the backoff
        // below only ever applies to a child dying or being killed, never
        // to `--cpu-max-tasks-per-child`, which can be legitimately frequent.
        let mut died_unplanned = false;
        let mut completed: usize = 0;
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
                died_unplanned = true;
                break; // exits while: child gets killed + respawned below
            }

            match timeout(Duration::from_millis(job.timeout_ms), lines.next_line()).await {
                Ok(Ok(Some(resp))) => {
                    let _ = job.resp.send(CpuOutcome::Resp(resp));
                    completed += 1;
                    if recycle > 0 && completed >= recycle {
                        info!(
                            "cpu[{idx}] pid={pid}: recycled after {completed} tasks \
                             (--cpu-max-tasks-per-child {recycle})"
                        );
                        respawn = true;
                    }
                }
                Ok(_) => {
                    // EOF or read error: child died mid-task. Read its exit
                    // status (WIFSIGNALED and the signal number) so an
                    // operator can tell a segfault, an OOM kill, and any
                    // other unprompted death apart, since by the time this
                    // fires the child cannot self report why it is gone.
                    //
                    // Polled rather than probed once: the pipe reaches EOF
                    // when the kernel tears the child's fds down, which can
                    // land before its exit status is reapable, so a single
                    // try_wait loses the race often enough to drop the
                    // signal number from the log (it did, intermittently, in
                    // tests/e2e_forkserver.rs). Bounded and never a blocking
                    // wait: EOF does not prove the child is gone, since one
                    // can close stdout and keep running, and this path must
                    // not hang on that case.
                    let mut cause = String::new();
                    for _ in 0..100 {
                        match child.try_wait() {
                            Ok(Some(status)) => {
                                cause = match status.signal() {
                                    Some(sig) => format!(" (signal {sig})"),
                                    None => format!(" (exit code {:?})", status.code()),
                                };
                                break;
                            }
                            Ok(None) => tokio::time::sleep(Duration::from_millis(2)).await,
                            Err(_) => break,
                        }
                    }
                    warn!("cpu[{idx}] pid={pid}: child died mid-task{cause} (WorkerLost)");
                    let _ = job.resp.send(CpuOutcome::Lost);
                    respawn = true;
                    died_unplanned = true;
                }
                Err(_) => {
                    warn!(
                        "cpu[{idx}] pid={pid}: hard timeout after {}ms; SIGKILL + respawn",
                        job.timeout_ms
                    );
                    let _ = job.resp.send(CpuOutcome::Timeout);
                    respawn = true;
                    died_unplanned = true;
                }
            }
            counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
        }

        let _ = child.start_kill();
        let _ = child.wait().await;
        track_pid(&pids, pid, false);

        if died_unplanned {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(2));
        } else {
            backoff = Duration::from_millis(100);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Item 2 (audit): a write error (EPIPE/ECONNRESET, e.g. from a child
    /// that already died) must resolve to `ChildGone::Exited`, same as a
    /// read EOF, not `Wedged`: the pid may already have been reused by an
    /// unrelated process. Hands `serve_child` a REAL, unrelated process's
    /// pid as the "child" pid. Uses TWO independent socketpairs, not one:
    /// with item 1's fix, reads and writes are polled concurrently by the
    /// same select! loop, so closing one shared connection makes read EOF
    /// and the write error race each other for which arm fires first,
    /// which would make this test flaky, since read EOF already resolved
    /// to `Exited` before this fix and would mask a regression here half
    /// the time. Keeping the read side's peer alive and silent removes that
    /// race: `lines.next_line()` can never resolve, so only the write side
    /// (whose peer IS dropped) decides the outcome. If the fix regresses
    /// and kills the pid again on a write error, this innocent bystander
    /// process dies.
    #[tokio::test]
    async fn write_error_does_not_rekill_a_possibly_reused_pid() {
        let mut bystander = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .expect("spawn bystander");
        let bystander_pid = bystander.id();

        let (write_worker, write_peer) = tokio::net::UnixStream::pair().unwrap();
        let (read_worker, _read_peer) = tokio::net::UnixStream::pair().unwrap();
        drop(write_peer); // the write side's peer is gone from the start
                          // _read_peer stays alive (and silent) for the whole test: nothing
                          // ever arrives, so the read arm can never fire.

        let (_unused_read_half, write) = write_worker.into_split();
        let (read, _unused_write_half) = read_worker.into_split();
        let conn = ChildConn {
            lines: BufReader::new(read).lines(),
            write,
            pid: bystander_pid,
            concurrency: 1,
        };

        let (tx, rx) = async_channel::bounded::<CpuJob>(1);
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.try_send(CpuJob {
            id: "t1".into(),
            req_line: "x".repeat(1_000_000),
            timeout_ms: 5_000,
            resp: resp_tx,
        })
        .unwrap();
        drop(tx);

        let counters = Arc::new(Counters::default());
        let pids = Arc::new(Mutex::new(Vec::new()));
        serve_child(
            0,
            conn,
            ServeCfg {
                prefetch: 0,
                recycle: 0,
            },
            &rx,
            &counters,
            &pids,
        )
        .await;

        let outcome = resp_rx.await.expect("resp channel closed");
        assert!(
            matches!(outcome, CpuOutcome::Lost),
            "a dead child's write error must still resolve the request as Lost"
        );

        // The regression this guards: a naive fix kills again on any write
        // failure, which would SIGKILL this bystander if the kernel had
        // already reused its pid for it (exactly what happens once the
        // fork-server parent reaps the real child via SIGCHLD).
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            bystander.try_wait().expect("try_wait").is_none(),
            "a write error must not kill the pid again: it may already be reused"
        );
        let _ = bystander.kill();
        let _ = bystander.wait();
    }
}
