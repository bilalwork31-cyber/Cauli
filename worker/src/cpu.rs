//! CPU task pool (PROTOCOL §5.1), two modes:
//!
//! **Fork-server mode (default):** one parent
//! (`{python} -m cauli._exec --app {spec} --fork-server --connect {sock}
//! --child-threads {M}`) imports the app once, calls `gc.freeze()`, and forks
//! a child per `{"cmd":"fork"}` control line. Each child connects back to the
//! worker's unix socket, sends `{"ready": true, "pid": N, "concurrency": M}`,
//! and serves up to M requests at once, responses matched by `id`. Hard
//! timeout SIGKILLs the child and forks a replacement; child death fails its
//! in flight requests as WorkerLost (retryable). A broken control channel
//! respawns the parent and retries the pending fork; a healthy parent's fork
//! refusal (EAGAIN/ENOMEM) retries with backoff. If startup fails the pool
//! falls back to stdio mode. The socket lives in a 0700 directory and every
//! connection is checked against our uid (SO_PEERCRED).
//!
//! **Stdio mode (fallback, `--no-fork-server`):** each child is spawned
//! directly (`{python} -m cauli._exec --app {spec}`), speaks the protocol on
//! its stdin/stdout, one request in flight, killed and respawned on hard
//! timeout or death.
//!
//! Test hook: `CAULI_EXEC_CMD`, split on whitespace, replaces the child argv
//! (fork-server flags are appended to it). Compiled only under `cfg(test)` or
//! the `test-hooks` feature, so a release binary has no env driven way to
//! replace the cpu child command.

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
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};
use tokio::time::{sleep_until, timeout, Instant};
use tracing::{info, warn};

/// Handshake budget for the parent's server-ready line, a forked child's
/// ready line, and a control-channel fork reply.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a slot waits for a requested fork to connect before asking again.
/// `parent_control_loop` already retries failed forks; this covers a child
/// that forked but never completed its ready handshake.
const FORK_WAIT: Duration = Duration::from_secs(60);

pub enum CpuOutcome {
    /// Raw response line from the child.
    Resp(String),
    /// Hard timeout: child was SIGKILLed and respawned.
    Timeout,
    /// Child died or pipe broke: respawned.
    Lost,
}

pub struct CpuJob {
    /// Wire correlation id, unique per request (§5.1).
    pub id: String,
    pub req_line: String,
    pub timeout_ms: u64,
    pub resp: oneshot::Sender<CpuOutcome>,
}

#[derive(Clone)]
pub struct CpuPool {
    pub tx: async_channel::Sender<CpuJob>,
    /// Admission slots for cpu work: `workers * (child_threads + prefetch)`,
    /// what the pool can hold without queueing behind unstarted work. Without
    /// this gate entries wait with their PEL idle clocks running and get
    /// reclaimed, then dead lettered, having never run. A dispatch waiting
    /// here counts into `overflow`, which the fetch and recovery loops watch.
    pub admission: Arc<Semaphore>,
    /// Dispatch tasks blocked on a full backlog or an admission slot; the
    /// fetch loop pauses while > 0 so the cpu backlog stays bounded.
    pub overflow: Arc<AtomicUsize>,
    /// Fired when `overflow` falls back to zero, so the admission gates
    /// that pause on it resume at once instead of at their next poll.
    pub drained: Arc<Notify>,
    /// Live executor pids: fork-server parent + serving children (fork mode)
    /// or the spawned children (stdio mode). Killed on worker exit paths.
    pub child_pids: Arc<Mutex<Vec<u32>>>,
    /// Unix listener path (fork-server mode), removed on shutdown.
    pub sock_path: Option<Arc<PathBuf>>,
}

pub fn child_argv(python: &str, app_spec: &str) -> (String, Vec<String>) {
    // Test hook only; see the module doc.
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

/// A pool with no executors, for an app with no `kind = "cpu"` task. Routing
/// is by registry kind, so `run_cpu_task` is unreachable; the receiver is
/// dropped so reaching it anyway fails as `WorkerLost` instead of parking.
pub fn disabled() -> CpuPool {
    let (tx, rx) = async_channel::bounded::<CpuJob>(1);
    drop(rx);
    CpuPool {
        tx,
        admission: Arc::new(Semaphore::new(1)),
        overflow: Arc::new(AtomicUsize::new(0)),
        drained: Arc::new(Notify::new()),
        child_pids: Arc::new(Mutex::new(Vec::new())),
        sock_path: None,
    }
}

/// Everything needed to start the pool, held by Ctx so it can start lazily on
/// the first cpu task (`--eager-cpu` starts it at boot).
#[derive(Clone)]
pub struct StartCfg {
    pub workers: usize,
    pub child_threads: usize,
    pub prefetch: usize,
    /// Recycle a child after this many completed tasks (0 = never), against
    /// leaky C extensions and dirtied CoW pages.
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
    // Twice the in flight capacity. Prefetched requests live in the children.
    let cap = (2 * workers * child_threads).max(1);
    let (tx, rx) = async_channel::bounded::<CpuJob>(cap);
    // Executing plus staged prefetch; see `CpuPool::admission`.
    let admission = Arc::new(Semaphore::new(
        workers
            .saturating_mul(child_threads.saturating_add(prefetch))
            .max(1),
    ));
    let overflow = Arc::new(AtomicUsize::new(0));
    let drained = Arc::new(Notify::new());
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
                    drained,
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
        drained,
        child_pids: pids,
        sock_path: None,
    }
}

/// SIGKILL every live executor and remove the listener socket. Executors also
/// carry PR_SET_PDEATHSIG, so a SIGKILLed worker cannot leak them either.
pub fn kill_children(pool: &CpuPool) {
    let pids = pool.child_pids.lock().unwrap().clone();
    for pid in pids {
        kill_pid(pid);
    }
    if let Some(p) = &pool.sock_path {
        cleanup_sock_path(p);
    }
}

/// Remove the socket file and its private directory. Safe after a partial
/// startup: `remove_dir` only succeeds once the directory is empty.
fn cleanup_sock_path(p: &std::path::Path) {
    let _ = std::fs::remove_file(p);
    if let Some(dir) = p.parent() {
        let _ = std::fs::remove_dir(dir);
    }
}

fn kill_pid(pid: u32) {
    // `kill(0, SIGKILL)` would kill our own process group; pid 1 is init.
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

/// A socket path inside a new 0700 directory under the temp dir. Connecting
/// needs access to the directory, so no other uid can reach the socket;
/// SO_PEERCRED in `read_child_ready` is defense in depth.
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

/// Pool shape: how many children, how each is launched, how deeply each is fed.
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
    // The socket file too, not just its directory, whatever the umask.
    std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", sock_path.display()))?;

    let mut parent = spawn_parent(&prog, &argv, &sock_path, child_threads)?;
    if let Err(e) = wait_server_ready(&mut parent).await {
        let _ = parent.child.start_kill();
        let _ = parent.child.wait().await;
        cleanup_sock_path(&sock_path);
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
    let idle_slots = Arc::new(IdleSlots::default());
    for i in 0..workers {
        tokio::spawn(slot_loop(
            i,
            fork_tx.clone(),
            conn_rx.clone(),
            ServeCfg {
                prefetch,
                recycle,
                idle_slots: idle_slots.clone(),
            },
            rx.clone(),
            counters.clone(),
            pids.clone(),
        ));
    }
    Ok(sock_path)
}

/// Owns the fork-server parent: one `{"cmd":"fork"}` per slot request, retried
/// until it succeeds. An `{"error": ...}` reply from a healthy parent retries
/// with backoff and leaves the parent alone; a broken control channel respawns
/// the parent and retries the same request against it.
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
            // All slots gone: worker exiting.
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
    /// A healthy parent replied `{"error": ...}` (e.g. EAGAIN/ENOMEM): no
    /// kill, no respawn.
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

/// Read `{"ready": true, "pid": N, "concurrency": M}` from a fresh child. The
/// peer's uid (SO_PEERCRED) must be ours, and the kernel's peer pid is used
/// for kill decisions rather than the claimed one. Concurrency is clamped to
/// `child_threads` so a bad value cannot defeat the backlog bound. The same
/// buffered reader is kept for serving so no bytes are lost.
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

/// Per-child serving knobs, fixed for the pool's lifetime.
#[derive(Clone)]
struct ServeCfg {
    prefetch: usize,
    recycle: usize,
    idle_slots: Arc<IdleSlots>,
}

/// Slots that could start a request now, forking ones included. A busy slot
/// stages prefetch work only while there are none, or the first slot to wake
/// drains a burst into its own queue while its siblings sit idle.
#[derive(Default)]
struct IdleSlots {
    count: AtomicUsize,
    /// Woken when `count` drops, so a slot holding back work re-checks at once.
    dropped: tokio::sync::Notify,
}

impl IdleSlots {
    fn add(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn remove(&self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        self.dropped.notify_waiters();
    }
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
    // A child that dies right after every fork must not be reforked at full
    // speed. Reset on a planned recycle.
    let mut backoff = Duration::from_millis(100);
    cfg.idle_slots.add();
    loop {
        if fork_tx.send(()).await.is_err() {
            break;
        }
        let conn = match timeout(FORK_WAIT, conn_rx.recv()).await {
            Ok(Ok(c)) => c,
            Ok(Err(_)) => break,
            Err(_) => {
                warn!("cpu[{idx}]: no forked child connected within {FORK_WAIT:?}; re-requesting");
                continue;
            }
        };
        info!(
            "cpu[{idx}]: fork child serving pid={} concurrency={}",
            conn.pid, conn.concurrency
        );
        match serve_child(idx, conn, cfg.clone(), &rx, &counters, &pids).await {
            ServeExit::Shutdown => break,
            ServeExit::Recycled => backoff = Duration::from_millis(100),
            ServeExit::Died => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
    cfg.idle_slots.remove();
}

struct Pending {
    resp: oneshot::Sender<CpuOutcome>,
    timeout_ms: u64,
    /// `None` while the request is prefetched behind earlier work, `Some` once
    /// the child has started it, so a queued task cannot time out unstarted.
    deadline: Option<Instant>,
}

/// A request not yet fully written to the child; at most one at a time, kept
/// out of `pending`/`order` until written. Written with plain `write`, never
/// `write_all`, so the select! loop keeps reading responses: otherwise both
/// sides block writing to each other's full buffer and deadlock.
struct PendingWrite {
    id: String,
    buf: Vec<u8>,
    sent: usize,
    resp: oneshot::Sender<CpuOutcome>,
    timeout_ms: u64,
    /// How long this write may stall with nothing in flight to excuse it:
    /// `min(timeout_ms, 5s)`, counted from `write_stall_since`.
    budget: Duration,
}

impl PendingWrite {
    fn remaining(&self) -> &[u8] {
        &self.buf[self.sent..]
    }
}

/// Saturating: a huge `timeout_ms` must not overflow `Instant` math.
fn deadline_for(timeout_ms: u64) -> Instant {
    Instant::now()
        .checked_add(Duration::from_millis(timeout_ms))
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400 * 365))
}

/// Start the hard timeout clock on the oldest unstarted requests until
/// `concurrency` are running. Called after every send and every completion.
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

/// Just the `id` of a child response line, borrowed; serde skips the rest
/// without allocating. `parse_pyresp` parses matched lines afterwards.
#[derive(serde::Deserialize)]
struct IdOnly<'a> {
    #[serde(borrow, default)]
    id: Option<&'a str>,
}

enum ChildGone {
    /// Read EOF or a failed write: the child is gone, possibly reaped by the
    /// parent already, so its pid may be reused. Must not be SIGKILLed.
    Exited,
    /// A request exceeded its hard timeout: SIGKILL the child.
    HardTimeout,
    /// A write stalled past its budget with nothing in flight to excuse it:
    /// presumed wedged. SIGKILL it.
    Wedged,
    /// `--cpu-max-tasks-per-child` reached with nothing pending: SIGKILL and
    /// fork a replacement.
    Recycled,
    /// The job channel closed: worker shutdown.
    Shutdown,
}

/// What `slot_loop` does once `serve_child` returns. A dead child is backed
/// off before the next fork; a planned recycle is not.
enum ServeExit {
    /// The job channel closed: worker shutdown, stop looping.
    Shutdown,
    /// Recycled: fork a replacement immediately, no backoff.
    Recycled,
    /// Gone for an unplanned reason (death, hard timeout, wedged): fork a
    /// replacement, but back off first.
    Died,
}

/// Serve one child connection: up to `concurrency` requests executing and up
/// to `prefetch` more staged in its socket buffer. Prefetch saves a full round
/// trip between tasks, which on small tasks rivals the task itself.
async fn serve_child(
    idx: usize,
    conn: ChildConn,
    cfg: ServeCfg,
    rx: &async_channel::Receiver<CpuJob>,
    counters: &Arc<Counters>,
    pids: &Arc<Mutex<Vec<u32>>>,
) -> ServeExit {
    let ServeCfg {
        prefetch,
        recycle,
        idle_slots,
    } = cfg;
    // slot_loop counted this slot idle; kept in step with `pending` below.
    let mut idle = true;
    let ChildConn {
        mut lines,
        mut write,
        pid,
        concurrency,
    } = conn;
    track_pid(pids, pid, true);
    let mut pending: HashMap<String, Pending> = HashMap::new();
    // Send order: the child reads in order, so this tells started from queued.
    let mut order: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let queue_depth = concurrency.saturating_add(prefetch);
    // Intake stops once completed plus in flight reaches the recycle budget,
    // so staged work drains before the recycle.
    let mut completed: usize = 0;
    let mut pending_write: Option<PendingWrite> = None;
    // When `pending_write` last became stalled with nothing in flight to
    // excuse it. Recomputed every iteration: a write first excused by busy
    // work is judged from the moment that excuse ran out.
    let mut write_stall_since: Option<Instant> = None;

    let gone = loop {
        let now_idle = pending.len() + usize::from(pending_write.is_some()) < concurrency;
        if now_idle != idle {
            if now_idle {
                idle_slots.add();
            } else {
                idle_slots.remove();
            }
            idle = now_idle;
        }
        let may_admit = idle || idle_slots.count.load(Ordering::SeqCst) == 0;
        let next_deadline = pending.values().filter_map(|p| p.deadline).min();
        if pending_write.is_some() && next_deadline.is_none() {
            write_stall_since.get_or_insert_with(Instant::now);
        } else {
            write_stall_since = None;
        }
        let write_fallback_at = write_stall_since
            .zip(pending_write.as_ref())
            .map(|(since, pw)| since + pw.budget);
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(l)) => {
                        let rid = serde_json::from_str::<IdOnly>(&l)
                            .ok()
                            .and_then(|v| v.id);
                        match rid.and_then(|id| pending.remove_entry(id)) {
                            Some((done_id, p)) => {
                                let _ = p.resp.send(CpuOutcome::Resp(l));
                                counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
                                order.retain(|x| *x != done_id);
                                // A slot freed: the next prefetched request
                                // starts now.
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
                            // Never log the line: it carries task data.
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
            // One write in progress at a time: the socket has one write side.
            job = rx.recv(), if pending_write.is_none() && pending.len() < queue_depth
                && may_admit
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
            // One `write()` per poll (see `PendingWrite`). A blocked write is
            // not a wedge by itself: a busy child does not read until it
            // finishes, which is backpressure from our own prefetch. In flight
            // deadlines bound it; with nothing in flight, `write_fallback_at`.
            //
            // An async block because select! constructs every branch future
            // up front, and `buf` borrows from `pending_write`, which may be
            // `None` while the branch is disabled.
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
                                // Armed by arm_started below.
                                deadline: None,
                            });
                            arm_started(&mut pending, &order, concurrency);
                        }
                    }
                    // A write error or a 0 byte write means the child is gone,
                    // like a read EOF: `Exited`, not `Wedged`, so no kill.
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
            _ = idle_slots.dropped.notified(), if !may_admit => {}
        }
    };

    // Expired requests resolve as Timeout, the rest as retryable WorkerLost.
    // An `Exited` pid may already be reused, so it is not killed.
    if !matches!(gone, ChildGone::Exited) {
        kill_pid(pid);
    }
    if let Some(pw) = pending_write {
        let _ = pw.resp.send(CpuOutcome::Lost);
    }
    let now = Instant::now();
    for (_, p) in pending.drain() {
        let out = match gone {
            // Only a request past its own deadline is a Timeout; unstarted
            // siblings stay retryable.
            ChildGone::HardTimeout if p.deadline.is_some_and(|d| d <= now) => CpuOutcome::Timeout,
            _ => CpuOutcome::Lost,
        };
        let _ = p.resp.send(out);
        counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
    }
    track_pid(pids, pid, false);
    if !idle {
        idle_slots.add();
    }
    match gone {
        ChildGone::Shutdown => ServeExit::Shutdown,
        ChildGone::Recycled => ServeExit::Recycled,
        ChildGone::Exited | ChildGone::HardTimeout | ChildGone::Wedged => ServeExit::Died,
    }
}

async fn stdio_child_loop(
    idx: usize,
    prog: String,
    argv: Vec<String>,
    recycle: usize,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) {
    // A child that dies instantly and repeatedly must not respawn at full
    // speed. Reset on a planned recycle.
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
        // True unless this is a planned recycle; only then is there backoff.
        let mut died_unplanned = false;
        let mut completed: usize = 0;
        while !respawn {
            let job = match rx.recv().await {
                Ok(j) => j,
                Err(_) => {
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
                break;
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
                    // Child died mid-task: log its exit signal so a segfault
                    // and an OOM kill can be told apart. Polled, bounded: EOF
                    // can land before the status is reapable, and a child can
                    // close stdout and keep running.
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

    /// A write error resolves to `ChildGone::Exited`, not `Wedged`, so the
    /// possibly reused pid is not killed. The "child" pid is a real bystander
    /// process. Two socketpairs: the read side stays silent, so only the
    /// write error decides the outcome.
    #[tokio::test]
    async fn write_error_does_not_rekill_a_possibly_reused_pid() {
        let mut bystander = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .expect("spawn bystander");
        let bystander_pid = bystander.id();

        let (write_worker, write_peer) = tokio::net::UnixStream::pair().unwrap();
        let (read_worker, _read_peer) = tokio::net::UnixStream::pair().unwrap();
        drop(write_peer);

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
                idle_slots: Arc::new(IdleSlots::default()),
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

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            bystander.try_wait().expect("try_wait").is_none(),
            "a write error must not kill the pid again: it may already be reused"
        );
        let _ = bystander.kill();
        let _ = bystander.wait();
    }
}
