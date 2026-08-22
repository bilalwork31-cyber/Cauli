//! The cauli worker runtime, as a library with exactly one public entry
//! point, [`run`].
//!
//! It lives here rather than in a `main.rs` because the crate ships the same
//! program under two names: `cauli-worker` for the source tree (itest, bench,
//! PROTOCOL.md and both READMEs all name that path) and `cauli-worker-bin`
//! for the published wheel, where the plain name belongs to the Python
//! console script that repairs the dynamic loader's libpython path first.
//! Pointing two `[[bin]]` targets at one `main.rs` is what cargo warns about
//! with "file found to be present in multiple build targets"; one library and
//! two three-line entry points (`src/main.rs`, `src/wheel_main.rs`) give each
//! target its own source file and compile the runtime once. See
//! worker/Cargo.toml for the features that select between the two names.

mod backoff;
mod broker;
mod cli;
mod clock;
mod cpu;
mod ctx;
mod dispatch;
mod envelope;
mod exec;
mod loops;
mod pyjson;
mod pyrt;
mod stats;
mod supervisor;

use clap::Parser;
use ctx::Ctx;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

/// Test-only hooks for the exit-path regression (worker/tests/exit_path.rs).
/// Gated the same way as `CAULI_EXEC_CMD` in cpu.rs: a plain `cargo build
/// --release` carries none of this, `cargo test --features test-hooks`
/// compiles it into the binary the e2e suite spawns as a subprocess.
#[cfg(any(test, feature = "test-hooks"))]
mod exit_path_test_hooks {
    use std::sync::OnceLock;

    static MARKER_PATH: OnceLock<String> = OnceLock::new();

    extern "C" fn write_marker() {
        if let Some(path) = MARKER_PATH.get() {
            let _ = std::fs::write(path, b"atexit ran\n");
        }
    }

    /// If `CAULI_TEST_ATEXIT_MARKER` names a file, register a real libc
    /// atexit handler that writes it -- an observable stand-in for the
    /// OPENSSL_cleanup handler `exit_now`'s doc comment describes, without
    /// depending on a specific library being linked in the test build.
    pub fn install() {
        if let Ok(path) = std::env::var("CAULI_TEST_ATEXIT_MARKER") {
            if MARKER_PATH.set(path).is_ok() {
                // SAFETY: `write_marker` only reads an already-set OnceLock
                // and calls std::fs::write; both are safe to run from libc's
                // atexit callback context.
                unsafe { libc::atexit(write_marker) };
            }
        }
    }

    /// If set, panic right now. Called right after `PyRuntime::init`
    /// returns, so the panic lands after the interpreter's daemon asyncio
    /// loop threads and the async-submit thread are already running -- the
    /// exact "threads still live" condition `exit_now` exists for.
    pub fn maybe_panic_after_pyrt_init() {
        if std::env::var_os("CAULI_TEST_PANIC_AFTER_PYRT_INIT").is_some() {
            panic!("test-hooks: forced main-thread panic after PyRuntime::init");
        }
    }
}

/// Run the worker and terminate the process. Never returns.
///
/// Both `[[bin]]` targets are nothing but a call to this: the name a build
/// produces is a packaging decision (see the crate docs above), not a
/// behavioural one, so there is exactly one copy of the program.
pub fn run() -> ! {
    // A panic that unwinds out of real_main must not be allowed to unwind
    // out of `run` itself: past that point the C runtime returns from
    // crt0's real `main` and calls ordinary libc `exit()` -- the exact
    // atexit/DSO-teardown race exit_now exists to prevent (see its doc
    // comment below), and an uncaught panic here is the one path that used
    // to bypass it. catch_unwind only guards this top-level call; it does
    // not change how panics inside individual tasks/threads are handled
    // (those already catch their own -- Cargo.toml is deliberately not
    // panic = "abort").
    let code = std::panic::catch_unwind(real_main).unwrap_or(101);
    // Explicit exit: sync pool threads and Python daemon threads must not
    // keep the process alive.
    exit_now(code);
}

/// Terminate the process immediately, WITHOUT running libc `atexit` handlers
/// or shared-library destructors.
///
/// `std::process::exit` calls libc `exit()`, and `exit()` runs every
/// registered atexit handler and DSO destructor on the calling thread. That
/// teardown assumes a process which is effectively single threaded by then.
/// This one never is, by design: the sync io pool threads, the embedded
/// interpreter's asyncio loop threads (daemon threads the shim never joins),
/// and any threads the task code itself started -- a psycopg or SQLAlchemy
/// connection pool, a requests session -- are all still running or still
/// unwinding at that moment, and none of them are joinable from here.
///
/// A handler that frees process-wide state then races them. The one that
/// actually fires is `OPENSSL_cleanup`, registered via `atexit` by the libssl
/// that a database driver's libpq links: it tears down the global OpenSSL
/// state while those same threads are running their own per-thread OpenSSL
/// teardown on the way out. The process then dies of "double free or
/// corruption (fasttop)" / "malloc_consolidate(): unaligned fastbin chunk
/// detected" AFTER every task has already completed and been acked -- a
/// corrupt heap reported as a successful drain. Measured on the psycopg3 sync
/// lane at --io-threads 80: ~39% of shutdowns aborted, 0% with this.
///
/// `_exit` is the thread-safe primitive for "stop this process now": it skips
/// the handlers and goes straight to the kernel. Nothing here depends on them.
/// Every resource with an owner outside this process (cpu children, the fork
/// server socket) is released explicitly before this is reached, and Rust
/// destructors never ran under `process::exit` either.
///
/// Only Rust's own buffered stdout needs draining first -- that is where
/// tracing writes. The embedded interpreter's `sys.stdout` buffer is not
/// flushed, but it never was: CPython only flushes it from `Py_Finalize`,
/// which an embedded worker with live daemon threads must not call.
///
/// Callers: this `main`, the forced exit on a second signal (130), and
/// `loops::wedge_loop` on a confirmed event loop wedge
/// (`loops::WEDGE_EXIT_CODE`), which is the one that runs with the whole
/// process still executing tasks.
fn exit_now(code: i32) -> ! {
    use std::io::Write;
    let _ = std::io::stdout().flush();
    // SAFETY: `_exit` is async-signal-safe and valid to call from any thread
    // at any time; it does not return.
    unsafe { libc::_exit(code) }
}

fn real_main() -> i32 {
    #[cfg(any(test, feature = "test-hooks"))]
    exit_path_test_hooks::install();

    let args = match cli::Args::try_parse() {
        Ok(a) => a,
        Err(e) => {
            let help = matches!(
                e.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            let _ = e.print();
            return if help { 0 } else { 1 };
        }
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Pure-argument validation runs before the embedded interpreter comes up:
    // a bad value must fail once, loudly — not crash-loop N supervised
    // children through a Python startup each.
    // M8: floor CLI values that would otherwise storm-duplicate or fetch
    // unbounded amounts of work (0 is not a safe "unlimited" here).
    if args.batch == 0 {
        error!("--batch must be >= 1 (0 means unlimited XREADGROUP fetch)");
        return 1;
    }
    if args.visibility_timeout == 0 {
        error!("--visibility-timeout must be >= 1 (0 reclaims every in-flight task on nearly every tick)");
        return 1;
    }
    if args.max_envelope_bytes == 0 {
        error!(
            "--max-envelope-bytes must be >= 1 (0 dead letters every single message as oversize)"
        );
        return 1;
    }
    if args.redis_timeout == 0 {
        error!(
            "--redis-timeout must be >= 1 (0 would time out every redis round trip immediately)"
        );
        return 1;
    }
    // FS-10: an absurd value (e.g. a typo'd extra digit) would eagerly
    // allocate a `2 * cpu_workers * cpu_child_threads`-sized channel and ask
    // Python to start that many threads per child; reject early with a clear
    // message instead of an opaque allocation failure or a Python startup hang.
    if args.cpu_child_threads == 0 || args.cpu_child_threads > 1024 {
        error!(
            "--cpu-child-threads must be within [1, 1024] (got {})",
            args.cpu_child_threads
        );
        return 1;
    }
    if args.concurrency == Some(0) {
        error!("-c/--concurrency must be >= 1");
        return 1;
    }
    if args.procs == Some(0) {
        error!("--procs must be >= 1");
        return 1;
    }
    for q in &args.queues {
        if !cli::valid_queue_name(q) {
            error!("invalid queue name {q:?} (must match [a-zA-Z0-9_.-]+)");
            return 1;
        }
    }

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let resolved = cli::resolve(&args, cores);
    // FS-11: the ceiling `--cpu-child-threads` already gets, applied to the
    // two io knobs that eagerly allocate. Checked on the RESOLVED per-process
    // values rather than the raw flags, because `-c` derives both (§ cli.rs
    // resolve), so `-c 100000000` reaches the sync pool and the admission
    // gate with an absurd value without either flag being typed.
    if let Some(msg) = io_limits_error(&resolved) {
        error!("{msg}");
        return 1;
    }
    if args.print_plan {
        print_plan(&args, &resolved, cores);
        return 0;
    }
    if resolved.procs > 1 {
        info!(
            "cauli-worker supervising {} procs: app={} c={} -> per-proc io_threads={} io_concurrency={} cpu_workers={}",
            resolved.procs,
            args.app_spec(),
            args.concurrency.map_or("unset".into(), |c| c.to_string()),
            resolved.io_threads,
            resolved.io_concurrency,
            resolved.cpu_workers
        );
        return supervisor::run(&args, &resolved);
    }

    // Embedded CPython: init interpreter, shim, app, asyncio loops (§ pyrt.rs).
    let (pyrt, appcfg) = match pyrt::PyRuntime::init(args.app_spec(), args.io_loops) {
        Ok(v) => v,
        Err(e) => {
            error!("startup failed: {e:#}");
            return 1;
        }
    };
    #[cfg(any(test, feature = "test-hooks"))]
    exit_path_test_hooks::maybe_panic_after_pyrt_init();

    if appcfg.tasks.is_empty() {
        warn!("app has no registered tasks");
    }
    let redis_url = args
        .redis_url
        .clone()
        .or_else(|| std::env::var("CAULI_REDIS_URL").ok())
        .unwrap_or_else(|| appcfg.redis_url.clone());
    let queues: Vec<String> = if args.queues.is_empty() {
        vec![appcfg.default_queue.clone()]
    } else {
        args.queues.clone()
    };
    for q in &queues {
        if !cli::valid_queue_name(q) {
            error!("invalid queue name {q:?} (must match [a-zA-Z0-9_.-]+)");
            return 1;
        }
    }
    info!(
        "cauli-worker starting: app={} queues={:?} redis={} tasks={} io_threads={} io_concurrency={} cpu_workers={}",
        args.app_spec(),
        queues,
        redact_redis_url(&redis_url),
        appcfg.tasks.len(),
        resolved.io_threads,
        resolved.io_concurrency,
        resolved.cpu_workers
    );
    // H1 operator diagnostic: loops::recovery_loop's per-envelope idle check
    // already prevents reclaiming a still-running task regardless of this
    // default, but a task whose registered timeout_ms is >= the visibility
    // floor is a strong signal of a misconfigured deployment (the invariant
    // documented in PROTOCOL.md §4.4: visibility_timeout should exceed your
    // longest task) — warn loudly at startup so operators catch it early.
    let vt_ms = visibility_timeout_ms(args.visibility_timeout);
    for (name, spec) in &appcfg.tasks {
        if spec.timeout_ms >= vt_ms {
            warn!(
                task = %name, timeout_ms = spec.timeout_ms, visibility_timeout_s = args.visibility_timeout,
                "task timeout_ms >= visibility_timeout*1000 (PROTOCOL.md §4.4 invariant \
                 violated) -- consider raising --visibility-timeout"
            );
        }
    }
    // Same diagnostic, mirrored for idemp_ttl (section 4.5): it is one
    // global value while a task's own timeout_ms is not, and nothing else
    // cross checks them. If a task legitimately runs longer than idemp_ttl,
    // its idempotency key expires while the task is still running, and a
    // second attempt with the same key then claims Fresh: the exact
    // duplicate concurrent execution the key exists to prevent.
    let idemp_ttl_ms = idemp_ttl_ms(appcfg.idemp_ttl);
    for (name, spec) in &appcfg.tasks {
        if spec.timeout_ms >= idemp_ttl_ms {
            warn!(
                task = %name, timeout_ms = spec.timeout_ms, idemp_ttl_s = appcfg.idemp_ttl,
                "task timeout_ms >= idemp_ttl*1000: an idempotency key can expire while \
                 this task is still running, so a second attempt with the same key would \
                 claim Fresh and run concurrently with the first. Consider raising idemp_ttl."
            );
        }
    }
    // The third cross check the other two imply and nothing performed: §4.7's
    // drain waits `--drain-timeout` and then `_exit`s, killing whatever is
    // still running. The default pair is 30s of drain against a 300s task
    // timeout, so a rolling deploy kills every task older than 30 seconds and
    // the entry survives only because it stays pending for recovery. Warned
    // ONCE, naming the longest registered task, rather than per task like the
    // two loops above: with stock defaults every task in every app trips it,
    // and a per task warning would be a wall of text at every boot.
    if let Some((name, timeout_ms)) = longest_timeout_past_drain(&appcfg.tasks, args.drain_timeout)
    {
        warn!(
            task = %name, timeout_ms, drain_timeout_s = args.drain_timeout,
            "a registered task can run longer than --drain-timeout: on graceful shutdown \
             (§4.7) anything still running at the deadline is killed mid task and left \
             pending for redelivery. Raise --drain-timeout past your longest task, or \
             lower the task's timeout."
        );
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(run_worker(args, resolved, pyrt, appcfg, redis_url, queues))
}

async fn run_worker(
    args: cli::Args,
    resolved: cli::Resolved,
    pyrt: Arc<pyrt::PyRuntime>,
    appcfg: pyrt::AppConfig,
    redis_url: String,
    queues: Vec<String>,
) -> i32 {
    install_tls_provider();
    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            error!("bad redis url {:?}: {e}", redact_redis_url(&redis_url));
            return 1;
        }
    };
    // Config level timeout, not tokio::time::timeout wrapped around each
    // call: a caller side timeout only abandons the caller's own wait, the
    // ConnectionManager itself never observes an Err, so its internal
    // reconnect_if_io_error! never fires and the same wedged socket gets
    // reused by every later call. Setting response_timeout here makes the
    // manager itself see a timeout as a genuine Err (it converts to
    // ErrorKind::IoError), which activates that already existing reconnect
    // path instead of leaving it dormant. Do not "simplify" this back to a
    // per call tokio::timeout; it silently breaks reconnection.
    let conn_cfg = ConnectionManagerConfig::new()
        .set_response_timeout(Duration::from_secs(args.redis_timeout))
        .set_connection_timeout(Duration::from_secs(args.redis_timeout));
    let mut write_conn =
        match ConnectionManager::new_with_config(client.clone(), conn_cfg.clone()).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "cannot connect to redis at {}: {e}",
                    redact_redis_url(&redis_url)
                );
                return 1;
            }
        };
    // Dedicated connection for blocking XREADGROUP so BLOCK never stalls writes.
    let fetch_conn = match ConnectionManager::new_with_config(client, conn_cfg).await {
        Ok(c) => c,
        Err(e) => {
            error!("cannot open fetch connection: {e}");
            return 1;
        }
    };
    // Redis Cluster is refused here, at the first reachable point, rather
    // than diagnosed later: on a cluster every delayed and retried task is
    // already lost by the time loops::report_mover_error gets to name the
    // cause (docs/decisions/redis-cluster.md).
    if let Some(info) = probe_cluster_info(&mut write_conn).await {
        let override_raw = std::env::var(ALLOW_REDIS_CLUSTER_ENV).ok();
        match cluster_decision(&info, override_raw.as_deref(), &redis_url) {
            ClusterDecision::Start => {}
            ClusterDecision::StartAnyway(msg) => warn!("{msg}"),
            ClusterDecision::Refuse(msg) => {
                error!("{msg}");
                return 1;
            }
        }
    }
    if let Err(e) = broker::ensure_groups(&mut write_conn, &queues).await {
        error!("XGROUP CREATE failed: {e}");
        return 1;
    }
    // Anchor the wall clock on redis before anything can read it. Blocking
    // once here is free: reaching redis is already a precondition of the call
    // above. See clock.rs for why absolute instants must not come from this
    // host's own clock, and why the anchor is sampled rather than read per call.
    clock::init(&mut write_conn).await;
    tokio::spawn(clock::sampler_loop(write_conn.clone()));

    // §9.2 queue TTLs, seconds -> ms. Logged so an operator can see at a
    // glance that entries in this deployment have a bounded shelf life;
    // silently dropping work is only acceptable when it is announced.
    let queue_ttl_ms: std::collections::HashMap<String, u64> = appcfg
        .queue_ttl
        .iter()
        .filter(|(_, secs)| secs.is_finite() && **secs > 0.0)
        .map(|(q, secs)| (q.clone(), (*secs * 1000.0) as u64))
        .collect();
    if !queue_ttl_ms.is_empty() {
        info!("queue TTLs active (§9.2, ms): {queue_ttl_ms:?}");
    }

    let counters = Arc::new(stats::Counters::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // PROTOCOL §1: "{hostname}:{pid}" (any unique string is acceptable) --
    // no per-loop/per-thread `n` component exists in this worker (one fetch
    // loop per process), so it is dropped rather than hardcoded to a
    // meaningless constant.
    let consumer = format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    );
    let io_concurrency = resolved.io_concurrency;
    // Only pay for cpu executors if the app actually has cpu tasks, and even
    // then only once one arrives (ctx.cpu_pool). Routing is by REGISTRY kind
    // (dispatch.rs), so with no cpu-kind task registered the cpu path is
    // unreachable and the fork-server parent + N children would be pure
    // resident memory for work that can never arrive.
    let needs_cpu = appcfg.tasks.values().any(|s| s.kind == "cpu");
    let cpu_python = resolve_python(args.python.as_deref());
    let cpu_cfg = needs_cpu.then(|| cpu::StartCfg {
        workers: resolved.cpu_workers,
        child_threads: args.cpu_child_threads,
        prefetch: args.cpu_prefetch,
        recycle: args.cpu_max_tasks_per_child,
        python: cpu_python,
        app_spec: args.app_spec().to_string(),
        no_fork_server: args.no_fork_server,
    });
    if !needs_cpu {
        info!("no kind=\"cpu\" tasks registered: cpu pool disabled");
    } else if !args.eager_cpu {
        info!(
            "cpu tasks registered: pool of {} children starts on the first \
             cpu task (--eager-cpu warms it at boot instead)",
            resolved.cpu_workers
        );
    }
    // The same gate on the io side (see `sync_pool_threads`): an app whose io
    // tasks are all `async def` can never reach a sync pool thread.
    let sync_threads = sync_pool_threads(&appcfg.tasks, resolved.io_threads);
    if sync_threads < resolved.io_threads {
        info!(
            "no sync (non-async) io tasks registered: sync pool started with \
             {sync_threads} thread instead of {}",
            resolved.io_threads
        );
    }
    // One set per queue, built once: the dispatch path only ever inserts
    // into and removes from an existing entry, never grows this map.
    let inflight_entries = queues
        .iter()
        .map(|q| (q.clone(), std::sync::Mutex::new(Default::default())))
        .collect();
    let ctx = Arc::new(Ctx {
        io_sem: Arc::new(tokio::sync::Semaphore::new(io_concurrency)),
        io_concurrency,
        inflight_entries,
        sync_pool: pyrt::SyncPool::start(pyrt.clone(), sync_threads, io_concurrency),
        cpu: tokio::sync::OnceCell::new(),
        cpu_cfg,
        registry: appcfg.tasks,
        redis: write_conn,
        counters,
        pyrt,
        result_ttl: appcfg.result_ttl,
        idemp_ttl: appcfg.idemp_ttl,
        queue_ttl_ms,
        queues,
        consumer,
        shutdown: shutdown_rx,
        args,
    });
    if ctx.args.eager_cpu && needs_cpu {
        let _ = ctx.cpu_pool().await;
    }

    spawn_signal_task(shutdown_tx, ctx.clone());
    tokio::spawn(loops::mover_loop(ctx.clone()));
    tokio::spawn(loops::recovery_loop(ctx.clone()));
    tokio::spawn(loops::stats_loop(ctx.clone()));
    tokio::spawn(loops::wedge_loop(ctx.clone()));

    loops::fetch_loop(ctx.clone(), fetch_conn).await; // returns on shutdown

    // §4.7 drain: mover + acks keep running; wait for in flight tasks.
    let deadline = Instant::now() + Duration::from_secs(ctx.args.drain_timeout);
    while ctx.counters.inflight_total.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let left = ctx.counters.inflight_total.load(Ordering::SeqCst);
    let code = if left > 0 {
        warn!("drain timeout: leaving {left} tasks pending for recovery (§4.4)");
        DRAIN_TIMEOUT_EXIT_CODE
    } else {
        info!("drained cleanly");
        0
    };
    info!("{}", ctx.counters.stats_line());
    if let Some(pool) = ctx.cpu.get() {
        cpu::kill_children(pool);
    }
    code
}

fn spawn_signal_task(shutdown_tx: watch::Sender<bool>, ctx: Arc<Ctx>) {
    tokio::spawn(async move {
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = term.recv() => info!("SIGTERM: stop fetching, draining"),
            _ = int.recv() => info!("SIGINT: stop fetching, draining"),
        }
        let _ = shutdown_tx.send(true);
        tokio::select! {
            _ = term.recv() => {},
            _ = int.recv() => {},
        }
        warn!("second signal: forced exit 130");
        // FS-8: this path bypasses run_worker's normal drain-then-cleanup
        // tail entirely (exit_now below never returns), so it must do
        // its own cpu pool cleanup or the fork-server socket file (and,
        // absent PDEATHSIG, its children) would be abandoned.
        if let Some(pool) = ctx.cpu.get() {
            cpu::kill_children(pool);
        }
        // Not `process::exit`: see exit_now. This path is strictly worse for
        // atexit handlers than the graceful one -- it runs from a tokio task
        // with tasks still executing on the pool threads.
        exit_now(130);
    });
}

/// Operator override for the Redis Cluster startup refusal.
///
/// An environment variable rather than a CLI flag: the refusal is the part
/// that matters and it ships without widening the flag surface, while the
/// `--allow-redis-cluster` name that docs/decisions/redis-cluster.md proposed
/// stays free for whoever adds it to cli.rs.
const ALLOW_REDIS_CLUSTER_ENV: &str = "CAULI_ALLOW_REDIS_CLUSTER";

/// What startup does about the topology redis just reported.
#[derive(Debug, PartialEq, Eq)]
enum ClusterDecision {
    /// Not a cluster: start normally, say nothing.
    Start,
    /// A cluster, and the operator opted in: start, but say what they bought.
    StartAnyway(String),
    /// A cluster: refuse, with the message an operator needs.
    Refuse(String),
}

/// Ask redis once whether it runs in cluster mode. `None` when the question
/// could not be answered.
///
/// The command is `INFO cluster`, NOT `CLUSTER INFO`. Those are not synonyms
/// and only one of them answers this question: `CLUSTER INFO` reports the
/// health of a cluster (`cluster_state`, slot counts, known nodes) and carries
/// no `cluster_enabled` field at all, while a standalone server refuses it
/// outright with "ERR This instance has cluster support disabled". `INFO`'s
/// Cluster section is the one both topologies answer, and it holds the single
/// field that decides. Probing the wrong one reads as "not a cluster" on a
/// real cluster, which is the failure this refusal exists to prevent; the
/// pairing is pinned against a live server of each topology by
/// `the_probe_reads_the_topology_off_a_real_server`.
///
/// A failed probe never blocks startup: every redis answers `INFO`, so an
/// error here means an ACL or a proxy in front of a deployment that has been
/// working, and refusing to boot on it would turn a diagnostic into an outage.
async fn probe_cluster_info(conn: &mut ConnectionManager) -> Option<String> {
    match redis::cmd("INFO")
        .arg("cluster")
        .query_async::<String>(conn)
        .await
    {
        Ok(info) => Some(info),
        Err(e) => {
            debug!("INFO cluster probe failed ({e}); assuming a standalone server");
            None
        }
    }
}

/// True when an `INFO cluster` reply says this server runs in cluster mode.
///
/// The reply is a `# Cluster` header followed by flat `field:value` lines with
/// CRLF endings; the header carries no colon and drops out of the parse. A
/// standalone server answers the same command with `cluster_enabled:0`, so
/// that one field is the entire signal. `cluster_state` is deliberately not
/// consulted: it belongs to a different command's reply, and a cluster whose
/// slots are not yet assigned is still a cluster.
fn cluster_info_says_enabled(info: &str) -> bool {
    info.lines()
        .filter_map(|line| line.split_once(':'))
        .any(|(k, v)| k.trim() == "cluster_enabled" && v.trim() == "1")
}

/// Whether the operator knowingly opted in, given the raw variable.
///
/// Pure over the string rather than reading the environment itself, so the
/// accepted spellings are testable without mutating process-wide state from a
/// test that runs in parallel with every other one.
fn cluster_override_enabled(raw: Option<&str>) -> bool {
    let normalized = raw.map(|v| v.trim().to_ascii_lowercase());
    matches!(normalized.as_deref(), Some("1" | "true" | "yes" | "on"))
}

/// The startup decision for an `INFO cluster` reply.
///
/// Split out from the probe so both the refusal and its message are testable
/// without a redis, matching how `io_limits_error` returns its message rather
/// than logging it.
fn cluster_decision(info: &str, override_raw: Option<&str>, redis_url: &str) -> ClusterDecision {
    if !cluster_info_says_enabled(info) {
        return ClusterDecision::Start;
    }
    let redis = redact_redis_url(redis_url);
    if cluster_override_enabled(override_raw) {
        return ClusterDecision::StartAnyway(format!(
            "redis at {redis} runs in cluster mode and {ALLOW_REDIS_CLUSTER_ENV} is set: \
             starting anyway. Delayed tasks, retries and cauli-beat lose work silently on \
             this topology, and fetching two or more queues fails with CROSSSLOT."
        ));
    }
    ClusterDecision::Refuse(format!(
        "redis at {redis} runs in cluster mode (INFO cluster reports cluster_enabled:1), \
         which cauli does not support: standalone and Sentinel only. cauli:q:{{queue}} and \
         cauli:delayed:{{queue}} never share a hash slot, so a delayed or retried task leaves \
         the stream without ever reaching the delayed set -- silent loss, not a visible error \
         -- and fetching two or more queues fails with CROSSSLOT. Point the worker at a \
         standalone or Sentinel fronted redis, or set {ALLOW_REDIS_CLUSTER_ENV}=1 to start \
         anyway and accept that loss (docs/decisions/redis-cluster.md)."
    ))
}

/// --print-plan: the derived execution plan, human first. Runs before any
/// Python or Redis so it is safe anywhere, including boxes without the app.
fn print_plan(args: &cli::Args, r: &cli::Resolved, cores: usize) {
    let c = args
        .concurrency
        .map_or("unset (standalone defaults)".to_string(), |c| c.to_string());
    println!("cauli-worker execution plan");
    println!("  cores detected     {cores}");
    println!("  -c (total)         {c}");
    println!("  worker processes   {}", r.procs);
    println!("  per process:");
    println!(
        "    io tasks in flight  {}  (async + sync together)",
        r.io_concurrency
    );
    println!("    sync io threads     {}", r.io_threads);
    println!("    asyncio loops       {}", args.io_loops);
    println!(
        "    cpu children        {}  ({}; only if the app registers kind=\"cpu\" tasks)",
        r.cpu_workers,
        if args.eager_cpu {
            "started at boot: --eager-cpu"
        } else {
            "started on first cpu task"
        }
    );
    println!(
        "  totals: {} io tasks in flight, {} sync threads, up to {} cpu children",
        plan_total(r.io_concurrency, r.procs),
        plan_total(r.io_threads, r.procs),
        plan_total(r.cpu_workers, r.procs)
    );
    println!("  override any value with its flag; see --help and docs/CONFIGURATION.md");
}

/// Per process value times process count, for the --print-plan totals line.
/// Saturating, matching the overflow safe style used elsewhere for hostile or
/// just huge input (dispatch.rs, envelope.rs, exec.rs, cpu.rs, and
/// visibility_timeout_ms below): release builds have no overflow-checks, so a
/// plain multiply would wrap silently instead, e.g. a 19 digit -c wrapping
/// the printed total down to single digits.
fn plan_total(per_process: usize, procs: usize) -> usize {
    per_process.saturating_mul(procs)
}

/// Exit code for a graceful shutdown that ran out of `--drain-timeout` with
/// tasks still executing. A clean drain still exits 0; this one says the
/// process was cut short and those entries are coming back through recovery,
/// which is the difference an orchestrator or a CI job needs and could not
/// see when both outcomes returned 0. Neighbour of `loops::WEDGE_EXIT_CODE`
/// (87), and outside the ranges a shell attributes to signals (128 + n) or to
/// a panic (101).
const DRAIN_TIMEOUT_EXIT_CODE: i32 = 88;

/// The registered task whose timeout outlives the drain window, and by how
/// much, or `None` when every task can finish inside it.
///
/// Ties are broken toward the longest timeout, and then by name so the
/// warning is stable across runs (a `HashMap` iterates in a different order
/// every process start, and an operator comparing two boots should not see
/// the same fleet report different tasks).
fn longest_timeout_past_drain(
    registry: &std::collections::HashMap<String, pyrt::TaskSpec>,
    drain_timeout_s: u64,
) -> Option<(&str, u64)> {
    let drain_ms = drain_timeout_s.saturating_mul(1000);
    registry
        .iter()
        .filter(|(_, spec)| spec.timeout_ms > drain_ms)
        .max_by(|(a_name, a), (b_name, b)| {
            a.timeout_ms
                .cmp(&b.timeout_ms)
                .then_with(|| b_name.as_str().cmp(a_name.as_str()))
        })
        .map(|(name, spec)| (name.as_str(), spec.timeout_ms))
}

/// Per process ceiling for `--io-threads`. Four times the sync knee cli.rs
/// documents (~1000 threads per process, past which throughput and latency
/// fall together), so nothing tunable is refused. Above it there is no
/// configuration, only a typo: `SyncPool::start` spawns every thread eagerly,
/// each one an OS thread with an 8 MB stack pinning a CPython thread state,
/// and with no ceiling it spawns until the OS refuses and then panics.
const MAX_IO_THREADS: usize = 4096;

/// Per process ceiling for `--io-concurrency`. The measured band ends at 128
/// per process (see the flag's own long help) and the standalone default is
/// 256, so 65536 is 256x past anything this repo has measured. Above it the
/// sync pool's bounded channel, allocated at this size before a single task
/// arrives, is the first thing to fail.
const MAX_IO_CONCURRENCY: usize = 65_536;

/// The operator facing message for a resolved io plan that cannot be built,
/// or `None` when it can.
///
/// Returns the message instead of logging it so the bound itself is testable
/// without a tracing subscriber, and so the caller keeps the single exit path
/// the other startup validations use.
fn io_limits_error(r: &cli::Resolved) -> Option<String> {
    if r.io_threads > MAX_IO_THREADS {
        return Some(format!(
            "--io-threads resolves to {} per process, above the ceiling of {MAX_IO_THREADS} \
             (each is an OS thread with an 8 MB stack and a pinned CPython thread state; the \
             sync knee is ~1000 per process). Lower --io-threads, or -c if it derived this.",
            r.io_threads
        ));
    }
    if r.io_concurrency > MAX_IO_CONCURRENCY {
        return Some(format!(
            "--io-concurrency resolves to {} per process, above the ceiling of \
             {MAX_IO_CONCURRENCY} (the measured band ends at 128 per process, and the sync \
             pool's channel is allocated at this size up front). Lower --io-concurrency, or \
             -c if it derived this.",
            r.io_concurrency
        ));
    }
    None
}

/// Threads the sync io pool starts with: the operator's `--io-threads` when
/// the app registers at least one SYNC io task, and the pool's own floor when
/// it registers none.
///
/// Dispatch routes by REGISTRY kind (dispatch.rs), so `exec::run_sync_task`
/// is reachable only for a registered task with `kind != "cpu"` and
/// `is_async == false`. An app that registers none of those can never touch
/// the sync pool, and the pool is expensive dead weight: `SyncPool::start`
/// spawns every thread eagerly and each one pins a CPython thread state plus
/// an 8 MB stack for the process lifetime. On the auto derived defaults that
/// is up to 86 threads per process, 516 across a 6 process deployment, bought
/// by an app whose tasks are all `async def`. This is the `needs_cpu` gate
/// applied to the io side.
///
/// It floors at 1, not 0, because `SyncPool::start` clamps its own thread
/// count up to 1 (pyrt.rs): a gated-off pool still keeps one idle standby
/// thread, and returning 0 here would misreport what actually starts.
fn sync_pool_threads(
    registry: &std::collections::HashMap<String, pyrt::TaskSpec>,
    io_threads: usize,
) -> usize {
    let needs_sync = registry.values().any(|s| s.kind != "cpu" && !s.is_async);
    if needs_sync {
        io_threads
    } else {
        1
    }
}

/// The interpreter cpu children are spawned with: the operator's `--python`
/// when they set one, otherwise this worker's own embedded interpreter, and
/// only as a last resort the historical `python3` off `PATH` (reachable only
/// on an embedding that reports no `sys.executable`).
fn resolve_python(flag: Option<&str>) -> String {
    match flag {
        Some(p) => p.to_string(),
        None => match pyrt::interpreter_executable() {
            Some(exe) => {
                info!("--python not set: cpu children use the embedded interpreter {exe}");
                exe
            }
            None => {
                warn!(
                    "--python not set and the embedded interpreter reports no \
                     sys.executable: falling back to \"python3\" off PATH, which \
                     will not see a venv that was never activated"
                );
                "python3".to_string()
            }
        },
    }
}

/// Install the rustls crypto provider for `rediss://` before any connection
/// is opened. `redis` pulls rustls in with default features off, so no
/// provider is compiled in on its side and `ClientConfig::builder()` would
/// panic ("no process-level CryptoProvider available") on the first TLS
/// handshake. Installing `ring` here rather than relying on rustls's
/// install-from-crate-features fallback keeps the choice deterministic: a
/// future dependency that also enables `aws-lc-rs` would turn that fallback
/// into a panic instead of a decision.
///
/// Idempotent and infallible by design: `install_default` returns `Err` only
/// when a provider is already installed, which is exactly as good an outcome.
fn install_tls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Mask `user:password@` userinfo, and `password=`/`username=` query
/// parameters, before a redis URL reaches logs or error messages (audit M4:
/// both are shapes redis-py accepts as real credentials, so either would
/// otherwise land in plaintext logs).
fn redact_redis_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let scheme = &url[..scheme_end];
    let rest = &url[scheme_end + 3..];
    // Authority ends at the first '/', '?' or '#'; '@' means the
    // userinfo/host boundary only within it, so an '@' inside a later
    // `?password=` value (masked separately below) can never be mistaken
    // for a second one and corrupt the visible host.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    // Last '@', not first: the `url` crate (and the client that uses it)
    // resolves a password containing a literal '@' the same way, so
    // splitting at the first one would leave that password's own tail in
    // plaintext right after the mask.
    let new_authority = match authority.rfind('@') {
        Some(at) => format!("***@{}", &authority[at + 1..]),
        None => authority.to_string(),
    };
    format!(
        "{scheme}://{new_authority}{}",
        redact_query_credentials(tail)
    )
}

/// Mask `password=`/`username=` VALUES in a URL's query string (the form
/// redis-py accepts straight as connection kwargs, no userinfo involved).
/// Keys and every other query parameter stay visible.
fn redact_query_credentials(tail: &str) -> String {
    let Some(q) = tail.find('?') else {
        return tail.to_string();
    };
    let (path, rest) = (&tail[..q], &tail[q + 1..]);
    let (query, fragment) = match rest.find('#') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    let masked = query
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((k, _)) if k == "password" || k == "username" => format!("{k}=***"),
            _ => pair.to_string(),
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{masked}{fragment}")
}

/// Seconds to milliseconds for the visibility timeout. Saturating, matching
/// the overflow safe style used elsewhere for hostile or just huge input
/// (dispatch.rs, envelope.rs, exec.rs, cpu.rs): release builds have no
/// overflow-checks, so a plain multiply would wrap silently instead.
fn visibility_timeout_ms(visibility_timeout_s: u64) -> u64 {
    visibility_timeout_s.saturating_mul(1000)
}

/// Seconds to milliseconds for idemp_ttl, same reasoning and same
/// saturating style as `visibility_timeout_ms` above.
fn idemp_ttl_ms(idemp_ttl_s: u64) -> u64 {
    idemp_ttl_s.saturating_mul(1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visibility_timeout_ms_saturates_instead_of_wrapping() {
        assert_eq!(visibility_timeout_ms(60), 60_000);
        assert_eq!(visibility_timeout_ms(u64::MAX), u64::MAX);
    }

    #[test]
    fn idemp_ttl_ms_saturates_instead_of_wrapping() {
        assert_eq!(idemp_ttl_ms(86_400), 86_400_000);
        assert_eq!(idemp_ttl_ms(u64::MAX), u64::MAX);
    }

    /// F4 reproduction: idemp_ttl and a task's own timeout_ms are unrelated
    /// numbers nothing else cross checks. The default idemp_ttl (86400s)
    /// comfortably outlives the default task timeout (300s / 300_000ms); a
    /// short idemp_ttl against that same default timeout is exactly the
    /// dangerous combination the new startup warning must catch.
    #[test]
    fn idemp_ttl_shorter_than_task_timeout_is_detected() {
        assert!(
            300_000 < idemp_ttl_ms(86_400),
            "default idemp_ttl must be safe"
        );
        assert!(
            300_000 >= idemp_ttl_ms(60),
            "a 60s idemp_ttl against a 300s task timeout must be flagged"
        );
    }

    /// Audit (perf): the sync pool used to start `--io-threads` threads
    /// whatever the app registered, so a pure `async def` workload paid for
    /// up to 86 idle OS threads per process (516 across the 6 process
    /// deployment the memory claim was measured on). Only a registered sync
    /// io task can reach `exec::run_sync_task`, so only that should size the
    /// pool.
    #[test]
    fn sync_pool_is_sized_by_the_registry_not_by_the_flag_alone() {
        let spec = |kind: &str, is_async: bool| pyrt::TaskSpec {
            kind: kind.to_string(),
            is_async,
            timeout_ms: 300_000,
        };
        let registry = |entries: &[(&str, bool)]| {
            entries
                .iter()
                .enumerate()
                .map(|(i, (kind, is_async))| (format!("t{i}"), spec(kind, *is_async)))
                .collect::<std::collections::HashMap<_, _>>()
        };

        // Nothing that can reach the pool: gated down to the floor.
        for gated in [
            registry(&[]),
            registry(&[("io", true)]),
            registry(&[("cpu", false)]),
            registry(&[("cpu", false), ("io", true), ("io", true)]),
        ] {
            assert_eq!(sync_pool_threads(&gated, 86), 1);
        }

        // One sync io task anywhere in the registry restores the full pool.
        assert_eq!(sync_pool_threads(&registry(&[("io", false)]), 86), 86);
        assert_eq!(
            sync_pool_threads(
                &registry(&[("io", true), ("cpu", false), ("io", false)]),
                86
            ),
            86
        );

        // The gate never asks for zero threads: SyncPool::start clamps to 1
        // and stats/`sync_live` would then report a pool that does not exist.
        assert_eq!(sync_pool_threads(&registry(&[]), 0), 1);
    }

    /// Audit FS-11: `--cpu-child-threads` is rejected outside [1, 1024] in
    /// this same file, while the two io knobs had only `.max(1)`. The bound
    /// has to sit on the RESOLVED plan, because `-c` derives both.
    #[test]
    fn io_limits_reject_only_what_cannot_be_built() {
        let plan = |io_threads, io_concurrency| cli::Resolved {
            procs: 1,
            io_threads,
            io_concurrency,
            cpu_workers: 1,
        };

        // Everything the derivation and the documented bands can produce.
        assert_eq!(io_limits_error(&plan(1, 1)), None);
        assert_eq!(io_limits_error(&plan(64, 256)), None); // standalone defaults
        assert_eq!(io_limits_error(&plan(1000, 1024)), None); // past the sync knee, still allowed
        assert_eq!(
            io_limits_error(&plan(MAX_IO_THREADS, MAX_IO_CONCURRENCY)),
            None
        );

        // `--io-threads 100000`: SyncPool::start would spawn until the OS
        // refuses and then panic.
        let msg = io_limits_error(&plan(100_000, 256)).expect("rejected");
        assert!(msg.contains("--io-threads"), "{msg}");
        assert!(msg.contains("100000"), "{msg}");

        // `-c 100000000` with no flags: io_concurrency derives to c/procs.
        let msg = io_limits_error(&plan(64, 12_500_000)).expect("rejected");
        assert!(msg.contains("--io-concurrency"), "{msg}");
        assert!(msg.contains("-c"), "{msg}");

        // Off by one on both, in both directions.
        assert!(io_limits_error(&plan(MAX_IO_THREADS + 1, 256)).is_some());
        assert!(io_limits_error(&plan(64, MAX_IO_CONCURRENCY + 1)).is_some());
    }

    /// Audit (failure modes): startup cross checks `timeout_ms` against
    /// `visibility_timeout` and `idemp_ttl` but never against
    /// `--drain-timeout`, whose default (30s) is a tenth of the default task
    /// timeout (300s) -- so a rolling deploy kills every task older than 30
    /// seconds and nothing at startup said so.
    #[test]
    fn drain_timeout_is_cross_checked_against_the_longest_task() {
        let spec = |timeout_ms| pyrt::TaskSpec {
            kind: "io".to_string(),
            is_async: true,
            timeout_ms,
        };
        let registry = |entries: &[(&str, u64)]| {
            entries
                .iter()
                .map(|(name, ms)| (name.to_string(), spec(*ms)))
                .collect::<std::collections::HashMap<_, _>>()
        };

        // The shipped defaults are the case that trips it.
        assert_eq!(
            longest_timeout_past_drain(&registry(&[("t", 300_000)]), 30),
            Some(("t", 300_000))
        );
        // Everything finishes inside the window: silent.
        assert_eq!(longest_timeout_past_drain(&registry(&[]), 30), None);
        assert_eq!(
            longest_timeout_past_drain(&registry(&[("t", 30_000), ("u", 1_000)]), 30),
            None,
            "equal to the window still fits; the drain only kills what is still running"
        );
        // One long task among short ones is the one named.
        assert_eq!(
            longest_timeout_past_drain(
                &registry(&[("short", 1_000), ("long", 600_000), ("mid", 45_000)]),
                30
            ),
            Some(("long", 600_000))
        );
        // Ties resolve by name, not by HashMap order, so two boots of the
        // same fleet report the same task.
        for _ in 0..8 {
            assert_eq!(
                longest_timeout_past_drain(&registry(&[("b", 90_000), ("a", 90_000)]), 30),
                Some(("a", 90_000))
            );
        }
        // No wrap on an absurd --drain-timeout, matching visibility_timeout_ms.
        assert_eq!(
            longest_timeout_past_drain(&registry(&[("t", u64::MAX)]), u64::MAX),
            None
        );
    }

    #[test]
    fn plan_total_saturates_instead_of_wrapping() {
        assert_eq!(plan_total(84, 6), 504);
        assert_eq!(plan_total(usize::MAX, 2), usize::MAX);
    }

    #[test]
    fn redacts_userinfo() {
        assert_eq!(
            redact_redis_url("redis://user:hunter2@host.example:6379/0"),
            "redis://***@host.example:6379/0"
        );
    }

    #[test]
    fn leaves_urls_without_userinfo_alone() {
        assert_eq!(
            redact_redis_url("redis://host.example:6379/0"),
            "redis://host.example:6379/0"
        );
    }

    #[test]
    fn redacts_password_containing_at_sign() {
        // M4 follow up: userinfo is split at the LAST '@', matching how the
        // `url` crate (and the redis client that uses it) actually parses
        // it. Splitting at the first one used to leave the rest of a
        // password containing '@' sitting in plaintext right after the mask.
        assert_eq!(
            redact_redis_url("redis://user:p@ss@dbhost:6379/0"),
            "redis://***@dbhost:6379/0"
        );
    }

    #[test]
    fn redacts_query_string_credentials() {
        // redis-py accepts `password=`/`username=` straight off the query
        // string as connection kwargs, no userinfo involved, so this form
        // carries a real credential even with no '@' anywhere in the URL.
        assert_eq!(
            redact_redis_url("redis://dbhost:6379/0?password=s3cr3t"),
            "redis://dbhost:6379/0?password=***"
        );
        assert_eq!(
            redact_redis_url("redis://dbhost:6379/0?username=svc&password=s3cr3t"),
            "redis://dbhost:6379/0?username=***&password=***"
        );
    }

    #[test]
    fn redacts_userinfo_and_query_credentials_together() {
        assert_eq!(
            redact_redis_url("redis://user:p@ss@dbhost:6379/0?password=alsosecret"),
            "redis://***@dbhost:6379/0?password=***"
        );
    }

    /// Audit BLOCKER (TLS). Built with no TLS feature on the redis crate,
    /// `rediss://` fails inside `Client::open` itself: the scheme is
    /// recognized but the arm that builds `ConnectionAddr::TcpTls` is
    /// `#[cfg]`'d out, so the worker exits 1 at startup on Upstash, Azure
    /// Cache or any ElastiCache with encryption in transit, while redis-py
    /// keeps enqueueing into it. This asserts the URL both parses AND
    /// selects the TLS transport.
    #[test]
    fn rediss_url_opens_and_selects_the_tls_transport() {
        let client =
            redis::Client::open("rediss://user:pw@redis.example:6380/0").expect("rediss:// opens");
        assert!(
            matches!(
                client.get_connection_info().addr,
                redis::ConnectionAddr::TcpTls { .. }
            ),
            "rediss:// must resolve to a TLS transport, got {:?}",
            client.get_connection_info().addr
        );
    }

    /// The plain scheme must stay plaintext: enabling TLS support must not
    /// quietly upgrade `redis://`, which is what every local and itest
    /// deployment uses.
    #[test]
    fn redis_url_stays_plaintext() {
        let client = redis::Client::open("redis://127.0.0.1:6392/0").expect("redis:// opens");
        assert!(matches!(
            client.get_connection_info().addr,
            redis::ConnectionAddr::Tcp(..)
        ));
    }

    #[test]
    fn tls_provider_install_is_idempotent() {
        install_tls_provider();
        install_tls_provider(); // second call must not panic
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    /// The other half of the blocker: opening the client is not enough, the
    /// handshake has to actually run. A listener that accepts and closes
    /// immediately makes the TLS handshake fail at the transport, which is
    /// proof we got past URL parsing and into a real connection attempt --
    /// the failure mode being fixed produced `InvalidClientConfig` before a
    /// single byte was sent.
    #[tokio::test]
    async fn rediss_reaches_a_real_connection_attempt() {
        install_tls_provider();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((sock, _)) = listener.accept() {
                let _ = sock.shutdown(std::net::Shutdown::Both);
            }
        });
        let client = redis::Client::open(format!("rediss://127.0.0.1:{port}/0")).expect("open");
        let err = client
            .get_multiplexed_async_connection()
            .await
            .expect_err("a closed socket cannot complete a TLS handshake");
        assert_ne!(
            err.kind(),
            redis::ErrorKind::InvalidClientConfig,
            "rediss:// must fail at the handshake, not at client construction: {err}"
        );
    }

    /// Verbatim `INFO cluster` from a cluster-enabled node (redis 7.0.15),
    /// section header and CRLF endings and all.
    const CLUSTER_INFO_ENABLED: &str = "# Cluster\r\n\
                                        cluster_enabled:1\r\n";
    /// The same command against a standalone server, which answers it too.
    const CLUSTER_INFO_DISABLED: &str = "# Cluster\r\n\
                                         cluster_enabled:0\r\n";
    /// Verbatim `CLUSTER INFO` from that same cluster node, which is the
    /// command this probe must NOT use: it describes cluster health and never
    /// mentions `cluster_enabled`, so probing it reads a real cluster as a
    /// standalone and the refusal never fires (see `probe_cluster_info`).
    const WRONG_COMMAND_REPLY: &str = "cluster_state:fail\r\n\
                                       cluster_slots_assigned:0\r\n\
                                       cluster_known_nodes:1\r\n\
                                       cluster_size:1\r\n";
    const TEST_REDIS_URL: &str = "redis://user:hunter2@cluster.example:6379/0";

    /// docs/decisions/redis-cluster.md: pointing the worker at a Redis
    /// Cluster used to start cleanly and then lose work, because
    /// cauli:q:{queue} and cauli:delayed:{queue} never share a hash slot --
    /// a retried task leaves the stream and never reaches the delayed set.
    /// loops::report_mover_error names the cause, but only on the mover path
    /// and only after the loss. Startup has to refuse instead.
    #[test]
    fn cluster_mode_is_refused_at_startup() {
        match cluster_decision(CLUSTER_INFO_ENABLED, None, TEST_REDIS_URL) {
            ClusterDecision::Refuse(msg) => {
                assert!(msg.contains("cluster mode"), "names the topology: {msg}");
                assert!(
                    msg.contains("standalone and Sentinel"),
                    "names the supported ones: {msg}"
                );
                assert!(
                    msg.contains(ALLOW_REDIS_CLUSTER_ENV),
                    "names the override: {msg}"
                );
                assert!(
                    !msg.contains("hunter2"),
                    "the refusal goes through redact_redis_url: {msg}"
                );
            }
            other => panic!("cluster mode must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_standalone_server_starts_silently() {
        assert_eq!(
            cluster_decision(CLUSTER_INFO_DISABLED, None, TEST_REDIS_URL),
            ClusterDecision::Start
        );
        // Nothing usable in the reply is not evidence of a cluster: an
        // unreadable probe must never keep a working deployment from booting.
        assert_eq!(
            cluster_decision("", None, TEST_REDIS_URL),
            ClusterDecision::Start
        );
        assert!(!cluster_info_says_enabled("cluster_enabled:11\r\n"));
        assert!(!cluster_info_says_enabled("not_cluster_enabled:1\r\n"));
    }

    /// The refusal shipped inert because it asked the wrong command.
    /// `CLUSTER INFO` reports cluster HEALTH and has no `cluster_enabled`
    /// field, so its reply reads as "not a cluster" no matter the topology --
    /// and a standalone answers it with a flat error, which is why the bug
    /// never showed up as a false refusal either. Pin the shape of the reply
    /// that must never be fed to this parser.
    #[test]
    fn the_health_command_reply_can_never_prove_a_cluster() {
        assert!(
            !cluster_info_says_enabled(WRONG_COMMAND_REPLY),
            "CLUSTER INFO carries no cluster_enabled: probing it disarms the refusal"
        );
        assert!(WRONG_COMMAND_REPLY.contains("cluster_state"));
        assert!(!WRONG_COMMAND_REPLY.contains("cluster_enabled"));
        // The reply the probe does read says it in one field, on both sides.
        assert!(cluster_info_says_enabled(CLUSTER_INFO_ENABLED));
        assert!(!cluster_info_says_enabled(CLUSTER_INFO_DISABLED));
    }

    #[test]
    fn the_cluster_override_is_explicit_opt_in_only() {
        for raw in ["1", "true", "TRUE", " yes ", "on"] {
            assert!(
                matches!(
                    cluster_decision(CLUSTER_INFO_ENABLED, Some(raw), TEST_REDIS_URL),
                    ClusterDecision::StartAnyway(_)
                ),
                "{raw:?} must opt in"
            );
        }
        for raw in ["", "0", "false", "no", "maybe"] {
            assert!(
                matches!(
                    cluster_decision(CLUSTER_INFO_ENABLED, Some(raw), TEST_REDIS_URL),
                    ClusterDecision::Refuse(_)
                ),
                "{raw:?} must not opt in"
            );
        }
        assert!(matches!(
            cluster_decision(CLUSTER_INFO_ENABLED, None, TEST_REDIS_URL),
            ClusterDecision::Refuse(_)
        ));
    }

    /// A throwaway redis-server this test owns, on ports dedicated to these
    /// tests: never :6409/:6410/:6422 (broker.rs), :6392 (worker/tests
    /// common), :6391 (py/itest), and never :6379.
    ///
    /// Slots are deliberately left unassigned: the node reports
    /// cluster_enabled:1 from the moment it boots, and the refusal has to
    /// rest on that field alone rather than on a healthy cluster_state.
    struct ProbeRedis {
        port: u16,
    }

    impl ProbeRedis {
        fn start(port: u16, cluster: bool) -> Self {
            let _ = std::process::Command::new("redis-cli")
                .args(["-p", &port.to_string(), "shutdown", "nosave"])
                .output();
            std::thread::sleep(Duration::from_millis(150));
            let mut args = vec![
                "--port".to_string(),
                port.to_string(),
                "--save".to_string(),
                String::new(),
                "--appendonly".to_string(),
                "no".to_string(),
                "--daemonize".to_string(),
                "yes".to_string(),
            ];
            if cluster {
                // A stale nodes.conf from an earlier run brings the node up
                // with someone else-s view of the cluster: start blank.
                let dir = std::env::temp_dir().join(format!("cauli-probe-cluster-{port}"));
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).expect("cluster test dir");
                args.push("--cluster-enabled".to_string());
                args.push("yes".to_string());
                args.push("--cluster-config-file".to_string());
                args.push(
                    dir.join("nodes.conf")
                        .to_str()
                        .expect("utf8 tmp path")
                        .to_string(),
                );
            }
            let out = std::process::Command::new("redis-server")
                .args(&args)
                .output()
                .expect("redis-server spawn");
            assert!(out.status.success(), "redis-server failed: {out:?}");
            for _ in 0..50 {
                let ping = std::process::Command::new("redis-cli")
                    .args(["-p", &port.to_string(), "ping"])
                    .output();
                if ping
                    .map(|o| String::from_utf8_lossy(&o.stdout).contains("PONG"))
                    .unwrap_or(false)
                {
                    return Self { port };
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            panic!("redis on {port} did not answer PING");
        }

        fn url(&self) -> String {
            format!("redis://127.0.0.1:{}/0", self.port)
        }
    }

    impl Drop for ProbeRedis {
        fn drop(&mut self) {
            let _ = std::process::Command::new("redis-cli")
                .args(["-p", &self.port.to_string(), "shutdown", "nosave"])
                .output();
        }
    }

    /// The refusal is only worth as much as the probe under it, so this runs
    /// it against real servers of both topologies rather than against the
    /// samples above. This is the test that caught the probe asking `CLUSTER
    /// INFO`: every unit test passed on hand written samples while a live
    /// cluster node started cleanly and lost work.
    #[tokio::test]
    async fn the_probe_reads_the_topology_off_a_real_server() {
        for (port, cluster) in [(6430u16, true), (6431, false)] {
            let redis = ProbeRedis::start(port, cluster);
            let client = redis::Client::open(redis.url()).expect("client");
            let mut conn = ConnectionManager::new(client)
                .await
                .expect("connection manager");
            let info = probe_cluster_info(&mut conn)
                .await
                .expect("both topologies must answer the probe, not just one");
            assert_eq!(cluster_info_says_enabled(&info), cluster, "{info}");
            let decision = cluster_decision(&info, None, &redis.url());
            if cluster {
                assert!(
                    matches!(decision, ClusterDecision::Refuse(_)),
                    "a real cluster node must be refused, got {decision:?}"
                );
            } else {
                assert_eq!(decision, ClusterDecision::Start);
            }
        }
    }
}
