//! The cauli worker runtime. One public item, [`run`], shared by the
//! `cauli-worker` and `cauli-worker-bin` entry points (see worker/Cargo.toml).

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod backoff;
mod broker;
mod cli;
mod clock;
mod cpu;
mod ctx;
mod dispatch;
mod envelope;
mod exec;
mod logsink;
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

/// Hooks for worker/tests/exit_path.rs; absent from a release build.
#[cfg(any(test, feature = "test-hooks"))]
mod exit_path_test_hooks {
    use std::sync::OnceLock;

    static MARKER_PATH: OnceLock<String> = OnceLock::new();

    extern "C" fn write_marker() {
        if let Some(path) = MARKER_PATH.get() {
            let _ = std::fs::write(path, b"atexit ran\n");
        }
    }

    /// A real atexit handler writing `CAULI_TEST_ATEXIT_MARKER`: a stand in
    /// for the `OPENSSL_cleanup` handler `exit_now` must skip.
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

    /// Panics once the interpreter's threads are live, the condition
    /// `exit_now` exists for.
    pub fn maybe_panic_after_pyrt_init() {
        if std::env::var_os("CAULI_TEST_PANIC_AFTER_PYRT_INIT").is_some() {
            panic!("test-hooks: forced main-thread panic after PyRuntime::init");
        }
    }
}

/// Run the worker and terminate the process. Never returns.
pub fn run() -> ! {
    // A panic escaping `run` would reach libc `exit()` and the atexit race
    // `exit_now` prevents.
    let code = std::panic::catch_unwind(real_main).unwrap_or(101);
    exit_now(code);
}

/// Longest `exit_now` waits for queued log lines to reach stdout.
const EXIT_LOG_FLUSH_BUDGET: Duration = Duration::from_secs(1);

/// Terminate via `_exit`, skipping atexit handlers and DSO destructors.
///
/// Pool, asyncio and task threads are still live at exit. libc `exit()` would
/// run `OPENSSL_cleanup` (registered by libpq's libssl) under them and corrupt
/// the heap after a clean drain. Only the log sink is drained first, bounded
/// and without the stdout lock a stalled writer may hold.
fn exit_now(code: i32) -> ! {
    logsink::flush(EXIT_LOG_FLUSH_BUDGET);
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
    // Never stdout directly: a stalled reader would freeze the runtime.
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(logsink::LogSink::start())
        .init();

    // Validated before Python starts, so a bad flag fails once instead of
    // crash looping every supervised child.
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
    // Sizes an eager channel and a thread count per child.
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
    // On resolved values: `-c` derives both io knobs without either flag set.
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
    // Warned once, for the longest task: stock defaults (30s drain, 300s
    // timeout) trip it for every task.
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

    // Each of `procs` processes gets its share of the cores; tokio threads
    // never run Python. TOKIO_WORKER_THREADS, when set, still wins.
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();
    if std::env::var_os("TOKIO_WORKER_THREADS").is_none() {
        rt_builder.worker_threads((cores / resolved.procs).max(1));
    }
    let rt = rt_builder.build().expect("tokio runtime");
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
    // Config level timeout, never a per call tokio::time::timeout: only this
    // makes the manager see the timeout as an IoError and reconnect; a caller
    // side timeout leaves it reusing the wedged socket.
    let conn_cfg = ConnectionManagerConfig::new()
        .set_max_delay(REDIS_RETRY_MAX_DELAY)
        .set_number_of_retries(REDIS_RETRIES)
        .set_response_timeout(Some(Duration::from_secs(args.redis_timeout)))
        .set_connection_timeout(Some(Duration::from_secs(args.redis_timeout)));
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
    // Refused up front: on a cluster delayed and retried tasks are lost
    // before the mover can report it (docs/architecture.md).
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
    // Before anything reads the clock (clock.rs).
    clock::init(&mut write_conn).await;
    tokio::spawn(clock::sampler_loop(write_conn.clone()));

    // §9.2 queue TTLs, logged because dropping work must be announced.
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
    // PROTOCOL §1 consumer name; one fetch loop per process, so no `n` part.
    let consumer = format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    );
    let io_concurrency = resolved.io_concurrency;
    // Routing is by registry kind, so with no cpu task the pool is unreachable
    // and never started; with one, it starts lazily on the first cpu task.
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
    let sync_threads = sync_pool_threads(&appcfg.tasks, resolved.io_threads);
    if sync_threads < resolved.io_threads {
        info!(
            "no sync (non-async) io tasks registered: sync pool started with \
             {sync_threads} thread instead of {}",
            resolved.io_threads
        );
    }
    // Built once; dispatch never adds a key, so the map needs no lock.
    let inflight_entries = queues
        .iter()
        .map(|q| (q.clone(), std::sync::Mutex::new(Default::default())))
        .collect();
    let acks = broker::AckBufs::start(&write_conn, &queues, args.ack_batch, args.ack_flush_ms);
    let ctx = Arc::new(Ctx {
        io_sem: Arc::new(tokio::sync::Semaphore::new(io_concurrency)),
        io_concurrency,
        inflight_entries,
        acks,
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
    tokio::spawn(loops::trim_loop(ctx.clone()));
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

/// A repeat signal within this gap is the same stop request, not a force.
const SECOND_SIGNAL_MIN_GAP: Duration = Duration::from_secs(1);

fn spawn_signal_task(shutdown_tx: watch::Sender<bool>, ctx: Arc<Ctx>) {
    tokio::spawn(async move {
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = term.recv() => info!("SIGTERM: stop fetching, draining"),
            _ = int.recv() => info!("SIGINT: stop fetching, draining"),
        }
        let _ = shutdown_tx.send(true);
        // One stop often arrives twice: systemd signals both this process
        // and the supervisor, which forwards its own.
        let first = tokio::time::Instant::now();
        loop {
            tokio::select! {
                _ = term.recv() => {},
                _ = int.recv() => {},
            }
            if first.elapsed() >= SECOND_SIGNAL_MIN_GAP {
                break;
            }
            info!("signal within {SECOND_SIGNAL_MIN_GAP:?} of the first: same stop request, still draining");
        }
        warn!("second signal: forced exit 130");
        // Skips run_worker's cleanup tail, so the cpu children and the fork
        // server socket are released here.
        if let Some(pool) = ctx.cpu.get() {
            cpu::kill_children(pool);
        }
        exit_now(130);
    });
}

/// Capped so a recovered redis is not met by one long sleep; six retries fail
/// an unreachable redis at startup within seconds.
const REDIS_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);
const REDIS_RETRIES: usize = 6;

/// Operator override for the Redis Cluster startup refusal.
const ALLOW_REDIS_CLUSTER_ENV: &str = "CAULI_ALLOW_REDIS_CLUSTER";

#[derive(Debug, PartialEq, Eq)]
enum ClusterDecision {
    Start,
    /// Cluster, and the operator opted in.
    StartAnyway(String),
    Refuse(String),
}

/// `INFO cluster`, not `CLUSTER INFO`: only the former answers on both
/// topologies and carries `cluster_enabled`. A failed probe (ACL, proxy)
/// returns `None` and never blocks startup.
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

/// `cluster_enabled:1` is the whole signal; a cluster with unassigned slots
/// is still a cluster.
fn cluster_info_says_enabled(info: &str) -> bool {
    info.lines()
        .filter_map(|line| line.split_once(':'))
        .any(|(k, v)| k.trim() == "cluster_enabled" && v.trim() == "1")
}

/// Takes the raw value, not the env, so tests need not mutate the process env.
fn cluster_override_enabled(raw: Option<&str>) -> bool {
    let normalized = raw.map(|v| v.trim().to_ascii_lowercase());
    matches!(normalized.as_deref(), Some("1" | "true" | "yes" | "on"))
}

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
         anyway and accept that loss (docs/architecture.md)."
    ))
}

/// --print-plan. Needs no Python, app or Redis.
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
        "    tokio runtime threads {}  (redis io and dispatch; never run Python)",
        (cores / r.procs).max(1)
    );
    println!(
        "    ack flush           {} completions or {} ms, whichever first (per queue)",
        args.ack_batch.max(1),
        args.ack_flush_ms
    );
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
        "  totals: {} io tasks in flight, {} sync threads, {} tokio threads, \
up to {} cpu children",
        plan_total(r.io_concurrency, r.procs),
        plan_total(r.io_threads, r.procs),
        plan_total((cores / r.procs).max(1), r.procs),
        plan_total(r.cpu_workers, r.procs)
    );
    println!("  override any value with its flag; see --help and docs/configuration.md");
}

/// Saturating: a 19 digit -c would otherwise wrap the printed total.
fn plan_total(per_process: usize, procs: usize) -> usize {
    per_process.saturating_mul(procs)
}

/// Drain ran out of `--drain-timeout` with tasks still running; they return
/// through recovery. Clear of signal codes (128 + n) and panic (101).
const DRAIN_TIMEOUT_EXIT_CODE: i32 = 88;

/// The longest task timeout past the drain window, ties broken by name so
/// the warning is stable across boots.
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

/// Four times the ~1000 thread sync knee: above it is a typo, and the eager
/// spawn would run until the OS refuses and then panic.
const MAX_IO_THREADS: usize = 4096;

/// 256x the measured band; the sync pool channel is allocated at this size.
const MAX_IO_CONCURRENCY: usize = 65_536;

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

/// `--io-threads` when a sync io task is registered, else 1: an all `async
/// def` app can never reach the eagerly spawned pool. 1, not 0, because
/// `SyncPool::start` clamps to 1 and this must report what starts.
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

/// `--python`, else the embedded interpreter, else `python3` off `PATH`.
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

/// redis builds rustls without a provider, so the first `rediss://` handshake
/// would panic. Installed explicitly so a dependency enabling `aws-lc-rs`
/// cannot make the fallback ambiguous. `Err` only means already installed.
fn install_tls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Mask userinfo and query credentials before a redis URL is logged.
fn redact_redis_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let scheme = &url[..scheme_end];
    let rest = &url[scheme_end + 3..];
    // Up to the query, not the first '/' or '#': both occur unencoded in
    // passwords (base64). Last '@', for passwords containing '@'.
    let userinfo_end = rest.find('?').unwrap_or(rest.len());
    let (mask, rest) = match rest[..userinfo_end].rfind('@') {
        Some(at) => ("***@", &rest[at + 1..]),
        None => ("", rest),
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    format!(
        "{scheme}://{mask}{authority}{}",
        redact_query_credentials(tail)
    )
}

/// Masks credential values only; keys and other parameters stay visible.
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
            Some((k, _)) if is_credential_key(k) => format!("{k}=***"),
            _ => pair.to_string(),
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{masked}{fragment}")
}

/// `password`/`username` are redis-py's query credentials, `pass`/`user` are
/// redis-rs's for `redis+unix://`. Both clients percent-decode the key, so
/// any key holding a '%' is treated as a possible credential too.
fn is_credential_key(key: &str) -> bool {
    matches!(key, "password" | "username" | "pass" | "user") || key.contains('%')
}

#[cfg(test)]
mod tests {
    use super::*;

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

        for gated in [
            registry(&[]),
            registry(&[("io", true)]),
            registry(&[("cpu", false)]),
            registry(&[("cpu", false), ("io", true), ("io", true)]),
        ] {
            assert_eq!(sync_pool_threads(&gated, 86), 1);
        }

        assert_eq!(sync_pool_threads(&registry(&[("io", false)]), 86), 86);
        assert_eq!(
            sync_pool_threads(
                &registry(&[("io", true), ("cpu", false), ("io", false)]),
                86
            ),
            86
        );

        assert_eq!(sync_pool_threads(&registry(&[]), 0), 1);
    }

    #[test]
    fn io_limits_reject_only_what_cannot_be_built() {
        let plan = |io_threads, io_concurrency| cli::Resolved {
            procs: 1,
            io_threads,
            io_concurrency,
            cpu_workers: 1,
        };

        assert_eq!(io_limits_error(&plan(1, 1)), None);
        assert_eq!(io_limits_error(&plan(64, 256)), None); // standalone defaults
        assert_eq!(io_limits_error(&plan(1000, 1024)), None); // past the sync knee, still allowed
        assert_eq!(
            io_limits_error(&plan(MAX_IO_THREADS, MAX_IO_CONCURRENCY)),
            None
        );

        let msg = io_limits_error(&plan(100_000, 256)).expect("rejected");
        assert!(msg.contains("--io-threads"), "{msg}");
        assert!(msg.contains("100000"), "{msg}");

        // `-c 100000000` with no flags: io_concurrency derives to c/procs.
        let msg = io_limits_error(&plan(64, 12_500_000)).expect("rejected");
        assert!(msg.contains("--io-concurrency"), "{msg}");
        assert!(msg.contains("-c"), "{msg}");

        assert!(io_limits_error(&plan(MAX_IO_THREADS + 1, 256)).is_some());
        assert!(io_limits_error(&plan(64, MAX_IO_CONCURRENCY + 1)).is_some());
    }

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

        // The shipped defaults trip it.
        assert_eq!(
            longest_timeout_past_drain(&registry(&[("t", 300_000)]), 30),
            Some(("t", 300_000))
        );
        assert_eq!(longest_timeout_past_drain(&registry(&[]), 30), None);
        assert_eq!(
            longest_timeout_past_drain(&registry(&[("t", 30_000), ("u", 1_000)]), 30),
            None,
            "equal to the window still fits; the drain only kills what is still running"
        );
        assert_eq!(
            longest_timeout_past_drain(
                &registry(&[("short", 1_000), ("long", 600_000), ("mid", 45_000)]),
                30
            ),
            Some(("long", 600_000))
        );
        // Ties by name, whatever the HashMap order.
        for _ in 0..8 {
            assert_eq!(
                longest_timeout_past_drain(&registry(&[("b", 90_000), ("a", 90_000)]), 30),
                Some(("a", 90_000))
            );
        }
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
        // Split at the last '@', as the `url` crate parses it.
        assert_eq!(
            redact_redis_url("redis://user:p@ss@dbhost:6379/0"),
            "redis://***@dbhost:6379/0"
        );
    }

    #[test]
    fn redacts_query_string_credentials() {
        // redis-py reads these as connection kwargs.
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
    fn redacts_unix_socket_and_percent_encoded_query_credentials() {
        assert_eq!(
            redact_redis_url("redis+unix:///run/redis.sock?db=1&user=svc&pass=s3cr3t"),
            "redis+unix:///run/redis.sock?db=1&user=***&pass=***"
        );
        assert_eq!(
            redact_redis_url("redis://dbhost:6379/0?%70assword=s3cr3t"),
            "redis://dbhost:6379/0?%70assword=***"
        );
    }

    #[test]
    fn redacts_password_with_unencoded_slash_or_hash() {
        assert_eq!(
            redact_redis_url("redis://u:Zm9v/YmFy@dbhost:6379/0"),
            "redis://***@dbhost:6379/0"
        );
        assert_eq!(
            redact_redis_url("redis://u:abc#def@dbhost:6379/0"),
            "redis://***@dbhost:6379/0"
        );
        assert_eq!(
            redact_redis_url("redis://dbhost:6379/0?password=a@b"),
            "redis://dbhost:6379/0?password=***"
        );
    }

    #[test]
    fn redacts_userinfo_and_query_credentials_together() {
        assert_eq!(
            redact_redis_url("redis://user:p@ss@dbhost:6379/0?password=alsosecret"),
            "redis://***@dbhost:6379/0?password=***"
        );
    }

    /// Without the redis TLS feature `rediss://` fails in `Client::open`.
    #[test]
    fn rediss_url_opens_and_selects_the_tls_transport() {
        let client =
            redis::Client::open("rediss://user:pw@redis.example:6380/0").expect("rediss:// opens");
        assert!(
            matches!(
                client.get_connection_info().addr(),
                redis::ConnectionAddr::TcpTls { .. }
            ),
            "rediss:// must resolve to a TLS transport, got {:?}",
            client.get_connection_info().addr()
        );
    }

    #[test]
    fn redis_url_stays_plaintext() {
        let client = redis::Client::open("redis://127.0.0.1:6392/0").expect("redis:// opens");
        assert!(matches!(
            client.get_connection_info().addr(),
            redis::ConnectionAddr::Tcp(..)
        ));
    }

    #[test]
    fn tls_provider_install_is_idempotent() {
        install_tls_provider();
        install_tls_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    /// A listener that closes at once: failing at the handshake, not with
    /// `InvalidClientConfig`, proves a real TLS attempt ran.
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

    /// Verbatim `INFO cluster` from redis 7.0.15.
    const CLUSTER_INFO_ENABLED: &str = "# Cluster\r\n\
                                        cluster_enabled:1\r\n";
    const CLUSTER_INFO_DISABLED: &str = "# Cluster\r\n\
                                         cluster_enabled:0\r\n";
    /// Verbatim `CLUSTER INFO` from the same node: the command not to probe.
    const WRONG_COMMAND_REPLY: &str = "cluster_state:fail\r\n\
                                       cluster_slots_assigned:0\r\n\
                                       cluster_known_nodes:1\r\n\
                                       cluster_size:1\r\n";
    const TEST_REDIS_URL: &str = "redis://user:hunter2@cluster.example:6379/0";

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
        // An unreadable reply must never block boot.
        assert_eq!(
            cluster_decision("", None, TEST_REDIS_URL),
            ClusterDecision::Start
        );
        assert!(!cluster_info_says_enabled("cluster_enabled:11\r\n"));
        assert!(!cluster_info_says_enabled("not_cluster_enabled:1\r\n"));
    }

    #[test]
    fn the_health_command_reply_can_never_prove_a_cluster() {
        assert!(
            !cluster_info_says_enabled(WRONG_COMMAND_REPLY),
            "CLUSTER INFO carries no cluster_enabled: probing it disarms the refusal"
        );
        assert!(WRONG_COMMAND_REPLY.contains("cluster_state"));
        assert!(!WRONG_COMMAND_REPLY.contains("cluster_enabled"));
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

    /// Throwaway redis on ports no other suite uses (not 6409, 6410, 6422,
    /// 6392, 6391, 6379). Slots stay unassigned on purpose.
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
                // A stale nodes.conf would carry an old cluster view.
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

    /// Against real servers of both topologies, not hand written samples.
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
