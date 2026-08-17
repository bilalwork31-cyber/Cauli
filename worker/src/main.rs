mod backoff;
mod broker;
mod cli;
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
use redis::aio::ConnectionManager;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tracing::{error, info, warn};

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

fn main() {
    // A panic that unwinds out of real_main must not be allowed to unwind
    // out of main itself: past that point the C runtime returns from
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
    if args.print_plan {
        print_plan(&args, &resolved, cores);
        return 0;
    }
    if resolved.procs > 1 {
        info!(
            "cauli-worker supervising {} procs: app={} c={} -> per-proc io_threads={} io_concurrency={} cpu_workers={}",
            resolved.procs,
            args.app,
            args.concurrency.map_or("unset".into(), |c| c.to_string()),
            resolved.io_threads,
            resolved.io_concurrency,
            resolved.cpu_workers
        );
        return supervisor::run(&args, &resolved);
    }

    // Embedded CPython: init interpreter, shim, app, asyncio loops (§ pyrt.rs).
    let (pyrt, appcfg) = match pyrt::PyRuntime::init(&args.app, args.io_loops) {
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
        args.app,
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
    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            error!("bad redis url {:?}: {e}", redact_redis_url(&redis_url));
            return 1;
        }
    };
    let mut write_conn = match ConnectionManager::new(client.clone()).await {
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
    let fetch_conn = match ConnectionManager::new(client).await {
        Ok(c) => c,
        Err(e) => {
            error!("cannot open fetch connection: {e}");
            return 1;
        }
    };
    if let Err(e) = broker::ensure_groups(&mut write_conn, &queues).await {
        error!("XGROUP CREATE failed: {e}");
        return 1;
    }

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
    let cpu_cfg = needs_cpu.then(|| cpu::StartCfg {
        workers: resolved.cpu_workers,
        child_threads: args.cpu_child_threads,
        prefetch: args.cpu_prefetch,
        recycle: args.cpu_max_tasks_per_child,
        python: args.python.clone(),
        app_spec: args.app.clone(),
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
    let ctx = Arc::new(Ctx {
        io_sem: Arc::new(tokio::sync::Semaphore::new(io_concurrency)),
        sync_pool: pyrt::SyncPool::start(pyrt.clone(), resolved.io_threads, io_concurrency),
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

    loops::fetch_loop(ctx.clone(), fetch_conn).await; // returns on shutdown

    // §4.7 drain: mover + acks keep running; wait for in flight tasks.
    let deadline = Instant::now() + Duration::from_secs(ctx.args.drain_timeout);
    while ctx.counters.inflight_total.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let left = ctx.counters.inflight_total.load(Ordering::SeqCst);
    if left > 0 {
        warn!("drain timeout: leaving {left} tasks pending for recovery (§4.4)");
    } else {
        info!("drained cleanly");
    }
    info!("{}", ctx.counters.stats_line());
    if let Some(pool) = ctx.cpu.get() {
        cpu::kill_children(pool);
    }
    0
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
        r.io_concurrency * r.procs,
        r.io_threads * r.procs,
        r.cpu_workers * r.procs
    );
    println!("  override any value with its flag; see --help and docs/CONFIGURATION.md");
}

/// Mask `user:password@` userinfo, and `password=`/`username=` query
/// parameters, before a redis URL reaches logs or error messages (audit M4
/// — both are shapes redis-py accepts as real credentials, so either would
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visibility_timeout_ms_saturates_instead_of_wrapping() {
        assert_eq!(visibility_timeout_ms(60), 60_000);
        assert_eq!(visibility_timeout_ms(u64::MAX), u64::MAX);
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
}
