mod backoff;
mod broker;
mod cli;
mod cpu;
mod ctx;
mod dispatch;
mod envelope;
mod exec;
mod loops;
mod pyrt;
mod stats;

use clap::Parser;
use ctx::Ctx;
use redis::aio::ConnectionManager;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tracing::{error, info, warn};

fn main() {
    let code = real_main();
    // Explicit exit: sync pool threads and Python daemon threads must not
    // keep the process alive.
    std::process::exit(code);
}

fn real_main() -> i32 {
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

    // Embedded CPython: init interpreter, shim, app, asyncio loops (§ pyrt.rs).
    let (pyrt, appcfg) = match pyrt::PyRuntime::init(&args.app, args.io_loops) {
        Ok(v) => v,
        Err(e) => {
            error!("startup failed: {e:#}");
            return 1;
        }
    };
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
    info!(
        "cauli-worker starting: app={} queues={:?} redis={} tasks={}",
        args.app,
        queues,
        redact_redis_url(&redis_url),
        appcfg.tasks.len()
    );
    // H1 operator diagnostic: loops::recovery_loop's per-envelope idle check
    // already prevents reclaiming a still-running task regardless of this
    // default, but a task whose registered timeout_ms is >= the visibility
    // floor is a strong signal of a misconfigured deployment (the invariant
    // documented in PROTOCOL.md §4.4: visibility_timeout should exceed your
    // longest task) — warn loudly at startup so operators catch it early.
    let vt_ms = args.visibility_timeout * 1000;
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
    rt.block_on(run_worker(args, pyrt, appcfg, redis_url, queues))
}

async fn run_worker(
    args: cli::Args,
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

    let counters = Arc::new(stats::Counters::default());
    let cpu_workers = args.cpu_workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });
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
    let io_concurrency = args.io_concurrency.max(1);
    let ctx = Arc::new(Ctx {
        io_sem: Arc::new(tokio::sync::Semaphore::new(io_concurrency)),
        sync_pool: pyrt::SyncPool::start(pyrt.clone(), args.io_threads, io_concurrency),
        cpu: cpu::start(cpu_workers, &args.python, &args.app, counters.clone()),
        registry: appcfg.tasks,
        redis: write_conn,
        counters,
        pyrt,
        result_ttl: appcfg.result_ttl,
        idemp_ttl: appcfg.idemp_ttl,
        queues,
        consumer,
        shutdown: shutdown_rx,
        args,
    });

    spawn_signal_task(shutdown_tx);
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
    cpu::kill_children(&ctx.cpu);
    0
}

fn spawn_signal_task(shutdown_tx: watch::Sender<bool>) {
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
        std::process::exit(130);
    });
}

/// Mask `user:password@` userinfo before a redis URL reaches logs or error
/// messages (audit M4 — `redis://user:password@host/0` is a common shape and
/// the password would otherwise land in plaintext logs).
fn redact_redis_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at) = after_scheme.find('@') {
            return format!("{}://***@{}", &url[..scheme_end], &after_scheme[at + 1..]);
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
