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
        .or_else(|| std::env::var("RUPY_REDIS_URL").ok())
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
        "rupy-worker starting: app={} queues={:?} redis={} tasks={}",
        args.app,
        queues,
        redis_url,
        appcfg.tasks.len()
    );

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
            error!("bad redis url {redis_url:?}: {e}");
            return 1;
        }
    };
    let mut write_conn = match ConnectionManager::new(client.clone()).await {
        Ok(c) => c,
        Err(e) => {
            error!("cannot connect to redis at {redis_url}: {e}");
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
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let consumer = format!(
        "{}:{}:0",
        gethostname::gethostname().to_string_lossy(),
        std::process::id()
    );
    let ctx = Arc::new(Ctx {
        io_sem: Arc::new(tokio::sync::Semaphore::new(args.io_concurrency.max(1))),
        sync_pool: pyrt::SyncPool::start(pyrt.clone(), args.io_threads),
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
