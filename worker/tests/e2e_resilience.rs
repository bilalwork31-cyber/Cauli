//! Two operational events that used to leave a worker alive and useless.
//!
//! 1. A wedged asyncio loop (a blocking call inside an `async def`) ends every
//!    async task in the process for as long as it lives, with every field in
//!    the stats line reading normal. The worker must notice and exit so a
//!    supervisor restarts it, and must NOT exit for a loop that is merely
//!    slow.
//! 2. A redis that comes back EMPTY loses the consumer group, and NOGROUP used
//!    to be a generic warning retried every 500ms forever. The worker must
//!    recreate the group, say what was lost, and resume.

mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Its own broker: this one gets flushed out from under the worker.
const RESET_PORT: u16 = 6422;
/// Shared by the two wedge tests, which use separate queues and separate
/// worker processes and never write to each other's keys.
const WEDGE_PORT: u16 = 6423;

/// `loops::WEDGE_EXIT_CODE`.
const WEDGE_EXIT_CODE: i32 = 87;

fn url(port: u16) -> String {
    format!("redis://127.0.0.1:{port}/0")
}

fn redis_cli(port: u16, args: &[&str]) -> String {
    let out = Command::new("redis-cli")
        .arg("-p")
        .arg(port.to_string())
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn start_redis(port: u16) {
    let _ = redis_cli(port, &["shutdown", "nosave"]);
    std::thread::sleep(Duration::from_millis(200));
    let out = Command::new("redis-server")
        .args([
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
            "--daemonize",
            "yes",
        ])
        .output()
        .expect("redis-server spawn");
    assert!(out.status.success(), "redis-server failed: {out:?}");
    for _ in 0..50 {
        if redis_cli(port, &["ping"]).contains("PONG") {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("redis on {port} did not answer PING");
}

/// One shared instance for the wedge tests, whichever gets there first.
fn wedge_redis() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| start_redis(WEDGE_PORT));
}

struct Worker(Child);

impl Worker {
    /// The e2e fixture app on `port`, with its stdout (where tracing writes)
    /// captured to `log`.
    fn spawn(port: u16, queues: &str, extra: &[&str], log: &Path) -> Worker {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_cauli-worker"));
        cmd.current_dir(common::fixtures_dir())
            .args(["--app", "fixture_app:app", "--queues", queues])
            .args(["--redis-url", &url(port)])
            .args(["--io-threads", "4", "--stats-interval", "2"])
            .args(["--log-level", "info"])
            .args(extra)
            .stdout(std::fs::File::create(log).expect("worker log"))
            .stderr(Stdio::inherit());
        Worker(cmd.spawn().expect("worker spawn"))
    }

    fn exit_code_within(&mut self, secs: u64) -> Option<i32> {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if let Ok(Some(st)) = self.0.try_wait() {
                return Some(st.code().unwrap_or(-1));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        None
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.0.try_wait(), Ok(None))
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn log_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("cauli-{name}-{}.log", std::process::id()))
}

fn log_contains(path: &Path, needle: &str) -> bool {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .contains(needle)
}

async fn enqueue(
    conn: &mut redis::aio::MultiplexedConnection,
    queue: &str,
    task: &str,
    seconds: f64,
) -> String {
    let (id, env) = common::envelope(task, queue, |v| {
        v["kwargs"] = serde_json::json!({ "seconds": seconds });
    });
    common::xadd(conn, queue, &env.to_string()).await;
    id
}

async fn conn(port: u16) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url(port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// The wedge itself. `fx.async_block` sleeps synchronously inside an `async
/// def`, so with `--io-loops 1` the first one owns the only loop thread for
/// its whole duration and the two behind it are Tasks the loop will never
/// start, let alone complete. Nothing in the process can take that thread
/// back, so the worker has to exit and let its supervisor restart it.
#[tokio::test]
async fn a_wedged_async_loop_exits_the_process() {
    wedge_redis();
    let log = log_path("wedge");
    let mut c = conn(WEDGE_PORT).await;
    let mut w = Worker::spawn(
        WEDGE_PORT,
        "wedgeq",
        &["--io-loops", "1", "--io-concurrency", "8"],
        &log,
    );
    for _ in 0..3 {
        enqueue(&mut c, "wedgeq", "fx.async_block", 600.0).await;
    }

    let code = w.exit_code_within(90);
    assert_eq!(
        code,
        Some(WEDGE_EXIT_CODE),
        "a wedged loop must exit with its own code so a supervisor restarts it \
         and the reason is legible; log: {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    assert!(
        log_contains(&log, "wedged async event loop confirmed"),
        "the exit must be announced with its own message, not just a code"
    );
    let _ = std::fs::remove_file(&log);
}

/// The other half of the property, and the reason the verdict needs two
/// signals: the same blocking call, bounded. Eight one second blocks hold the
/// only loop thread for longer than the whole detection window put together,
/// but a task completes between each of them, so this is a slow worker and
/// not a dead one. It must still be running, and it must still be reporting
/// the lag it measured.
#[tokio::test]
async fn a_slow_async_loop_keeps_running() {
    wedge_redis();
    let log = log_path("slow");
    let mut c = conn(WEDGE_PORT).await;
    let mut w = Worker::spawn(
        WEDGE_PORT,
        "slowq",
        &["--io-loops", "1", "--io-concurrency", "8"],
        &log,
    );
    let mut ids = Vec::new();
    for _ in 0..8 {
        ids.push(enqueue(&mut c, "slowq", "fx.async_block", 1.0).await);
    }
    for id in &ids {
        common::wait_result(&mut c, id, 90).await;
    }
    assert!(
        w.is_alive(),
        "a loop that blocks in bounded chunks is slow, not wedged; log: {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    assert!(
        common::wait_stats_field_nonzero(&log, "loop_lag_ms=", 20)
            .await
            .is_some(),
        "the measured loop lag must reach the stats line: it is the only field \
         that moves when the async lane is starved"
    );
    let _ = std::fs::remove_file(&log);
}

/// A redis restarted with no persistence (the ElastiCache default), evicted,
/// restored from backup or simply FLUSHALLed: the stream and the consumer
/// group are gone while the connection stays up. XREADGROUP then answers
/// NOGROUP forever, and the worker used to log that as an ordinary retry.
#[tokio::test]
async fn an_emptied_redis_is_named_and_recovered_from() {
    start_redis(RESET_PORT);
    let log = log_path("reset");
    let mut c = conn(RESET_PORT).await;
    let mut w = Worker::spawn(RESET_PORT, "resetq", &["--io-concurrency", "8"], &log);

    let before = enqueue(&mut c, "resetq", "fx.aecho", 0.0).await;
    common::wait_result(&mut c, &before, 60).await;

    // The event: everything the group knew is gone, the socket is not.
    assert_eq!(redis_cli(RESET_PORT, &["flushall"]), "OK");

    let after = enqueue(&mut c, "resetq", "fx.aecho", 0.0).await;
    common::wait_result(&mut c, &after, 60).await;
    assert!(
        w.is_alive(),
        "the worker self heals rather than dying: the stream is where new work \
         keeps arriving"
    );
    assert!(
        log_contains(&log, "redis has no consumer group"),
        "an emptied broker must be distinguishable from a connection blip; log: {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    let _ = std::fs::remove_file(&log);
    let _ = redis_cli(RESET_PORT, &["shutdown", "nosave"]);
}
