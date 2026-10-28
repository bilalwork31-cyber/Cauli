//! A redis that accepts TCP but never answers must not hang a
//! ConnectionManager call forever, and the connection recovers once it thaws.
//!
//! Uses its own redis-server: freezing the shared :6392 instance with SIGSTOP
//! would stall every other e2e test.

mod common;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const PORT: u16 = 6390;

fn url() -> String {
    format!("redis://127.0.0.1:{PORT}/0")
}

/// A redis-server this test owns, frozen with SIGSTOP to simulate "alive
/// socket, dead application": no reset, no prompt error.
struct FrozenRedis(Child);

impl FrozenRedis {
    fn start() -> Self {
        let _ = Command::new("redis-cli")
            .args(["-p", &PORT.to_string(), "shutdown", "nosave"])
            .output();
        std::thread::sleep(Duration::from_millis(200));
        let child = Command::new("redis-server")
            .args([
                "--port",
                &PORT.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("redis-server spawn");
        let this = FrozenRedis(child);
        for _ in 0..50 {
            let ping = Command::new("redis-cli")
                .args(["-p", &PORT.to_string(), "ping"])
                .output();
            if ping
                .map(|o| String::from_utf8_lossy(&o.stdout).contains("PONG"))
                .unwrap_or(false)
            {
                return this;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("redis on {PORT} did not answer PING");
    }

    fn freeze(&self) {
        // SAFETY: kill(2) with a signal number and this process's own known
        // child pid is always a valid, safe call.
        unsafe { libc::kill(self.0.id() as i32, libc::SIGSTOP) };
    }

    fn thaw(&self) {
        unsafe { libc::kill(self.0.id() as i32, libc::SIGCONT) };
    }
}

impl Drop for FrozenRedis {
    fn drop(&mut self) {
        // Thaw first so a test that panicked while frozen still gets a plain reap.
        self.thaw();
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// With response_timeout set, a call against a frozen redis returns an
/// reconnect worthy IoError within the bound, and the same manager recovers
/// after thaw.
#[tokio::test(flavor = "multi_thread")]
async fn frozen_redis_times_out_and_recovers_configured() {
    let redis_proc = FrozenRedis::start();
    let client = redis::Client::open(url()).unwrap();

    // Connected while healthy: an existing connection going silent, not a cold connect.
    let configured = ConnectionManager::new_with_config(
        client,
        ConnectionManagerConfig::new()
            .set_response_timeout(Some(Duration::from_millis(700)))
            .set_connection_timeout(Some(Duration::from_millis(700))),
    )
    .await
    .unwrap();

    redis_proc.freeze();

    // response_timeout 700ms: the call errors well inside the 3s outer bound.
    let mut c = configured.clone();
    let t0 = Instant::now();
    let result: Result<redis::RedisResult<String>, _> =
        tokio::time::timeout(Duration::from_secs(3), async move {
            redis::cmd("PING").query_async(&mut c).await
        })
        .await;
    let elapsed = t0.elapsed();
    let err = result
        .expect("must return within the outer 3s bound, not hang")
        .expect_err("must be an Err: redis is still frozen");
    assert!(
        elapsed < Duration::from_secs(2),
        "response_timeout=700ms should fire well under the 3s outer bound, took {elapsed:?}"
    );
    assert!(
        err.is_io_error(),
        "a response_timeout elapsing must convert to ErrorKind::IoError: \
         the exact condition reconnect_if_io_error! checks before \
         reconnecting. Got {err:?}"
    );

    // Thaw: the same manager serves again, so reconnect actually fired.
    redis_proc.thaw();
    let mut c = configured.clone();
    let recovered: String = tokio::time::timeout(Duration::from_secs(5), async move {
        redis::cmd("PING").query_async(&mut c).await
    })
    .await
    .expect("must not hang once redis has thawed")
    .expect("must succeed: reconnect_if_io_error! must have recovered the connection");
    assert_eq!(recovered, "PONG");
}

/// The real binary survives its redis going silent and resumes fetching:
/// --redis-timeout reaches both connections.
#[tokio::test(flavor = "multi_thread")]
async fn worker_binary_survives_a_frozen_redis() {
    const WORKER_PORT: u16 = 6393;
    let _ = Command::new("redis-cli")
        .args(["-p", &WORKER_PORT.to_string(), "shutdown", "nosave"])
        .output();
    std::thread::sleep(Duration::from_millis(200));
    let redis_child = Command::new("redis-server")
        .args([
            "--port",
            &WORKER_PORT.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("redis-server spawn");

    struct RedisGuard(Child);
    impl Drop for RedisGuard {
        fn drop(&mut self) {
            unsafe { libc::kill(self.0.id() as i32, libc::SIGCONT) };
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let redis_guard = RedisGuard(redis_child);
    for _ in 0..50 {
        let ping = Command::new("redis-cli")
            .args(["-p", &WORKER_PORT.to_string(), "ping"])
            .output();
        if ping
            .map(|o| String::from_utf8_lossy(&o.stdout).contains("PONG"))
            .unwrap_or(false)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let worker_url = format!("redis://127.0.0.1:{WORKER_PORT}/0");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cauli-worker"));
    cmd.current_dir(common::fixtures_dir())
        .args(["--app", "fixture_app:app", "--queues", "default"])
        .args(["--redis-url", &worker_url])
        // Short to keep the test fast, above the ~1s jitter floor in --help.
        .args(["--redis-timeout", "2"])
        .args(["--io-threads", "4", "--io-concurrency", "8"])
        .args(["--cpu-workers", "1", "--stats-interval", "1"])
        .args(["--log-level", "debug"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let worker_child = cmd.spawn().expect("worker spawn");

    struct WorkerGuard(Child);
    impl Drop for WorkerGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut worker = WorkerGuard(worker_child);

    let mut c = redis::Client::open(worker_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    common::wait_group(&mut c, "default", 20).await;

    // Baseline: the worker is healthy before the outage.
    let (id, e) = common::envelope("fx.echo", "default", |v| {
        v["args"] = serde_json::json!(["before"]);
    });
    common::xadd(&mut c, "default", &e.to_string()).await;
    let r = common::wait_result(&mut c, &id, 15).await;
    assert_eq!(r["status"], "success");

    // Frozen longer than --redis-timeout, spanning a fetch poll.
    unsafe { libc::kill(redis_guard.0.id() as i32, libc::SIGSTOP) };
    tokio::time::sleep(Duration::from_secs(4)).await;
    unsafe { libc::kill(redis_guard.0.id() as i32, libc::SIGCONT) };

    // The worker survived: no panic, no exit.
    assert!(
        worker.0.try_wait().unwrap().is_none(),
        "worker must still be running after a redis outage it can recover from"
    );

    // Recovery: it must fetch and complete new work again.
    let (id, e) = common::envelope("fx.echo", "default", |v| {
        v["args"] = serde_json::json!(["after"]);
    });
    common::xadd(&mut c, "default", &e.to_string()).await;
    let r = common::wait_result(&mut c, &id, 20).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"]["args"], serde_json::json!(["after"]));
}
