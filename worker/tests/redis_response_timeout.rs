//! Regression test for the "unbounded redis wait" bug (audit cycle 8, D2
//! follow up): a redis that accepts the TCP connection but never answers
//! must not hang a ConnectionManager call forever, and once it thaws the
//! connection must recover on its own, with no restart.
//!
//! Both tests use a dedicated throwaway redis-server on a port this file
//! fully owns (never the shared :6392 instance in worker/tests/common,
//! never itest's/bench's :6391/6394/6395, and never 6379). Freezing the
//! shared :6392 instance with SIGSTOP would stall every other e2e test
//! running concurrently against it; see HANDOFF.md's port table.

mod common;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const PORT: u16 = 6390;

fn url() -> String {
    format!("redis://127.0.0.1:{PORT}/0")
}

/// A throwaway redis-server this test fully owns, freezable with
/// SIGSTOP/SIGCONT to simulate "alive socket, dead application". Unlike a
/// killed server, a frozen one never resets the connection or errors
/// promptly. That is the exact case an unset response_timeout cannot survive.
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
        // Thaw before killing: SIGKILL does terminate a stopped process on
        // Linux without a prior SIGCONT, but thawing first keeps this a
        // plain reap rather than relying on that if a test above panicked
        // while redis was still frozen.
        self.thaw();
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The bug and the fix, proven back to back against the exact types
/// main.rs constructs. `ConnectionManager::new` (no config: what main.rs
/// called before this fix, and still the crate's own default when nobody
/// sets a response_timeout) hangs a call against a frozen redis well past
/// any real operation's latency, proof of the indefinite wait the audit
/// found. `ConnectionManager::new_with_config` with response_timeout and
/// connection_timeout set (what main.rs calls now) instead returns an Err,
/// inside the configured bound, and that Err carries the IoError kind the
/// crate's own reconnect_if_io_error! macro already treats as reconnect
/// worthy, so once redis thaws, the SAME manager recovers on its own.
#[tokio::test(flavor = "multi_thread")]
async fn frozen_redis_hangs_unconfigured_times_out_and_recovers_configured() {
    let redis_proc = FrozenRedis::start();
    let client = redis::Client::open(url()).unwrap();

    // Both managers connect while redis is still healthy: this test targets
    // an existing connection going silent mid steady state (the fetch loop,
    // idemp_claim, the mover, recovery), not a cold connect.
    let unconfigured = ConnectionManager::new(client.clone()).await.unwrap();
    let configured = ConnectionManager::new_with_config(
        client,
        ConnectionManagerConfig::new()
            .set_response_timeout(Duration::from_millis(700))
            .set_connection_timeout(Duration::from_millis(700)),
    )
    .await
    .unwrap();

    redis_proc.freeze();

    // Before: no response_timeout configured, so PING against a frozen
    // redis must not resolve at all within a bound that is ~600x a healthy
    // PING's real latency (low single digit milliseconds). An indefinite
    // wait cannot be literally waited out in a test; this generous bound is
    // the accepted stand in (the same reasoning the audit's own 30s/35s D2
    // measurement used).
    let mut c = unconfigured.clone();
    let hung = tokio::time::timeout(Duration::from_secs(3), async move {
        let _: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut c).await;
    })
    .await;
    assert!(
        hung.is_err(),
        "PING against a frozen redis must not resolve at all with no \
         response_timeout configured. This is the bug"
    );

    // After: response_timeout=700ms configured, so the SAME kind of call
    // against the SAME frozen redis returns an Err, bounded, well inside
    // the 3s outer bound.
    let mut c = configured.clone();
    let t0 = Instant::now();
    let result: Result<redis::RedisResult<String>, _> =
        tokio::time::timeout(Duration::from_secs(3), async move {
            redis::cmd("PING").query_async(&mut c).await
        })
        .await;
    let elapsed = t0.elapsed();
    let err = result
        .expect("must return within the outer 3s bound, not hang like the unconfigured case")
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

    // Recovery: thaw, and the SAME manager (no new connection, no restart)
    // must serve requests again: proof the previously dormant reconnect
    // path actually fired rather than merely being eligible to.
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

/// System level companion to the test above: the real cauli-worker binary,
/// talking to a redis it owns, must survive that redis going silent mid run
/// and keep fetching once it comes back. Proves the --redis-timeout wiring
/// in main.rs (args.redis_timeout -> ConnectionManagerConfig, used for both
/// the write connection and the dedicated fetch connection) is actually
/// connected, not just that the underlying ConnectionManager mechanism
/// works in isolation.
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
        // Short on purpose: keeps the outage window (and this test) fast.
        // Still comfortably above the "ordinary jitter" floor the flag's
        // help text documents (~1s).
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

    // Outage: redis goes silent (frozen, not killed) for longer than
    // --redis-timeout, spanning at least one fetch loop poll.
    unsafe { libc::kill(redis_guard.0.id() as i32, libc::SIGSTOP) };
    tokio::time::sleep(Duration::from_secs(4)).await;
    unsafe { libc::kill(redis_guard.0.id() as i32, libc::SIGCONT) };

    // The worker process itself must have survived the outage: no panic, no
    // exit, unlike what an unbounded wait risks under load elsewhere (see
    // the drain-timeout finding this fix does not touch).
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
