//! Shared helpers for the e2e suites (throwaway redis on 6392, worker spawn,
//! envelope building, polling assertions).
#![allow(dead_code)]

use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub const PORT: u16 = 6392;

pub fn redis_url() -> String {
    format!("redis://127.0.0.1:{PORT}/0")
}

pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

pub fn start_redis() {
    // kill any leftover instance, then start a fresh throwaway one
    let _ = Command::new("redis-cli")
        .args(["-p", &PORT.to_string(), "shutdown", "nosave"])
        .output();
    std::thread::sleep(Duration::from_millis(200));
    let out = Command::new("redis-server")
        .args([
            "--port",
            &PORT.to_string(),
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
        let ping = Command::new("redis-cli")
            .args(["-p", &PORT.to_string(), "ping"])
            .output();
        if ping
            .map(|o| String::from_utf8_lossy(&o.stdout).contains("PONG"))
            .unwrap_or(false)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("redis on {PORT} did not answer PING");
}

pub fn stop_redis() {
    let _ = Command::new("redis-cli")
        .args(["-p", &PORT.to_string(), "shutdown", "nosave"])
        .output();
}

pub async fn conn() -> redis::aio::MultiplexedConnection {
    redis::Client::open(redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

pub struct Worker(pub Child);

impl Worker {
    pub fn spawn(queues: &str, extra: &[&str]) -> Worker {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rupy-worker"));
        cmd.current_dir(fixtures_dir())
            .args(["--app", "fixture_app:app", "--queues", queues])
            .args(["--redis-url", &redis_url()]);
        // clap rejects a scalar flag passed twice ("cannot be used multiple
        // times"), so only add each default when `extra` doesn't already
        // specify it -- this lets a test override e.g. --io-threads via
        // `extra` without colliding with the default below.
        let has = |flag: &str| extra.contains(&flag);
        if !has("--io-threads") {
            cmd.args(["--io-threads", "8"]);
        }
        if !has("--io-concurrency") {
            cmd.args(["--io-concurrency", "32"]);
        }
        if !has("--cpu-workers") {
            cmd.args(["--cpu-workers", "2"]);
        }
        if !has("--stats-interval") {
            cmd.args(["--stats-interval", "5"]);
        }
        if !has("--log-level") {
            cmd.args(["--log-level", "debug"]);
        }
        cmd.args(extra)
            .env(
                "RUPY_EXEC_CMD",
                format!("python3 {}", fixtures_dir().join("fake_exec.py").display()),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        Worker(cmd.spawn().expect("worker spawn"))
    }

    pub fn signal(&self, sig: i32) {
        unsafe {
            libc::kill(self.0.id() as i32, sig);
        }
    }

    /// Wait for exit up to `secs`; returns exit code.
    pub fn wait_code(&mut self, secs: u64) -> i32 {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if let Ok(Some(st)) = self.0.try_wait() {
                return st.code().unwrap_or(-1);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("worker did not exit within {secs}s");
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

static SEQ: AtomicU64 = AtomicU64::new(1);

/// 32-char lowercase hex id, unique per test run.
pub fn unique_id() -> String {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{:016x}{:016x}",
        t as u64,
        (std::process::id() as u64) << 32 | n
    )
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Full §2 envelope with sane defaults; mutate via `patch`.
pub fn envelope(task: &str, queue: &str, patch: impl FnOnce(&mut Value)) -> (String, Value) {
    let id = unique_id();
    let mut v = json!({
        "v": 1, "id": id, "task": task, "args": [], "kwargs": {},
        "queue": queue, "kind": "io", "retries": 0, "max_retries": 3,
        "backoff_base_ms": 500, "backoff_factor": 2.0, "backoff_max_ms": 60000,
        "jitter": true, "timeout_ms": 300000, "soft_timeout_ms": null,
        "idempotency_key": null, "store_result": true,
        "enqueued_at": now_ms(), "not_before": null
    });
    patch(&mut v);
    (id, v)
}

pub async fn xadd(c: &mut redis::aio::MultiplexedConnection, queue: &str, payload: &str) {
    let _: String = redis::cmd("XADD")
        .arg(format!("rupy:q:{queue}"))
        .arg("*")
        .arg("e")
        .arg(payload)
        .query_async(c)
        .await
        .unwrap();
}

/// Poll rupy:result:{id} until it exists (or timeout) and parse it.
pub async fn wait_result(c: &mut redis::aio::MultiplexedConnection, id: &str, secs: u64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let r: Option<String> = redis::cmd("GET")
            .arg(format!("rupy:result:{id}"))
            .query_async(c)
            .await
            .unwrap();
        if let Some(s) = r {
            return serde_json::from_str(&s).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no result for {id} within {secs}s");
}

/// Poll the DLQ stream for an entry whose envelope/raw `e` contains `needle`.
/// Returns (reason, error_string, e_field).
pub async fn wait_dlq(
    c: &mut redis::aio::MultiplexedConnection,
    queue: &str,
    needle: &str,
    secs: u64,
) -> (String, String, String) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
            .arg(format!("rupy:dlq:{queue}"))
            .arg("-")
            .arg("+")
            .query_async(c)
            .await
            .unwrap();
        for (_sid, kv) in &entries {
            let get = |k: &str| {
                kv.chunks(2)
                    .find(|c| c[0] == k)
                    .map(|c| c[1].clone())
                    .unwrap_or_default()
            };
            let e = get("e");
            if e.contains(needle) {
                return (get("reason"), get("error"), e);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no DLQ entry containing {needle:?} in {queue} within {secs}s");
}

/// Wait until the consumer group exists on the queue (worker startup barrier).
pub async fn wait_group(c: &mut redis::aio::MultiplexedConnection, queue: &str, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        let r: redis::RedisResult<redis::Value> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(format!("rupy:q:{queue}"))
            .query_async(c)
            .await;
        if r.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("consumer group on {queue} never appeared");
}
