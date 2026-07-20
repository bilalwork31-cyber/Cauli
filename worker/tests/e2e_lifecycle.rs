//! Lifecycle e2e: SIGTERM graceful drain (§4.7) and SIGKILL crash recovery
//! via XPENDING/XCLAIM (§4.4). Separate binary so it owns its redis instance
//! (cargo runs test binaries sequentially).
mod common;
use common::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn e2e_sigterm_drain_and_sigkill_recovery() {
    start_redis();
    let mut c = conn().await;
    let _: String = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();

    // --- SIGTERM drain: in flight task finishes, exit code 0 ---
    let mut wd = Worker::spawn("drainq", &["--drain-timeout", "15"]);
    wait_group(&mut c, "drainq", 20).await;
    let (id, e) = envelope("fx.slow", "drainq", |v| v["args"] = json!([1.5]));
    xadd(&mut c, "drainq", &e.to_string()).await;
    // let the worker pick it up, then signal mid-task
    wait_inflight(&mut c, "drainq", 10).await;
    wd.signal(libc::SIGTERM);
    let code = wd.wait_code(20);
    assert_eq!(code, 0, "graceful drain must exit 0");
    let r = wait_result(&mut c, &id, 5).await;
    assert_eq!(r["status"], "success", "in flight task must finish during drain");
    assert_eq!(r["result"], "slow-done");
    let pend: i64 = xpending_count(&mut c, "drainq").await;
    assert_eq!(pend, 0, "drained worker must leave nothing pending");
    drop(wd);

    // --- SIGKILL recovery: second worker claims after visibility timeout ---
    let mut w1 = Worker::spawn("killq", &["--visibility-timeout", "60"]);
    wait_group(&mut c, "killq", 20).await;
    let (id, e) = envelope("fx.slow", "killq", |v| v["args"] = json!([3.0]));
    xadd(&mut c, "killq", &e.to_string()).await;
    wait_inflight(&mut c, "killq", 10).await; // w1 started executing
    w1.signal(libc::SIGKILL);
    let _ = w1.wait_code(10); // reaped (signal death -> code -1 is fine)
    // entry is still pending for w1's consumer
    assert_eq!(xpending_count(&mut c, "killq").await, 1);

    let w2 = Worker::spawn("killq", &["--visibility-timeout", "2"]);
    // w2's recovery loop (period vt/2 = 1s) claims after idle > 2s, re-executes
    let r = wait_result(&mut c, &id, 25).await;
    assert_eq!(r["status"], "success", "claimed task must run to completion");
    assert_eq!(r["result"], "slow-done");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(xpending_count(&mut c, "killq").await, 0, "claimed entry must be acked");
    drop(w2);
    drop(w1);
    stop_redis();
}

async fn xpending_count(c: &mut redis::aio::MultiplexedConnection, queue: &str) -> i64 {
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(format!("rupy:q:{queue}")).arg("rupy")
        .query_async(c).await.unwrap();
    // summary form: [count, min, max, consumers]
    match v {
        redis::Value::Array(items) => match items.first() {
            Some(n) => redis::from_redis_value::<i64>(n).unwrap_or(0),
            None => 0,
        },
        _ => 0,
    }
}

/// Wait until the queue has at least one pending (delivered) entry.
async fn wait_inflight(c: &mut redis::aio::MultiplexedConnection, queue: &str, secs: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if xpending_count(c, queue).await > 0 {
            // give the executor a beat to actually enter the task
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("task never became pending on {queue}");
}
