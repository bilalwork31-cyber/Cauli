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
    assert_eq!(
        r["status"], "success",
        "in flight task must finish during drain"
    );
    assert_eq!(r["result"], "slow-done");
    let pend: i64 = xpending_count(&mut c, "drainq").await;
    assert_eq!(pend, 0, "drained worker must leave nothing pending");
    drop(wd);

    // --- SIGKILL recovery: second worker claims after visibility timeout ---
    let mut w1 = Worker::spawn("killq", &["--visibility-timeout", "60"]);
    wait_group(&mut c, "killq", 20).await;
    // H1: the recovery loop's reclaim threshold is max(visibility_timeout,
    // envelope.timeout_ms + grace), so this envelope needs an explicit low
    // timeout_ms (default is 300_000ms) for reclaim to happen quickly here.
    let (id, e) = envelope("fx.slow", "killq", |v| {
        v["args"] = json!([3.0]);
        v["timeout_ms"] = json!(4000);
    });
    xadd(&mut c, "killq", &e.to_string()).await;
    wait_inflight(&mut c, "killq", 10).await; // w1 started executing
    w1.signal(libc::SIGKILL);
    let _ = w1.wait_code(10); // reaped (signal death -> code -1 is fine)
                              // entry is still pending for w1's consumer
    assert_eq!(xpending_count(&mut c, "killq").await, 1);

    let w2 = Worker::spawn("killq", &["--visibility-timeout", "2"]);
    // w2's recovery loop (period vt/2 = 1s) claims once idle exceeds
    // max(2000, 4000+2000) = 6000ms, then re-executes (3s task).
    let r = wait_result(&mut c, &id, 25).await;
    assert_eq!(
        r["status"], "success",
        "claimed task must run to completion"
    );
    assert_eq!(r["result"], "slow-done");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        xpending_count(&mut c, "killq").await,
        0,
        "claimed entry must be acked"
    );
    drop(w2);
    drop(w1);

    h1_visibility_floor_does_not_reclaim_long_task(&mut c).await;
    h2_sync_pool_survives_hard_timeout_abandonment(&mut c).await;
    m8_cli_floors_reject_zero();
    mem1_async_backstop_fires_cleanly(&mut c).await;

    stop_redis();
}

/// MEM-1 regression: when an async task's event-loop thread is wedged by a
/// synchronous blocking call (so Python's own `asyncio.wait_for` timeout can
/// never fire), the Rust-side backstop (timeout_ms + grace) must still
/// resolve the task cleanly as a TimeoutError. This is specifically the path
/// that calls `PyRuntime::cancel` to drop the pending-completion map entry;
/// without it, each such backstop firing used to leak that entry (and the
/// coroutine/args/kwargs it references) forever.
async fn mem1_async_backstop_fires_cleanly(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("mem1q", &["--io-loops", "1"]);
    wait_group(c, "mem1q", 20).await;

    let (id, e) = envelope("fx.async_block", "mem1q", |v| {
        v["args"] = json!([3.0]); // sleeps well past the 2.2s backstop below
        v["timeout_ms"] = json!(200);
        v["max_retries"] = json!(0);
    });
    xadd(c, "mem1q", &e.to_string()).await;
    let r = wait_result(c, &id, 8).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeoutError");
    assert!(
        r["error"]["message"].as_str().unwrap().contains("grace"),
        "must be the Rust-side backstop firing, not a python-side wait_for timeout"
    );

    // dispatch already returned once the backstop fired (well before the 3s
    // sleep completes), so shutdown should not need to wait for it.
    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(15), 0);
    drop(w);
}

/// M8 regression: `--batch 0` and `--visibility-timeout 0` must be rejected
/// at startup (exit 1) rather than accepted -- 0 would mean "unlimited"
/// XREADGROUP fetch, or a recovery loop that reclaims every in-flight task on
/// nearly every tick.
fn m8_cli_floors_reject_zero() {
    let mut bad_batch = Worker::spawn("m8q", &["--batch", "0"]);
    assert_eq!(bad_batch.wait_code(15), 1, "--batch 0 must be rejected");
    drop(bad_batch);

    let mut bad_vt = Worker::spawn("m8q", &["--visibility-timeout", "0"]);
    assert_eq!(
        bad_vt.wait_code(15),
        1,
        "--visibility-timeout 0 must be rejected"
    );
    drop(bad_vt);
}

/// H1 regression: a task sleeping well past the visibility_timeout floor,
/// with a (default, large) timeout_ms, must NOT be reclaimed and re-executed
/// concurrently. Uses a low --visibility-timeout (1s) specifically so the
/// recovery loop's XPENDING IDLE floor is satisfied quickly and repeatedly
/// while the task is still legitimately running for several seconds; without
/// the per-envelope idle check this would XCLAIM and re-dispatch the same
/// entry a second time.
async fn h1_visibility_floor_does_not_reclaim_long_task(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("h1q", &["--visibility-timeout", "1"]);
    wait_group(c, "h1q", 20).await;

    let cf = format!("/tmp/cauli-h1-{}", unique_id());
    let (id, e) = envelope("fx.slow_counted", "h1q", |v| v["args"] = json!([cf, 4.0]));
    xadd(c, "h1q", &e.to_string()).await;
    wait_inflight(c, "h1q", 10).await;

    // Give the recovery loop (period = max(vt/2, 500ms) = 500ms here) several
    // chances to (wrongly, pre-fix) reclaim: well past the 1s visibility
    // floor, well short of the 4s sleep or the default 300s task timeout.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"], 1, "task must have executed exactly once");
    let contents = std::fs::read_to_string(&cf).unwrap();
    assert_eq!(
        contents.trim(),
        "1",
        "counter file must show exactly one invocation"
    );
    let _ = std::fs::remove_file(&cf);

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(15), 0);
    drop(w);
}

/// H2 regression: after a sync-pool hard-timeout abandonment, the pool must
/// still execute NEW tasks at full capacity (a replacement thread is spawned
/// immediately) rather than waiting for the wedged original thread to free
/// up. Uses --io-threads 1 so the pool's sole thread is deterministically
/// occupied by the abandoned task for its full sleep duration.
async fn h2_sync_pool_survives_hard_timeout_abandonment(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("h2q", &["--io-threads", "1"]);
    wait_group(c, "h2q", 20).await;

    // Task A: sleeps 3s but hard-times-out at 300ms, so the dispatcher gives
    // up quickly while the sole pool thread keeps sleeping in the background
    // (wedged from the pool's point of view for ~3s).
    let (id_a, e_a) = envelope("fx.slow", "h2q", |v| {
        v["args"] = json!([3.0]);
        v["timeout_ms"] = json!(300);
        v["max_retries"] = json!(0);
    });
    xadd(c, "h2q", &e_a.to_string()).await;
    let r_a = wait_result(c, &id_a, 5).await;
    assert_eq!(r_a["status"], "failure");
    assert_eq!(r_a["error"]["type"], "TimeoutError");

    // Task B: a fast task submitted right after. With the sole original
    // thread still wedged in A's sleep, this only completes quickly if a
    // replacement thread was spawned when A's hard timeout fired.
    let (id_b, e_b) = envelope("fx.echo", "h2q", |v| {
        v["args"] = json!(["capacity-restored"])
    });
    xadd(c, "h2q", &e_b.to_string()).await;
    let r_b = wait_result(c, &id_b, 2).await;
    assert_eq!(
        r_b["status"], "success",
        "pool must serve new tasks at full capacity after an abandonment"
    );

    // A's underlying sleep finishes in the background; its already-recorded
    // failure result must not be disturbed when that happens.
    tokio::time::sleep(std::time::Duration::from_millis(3200)).await;
    let a_final: String = redis::cmd("GET")
        .arg(format!("cauli:result:{id_a}"))
        .query_async(c)
        .await
        .unwrap();
    let a_final: serde_json::Value = serde_json::from_str(&a_final).unwrap();
    assert_eq!(
        a_final["status"], "failure",
        "abandoned job's late completion must not overwrite its result"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(15), 0);
    drop(w);
}

async fn xpending_count(c: &mut redis::aio::MultiplexedConnection, queue: &str) -> i64 {
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(format!("cauli:q:{queue}"))
        .arg("cauli")
        .query_async(c)
        .await
        .unwrap();
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
