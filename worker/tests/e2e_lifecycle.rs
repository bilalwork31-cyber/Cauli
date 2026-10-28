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
    // Reclaim needs idle > max(visibility_timeout, timeout_ms + grace), so a
    // low timeout_ms keeps this fast.
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

    back_to_back_stop_signals_still_drain(&mut c).await;
    kill9_mid_ack_flush_loses_nothing(&mut c).await;
    h1_visibility_floor_does_not_reclaim_long_task(&mut c).await;
    h2_sync_pool_survives_hard_timeout_abandonment(&mut c).await;
    m8_cli_floors_reject_zero();
    mem1_async_backstop_fires_cleanly(&mut c).await;
    bulk_recovery_drains_backlog_per_tick(&mut c).await;

    stop_redis();
}

/// One stop delivered as two signals (systemd signals worker and supervisor;
/// a SIGKILLed supervisor sends PDEATHSIG per thread) must still drain, not 130.
async fn back_to_back_stop_signals_still_drain(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("dblsigq", &["--drain-timeout", "15"]);
    wait_group(c, "dblsigq", 20).await;
    let (id, e) = envelope("fx.slow", "dblsigq", |v| v["args"] = json!([1.5]));
    xadd(c, "dblsigq", &e.to_string()).await;
    wait_inflight(c, "dblsigq", 10).await;
    w.signal(libc::SIGTERM);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    w.signal(libc::SIGTERM);
    assert_eq!(
        w.wait_code(20),
        0,
        "a repeated stop signal must not force exit 130"
    );
    let r = wait_result(c, &id, 5).await;
    assert_eq!(
        r["status"], "success",
        "in flight task must finish during drain"
    );
    drop(w);
}

/// §4.1: a `kill -9` while completions sit unflushed in the ack buffer must
/// lose no task. w1's huge flush window keeps everything unacked; every entry
/// must be redelivered (§4.4) and finish on w2.
async fn kill9_mid_ack_flush_loses_nothing(c: &mut redis::aio::MultiplexedConnection) {
    const N: usize = 40;
    let mut w1 = Worker::spawn(
        "ackflushq",
        &[
            "--visibility-timeout",
            "2",
            "--ack-flush-ms",
            "8000",
            "--ack-batch",
            "100000",
        ],
    );
    wait_group(c, "ackflushq", 20).await;
    let mut ids = Vec::with_capacity(N);
    for i in 0..N {
        let (id, e) = envelope("fx.echo", "ackflushq", |v| {
            v["args"] = json!([format!("survivor-{i}")]);
            v["timeout_ms"] = json!(1000);
        });
        xadd(c, "ackflushq", &e.to_string()).await;
        ids.push(id);
    }
    // Give w1 time to fetch and EXECUTE everything; with the 8s window
    // nothing may have flushed: every entry still pending, no result key.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert_eq!(
        xpending_count(c, "ackflushq").await as usize,
        N,
        "with an 8s flush window nothing may be acked yet"
    );
    let first: Option<String> = redis::cmd("GET")
        .arg(format!("cauli:result:{}", ids[0]))
        .query_async(c)
        .await
        .unwrap();
    assert!(
        first.is_none(),
        "results ride the flush pipeline: none may exist before the flush"
    );
    w1.signal(libc::SIGKILL);
    let _ = w1.wait_code(10);

    // --redis-timeout 1 shrinks the reclaim margin so redelivery happens in
    // seconds here: required idle = 1000 (timeout_ms) + 2000 (grace) +
    // max(1000, 2000) = 5s, recovery tick 1s.
    let w2 = Worker::spawn(
        "ackflushq",
        &["--visibility-timeout", "2", "--redis-timeout", "1"],
    );
    for id in &ids {
        let r = wait_result(c, id, 30).await;
        assert_eq!(
            r["status"], "success",
            "every buffered-but-unflushed completion must be redelivered and finish"
        );
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while xpending_count(c, "ackflushq").await > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "redelivered entries must end up acked"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    drop(w2);
    drop(w1);
}

/// After a `kill -9` with ~200 tasks in flight, recovery must reclaim the whole
/// eligible backlog per tick, not one `--batch` page.
async fn bulk_recovery_drains_backlog_per_tick(c: &mut redis::aio::MultiplexedConnection) {
    const N: usize = 192;
    let pool_flags = [
        "--io-threads",
        "64",
        "--io-concurrency",
        "256",
        "--visibility-timeout",
        "2",
    ];
    let mut w1 = Worker::spawn("bulkq", &pool_flags);
    wait_group(c, "bulkq", 20).await;

    // 1s sleepers, timeout_ms 5000 (covers the ~2s worst in-pool queue wait
    // behind 64 threads), so required_idle = max(2000, 5000+2000) = 7s.
    let mut ids = Vec::with_capacity(N);
    for _ in 0..N {
        let (id, e) = envelope("fx.slow", "bulkq", |v| {
            v["args"] = json!([1.0]);
            v["timeout_ms"] = json!(5000);
        });
        xadd(c, "bulkq", &e.to_string()).await;
        ids.push(id);
    }
    // all delivered (in w1's PEL) before any completes (first ack at ~1s)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while (xpending_count(c, "bulkq").await as usize) < N {
        assert!(
            std::time::Instant::now() < deadline,
            "tasks never became pending"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    w1.signal(libc::SIGKILL);
    let _ = w1.wait_code(10);
    assert_eq!(xpending_count(c, "bulkq").await as usize, N);

    let w2 = Worker::spawn("bulkq", &pool_flags);
    let t0 = std::time::Instant::now();
    let mut first_result_at: Option<std::time::Duration> = None;
    let mut remaining: std::collections::HashSet<String> = ids.iter().cloned().collect();
    let deadline = t0 + std::time::Duration::from_secs(30);
    while !remaining.is_empty() && std::time::Instant::now() < deadline {
        let keys: Vec<String> = remaining
            .iter()
            .map(|id| format!("cauli:result:{id}"))
            .collect();
        let found: Vec<Option<String>> =
            redis::cmd("MGET").arg(&keys).query_async(c).await.unwrap();
        let done: Vec<String> = remaining
            .iter()
            .zip(&found)
            .filter(|(_, f)| f.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        if !done.is_empty() && first_result_at.is_none() {
            first_result_at = Some(t0.elapsed());
        }
        for id in done {
            remaining.remove(&id);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let total = t0.elapsed();
    assert!(
        remaining.is_empty(),
        "{} of {N} tasks still unrecovered after {total:?}",
        remaining.len()
    );
    let spread = total - first_result_at.unwrap();
    // Idle ~7s + one 1s tick + 3s of execution ≈ 11-12s; one page per tick needs
    // ~21s total with ~12s spread. Bounds sit between.
    assert!(
        total < std::time::Duration::from_secs(17),
        "recovery took {total:?}; per-tick reclaim is trickling again (pre-fix behavior ~21s)"
    );
    assert!(
        spread < std::time::Duration::from_secs(7),
        "first-to-last result spread {spread:?} looks claim-bound, not execution-bound"
    );
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        xpending_count(c, "bulkq").await,
        0,
        "every reclaimed entry must be acked"
    );
    drop(w2);
}

/// An async task wedging its loop with a blocking call never hits asyncio's
/// timeout; the Rust backstop must still resolve it as TimeLimitExceeded via
/// `PyRuntime::cancel`, which also frees its pending completion entry.
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
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");
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

/// `--batch 0`, `--visibility-timeout 0` and `--max-envelope-bytes 0` exit 1
/// at startup.
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

    let mut bad_meb = Worker::spawn("m8q", &["--max-envelope-bytes", "0"]);
    assert_eq!(
        bad_meb.wait_code(15),
        1,
        "--max-envelope-bytes 0 must be rejected"
    );
    drop(bad_meb);
}

/// A task running past a low visibility_timeout with a large timeout_ms must
/// not be reclaimed and run twice: reclaim also checks the envelope's idle.
async fn h1_visibility_floor_does_not_reclaim_long_task(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("h1q", &["--visibility-timeout", "1"]);
    wait_group(c, "h1q", 20).await;

    let cf = format!("/tmp/cauli-h1-{}", unique_id());
    let (id, e) = envelope("fx.slow_counted", "h1q", |v| v["args"] = json!([cf, 4.0]));
    xadd(c, "h1q", &e.to_string()).await;
    wait_inflight(c, "h1q", 10).await;

    // Several recovery ticks (500ms) past the 1s floor, short of the 4s sleep.
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

/// After a sync hard timeout abandons a thread, a replacement thread runs new
/// tasks at once. --io-threads 1 pins the wedged thread.
async fn h2_sync_pool_survives_hard_timeout_abandonment(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("h2q", &["--io-threads", "1"]);
    wait_group(c, "h2q", 20).await;

    // A sleeps 3s with a 300ms hard timeout, wedging the only pool thread.
    let (id_a, e_a) = envelope("fx.slow", "h2q", |v| {
        v["args"] = json!([3.0]);
        v["timeout_ms"] = json!(300);
        v["max_retries"] = json!(0);
    });
    xadd(c, "h2q", &e_a.to_string()).await;
    let r_a = wait_result(c, &id_a, 5).await;
    assert_eq!(r_a["status"], "failure");
    assert_eq!(r_a["error"]["type"], "TimeLimitExceeded");

    // B completes quickly only if a replacement thread was spawned.
    let (id_b, e_b) = envelope("fx.echo", "h2q", |v| {
        v["args"] = json!(["capacity-restored"])
    });
    xadd(c, "h2q", &e_b.to_string()).await;
    let r_b = wait_result(c, &id_b, 2).await;
    assert_eq!(
        r_b["status"], "success",
        "pool must serve new tasks at full capacity after an abandonment"
    );

    // A's late return must not overwrite its recorded failure.
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
            Some(n) => redis::from_redis_value_ref::<i64>(n).unwrap_or(0),
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
