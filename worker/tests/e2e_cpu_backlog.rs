//! Cpu backlog observability e2e: drives the cpu dispatch backlog channel
//! (worker/src/cpu.rs) to full and back to empty, and asserts that the
//! stats line and the log carry the depth and the zero/nonzero transition
//! instead of staying silent while the fetch loop's admission gate (§4,
//! loops.rs) pauses fetching for every lane, not just cpu. Separate binary
//! so it owns its redis instance (cargo runs test binaries sequentially).
mod common;
use common::*;
use serde_json::json;
use std::path::Path;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn e2e_cpu_backlog_observability() {
    start_redis();
    let mut c = conn().await;
    let _: String = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();

    let log_path = fixtures_dir().join(format!("cpu-backlog-{}.log", unique_id()));
    // capacity in flight = --cpu-workers times --cpu-child-threads = 1;
    // backlog channel cap = 2 times that = 2 (cpu.rs). --cpu-prefetch 0 is
    // required here: its default of 4 lets the one child stage extra jobs
    // in its own pending queue per child (queue_depth = concurrency plus
    // prefetch), which drains the shared backlog channel before it can ever
    // fill. With prefetch off, the channel is the only buffer, so one
    // occupier plus four more guarantees at least two land in the overflow
    // path, where the send itself blocks.
    let mut w = Worker::spawn_ex(
        "cbq",
        &[
            "--cpu-workers",
            "1",
            "--cpu-child-threads",
            "1",
            "--cpu-prefetch",
            "0",
            "--stats-interval",
            "1",
        ],
        &[],
        Some(&log_path),
    );
    wait_group(&mut c, "cbq", 20).await;

    // One long occupier holds the pool's single slot in flight for a
    // window wide enough that a 1s stats tick is guaranteed to land in it.
    let (occ_id, occ_e) = envelope("fx.cpu_slow", "cbq", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([2.5]);
        v["max_retries"] = json!(0);
    });
    xadd(&mut c, "cbq", &occ_e.to_string()).await;

    // Four more, short once they finally run. With capacity 1 in flight and
    // channel cap 2, at most 3 of these 5 total cpu sends can avoid the
    // overflow path. At least 2 must block on a full channel.
    let mut extra_ids = Vec::new();
    for _ in 0..4 {
        let (id, e) = envelope("fx.cpu_slow", "cbq", |v| {
            v["kind"] = json!("cpu");
            v["args"] = json!([0.05]);
            v["max_retries"] = json!(0);
        });
        xadd(&mut c, "cbq", &e.to_string()).await;
        extra_ids.push(id);
    }

    // Wait until the fetch loop has actually read all 5 cpu entries off the
    // stream (PEL count == 5) before adding the io entry below. The
    // admission gate only guards the fetch loop's NEXT XREADGROUP call, not
    // entries already inside a batch it already read; adding the io entry
    // before this point could let it ride along in that same batch and
    // dispatch immediately regardless of the gate, which would prove
    // nothing about the gate at all.
    let deadline = Instant::now() + Duration::from_secs(5);
    while xpending_count(&mut c, "cbq").await < 5 {
        assert!(
            Instant::now() < deadline,
            "fetch loop never picked up the 5 cpu entries"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Buffer past the fetch loop's own BLOCK 1000 window. XPENDING==5 only
    // proves the 5 cpu entries were already read; the fetch loop could
    // still be sitting inside the XREADGROUP call it issued right after
    // that read, gated open at the time it started, blocked for up to 1s
    // waiting for a 6th entry regardless of what the overflow counter does
    // meanwhile. Redis wakes a blocked XREADGROUP the instant new data
    // exists, so an io entry added inside that window would be handed to a
    // call that already passed the gate, proving nothing. Waiting past the
    // full 1s block guarantees any such call has timed out and the loop
    // has run the gate check again with the backlog already counted.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // An unrelated io task, added only now that the cpu backlog is already
    // formed: the admission gate is shared, so this must not even be
    // fetched while the backlog holds it shut. This is the actual symptom
    // visible to an operator that the bug report describes (io throughput
    // drops to zero); the gate behavior itself is intentional and is only
    // exercised here, not argued again.
    let (io_id, io_e) = envelope("fx.echo", "cbq", |v| v["args"] = json!(["x"]));
    xadd(&mut c, "cbq", &io_e.to_string()).await;

    // === the bug under test: while the backlog is full, this must be
    // visible. Today, before the fix, neither of the next two waits ever
    // succeeds.
    let stats_line = wait_stats_field_nonzero(&log_path, "cpu_backlog=", 8)
        .await
        .unwrap_or_else(|| {
            panic!(
                "no stats line ever showed cpu_backlog > 0 while the pool was \
                 saturated; log:\n{}",
                std::fs::read_to_string(&log_path).unwrap_or_default()
            )
        });
    assert!(
        stats_line.contains("stats: ") && stats_line.contains(" fetched="),
        "expected a full stats line, got: {stats_line}"
    );
    // Removed key: its own source note named async_rejected as the field that
    // actually moves during the wedge it was supposed to expose.
    assert!(
        !stats_line.contains("pending_async"),
        "pending_async must be gone from the stats line: {stats_line}"
    );

    let full_line = wait_log_contains(&log_path, "cpu backlog full", 6).await;
    assert!(
        full_line.is_some(),
        "no warning naming the cpu backlog while the channel was full; log:\n{}",
        std::fs::read_to_string(&log_path).unwrap_or_default()
    );
    assert!(full_line
        .unwrap()
        .contains("fetching paused for all lanes including io"));

    // the io entry must not even be fetched while the shared gate is shut
    let mut io_fetched_early = false;
    let deadline = Instant::now() + Duration::from_millis(600);
    while Instant::now() < deadline {
        if xpending_count(&mut c, "cbq").await >= 6 {
            io_fetched_early = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !io_fetched_early,
        "io entry was fetched while the cpu backlog should have held the \
         shared admission gate shut"
    );

    // === everything drains: occupier + the four short followers finish,
    // the io task then goes through, and the log shows the clear line too.
    let r = wait_result(&mut c, &occ_id, 15).await;
    assert_eq!(r["status"], "success");
    for id in &extra_ids {
        let r = wait_result(&mut c, id, 15).await;
        assert_eq!(r["status"], "success");
    }
    let r = wait_result(&mut c, &io_id, 15).await;
    assert_eq!(
        r["status"], "success",
        "io task must run once the cpu backlog clears"
    );

    let cleared_line = wait_log_contains(&log_path, "cpu backlog cleared", 10).await;
    assert!(
        cleared_line.is_some(),
        "no line marking the cpu backlog clearing after it drained; log:\n{}",
        std::fs::read_to_string(&log_path).unwrap_or_default()
    );
    assert!(cleared_line
        .unwrap()
        .contains("fetching resumed for all lanes"));

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
    stop_redis();
    let _ = std::fs::remove_file(&log_path);
}

async fn xpending_count(c: &mut redis::aio::MultiplexedConnection, queue: &str) -> i64 {
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(format!("cauli:q:{queue}"))
        .arg("cauli")
        .query_async(c)
        .await
        .unwrap();
    match v {
        redis::Value::Array(items) => match items.first() {
            Some(n) => redis::from_redis_value::<i64>(n).unwrap_or(0),
            None => 0,
        },
        _ => 0,
    }
}

/// Poll `path` for any line containing `needle`, up to `secs`. Returns the
/// last matching line, or None on timeout. The worker's own log flush
/// timing is not something this test controls, hence polling rather than a
/// fixed sleep.
async fn wait_log_contains(path: &Path, needle: &str, secs: u64) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let content = std::fs::read_to_string(path).unwrap_or_default();
        if let Some(line) = content.lines().rev().find(|l| l.contains(needle)) {
            return Some(line.to_string());
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
