//! Cpu backlog observability e2e: fill the cpu dispatch backlog (cpu.rs) and
//! drain it, asserting the stats line and log report the depth and the
//! transitions while the shared admission gate pauses fetching for every lane.
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
    // In flight capacity 1, backlog channel cap 2 (cpu.rs). --cpu-prefetch 0, or
    // the child's own prefetch queue drains the channel before it can fill.
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

    // The occupier spans at least one 1s stats tick.
    let (occ_id, occ_e) = envelope("fx.cpu_slow", "cbq", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([2.5]);
        v["max_retries"] = json!(0);
    });
    xadd(&mut c, "cbq", &occ_e.to_string()).await;

    // With capacity 1 and cap 2, at least 2 of the 5 cpu sends block.
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

    // Wait until all 5 cpu entries are read (PEL == 5): the gate guards only the
    // next XREADGROUP, so an io entry added earlier could ride in the same batch.
    let deadline = Instant::now() + Duration::from_secs(5);
    while xpending_count(&mut c, "cbq").await < 5 {
        assert!(
            Instant::now() < deadline,
            "fetch loop never picked up the 5 cpu entries"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Wait past the 1s XREADGROUP BLOCK: a call already blocked passed the gate
    // before the backlog formed and would hand over the io entry.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // The admission gate is shared, so this io task must not be fetched while
    // the backlog holds it shut.
    let (io_id, io_e) = envelope("fx.echo", "cbq", |v| v["args"] = json!(["x"]));
    xadd(&mut c, "cbq", &io_e.to_string()).await;

    // While the backlog is full, it must be visible.
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
    // Removed stats key, asserted absent.
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

    // Everything drains, the io task runs, and the log shows the clear line.
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
            Some(n) => redis::from_redis_value_ref::<i64>(n).unwrap_or(0),
            None => 0,
        },
        _ => 0,
    }
}

/// Poll `path` for a line containing `needle` for up to `secs`; returns the
/// last match.
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
