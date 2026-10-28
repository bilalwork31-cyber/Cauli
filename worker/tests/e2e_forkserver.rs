//! Fork server e2e: threaded child pipelining, kill and respawn, soft timeout
//! in threaded mode, and the stdio fallback. Own binary, own redis. Uses the
//! CAULI_EXEC_CMD hook -> tests/fixtures/fake_exec.py (both §5.1 modes).
mod common;
use common::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn e2e_fork_server() {
    start_redis();
    let mut c = conn().await;
    let _: String = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();

    threaded_child_pipelining(&mut c).await;
    idle_children_take_a_burst_before_prefetch_stages_it(&mut c).await;
    kill_and_respawn_via_fork(&mut c).await;
    threaded_soft_timeout(&mut c).await;
    fallback_stdio_mode(&mut c).await;
    cpu_unknown_id_log_does_not_leak_payload(&mut c).await;
    busy_child_write_backpressure_is_not_wedged(&mut c).await;
    genuinely_wedged_child_is_still_killed_despite_prefetch_stall(&mut c).await;
    instant_repeated_child_death_is_backed_off(&mut c).await;
    instant_repeated_child_death_is_backed_off_stdio(&mut c).await;
    selfsignal_death_is_logged_with_signal_number_stdio(&mut c).await;

    stop_redis();
}

/// One child advertising concurrency 4 runs four 1.2s tasks interleaved on
/// the same pid, well under the 4.8s serial time.
async fn threaded_child_pipelining(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("fsq", &["--cpu-workers", "1", "--cpu-child-threads", "4"]);
    wait_group(c, "fsq", 20).await;

    let t0 = std::time::Instant::now();
    let mut ids = Vec::new();
    for _ in 0..4 {
        let (id, e) = envelope("fx.cpu_slow_pid", "fsq", |v| {
            v["kind"] = json!("cpu");
            v["args"] = json!([1.2]);
            v["max_retries"] = json!(0);
        });
        xadd(c, "fsq", &e.to_string()).await;
        ids.push(id);
    }
    let mut pids = std::collections::HashSet::new();
    for id in &ids {
        let r = wait_result(c, id, 20).await;
        assert_eq!(r["status"], "success");
        pids.insert(r["result"]["pid"].as_u64().unwrap());
    }
    let elapsed = t0.elapsed();
    assert_eq!(
        pids.len(),
        1,
        "all four tasks must run on the ONE child (got pids {pids:?})"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(3600),
        "4 x 1.2s tasks on one 4-thread child must interleave, took {elapsed:?}"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// Two tasks arriving together on two idle single threaded children run one
/// per child, not both staged on the first slot to wake.
async fn idle_children_take_a_burst_before_prefetch_stages_it(
    c: &mut redis::aio::MultiplexedConnection,
) {
    let mut w = Worker::spawn("burstq", &["--cpu-workers", "2", "--eager-cpu"]);
    wait_group(c, "burstq", 20).await;
    // Both children forked and connected before the burst.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let t0 = std::time::Instant::now();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let (id, e) = envelope("fx.cpu_slow_pid", "burstq", |v| {
            v["kind"] = json!("cpu");
            v["args"] = json!([1.5]);
            v["max_retries"] = json!(0);
        });
        xadd(c, "burstq", &e.to_string()).await;
        ids.push(id);
    }
    let mut pids = std::collections::HashSet::new();
    for id in &ids {
        let r = wait_result(c, id, 20).await;
        assert_eq!(r["status"], "success");
        pids.insert(r["result"]["pid"].as_u64().unwrap());
    }
    let elapsed = t0.elapsed();
    assert_eq!(pids.len(), 2, "one task per idle child (got pids {pids:?})");
    assert!(
        elapsed < std::time::Duration::from_millis(2700),
        "2 x 1.5s tasks on 2 idle children must run in parallel, took {elapsed:?}"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// A child that os._exit(9)s mid task is a retryable WorkerLost and the retry
/// succeeds on a replacement fork; a hard timeout SIGKILLs and replaces too.
async fn kill_and_respawn_via_fork(c: &mut redis::aio::MultiplexedConnection) {
    let log_path = std::env::temp_dir().join(format!("cauli-fs-lost-{}.log", unique_id()));
    let mut w = Worker::spawn_ex("fsq2", &["--cpu-workers", "1"], &[], Some(&log_path));
    wait_group(c, "fsq2", 20).await;

    // child death mid-task -> WorkerLost -> retry on a replacement fork
    let cf = format!("/tmp/cauli-fs-die-{}", unique_id());
    let (id, e) = envelope("fx.cpu_die_once", "fsq2", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([cf]);
        v["backoff_base_ms"] = json!(50);
        v["jitter"] = json!(false);
    });
    xadd(c, "fsq2", &e.to_string()).await;
    let r = wait_result(c, &id, 20).await;
    assert_eq!(
        r["status"], "success",
        "child death must be a retryable WorkerLost, retried on a fresh fork"
    );
    assert_eq!(r["result"], "revived");
    let _ = std::fs::remove_file(&cf);

    // Child deaths are counted, so an OOM kill or segfault is alertable.
    assert!(
        wait_stats_field_nonzero(&log_path, "cpu_lost=", 12)
            .await
            .is_some(),
        "no stats line reported cpu_lost > 0 after a child died mid task; log:\n{}",
        std::fs::read_to_string(&log_path).unwrap_or_default()
    );

    // hard timeout -> SIGKILL + replacement fork; pool keeps serving
    let (id, e) = envelope("fx.cpu_slow", "fsq2", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([30.0]);
        v["timeout_ms"] = json!(700);
        v["max_retries"] = json!(0);
    });
    xadd(c, "fsq2", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");
    let (id, e) = envelope("fx.cpu_echo", "fsq2", |v| v["kind"] = json!("cpu"));
    xadd(c, "fsq2", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(
        r["status"], "success",
        "pool must survive SIGKILL + replacement fork"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
    let _ = std::fs::remove_file(&log_path);
}

/// Soft timeout in a threaded (M=4) child: reported as SoftTimeLimitExceeded,
/// and the child keeps serving.
async fn threaded_soft_timeout(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("fsq3", &["--cpu-workers", "1", "--cpu-child-threads", "4"]);
    wait_group(c, "fsq3", 20).await;

    let (id, e) = envelope("fx.cpu_soft_slow", "fsq3", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([10.0]);
        v["soft_timeout_ms"] = json!(300);
        v["timeout_ms"] = json!(15000);
        v["max_retries"] = json!(0);
    });
    xadd(c, "fsq3", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "SoftTimeLimitExceeded");

    // the same (not respawned) child keeps serving other requests
    let (id, e) = envelope("fx.cpu_echo", "fsq3", |v| v["kind"] = json!("cpu"));
    xadd(c, "fsq3", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "success");

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// `--no-fork-server` stdio path end to end: success, hard timeout kill and
/// respawn, continued service.
async fn fallback_stdio_mode(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("fbq", &["--no-fork-server", "--cpu-workers", "2"]);
    wait_group(c, "fbq", 20).await;

    let (id, e) = envelope("fx.cpu_echo", "fbq", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([7]);
    });
    xadd(c, "fbq", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"]["args"], json!([7]));

    let (id, e) = envelope("fx.cpu_slow", "fbq", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([30.0]);
        v["timeout_ms"] = json!(700);
        v["max_retries"] = json!(0);
    });
    xadd(c, "fbq", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");

    let (id, e) = envelope("fx.cpu_echo", "fbq", |v| v["kind"] = json!("cpu"));
    xadd(c, "fbq", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(
        r["status"], "success",
        "stdio pool must survive kill+respawn"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// An unsolicited cpu child response (unknown id) must not log task data.
/// Fork server only: the stdio path does no id matching. fx.cpu_ghost sends
/// one such line before its real response.
async fn cpu_unknown_id_log_does_not_leak_payload(c: &mut redis::aio::MultiplexedConnection) {
    let log_path = fixtures_dir().join(format!("cpughost-{}.log", unique_id()));
    let mut w = Worker::spawn_ex("cpughost", &["--cpu-workers", "1"], &[], Some(&log_path));
    wait_group(c, "cpughost", 20).await;

    let (id, e) = envelope("fx.cpu_ghost", "cpughost", |v| v["kind"] = json!("cpu"));
    xadd(c, "cpughost", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(
        r["status"], "success",
        "the real request must still complete"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);

    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    let line = log
        .lines()
        .rev()
        .find(|l| l.contains("unknown or missing id"))
        .unwrap_or_else(|| panic!("no 'unknown or missing id' log line found:\n{log}"));
    assert!(
        !line.contains("GHOST_SECRET_MARKER"),
        "cpu unknown id log must not leak response payload content: {line}"
    );
    let _ = std::fs::remove_file(&log_path);
}

/// Prefetched writes stalled behind a busy single threaded child are the
/// worker's own backpressure, not a wedge: the busy child must not be killed
/// and no sibling lost. One 8s task, then four 700 KB arguments (under the
/// 1 MiB `--max-envelope-bytes`) that fill the socket buffer behind it.
async fn busy_child_write_backpressure_is_not_wedged(c: &mut redis::aio::MultiplexedConnection) {
    let log_path = fixtures_dir().join(format!("cpubusy-{}.log", unique_id()));
    let mut w = Worker::spawn_ex("busyq", &["--cpu-workers", "1"], &[], Some(&log_path));
    wait_group(c, "busyq", 20).await;

    let (slow_id, e) = envelope("fx.cpu_slow_pid", "busyq", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([8.0]);
        v["timeout_ms"] = json!(60_000);
        v["max_retries"] = json!(0);
    });
    xadd(c, "busyq", &e.to_string()).await;

    let big = "x".repeat(700_000);
    let mut big_ids = Vec::new();
    for _ in 0..4 {
        let (id, e) = envelope("fx.cpu_echo", "busyq", |v| {
            v["kind"] = json!("cpu");
            v["args"] = json!([big]);
            v["max_retries"] = json!(0);
        });
        xadd(c, "busyq", &e.to_string()).await;
        big_ids.push(id);
    }

    let r = wait_result(c, &slow_id, 30).await;
    assert_eq!(
        r["status"], "success",
        "a child legitimately busy on one task must not be SIGKILLed just because \
         prefetch writes for OTHER queued work are blocked behind it: {r}"
    );
    let mut pids = std::collections::HashSet::new();
    pids.insert(r["result"]["pid"].as_u64().unwrap());

    for id in &big_ids {
        let r = wait_result(c, id, 30).await;
        assert_eq!(
            r["status"], "success",
            "a prefetched sibling behind a busy (not wedged) child must not be lost: {r}"
        );
        assert_eq!(r["result"]["args"][0].as_str().unwrap().len(), 700_000);
        pids.insert(r["result"]["pid"].as_u64().unwrap());
    }
    assert_eq!(
        pids.len(),
        1,
        "all five requests must run on the SAME child (no SIGKILL + replacement fork): {pids:?}"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);

    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(
        !log.contains("SIGKILL"),
        "no child should have been killed for legitimate write backpressure:\n{log}"
    );
    let _ = std::fs::remove_file(&log_path);
}

/// A child that stops reading and responding must still be killed: same
/// stall shape, but the in flight request's short timeout passes.
async fn genuinely_wedged_child_is_still_killed_despite_prefetch_stall(
    c: &mut redis::aio::MultiplexedConnection,
) {
    let mut w = Worker::spawn("wedgeq", &["--cpu-workers", "1"]);
    wait_group(c, "wedgeq", 20).await;

    let (slow_id, e) = envelope("fx.cpu_slow", "wedgeq", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([30.0]); // far longer than its own timeout below
        v["timeout_ms"] = json!(1500);
        v["max_retries"] = json!(0);
    });
    xadd(c, "wedgeq", &e.to_string()).await;

    let big = "x".repeat(700_000);
    let (big_id, e) = envelope("fx.cpu_echo", "wedgeq", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([big]);
    });
    xadd(c, "wedgeq", &e.to_string()).await;

    let r = wait_result(c, &slow_id, 20).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");

    // The sibling blocked behind the wedged task recovers on the replacement.
    let r = wait_result(c, &big_id, 20).await;
    assert_eq!(r["status"], "success");

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// A child that dies instantly on every fork is backed off (escalating 100ms
/// to 2s), not reforked at full speed. Six independent tasks (`max_retries` 0),
/// not retries: retries go through the 250ms delayed mover (§4.3), which would
/// hide the backoff.
async fn instant_repeated_child_death_is_backed_off(c: &mut redis::aio::MultiplexedConnection) {
    // --cpu-prefetch 0: one task per child, one death per task, so the timing
    // measures consecutive backoff intervals.
    let mut w = Worker::spawn("backoffq", &["--cpu-workers", "1", "--cpu-prefetch", "0"]);
    wait_group(c, "backoffq", 20).await;

    let t0 = std::time::Instant::now();
    let mut ids = Vec::new();
    for _ in 0..6 {
        let (id, e) = envelope("fx.cpu_die_always", "backoffq", |v| {
            v["kind"] = json!("cpu");
            v["max_retries"] = json!(0);
        });
        xadd(c, "backoffq", &e.to_string()).await;
        ids.push(id);
    }
    for id in &ids {
        wait_dlq(c, "backoffq", id, 30).await;
    }
    let elapsed = t0.elapsed();
    assert!(
        elapsed > std::time::Duration::from_millis(300),
        "six tasks that each instantly crash their own child must be backed \
         off between fork attempts, not forked again at full OS speed: took \
         only {elapsed:?}"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// Same backoff, stdio mode (`--no-fork-server`).
async fn instant_repeated_child_death_is_backed_off_stdio(
    c: &mut redis::aio::MultiplexedConnection,
) {
    let mut w = Worker::spawn("backoffqstdio", &["--no-fork-server", "--cpu-workers", "1"]);
    wait_group(c, "backoffqstdio", 20).await;

    let t0 = std::time::Instant::now();
    let mut ids = Vec::new();
    for _ in 0..6 {
        let (id, e) = envelope("fx.cpu_die_always", "backoffqstdio", |v| {
            v["kind"] = json!("cpu");
            v["max_retries"] = json!(0);
        });
        xadd(c, "backoffqstdio", &e.to_string()).await;
        ids.push(id);
    }
    for id in &ids {
        wait_dlq(c, "backoffqstdio", id, 30).await;
    }
    let elapsed = t0.elapsed();
    assert!(
        elapsed > std::time::Duration::from_millis(300),
        "stdio mode: six tasks that each instantly crash their own child \
         must be backed off between spawn attempts too: took only {elapsed:?}"
    );

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// Stdio mode logs a child's terminating signal on "child died mid-task".
/// fx.cpu_selfsignal raises SIGSEGV (signal 11).
async fn selfsignal_death_is_logged_with_signal_number_stdio(
    c: &mut redis::aio::MultiplexedConnection,
) {
    let log_path = fixtures_dir().join(format!("selfsignal-{}.log", unique_id()));
    let mut w = Worker::spawn_ex(
        "selfsignalq",
        &["--no-fork-server", "--cpu-workers", "1"],
        &[],
        Some(&log_path),
    );
    wait_group(c, "selfsignalq", 20).await;

    let (id, e) = envelope("fx.cpu_selfsignal", "selfsignalq", |v| {
        v["kind"] = json!("cpu");
        v["max_retries"] = json!(0);
    });
    xadd(c, "selfsignalq", &e.to_string()).await;
    wait_dlq(c, "selfsignalq", &id, 20).await;

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);

    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    let line = log
        .lines()
        .rev()
        .find(|l| l.contains("child died mid-task"))
        .unwrap_or_else(|| panic!("no 'child died mid-task' log line found:\n{log}"));
    assert!(
        line.contains("(signal 11)"),
        "a SIGSEGV death must be logged with its signal number: {line}"
    );
    let _ = std::fs::remove_file(&log_path);
}
