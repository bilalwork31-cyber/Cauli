//! Fork-server e2e: threaded child pipelining, kill/respawn via replacement
//! forks, soft timeout in threaded mode, and the stdio fallback path.
//! Separate binary so it owns its redis instance (cargo runs test binaries
//! sequentially). Uses the CAULI_EXEC_CMD hook -> tests/fixtures/fake_exec.py
//! which implements both §5.1 modes.
mod common;
use common::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn e2e_fork_server() {
    start_redis();
    let mut c = conn().await;
    let _: String = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();

    threaded_child_pipelining(&mut c).await;
    kill_and_respawn_via_fork(&mut c).await;
    threaded_soft_timeout(&mut c).await;
    fallback_stdio_mode(&mut c).await;
    cpu_unknown_id_log_does_not_leak_payload(&mut c).await;
    busy_child_write_backpressure_is_not_wedged(&mut c).await;
    genuinely_wedged_child_is_still_killed_despite_prefetch_stall(&mut c).await;

    stop_redis();
}

/// With ONE child advertising concurrency 4, four 1.2s cpu tasks must
/// interleave on that single child: all four succeed on the SAME pid in far
/// less than the 4.8s serial time.
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

/// A child that os._exit(9)s mid-task is a retryable WorkerLost; the slot
/// requests a replacement fork and the retry succeeds on the fresh child.
/// Also exercises hard-timeout SIGKILL + replacement-fork on the same pool.
async fn kill_and_respawn_via_fork(c: &mut redis::aio::MultiplexedConnection) {
    let mut w = Worker::spawn("fsq2", &["--cpu-workers", "1"]);
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
    assert_eq!(r["error"]["type"], "TimeoutError");
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
}

/// Soft timeout in a threaded (M=4) child: injected via async-exc, reported
/// as SoftTimeLimitExceeded, while the child itself keeps serving.
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

/// `--no-fork-server` preserves the old spawn-per-child stdio path end to
/// end: success, hard timeout kill+respawn, and continued service (the
/// CAULI_EXEC_CMD override applies to this mode too).
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
    assert_eq!(r["error"]["type"], "TimeoutError");

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

/// Audit regression: a cpu child response whose id matches no pending
/// request used to log up to 256 bytes of the raw response line verbatim,
/// including result/error content from the task: the one log site in the
/// worker that still leaked task data. Only reachable with the fork server
/// pool (`serve_child_conn`'s pending map, which is keyed by id); the stdio
/// fallback path does no id matching at all, so it cannot exercise this
/// branch.
/// fx.cpu_ghost makes fake_exec.py send exactly one such unsolicited line
/// before its real response, so the log line the worker writes for it can
/// be checked.
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

/// Blocker regression: `--cpu-prefetch` (default 4) stages requests into a
/// busy child's socket buffer whether or not it is draining. A child with
/// `--cpu-child-threads` default 1 does not read again until it finishes
/// what it is executing, so prefetched writes queued behind one legitimately
/// long task block once the (~208 KiB default) unix socket buffer fills --
/// that is backpressure the worker itself created by prefetching, not
/// evidence the child is wedged. Before the fix this SIGKILLed the busy
/// child while it was still running (a healthy 8s task reported WorkerLost)
/// and lost a prefetched sibling; the same shape recurred on the
/// replacement child.
/// One task holds the child busy for 8s; four more each carry a 700 KB
/// argument (ordinary input, well under the 1 MiB `--max-envelope-bytes`
/// default) so their prefetch writes stall behind it.
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

/// The other direction of the same fix: a child that stops READING and stops
/// RESPONDING (genuinely wedged, not merely busy) must still be killed. Uses
/// the identical write stall shape as the healthy case above (a big payload
/// queued behind one already in flight), but this time the in flight
/// request's own timeout is short and the child never returns from it, so
/// nothing excuses the stall once that deadline passes.
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
    assert_eq!(r["error"]["type"], "TimeoutError");

    // the prefetched sibling, whose write was blocked behind the wedged
    // task, must still recover on the replacement child rather than hang
    // forever waiting on a write that will never unblock.
    let r = wait_result(c, &big_id, 20).await;
    assert_eq!(r["status"], "success");

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}
