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
