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
    instant_repeated_child_death_is_backed_off(&mut c).await;
    instant_repeated_child_death_is_backed_off_stdio(&mut c).await;
    selfsignal_death_is_logged_with_signal_number_stdio(&mut c).await;

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

    // The death has to be countable, not only loggable: folded into `failed`
    // as a generic WorkerLost, an OOM killed or segfaulting child is just a
    // scrolling warning with no number to alert on.
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
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");

    // the prefetched sibling, whose write was blocked behind the wedged
    // task, must still recover on the replacement child rather than hang
    // forever waiting on a write that will never unblock.
    let r = wait_result(c, &big_id, 20).await;
    assert_eq!(r["status"], "success");

    w.signal(libc::SIGTERM);
    assert_eq!(w.wait_code(20), 0);
    drop(w);
}

/// Item 3 (audit): a child that forks successfully and then dies instantly,
/// repeatedly, must be backed off before the next fork request (mirrors
/// `parent_control_loop`'s `Refused` backoff), not forked again at full OS
/// speed with no delay. fx.cpu_die_always os._exit(9)s unconditionally on
/// receipt, so every one of these SIX SEPARATE tasks (`max_retries` 0: one
/// attempt each, no task level retry involved) kills its own dedicated
/// child on `--cpu-workers 1`'s one slot. Deliberately six independent
/// tasks rather than retries of one: a retried task's next attempt is only
/// picked up by the 250ms delayed mover poll (§4.3), which would dominate
/// this timing and hide cpu.rs's own backoff entirely. Before the fix this
/// ran at full OS speed (measured 14.4 fork/crash cycles per second, 2 to
/// 5ms gaps); the escalating 100ms to 2s backoff makes even a handful of
/// cycles take well over the 300ms threshold below.
async fn instant_repeated_child_death_is_backed_off(c: &mut redis::aio::MultiplexedConnection) {
    // --cpu-prefetch 0: with the default prefetch, several of the six tasks
    // below can be admitted into the same soon to die child's socket
    // buffer before it ever reads the first one, so its single death
    // resolves multiple of them at once and only one backoff interval is
    // observed. Disabling prefetch forces one task per child, one death
    // per task, so the timing assertion actually reflects consecutive
    // backoff intervals rather than getting lucky on request batching.
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

/// Same regression, stdio mode (`--no-fork-server`): `stdio_child_loop`
/// used to go straight back to spawning a replacement with no delay either.
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

/// Item 4 (audit), stdio mode counterpart to the fork server parent's
/// reaper fix (`cauli._exec._reap_children`, covered at the Python level by
/// `py/tests/test_fork_server.py`): in stdio mode there is no separate
/// parent process, `stdio_child_loop` reaps its own child directly, and it
/// had the exact same gap, discarding the exit status entirely so a
/// segfault, an OOM kill and any other unprompted death all logged
/// identically. fx.cpu_selfsignal kills itself with SIGSEGV (signal 11),
/// standing in for a real crash; `child.try_wait()`'s WIFSIGNALED peek now
/// appends the signal number to the existing "child died mid-task" line.
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
