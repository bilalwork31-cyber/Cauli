//! Fixture e2e: worker vs throwaway redis :6392 (see tests/common/mod.rs).
//! One test fn so scenarios run sequentially against one worker instance.
mod common;
use common::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn e2e_main_flows() {
    start_redis();
    let mut c = conn().await;
    let _: String = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();

    let mut w = Worker::spawn("default", &[]);
    wait_group(&mut c, "default", 20).await;

    // 1. sync io success: result JSON written + stream trimmed
    let (id, e) = envelope("fx.echo", "default", |v| {
        v["args"] = json!([1, "a"]);
        v["kwargs"] = json!({"k": true});
    });
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 15).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"]["args"], json!([1, "a"]));
    assert_eq!(r["result"]["kwargs"]["k"], true);
    assert_eq!(r["error"], serde_json::Value::Null);
    assert!(r["finished_at"].as_u64().unwrap() > 0);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let len: u64 = redis::cmd("XLEN")
        .arg("cauli:q:default")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(len, 0, "stream not trimmed after success");

    // 2. async io task executes
    let (id, e) = envelope("fx.aecho", "default", |v| v["args"] = json!(["async-ok"]));
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 10).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"]["args"], json!(["async-ok"]));

    // 3. cpu task via fake_exec child pool
    let (id, e) = envelope("fx.cpu_echo", "default", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([7]);
    });
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 10).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"]["args"], json!([7]));

    // 4. failure -> delayed zset with retries+1 and correct score window
    let t0 = now_ms();
    let (id, e) = envelope("fx.fail", "default", |v| {
        v["backoff_base_ms"] = json!(3000);
        v["jitter"] = json!(false);
    });
    xadd(&mut c, "default", &e.to_string()).await;
    let mut found = None;
    for _ in 0..100 {
        let zs: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg("cauli:delayed:default")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query_async(&mut c)
            .await
            .unwrap();
        if let Some((m, s)) = zs.into_iter().find(|(m, _)| m.contains(&id)) {
            found = Some((m, s));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (member, score) = found.expect("retry never reached delayed zset");
    let env: serde_json::Value = serde_json::from_str(&member).unwrap();
    assert_eq!(env["retries"], 1, "retries not incremented");
    assert!(
        score >= (t0 + 3000) as f64 - 100.0,
        "score {score} below window"
    );
    assert!(
        score <= (t0 + 3000 + 4000) as f64,
        "score {score} above window"
    );
    let _: u64 = redis::cmd("ZREM")
        .arg("cauli:delayed:default")
        .arg(&member)
        .query_async(&mut c)
        .await
        .unwrap(); // stop the chain

    // 5. exhausted retries -> DLQ reason max_retries + failure result
    let (id, e) = envelope("fx.fail", "default", |v| {
        v["max_retries"] = json!(2);
        v["backoff_base_ms"] = json!(50);
        v["jitter"] = json!(false);
    });
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 15).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "ValueError");
    // §8 origin: a task exception is "task" through to the result document.
    assert_eq!(r["error"]["origin"], "task");
    assert!(r["error"]["traceback"]
        .as_str()
        .unwrap()
        .contains("ValueError"));
    let (reason, err, efield) = wait_dlq(&mut c, "default", &id, 5).await;
    assert_eq!(reason, "max_retries");
    assert!(err.contains("ValueError"));
    let dlq_env: serde_json::Value = serde_json::from_str(&efield).unwrap();
    assert_eq!(
        dlq_env["retries"], 2,
        "DLQ envelope should carry final retries"
    );

    // 6. retry task: fails first 2 times (counter file), then succeeds
    let cf = format!("/tmp/cauli-flaky-{}", unique_id());
    let (id, e) = envelope("fx.flaky", "default", |v| {
        v["args"] = json!([cf, 2]);
        v["backoff_base_ms"] = json!(50);
        v["jitter"] = json!(false);
    });
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 15).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"], 3, "flaky should succeed on 3rd attempt");

    // 7. cauli.Retry(countdown) forced retry then success
    let cf = format!("/tmp/cauli-retryonce-{}", unique_id());
    let (id, e) = envelope("fx.retry_once", "default", |v| v["args"] = json!([cf]));
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 15).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"], "after-retry");

    // 8. non-serializable return: final failure, no retry, and the DLQ reason
    // must not claim a retry budget was exhausted.
    let (id, e) = envelope("fx.bad_return", "default", |v| v["max_retries"] = json!(3));
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "SerializationError");
    let (reason, _, efield) = wait_dlq(&mut c, "default", &id, 5).await;
    assert_eq!(reason, "not_retryable");
    let dlq_env: serde_json::Value = serde_json::from_str(&efield).unwrap();
    assert_eq!(
        dlq_env["retries"], 0,
        "SerializationError must not consume retries"
    );

    idempotency_and_timeouts(&mut c).await;
    result_write_failure_is_not_counted_ok(&mut c).await;
    write_failure_is_not_counted_for_duplicate_final_and_terminal(&mut c).await;

    // graceful SIGTERM with nothing in flight -> exit 0
    w.signal(libc::SIGTERM);
    assert_eq!(
        w.wait_code(20),
        0,
        "worker should exit 0 on graceful shutdown"
    );
    drop(w);
    stop_redis();
}

async fn idempotency_and_timeouts(c: &mut redis::aio::MultiplexedConnection) {
    // idempotency: same key twice -> second resolves duplicate
    let key = format!("idk-{}", unique_id());
    let (id1, e1) = envelope("fx.echo", "default", |v| {
        v["idempotency_key"] = json!(key.clone())
    });
    xadd(c, "default", &e1.to_string()).await;
    let r1 = wait_result(c, &id1, 10).await;
    assert_eq!(r1["status"], "success");
    let (id2, e2) = envelope("fx.echo", "default", |v| {
        v["idempotency_key"] = json!(key.clone())
    });
    xadd(c, "default", &e2.to_string()).await;
    let r2 = wait_result(c, &id2, 10).await;
    assert_eq!(r2["status"], "duplicate");
    assert_eq!(r2["result"], serde_json::Value::Null);
    // The idemp key is stored hashed, so scan for it.
    let idemp_keys: Vec<String> = redis::cmd("KEYS")
        .arg("cauli:idemp:*")
        .query_async(c)
        .await
        .unwrap();
    assert_eq!(idemp_keys.len(), 1, "exactly one idemp key expected so far");
    let claimed: String = redis::cmd("GET")
        .arg(&idemp_keys[0])
        .query_async(c)
        .await
        .unwrap();
    assert_eq!(claimed, id1, "idemp key must hold first claimant id");

    // idempotency_key + fail once then succeed must retry to "success", not
    // resolve "duplicate" against its own claim.
    let cf = format!("/tmp/cauli-c1-idemp-retry-{}", unique_id());
    let retry_key = format!("idk-retry-{}", unique_id());
    let (id, e) = envelope("fx.flaky", "default", |v| {
        v["args"] = json!([cf, 1]); // fails once, succeeds on 2nd attempt
        v["idempotency_key"] = json!(retry_key);
        v["backoff_base_ms"] = json!(50);
        v["jitter"] = json!(false);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(
        r["status"], "success",
        "idempotency_key must not block a task's own retry"
    );
    assert_eq!(
        r["result"], 2,
        "flaky task should have succeeded on its 2nd attempt"
    );

    // timeout_ms u64::MAX must saturate, not wrap the backstop to near zero.
    let (id, e) = envelope("fx.aslow", "default", |v| {
        v["args"] = json!([3.0]);
        v["timeout_ms"] = json!(18446744073709551615u64);
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 8).await;
    assert_eq!(
        r["status"], "success",
        "u64::MAX timeout_ms must not wrap into a spurious timeout"
    );
    assert_eq!(r["result"], "aslow-done");

    // async hard timeout -> retryable TimeLimitExceeded; max_retries 0 -> DLQ now
    let (id, e) = envelope("fx.aslow", "default", |v| {
        v["args"] = json!([30.0]);
        v["timeout_ms"] = json!(700);
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");
    assert_eq!(r["error"]["origin"], "worker");
    let (reason, _, _) = wait_dlq(c, "default", &id, 5).await;
    assert_eq!(reason, "max_retries");

    // A task raising the builtin TimeoutError keeps that type and origin task.
    let (id, e) = envelope("fx.raise_timeout", "default", |v| {
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeoutError");
    assert_eq!(r["error"]["origin"], "task");

    // sync soft timeout via PyThreadState_SetAsyncExc
    let (id, e) = envelope("fx.soft_slow", "default", |v| {
        v["args"] = json!([5.0]);
        v["soft_timeout_ms"] = json!(300);
        v["timeout_ms"] = json!(10000);
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "SoftTimeLimitExceeded");
    // Injected by the worker but raised out of user code: origin task.
    assert_eq!(r["error"]["origin"], "task");

    // sync hard timeout: thread abandoned, retryable TimeLimitExceeded
    let (id, e) = envelope("fx.slow", "default", |v| {
        v["args"] = json!([3.0]);
        v["timeout_ms"] = json!(600);
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");
    assert_eq!(r["error"]["origin"], "worker");

    // cpu hard timeout: child SIGKILL + respawn, then pool still works
    let (id, e) = envelope("fx.cpu_slow", "default", |v| {
        v["kind"] = json!("cpu");
        v["args"] = json!([30.0]);
        v["timeout_ms"] = json!(700);
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeLimitExceeded");
    assert_eq!(r["error"]["origin"], "worker");
    let (id, e) = envelope("fx.cpu_echo", "default", |v| v["kind"] = json!("cpu"));
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(r["status"], "success", "cpu pool must survive kill+respawn");

    // unregistered task -> DLQ, no retry
    let (id, e) = envelope("no.such.task", "default", |v| v["max_retries"] = json!(5));
    xadd(c, "default", &e.to_string()).await;
    let (reason, _, _) = wait_dlq(c, "default", &id, 10).await;
    assert_eq!(reason, "unregistered");
    // A terminal DLQ with a recoverable id still writes a result, so get() resolves.
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "UnregisteredTask");
    // Dead lettered before running: origin worker.
    assert_eq!(r["error"]["origin"], "worker");

    // malformed payload -> DLQ with raw payload preserved
    let raw = "{this is not json";
    xadd(c, "default", raw).await;
    let (reason, _, efield) = wait_dlq(c, "default", "this is not json", 10).await;
    assert_eq!(reason, "malformed");
    assert_eq!(efield, raw);

    // An id outside [a-z0-9]{32} -> DLQ malformed, never executed.
    let (_, mut bad_id_env) = envelope("fx.echo", "default", |_| {});
    bad_id_env["id"] = json!("not-a-valid-32-char-lowercase-hex-id");
    xadd(c, "default", &bad_id_env.to_string()).await;
    let (reason, _, _) = wait_dlq(c, "default", "not-a-valid-32-char-lowercase-hex-id", 10).await;
    assert_eq!(reason, "malformed");
    // An id failing the charset gate is never used as a result key either.
    let none: Option<String> = redis::cmd("GET")
        .arg("cauli:result:not-a-valid-32-char-lowercase-hex-id")
        .query_async(c)
        .await
        .unwrap();
    assert!(none.is_none());

    // Unknown protocol version -> DLQ malformed; the id is valid, so a result is written.
    let (id, mut v99_env) = envelope("fx.echo", "default", |_| {});
    v99_env["v"] = json!(99);
    xadd(c, "default", &v99_env.to_string()).await;
    let (reason, _, _) = wait_dlq(c, "default", &id, 10).await;
    assert_eq!(reason, "malformed");
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");

    // kwargs of the wrong type is malformed at parse, never executed.
    let (id, e) = envelope("fx.echo", "default", |v| {
        v["kwargs"] = json!([1, 2]);
    });
    xadd(c, "default", &e.to_string()).await;
    let (reason, _, _) = wait_dlq(c, "default", &id, 10).await;
    assert_eq!(reason, "malformed");
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "Malformed");

    // timeout_ms 0 is malformed before execution; the id is still recoverable.
    let (id, e) = envelope("fx.echo", "default", |v| {
        v["timeout_ms"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let (reason, _, _) = wait_dlq(c, "default", &id, 10).await;
    assert_eq!(reason, "malformed");
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "Malformed");

    // An envelope over --max-envelope-bytes -> DLQ malformed before parse, with
    // only a truncated preview stored.
    let raw = format!(
        r#"{{"marker":"oversize-test","pad":"{}"}}"#,
        "x".repeat(2_000_000)
    );
    xadd(c, "default", &raw).await;
    let (reason, _, efield) = wait_dlq(c, "default", "oversize-test", 10).await;
    assert_eq!(reason, "malformed");
    assert!(
        efield.len() <= 4096,
        "oversize envelope must be stored truncated, not in full"
    );
}

/// A failed result write must not count `ok`. result_ttl=0 makes Redis reject
/// every `SET ... EX 0`, a real write failure.
async fn result_write_failure_is_not_counted_ok(c: &mut redis::aio::MultiplexedConnection) {
    let log_path = fixtures_dir().join(format!("badttl-{}.log", unique_id()));
    let mut bad = Worker::spawn_ex(
        "badttl",
        &[],
        &[("FIXTURE_RESULT_TTL", "0")],
        Some(&log_path),
    );
    wait_group(c, "badttl", 20).await;

    let (id, e) = envelope("fx.echo", "badttl", |v| v["args"] = json!(["x"]));
    xadd(c, "badttl", &e.to_string()).await;

    // The ack must not land without the result: the entry stays pending for
    // recovery. The logged write failure signals dispatch is done.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !std::fs::read_to_string(&log_path)
        .unwrap_or_default()
        .contains("success finish write failed")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "badttl entry was never dispatched"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let len: u64 = redis::cmd("XLEN")
        .arg("cauli:q:badttl")
        .query_async(c)
        .await
        .unwrap();
    assert_eq!(len, 1, "a refused result write must not delete the entry");
    let raw: Option<String> = redis::cmd("GET")
        .arg(format!("cauli:result:{id}"))
        .query_async(c)
        .await
        .unwrap();
    assert!(
        raw.is_none(),
        "SET ... EX 0 should have failed: no result key"
    );

    bad.signal(libc::SIGTERM);
    assert_eq!(bad.wait_code(20), 0, "badttl worker should exit 0");
    drop(bad);

    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    let stats = log
        .lines()
        .rev()
        .find(|l| l.contains("stats: ") && l.contains(" fetched="))
        .unwrap_or_else(|| panic!("no stats line in worker log:\n{log}"));
    assert!(
        stats.contains("fetched=1 ok=0 failed=1"),
        "a failed result write must not be counted as ok: {stats}"
    );
    let _ = std::fs::remove_file(&log_path);
}

/// Same check for the duplicate, final failure, terminal DLQ and expiry
/// branches: none may count its outcome when its write fails.
async fn write_failure_is_not_counted_for_duplicate_final_and_terminal(
    c: &mut redis::aio::MultiplexedConnection,
) {
    let log_path = fixtures_dir().join(format!("badttl2-{}.log", unique_id()));
    let mut bad = Worker::spawn_ex(
        "badttl2",
        &[],
        &[("FIXTURE_RESULT_TTL", "0")],
        Some(&log_path),
    );
    wait_group(c, "badttl2", 20).await;

    // Every result SET is rejected, so each step's logged write failure is its
    // completion signal.
    async fn wait_write_failures(log_path: &std::path::Path, n: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while std::fs::read_to_string(log_path)
            .unwrap_or_default()
            .matches("write failed")
            .count()
            < n
        {
            assert!(
                std::time::Instant::now() < deadline,
                "badttl2 entry {n} was never dispatched"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    // A: duplicate resolution must not count `ok` when its write fails.
    let key = format!("badttl2-idk-{}", unique_id());
    let (_id1, e1) = envelope("fx.echo", "badttl2", |v| {
        v["idempotency_key"] = json!(key.clone())
    });
    xadd(c, "badttl2", &e1.to_string()).await;
    wait_write_failures(&log_path, 1).await;
    let (_id2, e2) = envelope("fx.echo", "badttl2", |v| {
        v["idempotency_key"] = json!(key.clone())
    });
    xadd(c, "badttl2", &e2.to_string()).await;
    wait_write_failures(&log_path, 2).await;

    // B: never retryable final failure (SerializationError).
    let (_id3, e3) = envelope("fx.bad_return", "badttl2", |_| {});
    xadd(c, "badttl2", &e3.to_string()).await;
    wait_write_failures(&log_path, 3).await;

    // C: terminal DLQ for an unregistered task.
    let (_id4, e4) = envelope("fx.no_such_task", "badttl2", |_| {});
    xadd(c, "badttl2", &e4.to_string()).await;
    wait_write_failures(&log_path, 4).await;

    // D: expired before dispatch (PROTOCOL §9.1).
    let (_id5, e5) = envelope("fx.echo", "badttl2", |v| {
        v["expires_at"] = json!(now_ms().saturating_sub(60_000))
    });
    xadd(c, "badttl2", &e5.to_string()).await;
    wait_write_failures(&log_path, 5).await;

    bad.signal(libc::SIGTERM);
    assert_eq!(bad.wait_code(20), 0, "badttl2 worker should exit 0");
    drop(bad);

    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    let stats = log
        .lines()
        .rev()
        .find(|l| l.contains("stats: ") && l.contains(" fetched="))
        .unwrap_or_else(|| panic!("no stats line in worker log:\n{log}"));
    // Every write failed, so nothing counts except e1's `failed` from the
    // success gate.
    assert!(
        stats.contains("fetched=5 ok=0 failed=1 retried=0 dlq=0 expired=0"),
        "a failed write on the duplicate, final failure, terminal dlq or \
         expiry path must not be counted as ok, failed, dlq or expired: {stats}"
    );
    let _ = std::fs::remove_file(&log_path);
}
