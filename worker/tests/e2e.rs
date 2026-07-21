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
        .arg("rupy:q:default")
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
            .arg("rupy:delayed:default")
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
        .arg("rupy:delayed:default")
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
    let cf = format!("/tmp/rupy-flaky-{}", unique_id());
    let (id, e) = envelope("fx.flaky", "default", |v| {
        v["args"] = json!([cf, 2]);
        v["backoff_base_ms"] = json!(50);
        v["jitter"] = json!(false);
    });
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 15).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"], 3, "flaky should succeed on 3rd attempt");

    // 7. rupy.Retry(countdown) forced retry then success
    let cf = format!("/tmp/rupy-retryonce-{}", unique_id());
    let (id, e) = envelope("fx.retry_once", "default", |v| v["args"] = json!([cf]));
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 15).await;
    assert_eq!(r["status"], "success");
    assert_eq!(r["result"], "after-retry");

    // 8. non-serializable return: final failure, NO retry despite max_retries
    let (id, e) = envelope("fx.bad_return", "default", |v| v["max_retries"] = json!(3));
    xadd(&mut c, "default", &e.to_string()).await;
    let r = wait_result(&mut c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "SerializationError");
    let (reason, _, efield) = wait_dlq(&mut c, "default", &id, 5).await;
    assert_eq!(reason, "max_retries");
    let dlq_env: serde_json::Value = serde_json::from_str(&efield).unwrap();
    assert_eq!(
        dlq_env["retries"], 0,
        "SerializationError must not consume retries"
    );

    idempotency_and_timeouts(&mut c).await;

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
    // The idemp key is stored hashed (rupy:idemp:{fnv1a-hex}), not the raw
    // app-supplied key (M1 hardening), so scan for it rather than assuming
    // the literal key name.
    let idemp_keys: Vec<String> = redis::cmd("KEYS")
        .arg("rupy:idemp:*")
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

    // C1 regression: idempotency_key + a task that fails once then succeeds
    // must actually retry and finish "success", not silently resolve as
    // "duplicate" against its own earlier claim.
    let cf = format!("/tmp/rupy-c1-idemp-retry-{}", unique_id());
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

    // H3 regression: a crafted timeout_ms of u64::MAX must use saturating
    // arithmetic (not wrap the Rust-side backstop to a near-zero duration,
    // which would spuriously fail a task that legitimately takes a few
    // seconds) and must not panic.
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

    // async hard timeout -> retryable TimeoutError; max_retries 0 -> DLQ now
    let (id, e) = envelope("fx.aslow", "default", |v| {
        v["args"] = json!([30.0]);
        v["timeout_ms"] = json!(700);
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeoutError");
    let (reason, _, _) = wait_dlq(c, "default", &id, 5).await;
    assert_eq!(reason, "max_retries");

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

    // sync hard timeout: thread abandoned, retryable TimeoutError
    let (id, e) = envelope("fx.slow", "default", |v| {
        v["args"] = json!([3.0]);
        v["timeout_ms"] = json!(600);
        v["max_retries"] = json!(0);
    });
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 10).await;
    assert_eq!(r["status"], "failure");
    assert_eq!(r["error"]["type"], "TimeoutError");

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
    assert_eq!(r["error"]["type"], "TimeoutError");
    let (id, e) = envelope("fx.cpu_echo", "default", |v| v["kind"] = json!("cpu"));
    xadd(c, "default", &e.to_string()).await;
    let r = wait_result(c, &id, 15).await;
    assert_eq!(r["status"], "success", "cpu pool must survive kill+respawn");

    // unregistered task -> DLQ, no retry
    let (id, e) = envelope("no.such.task", "default", |v| v["max_retries"] = json!(5));
    xadd(c, "default", &e.to_string()).await;
    let (reason, _, _) = wait_dlq(c, "default", &id, 10).await;
    assert_eq!(reason, "unregistered");

    // malformed payload -> DLQ with raw payload preserved
    let raw = "{this is not json";
    xadd(c, "default", raw).await;
    let (reason, _, efield) = wait_dlq(c, "default", "this is not json", 10).await;
    assert_eq!(reason, "malformed");
    assert_eq!(efield, raw);

    // M1 regression: a crafted id that doesn't match [a-z0-9]{32} -> DLQ
    // malformed, never executed (protects rupy:result:{id} from collision).
    let (_, mut bad_id_env) = envelope("fx.echo", "default", |_| {});
    bad_id_env["id"] = json!("not-a-valid-32-char-lowercase-hex-id");
    xadd(c, "default", &bad_id_env.to_string()).await;
    let (reason, _, _) = wait_dlq(c, "default", "not-a-valid-32-char-lowercase-hex-id", 10).await;
    assert_eq!(reason, "malformed");

    // M2 regression: an envelope larger than --max-envelope-bytes (default 1
    // MiB) -> DLQ malformed before it is ever parsed, with only a truncated
    // preview stored (not the full oversize payload).
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
