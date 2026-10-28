//! Per-entry dispatch: parse -> idempotency -> route -> execute -> finish.

use crate::broker;
// Redis anchored clock: expiry deadlines and delayed scores are compared
// across workers (clock.rs).
use crate::clock::now_ms;
use crate::ctx::{Ctx, DecrGuard, InflightEntry, Outcome};
use crate::envelope::{self, Envelope, ErrorJson};
use crate::exec;
use serde_json::value::RawValue;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Called by the fetch and recovery loops. `raw` is stream field `e`.
pub fn spawn_dispatch(ctx: Arc<Ctx>, queue: String, stream_id: String, raw: Option<String>) {
    ctx.counters.fetched.fetch_add(1, Ordering::Relaxed);
    ctx.counters.inflight_total.fetch_add(1, Ordering::SeqCst);
    tokio::spawn(async move {
        // Both guards release on a panicking unwind too.
        let _guard = DecrGuard(&ctx.counters.inflight_total);
        let _held = InflightEntry::track(&ctx, &queue, &stream_id);
        process(&ctx, &queue, &stream_id, raw).await;
    });
}

/// PROTOCOL §2: 32 char lowercase hex. Stops a crafted `id` from
/// overwriting another task's result key, carrying cluster hash tags, or
/// being a key size DoS.
fn valid_task_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// PROTOCOL §2: `args` an array or null, `kwargs` an object or null.
/// Checked after the parse, not in `Deserialize`, so the id survives and a
/// caller in `AsyncResult.get()` gets "Malformed" instead of hanging.
fn args_kwargs_shape_ok(env: &Envelope) -> bool {
    let fits = |raw: &Option<Box<RawValue>>, open: u8| {
        raw.as_deref()
            .is_none_or(|r| r.get().as_bytes()[0] == open && parses_as_tree(r.get()))
    };
    fits(&env.args, b'[') && fits(&env.kwargs, b'{')
}

/// serde_json's 127 level limit minus the envelope object itself.
const MAX_ARG_NESTING: usize = 126;

/// False for what a raw capture accepts but a tree parse refuses: nesting
/// past `MAX_ARG_NESTING`, or half a `\u` surrogate pair. `json` is
/// otherwise valid, so only strings need skipping.
fn parses_as_tree(json: &str) -> bool {
    let b = json.as_bytes();
    // Fast path for almost every payload. A u8 count per 255 byte chunk is
    // what lets the compiler vectorize it.
    let opens: usize = b
        .chunks(255)
        .map(|c| {
            c.iter()
                .fold(0u8, |n, &x| n + u8::from(x == b'[' || x == b'{')) as usize
        })
        .sum();
    if opens <= MAX_ARG_NESTING && !b.contains(&b'\\') {
        return true;
    }
    let unit = |at: usize| {
        b.get(at..at + 4)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u16::from_str_radix(h, 16).ok())
    };
    let trail = |u: u16| (0xDC00..0xE000).contains(&u);
    let (mut depth, mut i, mut in_str) = (0usize, 0usize, false);
    while i < b.len() {
        match (in_str, b[i]) {
            (true, b'\\') if b[i + 1] == b'u' => {
                let u = unit(i + 2).expect("serde_json validated every \\u escape");
                i += 6;
                if trail(u) {
                    return false;
                }
                if (0xD800..0xDC00).contains(&u) {
                    if b.get(i..i + 2) != Some(b"\\u") || !unit(i + 2).is_some_and(trail) {
                        return false;
                    }
                    i += 6;
                }
                continue;
            }
            (true, b'\\') => i += 1,
            (true, b'"') => in_str = false,
            (false, b'"') => in_str = true,
            (false, b'[' | b'{') => {
                depth += 1;
                if depth > MAX_ARG_NESTING {
                    return false;
                }
            }
            (false, b']' | b'}') => depth -= 1,
            _ => {}
        }
        i += 1;
    }
    true
}

async fn process(ctx: &Arc<Ctx>, queue: &str, sid: &str, raw: Option<String>) {
    let raw = match raw {
        Some(r) => r,
        None => {
            // Every terminal reason warns at the default level: a version
            // skew that dead letters a whole queue must not need `debug`.
            warn!(queue, entry = %sid, "stream entry has no \"e\" field -> DLQ");
            return dlq_terminal(ctx, queue, sid, "", "malformed", None).await;
        }
    };
    // Bound parse cost before serde sees the payload.
    if raw.len() > ctx.args.max_envelope_bytes {
        let preview = envelope::safe_truncate(&raw, 4096);
        warn!(
            len = raw.len(),
            cap = ctx.args.max_envelope_bytes,
            "envelope exceeds max size -> DLQ"
        );
        return dlq_terminal(ctx, queue, sid, preview, "malformed", None).await;
    }
    let env = match serde_json::from_str::<Envelope>(&raw) {
        Ok(e) if e.v > envelope::PROTOCOL_VERSION => {
            warn!(
                v = e.v, supported = envelope::PROTOCOL_VERSION, id = %e.id,
                "envelope protocol version unsupported -> DLQ"
            );
            return dlq_terminal(ctx, queue, sid, &raw, "malformed", None).await;
        }
        Ok(e) if !args_kwargs_shape_ok(&e) => {
            warn!(id = %e.id, "args/kwargs wrong shape (must be array/object or null) -> DLQ");
            return dlq_terminal(ctx, queue, sid, &raw, "malformed", None).await;
        }
        Ok(e) if e.timeout_ms == 0 => {
            warn!(id = %e.id, "timeout_ms 0 rejected -> DLQ");
            return dlq_terminal(ctx, queue, sid, &raw, "malformed", None).await;
        }
        Ok(e) if !e.id.is_empty() && !e.task.is_empty() && valid_task_id(&e.id) => e,
        // Unparseable, empty id or task, or bad id charset. Bounded preview.
        _ => {
            warn!(
                queue, entry = %sid,
                preview = envelope::safe_truncate(&raw, 256),
                "envelope unparseable or its id/task is invalid -> DLQ"
            );
            return dlq_terminal(ctx, queue, sid, &raw, "malformed", None).await;
        }
    };
    let Some((is_cpu, is_async)) = ctx
        .registry
        .get(&env.task)
        .map(|spec| (spec.kind == "cpu", spec.is_async))
    else {
        warn!(task = %env.task, id = %env.id, "unregistered task -> DLQ");
        return dlq_terminal(ctx, queue, sid, &raw, "unregistered", None).await;
    };

    // §9.1 expiry is enforced only here: fresh deliveries, mover hand offs,
    // retries and §4.4 reclaims all converge on dispatch. Before the
    // idempotency claim, so an expired task cannot burn the key.
    if let Some(deadline) = env.expiry_deadline_ms(ctx.queue_ttl_ms(queue)) {
        let now = now_ms();
        if now > deadline {
            warn!(
                task = %env.task, id = %env.id, deadline, now,
                late_ms = now.saturating_sub(deadline),
                "task expired before execution -> DLQ"
            );
            let rj = envelope::result_expired(deadline, now);
            let store = env
                .store_result
                .then_some((env.id.as_str(), rj.as_str(), ctx.result_ttl));
            match broker::finish_dlq(&ctx.acks, queue, sid, &raw, "expired", None, store).await {
                Ok(()) => {
                    ctx.counters.expired.fetch_add(1, Ordering::Relaxed);
                    ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    error!(id = %env.id, "expired finish write failed: {e}");
                }
            }
            return;
        }
    }

    // §4.5 claim. Its TTL also depends on timeout_ms (broker::claim_ttl_s).
    if let Some(key) = env.idempotency_key.clone() {
        let mut conn = ctx.redis.clone();
        match broker::idemp_claim(&mut conn, &key, &env.id, ctx.idemp_ttl, env.timeout_ms).await {
            Ok(broker::IdempClaim::Fresh) | Ok(broker::IdempClaim::MineAgain) => {}
            Ok(broker::IdempClaim::Duplicate { claimant }) => {
                // Never released, even after the claimant is dead lettered
                // (partial side effects make suppression safer).
                debug!(id = %env.id, claimant = %claimant, "idempotency key held -> duplicate");
                let rj = envelope::result_duplicate(now_ms(), &claimant);
                let store = env.store_result.then_some(rj.as_str());
                match broker::finish_duplicate(
                    &ctx.acks,
                    queue,
                    sid,
                    &env.id,
                    store,
                    ctx.result_ttl,
                )
                .await
                {
                    Ok(()) => {
                        ctx.counters.ok.fetch_add(1, Ordering::Relaxed);
                        ctx.counters.duplicate.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!(id = %env.id, "duplicate finish write failed: {e}");
                    }
                }
                return;
            }
            Err(e) => {
                // Fail open, as PROTOCOL §4.5 specifies.
                warn!(id = %env.id, "idempotency SET failed ({e}); executing anyway");
            }
        }
    }

    // The registry kind wins over the envelope kind.
    let outcome = if is_cpu {
        exec::run_cpu_task(ctx, &env).await
    } else if is_async {
        exec::run_async_task(ctx, &env).await
    } else {
        exec::run_sync_task(ctx, &env).await
    };
    finish(ctx, queue, sid, env, outcome).await;
}

/// §4.1 / §4.2 completion handling.
async fn finish(ctx: &Arc<Ctx>, queue: &str, sid: &str, mut env: Envelope, outcome: Outcome) {
    let now = now_ms();
    match outcome {
        Outcome::Success(v) => {
            let rj = env.store_result.then(|| envelope::result_success(&v, now));
            let store = rj.as_deref();
            match broker::finish_success(&ctx.acks, queue, sid, &env.id, store, ctx.result_ttl)
                .await
            {
                Ok(()) => {
                    ctx.counters.ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    // Every finish branch counts only a write that landed,
                    // or the stats line hides a broker failure.
                    error!(id = %env.id, "success finish write failed: {e}");
                    ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Outcome::ForceRetry { countdown, err } => {
            if env.retries < env.max_retries {
                let cd_ms = countdown.map(|s| (s.max(0.0) * 1000.0).round() as u64);
                schedule_retry(ctx, queue, sid, &mut env, cd_ms).await;
            } else {
                final_failure(ctx, queue, sid, &env, &err, now, "max_retries").await;
            }
        }
        Outcome::Failure { err, retryable } => {
            if retryable && env.retries < env.max_retries {
                schedule_retry(ctx, queue, sid, &mut env, None).await;
            } else {
                // PROTOCOL §4.2: "max_retries" only when the budget ran out.
                let reason = if retryable {
                    "max_retries"
                } else {
                    "not_retryable"
                };
                final_failure(ctx, queue, sid, &env, &err, now, reason).await;
            }
        }
    }
}

/// Retry delay clamp. The delay is bounded, not the instant: a check on
/// `fire_at < now + horizon` passes whenever the clock itself is wrong.
const MAX_RETRY_DELAY_MS: u64 = 30 * 86_400_000; // 30 days

/// `Retry(countdown=...)` or the computed backoff, both unbounded at their
/// source, so the clamp lives where they meet.
fn retry_delay_ms(env: &Envelope, countdown_ms: Option<u64>) -> u64 {
    countdown_ms
        .unwrap_or_else(|| {
            crate::backoff::compute_backoff_ms(
                env.retries,
                env.backoff_base_ms,
                env.backoff_factor,
                env.backoff_max_ms,
                env.jitter,
            )
        })
        .min(MAX_RETRY_DELAY_MS)
}

/// §4.2 steps 1-4. `countdown_ms` overrides the computed backoff (cauli.Retry).
async fn schedule_retry(
    ctx: &Arc<Ctx>,
    queue: &str,
    sid: &str,
    env: &mut Envelope,
    countdown_ms: Option<u64>,
) {
    env.retries += 1;
    let d_ms = retry_delay_ms(env, countdown_ms);
    // Saturating: a wrapped score would fire immediately and hot loop.
    let fire_at = now_ms().saturating_add(d_ms);
    // Parsed from valid JSON moments ago, so it cannot fail.
    let ej = serde_json::to_string(env).expect("envelope serialize");
    match broker::finish_retry(&ctx.acks, queue, sid, &ej, fire_at).await {
        Ok(()) => {
            debug!(id = %env.id, retries = env.retries, delay_ms = d_ms, "scheduled retry");
            ctx.counters.retried.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            error!(id = %env.id, "retry write failed: {e}");
        }
    }
}

/// DLQ with `reason` ("max_retries" or "not_retryable") plus a failure result.
async fn final_failure(
    ctx: &Arc<Ctx>,
    queue: &str,
    sid: &str,
    env: &Envelope,
    err: &ErrorJson,
    now: u64,
    reason: &str,
) {
    let ej = serde_json::to_string(env).expect("envelope serialize");
    let rj = envelope::result_failure(err, now);
    let result = env
        .store_result
        .then_some((env.id.as_str(), rj.as_str(), ctx.result_ttl));
    match broker::finish_dlq(&ctx.acks, queue, sid, &ej, reason, Some(err), result).await {
        Ok(()) => {
            ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
            ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            error!(id = %env.id, "dlq write failed: {e}");
        }
    }
}

/// Best effort id for a terminal DLQ write, parsed from a 4096 byte preview
/// so a huge hostile payload is never fully parsed.
fn recover_id(raw_e: &str) -> Option<String> {
    // Only the id: some other field is what failed to parse. `raw_e` is
    // already bounded: an oversize envelope arrives here as its preview.
    #[derive(serde::Deserialize)]
    struct IdOnly {
        id: String,
    }
    match serde_json::from_str::<IdOnly>(raw_e) {
        Ok(e) if valid_task_id(&e.id) => Some(e.id),
        _ => None,
    }
}

/// Error for a task that never ran, typed so callers can tell it from a
/// real exception.
fn dlq_error(reason: &str) -> ErrorJson {
    let type_ = match reason {
        "malformed" => "Malformed",
        "unregistered" => "UnregisteredTask",
        "redelivery_limit" => "RedeliveryLimitExceeded",
        _ => "DeadLettered",
    };
    ErrorJson::new(
        type_,
        format!("task was dead lettered before it ran (reason {reason})"),
    )
}

/// Terminal DLQ, no retry. When an id is recoverable a failure result is
/// written too, so `AsyncResult.get()` without a timeout does not hang.
pub async fn dlq_terminal(
    ctx: &Arc<Ctx>,
    queue: &str,
    sid: &str,
    raw_e: &str,
    reason: &str,
    err: Option<&ErrorJson>,
) {
    let recovered_id = recover_id(raw_e);
    let synthesized = err.is_none().then(|| dlq_error(reason));
    let result_error = err.or(synthesized.as_ref());
    let result_json = match (&recovered_id, result_error) {
        (Some(_), Some(e)) => Some(envelope::result_failure(e, now_ms())),
        _ => None,
    };
    let store = match (&recovered_id, &result_json) {
        (Some(id), Some(rj)) => Some((id.as_str(), rj.as_str(), ctx.result_ttl)),
        _ => None,
    };
    match broker::finish_dlq(&ctx.acks, queue, sid, raw_e, reason, err, store).await {
        Ok(()) => {
            ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            error!(reason, "terminal dlq write failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_task_id_accepts_32_lowercase_hex() {
        assert!(valid_task_id("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(valid_task_id("0123456789abcdef0123456789abcdef"));
    }

    fn env_with(json: &str) -> Envelope {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn args_kwargs_shape_ok_accepts_array_object_or_null() {
        assert!(args_kwargs_shape_ok(&env_with(r#"{"id":"a","task":"t"}"#)));
        assert!(args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","args":null,"kwargs":null}"#
        )));
        assert!(args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","args":[],"kwargs":{}}"#
        )));
        assert!(args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","args":[1,"x"],"kwargs":{"k":true}}"#
        )));
    }

    #[test]
    fn args_kwargs_shape_ok_rejects_wrong_types() {
        assert!(!args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","kwargs":[1,2]}"#
        )));
        assert!(!args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","kwargs":"x"}"#
        )));
        assert!(!args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","kwargs":5}"#
        )));
        assert!(!args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","args":{"a":1}}"#
        )));
        assert!(!args_kwargs_shape_ok(&env_with(
            r#"{"id":"a","task":"t","args":"x"}"#
        )));
    }

    /// Must accept exactly what a `Value` parse accepts.
    #[test]
    fn args_kwargs_nesting_bound_matches_the_full_parse_it_replaced() {
        let nested = |open: &str, close: &str, depth: usize| {
            let mut s = open.repeat(depth - 1);
            s.push_str(r#"["\"[{",1]"#);
            s.push_str(&close.repeat(depth - 1));
            s
        };
        for depth in [
            MAX_ARG_NESTING - 1,
            MAX_ARG_NESTING,
            MAX_ARG_NESTING + 1,
            5000,
        ] {
            let args = format!(
                r#"{{"id":"a","task":"t","args":{}}}"#,
                nested("[", "]", depth)
            );
            let kwargs = format!(
                r#"{{"id":"a","task":"t","kwargs":{{"k":{}}}}}"#,
                nested("[", "]", depth - 1)
            );
            for raw in [args, kwargs] {
                let full_parse_ok = serde_json::from_str::<serde_json::Value>(&raw).is_ok();
                assert_eq!(full_parse_ok, depth <= MAX_ARG_NESTING, "depth {depth}");
                assert_eq!(
                    args_kwargs_shape_ok(&env_with(&raw)),
                    full_parse_ok,
                    "depth {depth}"
                );
            }
        }
    }

    #[test]
    fn surrogate_escapes_are_refused_exactly_where_the_full_parse_refused_them() {
        let cases = [
            (r#""\ud83d\ude00""#, true),
            (r#""\u00e9\u2028""#, true),
            (r#""\\ud800""#, true),
            (r#""a\udc80""#, false),
            (r#""\ud83d""#, false),
            (r#""\ud83dx""#, false),
            (r#""\ud83d\n""#, false),
            (r#""\ud83d\u0041""#, false),
            (r#""\ud83d\ud83d\ude00""#, false),
        ];
        for (text, ok) in cases {
            for raw in [
                format!(r#"{{"id":"a","task":"t","args":[1,{text}]}}"#),
                format!(r#"{{"id":"a","task":"t","kwargs":{{{text}:1}}}}"#),
            ] {
                let full_parse_ok = serde_json::from_str::<serde_json::Value>(&raw).is_ok();
                assert_eq!(full_parse_ok, ok, "{raw}");
                assert_eq!(args_kwargs_shape_ok(&env_with(&raw)), ok, "{raw}");
            }
        }
    }

    #[test]
    fn valid_task_id_rejects_malformed_ids() {
        assert!(!valid_task_id(""));
        assert!(!valid_task_id("too-short"));
        assert!(!valid_task_id(&"a".repeat(33))); // too long
        assert!(!valid_task_id(&"a".repeat(31))); // too short
        assert!(!valid_task_id(&"A".repeat(32))); // uppercase rejected
        assert!(!valid_task_id(&"g".repeat(32))); // right length, non-hex letter
        assert!(!valid_task_id(&format!("{{{}", "a".repeat(31))));
    }

    #[test]
    fn retry_delay_is_clamped_at_thirty_days() {
        let env = env_with(
            r#"{"id":"a","task":"t","retries":1,"backoff_base_ms":18446744073709551615,
                "backoff_max_ms":18446744073709551615,"jitter":false}"#,
        );
        assert_eq!(retry_delay_ms(&env, None), MAX_RETRY_DELAY_MS);
        assert_eq!(retry_delay_ms(&env, Some(u64::MAX)), MAX_RETRY_DELAY_MS);
        assert_eq!(retry_delay_ms(&env, Some(1_500)), 1_500);
        assert_eq!(
            retry_delay_ms(&env, Some(MAX_RETRY_DELAY_MS - 1)),
            MAX_RETRY_DELAY_MS - 1
        );
    }

    #[test]
    fn recover_id_from_a_fully_parseable_envelope() {
        let raw = r#"{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","task":"t"}"#;
        assert_eq!(
            recover_id(raw),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string())
        );
    }

    #[test]
    fn recover_id_from_an_envelope_malformed_in_another_field() {
        let raw = r#"{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","task":"t","idempotency_key":42,"max_retries":-1}"#;
        assert_eq!(
            recover_id(raw),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string())
        );
    }

    /// An unregistered task with large args is dead lettered before it runs;
    /// its caller still needs the failure result. Oversize envelopes never
    /// reach here whole: the size check passes only a preview.
    #[test]
    fn recover_id_from_an_envelope_larger_than_any_preview() {
        let raw = format!(
            r#"{{"pad":"{}","id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","task":"t"}}"#,
            "x".repeat(5000)
        );
        assert_eq!(
            recover_id(&raw),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string())
        );
    }

    #[test]
    fn recover_id_none_for_an_id_that_fails_the_charset_gate() {
        let raw = r#"{"id":"not-32-hex","task":"t"}"#;
        assert_eq!(recover_id(raw), None);
    }

    #[test]
    fn recover_id_none_for_unparseable_input() {
        assert_eq!(recover_id(""), None);
        assert_eq!(recover_id("not json"), None);
    }
}
