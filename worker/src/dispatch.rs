//! Per-entry dispatch: parse -> idempotency -> route -> execute -> finish.

use crate::broker;
use crate::ctx::{now_ms, Ctx, DecrGuard, Outcome};
use crate::envelope::{self, Envelope, ErrorJson};
use crate::exec;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// Entry point used by the fetch loop and the recovery loop.
/// `raw` is the value of stream field `e` (None if the field was missing).
pub fn spawn_dispatch(ctx: Arc<Ctx>, queue: String, stream_id: String, raw: Option<String>) {
    ctx.counters.fetched.fetch_add(1, Ordering::Relaxed);
    ctx.counters.inflight_total.fetch_add(1, Ordering::SeqCst);
    tokio::spawn(async move {
        // Panic-safe (MEM-3/H3): a panic anywhere in `process` must still
        // release this slot, or `inflight_total` never reaches 0 and every
        // shutdown burns the full --drain-timeout.
        let _guard = DecrGuard(&ctx.counters.inflight_total);
        process(&ctx, &queue, &stream_id, raw).await;
    });
}

/// Task id charset per PROTOCOL §2 ("32 char lowercase hex"). Rejecting
/// anything else worker-side (audit M1) stops a crafted `id` from colliding
/// with / overwriting another task's `cauli:result:{id}` key, from carrying
/// cluster hash-tags (`{...}`), or from being a key-size DoS.
fn valid_task_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

async fn process(ctx: &Arc<Ctx>, queue: &str, sid: &str, raw: Option<String>) {
    let raw = match raw {
        Some(r) => r,
        None => return dlq_terminal(ctx, queue, sid, "", "malformed", None).await,
    };
    // M2: bound parse/amplification cost before serde ever sees the payload.
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
        Ok(e) if !e.id.is_empty() && !e.task.is_empty() && valid_task_id(&e.id) => e,
        _ => return dlq_terminal(ctx, queue, sid, &raw, "malformed", None).await,
    };
    let Some(spec) = ctx.registry.get(&env.task).cloned() else {
        debug!(task = %env.task, id = %env.id, "unregistered task -> DLQ");
        return dlq_terminal(ctx, queue, sid, &raw, "unregistered", None).await;
    };

    // §9.1 expiry, checked at DISPATCH — the single enforcement point, and
    // deliberately so. It sits here rather than at enqueue (which cannot know
    // how long the entry will actually wait), in the delayed mover (which only
    // sees delayed entries, not backlogged ready ones) or in the fetch loop
    // (which has not parsed the envelope yet). Dispatch is the one place every
    // path converges: a fresh delivery, a mover hand-off, a scheduled retry
    // and a §4.4 crash reclaim all arrive here, so "expired work never runs"
    // holds for all of them with one check. It is also the only placement that
    // needs nothing from the broker, which is what lets a future SQS or
    // RabbitMQ backend inherit expiry for free.
    //
    // Before the idempotency claim: an expired task must not burn the
    // idempotency key and lock out a later, still-valid task with the same one.
    if let Some(deadline) = env.expiry_deadline_ms(ctx.queue_ttl_ms(queue)) {
        let now = now_ms();
        if now > deadline {
            debug!(
                task = %env.task, id = %env.id, deadline, now,
                late_ms = now.saturating_sub(deadline),
                "task expired before execution -> DLQ"
            );
            let rj = envelope::result_expired(deadline, now);
            let store = env
                .store_result
                .then_some((env.id.as_str(), rj.as_str(), ctx.result_ttl));
            let mut conn = ctx.redis.clone();
            if let Err(e) =
                broker::finish_dlq(&mut conn, queue, sid, &raw, "expired", None, store).await
            {
                error!(id = %env.id, "expired finish write failed: {e}");
            }
            ctx.counters.expired.fetch_add(1, Ordering::Relaxed);
            ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }

    // §4.5 idempotency guard, claimed at execution start.
    if let Some(key) = env.idempotency_key.clone() {
        let mut conn = ctx.redis.clone();
        match broker::idemp_claim(&mut conn, &key, &env.id, ctx.idemp_ttl).await {
            Ok(broker::IdempClaim::Fresh) | Ok(broker::IdempClaim::MineAgain) => {}
            Ok(broker::IdempClaim::Duplicate) => {
                let rj = envelope::result_duplicate(now_ms());
                let store = env.store_result.then_some(rj.as_str());
                if let Err(e) =
                    broker::finish_duplicate(&mut conn, queue, sid, &env.id, store, ctx.result_ttl)
                        .await
                {
                    error!(id = %env.id, "duplicate finish write failed: {e}");
                }
                ctx.counters.ok.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(e) => {
                // Fail open: at-least-once semantics allow execution; log it.
                // (PROTOCOL §4.5 documents this as an explicit, deliberate choice.)
                warn!(id = %env.id, "idempotency SET failed ({e}); executing anyway");
            }
        }
    }

    // Route by registry kind (registry authoritative over envelope kind).
    let outcome = if spec.kind == "cpu" {
        exec::run_cpu_task(ctx, &env).await
    } else if spec.is_async {
        exec::run_async_task(ctx, &env).await
    } else {
        exec::run_sync_task(ctx, &env).await
    };
    finish(ctx, queue, sid, env, outcome).await;
}

/// §4.1 / §4.2 completion handling.
async fn finish(ctx: &Arc<Ctx>, queue: &str, sid: &str, mut env: Envelope, outcome: Outcome) {
    let mut conn = ctx.redis.clone();
    let now = now_ms();
    match outcome {
        Outcome::Success(v) => {
            // Built only when it is actually stored: serializing a result the
            // caller has opted out of receiving is pure cost, and it scales
            // with the size of whatever the task returned.
            let rj = env.store_result.then(|| envelope::result_success(&v, now));
            let store = rj.as_deref();
            match broker::finish_success(&mut conn, queue, sid, &env.id, store, ctx.result_ttl)
                .await
            {
                Ok(()) => {
                    ctx.counters.ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    // A write failure must not count as ok: the stats line is
                    // the operator's first signal, and folding this in would
                    // hide a broker write failure behind a success number.
                    error!(id = %env.id, "success finish write failed: {e}");
                    ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Outcome::ForceRetry { countdown, err } => {
            if env.retries < env.max_retries {
                let cd_ms = countdown.map(|s| (s.max(0.0) * 1000.0).round() as u64);
                schedule_retry(ctx, &mut conn, queue, sid, &mut env, cd_ms).await;
            } else {
                final_failure(ctx, &mut conn, queue, sid, &env, &err, now).await;
            }
        }
        Outcome::Failure { err, retryable } => {
            if retryable && env.retries < env.max_retries {
                schedule_retry(ctx, &mut conn, queue, sid, &mut env, None).await;
            } else {
                final_failure(ctx, &mut conn, queue, sid, &env, &err, now).await;
            }
        }
    }
}

/// §4.2 steps 1-4. `countdown_ms` overrides the computed backoff (cauli.Retry).
async fn schedule_retry(
    ctx: &Arc<Ctx>,
    conn: &mut redis::aio::ConnectionManager,
    queue: &str,
    sid: &str,
    env: &mut Envelope,
    countdown_ms: Option<u64>,
) {
    env.retries += 1;
    let d_ms = countdown_ms.unwrap_or_else(|| {
        crate::backoff::compute_backoff_ms(
            env.retries,
            env.backoff_base_ms,
            env.backoff_factor,
            env.backoff_max_ms,
            env.jitter,
        )
    });
    // saturating_add: H3 — an attacker-chosen backoff_max_ms/countdown near
    // u64::MAX must not wrap fire_at to a tiny score (which would fire the
    // retry immediately, hot-looping until max_retries).
    let fire_at = now_ms().saturating_add(d_ms);
    // Infallible: `env` round-tripped through serde_json moments ago (parsed
    // from valid JSON, so no NaN/Infinity survived), so re-serializing it
    // cannot fail.
    let ej = serde_json::to_string(env).expect("envelope serialize");
    if let Err(e) = broker::finish_retry(conn, queue, sid, &ej, fire_at).await {
        error!(id = %env.id, "retry write failed: {e}");
    }
    debug!(id = %env.id, retries = env.retries, delay_ms = d_ms, "scheduled retry");
    ctx.counters.retried.fetch_add(1, Ordering::Relaxed);
}

/// Final failure: DLQ reason "max_retries" + failure result (if store_result).
async fn final_failure(
    ctx: &Arc<Ctx>,
    conn: &mut redis::aio::ConnectionManager,
    queue: &str,
    sid: &str,
    env: &Envelope,
    err: &ErrorJson,
    now: u64,
) {
    // Infallible: same reasoning as schedule_retry above.
    let ej = serde_json::to_string(env).expect("envelope serialize");
    let rj = envelope::result_failure(err, now);
    let result = env
        .store_result
        .then_some((env.id.as_str(), rj.as_str(), ctx.result_ttl));
    if let Err(e) =
        broker::finish_dlq(conn, queue, sid, &ej, "max_retries", Some(err), result).await
    {
        error!(id = %env.id, "dlq write failed: {e}");
    }
    ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
    ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
}

/// Best effort task id recovery for a terminal DLQ write: bounded to the
/// same preview cap as the oversize path (`safe_truncate`, 4096 bytes), so a
/// hostile huge `raw_e` can never turn this into the parse that §M2 exists
/// to avoid. A small envelope that is only oversize relative to a low
/// --max-envelope-bytes, or one already fully read (unregistered,
/// redelivery_limit), still parses inside that cap and its id comes back.
fn recover_id(raw_e: &str) -> Option<String> {
    let preview = envelope::safe_truncate(raw_e, 4096);
    match serde_json::from_str::<Envelope>(preview) {
        Ok(e) if valid_task_id(&e.id) => Some(e.id),
        _ => None,
    }
}

/// Synthetic error for a task that never ran: no Python exception exists,
/// so a type distinguishable from a real one (mirrors `result_expired`'s
/// "Expired") lets a caller's `except TaskFailedError` tell them apart.
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

/// Terminal DLQ for malformed / unregistered / redelivery_limit entries: no
/// retry. When a task id can be recovered from `raw_e` (see `recover_id`), a
/// failure result is written too, so a caller blocked in
/// `AsyncResult.get()` with no timeout gets an answer instead of waiting on
/// a `cauli:result:{id}` key that would otherwise never exist. Where no id
/// can be recovered there is nothing to key a result on, so none is
/// written, same as before. Error field in the DLQ stream entry is still
/// the empty string when `err` is None.
pub async fn dlq_terminal(
    ctx: &Arc<Ctx>,
    queue: &str,
    sid: &str,
    raw_e: &str,
    reason: &str,
    err: Option<&ErrorJson>,
) {
    let mut conn = ctx.redis.clone();
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
    if let Err(e) = broker::finish_dlq(&mut conn, queue, sid, raw_e, reason, err, store).await {
        error!(reason, "terminal dlq write failed: {e}");
    }
    ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_task_id_accepts_32_lowercase_hex() {
        assert!(valid_task_id("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(valid_task_id("0123456789abcdef0123456789abcdef"));
    }

    #[test]
    fn valid_task_id_rejects_malformed_ids() {
        assert!(!valid_task_id(""));
        assert!(!valid_task_id("too-short"));
        assert!(!valid_task_id(&"a".repeat(33))); // too long
        assert!(!valid_task_id(&"a".repeat(31))); // too short
        assert!(!valid_task_id(&"A".repeat(32))); // uppercase rejected
        assert!(!valid_task_id(&"g".repeat(32))); // right length, non-hex letter
                                                  // right length, one invalid char (hash-tag injection attempt)
        assert!(!valid_task_id(&format!("{{{}", "a".repeat(31))));
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
    fn recover_id_none_when_the_id_is_past_the_preview_cap() {
        // A hostile oversize payload never gets its id back: the preview
        // cap (4096 bytes) lands inside the padding, well before the id
        // field or the closing brace, so this can never become the
        // unbounded parse §M2 exists to avoid.
        let raw = format!(
            r#"{{"pad":"{}","id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","task":"t"}}"#,
            "x".repeat(5000)
        );
        assert_eq!(recover_id(&raw), None);
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
