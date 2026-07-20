//! Per-entry dispatch: parse -> idempotency -> route -> execute -> finish.

use crate::broker;
use crate::ctx::{now_ms, Ctx, Outcome};
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
        process(&ctx, &queue, &stream_id, raw).await;
        ctx.counters.inflight_total.fetch_sub(1, Ordering::SeqCst);
    });
}

async fn process(ctx: &Arc<Ctx>, queue: &str, sid: &str, raw: Option<String>) {
    let raw = match raw {
        Some(r) => r,
        None => return dlq_terminal(ctx, queue, sid, "", "malformed", None, None).await,
    };
    let env = match serde_json::from_str::<Envelope>(&raw) {
        Ok(e) if !e.id.is_empty() && !e.task.is_empty() => e,
        _ => return dlq_terminal(ctx, queue, sid, &raw, "malformed", None, None).await,
    };
    let Some(spec) = ctx.registry.get(&env.task).cloned() else {
        debug!(task = %env.task, id = %env.id, "unregistered task -> DLQ");
        return dlq_terminal(ctx, queue, sid, &raw, "unregistered", None, None).await;
    };

    // §4.5 idempotency guard, claimed at execution start.
    if let Some(key) = env.idempotency_key.clone() {
        let mut conn = ctx.redis.clone();
        match broker::idemp_claim(&mut conn, &key, &env.id, ctx.idemp_ttl).await {
            Ok(true) => {}
            Ok(false) => {
                let rj = envelope::result_duplicate(now_ms());
                let store = env.store_result.then_some(rj.as_str());
                if let Err(e) =
                    broker::finish_duplicate(&mut conn, queue, sid, &env.id, store, ctx.result_ttl).await
                {
                    error!(id = %env.id, "duplicate finish write failed: {e}");
                }
                ctx.counters.ok.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(e) => {
                // Fail open: at-least-once semantics allow execution; log it.
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
            let rj = envelope::result_success(&v, now);
            let store = env.store_result.then_some(rj.as_str());
            if let Err(e) =
                broker::finish_success(&mut conn, queue, sid, &env.id, store, ctx.result_ttl).await
            {
                error!(id = %env.id, "success finish write failed: {e}");
            }
            ctx.counters.ok.fetch_add(1, Ordering::Relaxed);
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

/// §4.2 steps 1-4. `countdown_ms` overrides the computed backoff (rupy.Retry).
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
    let fire_at = now_ms() + d_ms;
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
    let ej = serde_json::to_string(env).expect("envelope serialize");
    let rj = envelope::result_failure(err, now);
    let result = env
        .store_result
        .then_some((env.id.as_str(), rj.as_str(), ctx.result_ttl));
    if let Err(e) = broker::finish_dlq(conn, queue, sid, &ej, "max_retries", Some(err), result).await
    {
        error!(id = %env.id, "dlq write failed: {e}");
    }
    ctx.counters.failed.fetch_add(1, Ordering::Relaxed);
    ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
}

/// Terminal DLQ for malformed / unregistered / redelivery_limit entries
/// (no retry, no result key; error field empty string when None).
pub async fn dlq_terminal(
    ctx: &Arc<Ctx>,
    queue: &str,
    sid: &str,
    raw_e: &str,
    reason: &str,
    err: Option<&ErrorJson>,
    _unused: Option<()>,
) {
    let mut conn = ctx.redis.clone();
    if let Err(e) = broker::finish_dlq(&mut conn, queue, sid, raw_e, reason, err, None).await {
        error!(reason, "terminal dlq write failed: {e}");
    }
    ctx.counters.dlq.fetch_add(1, Ordering::Relaxed);
}
