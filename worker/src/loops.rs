//! Long-running loops: fetch (XREADGROUP), delayed mover, recovery
//! (XPENDING/XCLAIM), stats. All spawned from main.

use crate::broker;
use crate::ctx::{now_ms, Ctx};
use crate::dispatch::{dlq_terminal, spawn_dispatch};
use crate::envelope::{redelivery_limit, Envelope};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// §4 fetch loop. Gate: fetch only when io slots are free and no cpu dispatch
/// is blocked on a full backlog (bounded starvation, see ARCHITECTURE.md).
pub async fn fetch_loop(ctx: Arc<Ctx>, mut fetch_conn: redis::aio::ConnectionManager) {
    let keys: Vec<String> = ctx.queues.iter().map(|q| broker::q_key(q)).collect();
    let ids: Vec<&str> = ctx.queues.iter().map(|_| ">").collect();
    while !ctx.shutting_down() {
        if ctx.io_sem.available_permits() == 0 || ctx.cpu.overflow.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        }
        let opts = StreamReadOptions::default()
            .group("cauli", &ctx.consumer)
            .count(ctx.args.batch)
            .block(1000);
        let reply: Option<StreamReadReply> =
            match fetch_conn.xread_options(&keys, &ids, &opts).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("XREADGROUP failed: {e}; backing off 500ms");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
        let Some(reply) = reply else { continue };
        for sk in reply.keys {
            let queue = sk
                .key
                .strip_prefix("cauli:q:")
                .unwrap_or(&sk.key)
                .to_string();
            for entry in sk.ids {
                let raw = entry
                    .map
                    .get("e")
                    .and_then(|v| redis::from_redis_value::<String>(v).ok());
                spawn_dispatch(ctx.clone(), queue.clone(), entry.id.clone(), raw);
            }
        }
    }
    info!("fetch loop stopped (shutdown)");
}

/// §4.3 delayed mover: every 250ms per queue, single EVAL each.
pub async fn mover_loop(ctx: Arc<Ctx>) {
    let script = redis::Script::new(broker::MOVER_LUA);
    let mut conn = ctx.redis.clone();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        for q in &ctx.queues {
            if let Err(e) = broker::run_mover(&mut conn, &script, q, now_ms()).await {
                warn!(queue = %q, "delayed mover failed: {e}");
            }
        }
    }
}

/// §4.4 recovery: every visibility_timeout/2 per queue.
///
/// H1 fix: `--visibility-timeout` is a FLOOR, not the reclaim threshold for
/// every task. XPENDING IDLE uses it to shortlist candidates, but before
/// actually reclaiming (XCLAIM) any of them we peek the envelope (read-only;
/// does not touch the PEL) and require idle >= max(visibility_timeout_ms,
/// envelope.timeout_ms + grace). Without this, a single worker running a
/// legitimate long task (timeout_ms > visibility_timeout) would XCLAIM its
/// OWN in-flight entry and execute it a second time, concurrently, in the
/// same process — a production trap with the documented defaults (60s
/// visibility vs 300s default task timeout). Unparseable envelopes fall back
/// to the visibility_timeout floor alone (best we can do without knowing
/// their real timeout).
pub async fn recovery_loop(ctx: Arc<Ctx>) {
    let vt_ms = ctx.args.visibility_timeout * 1000;
    let period = Duration::from_millis((vt_ms / 2).max(500));
    let mut conn = ctx.redis.clone();
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if ctx.shutting_down() {
            continue; // no new work during drain; mover/acks keep running
        }
        for q in &ctx.queues {
            let pend = match broker::xpending_idle(&mut conn, q, vt_ms, ctx.args.batch).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(queue = %q, "XPENDING failed: {e}");
                    continue;
                }
            };
            for (entry_id, _consumer, idle, delivery_count) in pend {
                let peeked = match broker::peek_entry(&mut conn, q, &entry_id).await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(queue = %q, entry = %entry_id, "peek (XRANGE) failed: {e}");
                        continue;
                    }
                };
                let Some(raw_opt) = peeked else {
                    continue; // entry vanished (acked/claimed elsewhere) since XPENDING
                };
                let parsed = raw_opt
                    .as_deref()
                    .and_then(|r| serde_json::from_str::<Envelope>(r).ok());
                let required_idle_ms = match &parsed {
                    Some(env) => vt_ms.max(
                        env.timeout_ms
                            .saturating_add(crate::exec::BACKSTOP_GRACE_MS),
                    ),
                    None => vt_ms,
                };
                if idle < required_idle_ms {
                    // Still within its own timeout budget: legitimately
                    // running, not stuck. Do not reclaim yet.
                    continue;
                }

                let claimed =
                    match broker::xclaim_entry(&mut conn, q, &ctx.consumer, vt_ms, &entry_id).await
                    {
                        Ok(Some(raw)) => raw,
                        Ok(None) => continue, // another worker won the claim
                        Err(e) => {
                            warn!(queue = %q, entry = %entry_id, "XCLAIM failed: {e}");
                            continue;
                        }
                    };
                let limit = redelivery_limit(parsed.as_ref());
                if delivery_count > limit {
                    warn!(
                        queue = %q, entry = %entry_id, delivery_count, limit,
                        "redelivery limit exceeded -> DLQ"
                    );
                    dlq_terminal(
                        &ctx,
                        q,
                        &entry_id,
                        claimed.as_deref().unwrap_or(""),
                        "redelivery_limit",
                        None,
                    )
                    .await;
                } else {
                    info!(
                        queue = %q, entry = %entry_id, idle, delivery_count, required_idle_ms,
                        "claimed pending entry; re-executing"
                    );
                    // Same code path as a fresh delivery; retries NOT incremented.
                    spawn_dispatch(ctx.clone(), q.clone(), entry_id, claimed);
                }
            }
        }
    }
}

/// §7 stats line every --stats-interval seconds. `sync_live`/`sync_abandoned`
/// (H2) make sync-pool thread loss observable instead of a silent capacity
/// drip: sync_live is the pool's current thread count (initial + spawned
/// replacements), sync_abandoned is the cumulative count of hard-timeout
/// abandonments that triggered a replacement. `pending_async` (MEM-1) is the
/// async runtime's pending-completion map size; a number that only grows
/// signals a wedged event-loop thread even though `cancel` stops the
/// Rust-side bookkeeping from leaking on its own.
pub async fn stats_loop(ctx: Arc<Ctx>) {
    let mut tick = tokio::time::interval(Duration::from_secs(ctx.args.stats_interval.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        info!(
            "{} sync_live={} sync_abandoned={} pending_async={}",
            ctx.counters.stats_line(),
            ctx.sync_pool.live_threads.load(Ordering::Relaxed),
            ctx.sync_pool.abandoned.load(Ordering::Relaxed),
            ctx.pyrt.pending_len()
        );
    }
}
