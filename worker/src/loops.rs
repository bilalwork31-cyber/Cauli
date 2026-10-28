//! Long-running loops: fetch (XREADGROUP), delayed mover, recovery
//! (XPENDING/XCLAIM), stats. All spawned from main.

use crate::broker;
// Redis anchored: the mover cutoff and stream id ages compare redis values.
use crate::clock::now_ms;
use crate::ctx::Ctx;
use crate::dispatch::{dlq_terminal, spawn_dispatch};
use crate::envelope::{redelivery_limit, Envelope};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{error, info, warn};

/// XREADGROUP BLOCK window. Clamped to half of `--redis-timeout`: a window at
/// or above the client response deadline makes every empty poll time out and
/// reconnect, a permanent storm on an idle queue.
const FETCH_BLOCK_MS: u64 = 1_000;

fn fetch_block_ms(redis_timeout_s: u64) -> u64 {
    let half_deadline = redis_timeout_s.saturating_mul(1000) / 2;
    FETCH_BLOCK_MS.min(half_deadline).max(50)
}

/// §4 fetch loop. Fetches only when io slots are free and no cpu dispatch is
/// blocked on a full backlog (bounded starvation, see docs/architecture.md).
pub async fn fetch_loop(ctx: Arc<Ctx>, mut fetch_conn: redis::aio::ConnectionManager) {
    let keys: Vec<String> = ctx.queues.iter().map(|q| broker::q_key(q)).collect();
    let ids: Vec<&str> = ctx.queues.iter().map(|_| ">").collect();
    let streams = keys.len().max(1);
    let block_ms = fetch_block_ms(ctx.args.redis_timeout);
    if block_ms < FETCH_BLOCK_MS {
        info!(
            block_ms,
            redis_timeout_s = ctx.args.redis_timeout,
            "XREADGROUP BLOCK window shortened to stay clear of --redis-timeout"
        );
    }
    // N streams need N free slots (per stream COUNT below), capped at the
    // gate's capacity or a small gate with many queues would never fetch.
    let min_permits = streams.min(ctx.io_concurrency.max(1));
    while !ctx.shutting_down() {
        // A full cpu backlog pauses every lane, by design: the lane is inside
        // the envelope, unknown until after XREADGROUP, so the gate cannot
        // skip only cpu entries. `cpu_backlog()` makes the pause visible.
        let permits = ctx.io_sem.available_permits();
        if permits < min_permits || ctx.cpu_backlog() > 0 {
            gate_wait(
                &ctx.io_sem,
                cpu_drained(&ctx),
                min_permits,
                permits,
                GATE_POLL,
            )
            .await;
            continue;
        }
        // Fetch only what can START now. The PEL idle clock runs from
        // delivery, so an entry parked on the semaphore gets reclaimed while
        // never started, and repeated parking dead letters it as
        // redelivery_limit without one execution. COUNT is per stream, hence
        // the division; `min_permits` guarantees full pages from every
        // stream still fit.
        let per_stream = (ctx.args.batch.min(permits) / streams).max(1);
        let opts = StreamReadOptions::default()
            .group("cauli", &ctx.consumer)
            .count(per_stream)
            .block(block_ms as usize);
        let reply: Option<StreamReadReply> =
            match fetch_conn.xread_options(&keys, &ids, &opts).await {
                Ok(r) => r,
                Err(e) if broker::is_nogroup(&e) => {
                    recreate_groups(&ctx, &mut fetch_conn, &e).await;
                    continue;
                }
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
                    .and_then(|v| redis::from_redis_value_ref::<String>(v).ok());
                spawn_dispatch(ctx.clone(), queue.clone(), entry.id.clone(), raw);
            }
        }
    }
    info!("fetch loop stopped (shutdown)");
}

/// NOGROUP means the broker dataset was reset, not a connection blip. Self
/// heal rather than exit: the lost state cannot come back, and exiting turns
/// it into an outage where no supervisor runs. The loss is logged at error.
async fn recreate_groups(
    ctx: &Arc<Ctx>,
    conn: &mut redis::aio::ConnectionManager,
    err: &redis::RedisError,
) {
    error!(
        "redis has no consumer group for this worker's queues ({err}): the broker dataset was \
         reset (a restart with no persistence, an eviction, a FLUSHALL or a restore). This is \
         not a connection blip. Any task that was in flight is gone from the pending entries \
         list and will NOT be redelivered, and anything that was scheduled in the delayed set \
         (retries, countdowns, beat) is gone with it. Recreating the groups and resuming; see \
         PROTOCOL.md section 4 on the persistence this guarantee assumes"
    );
    if let Err(e) = broker::ensure_groups(conn, &ctx.queues).await {
        warn!("could not recreate consumer groups after NOGROUP: {e}; retrying in 500ms");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Caps how long one queue holds a tick while its delayed set refills as fast
/// as it drains, so the other queues still get swept.
const MOVER_ROUNDS_PER_TICK: usize = 32;

/// §4.3 delayed mover: every `--mover-interval` ms per queue, repeated within
/// the tick until a page comes back short, so `--mover-limit` does not cap
/// the drain rate.
pub async fn mover_loop(ctx: Arc<Ctx>) {
    let script = redis::Script::new(broker::MOVER_LUA);
    let mut conn = ctx.redis.clone();
    let limit = ctx.args.mover_limit.max(1);
    let mut tick = tokio::time::interval(Duration::from_millis(ctx.args.mover_interval.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        for q in &ctx.queues {
            for _ in 0..MOVER_ROUNDS_PER_TICK {
                match broker::run_mover(&mut conn, &script, q, now_ms(), limit).await {
                    Ok(n) if (n as usize) < limit => break,
                    Ok(_) => continue,
                    Err(e) => {
                        report_mover_error(q, &e);
                        break;
                    }
                }
            }
        }
    }
}

fn report_mover_error(q: &str, e: &anyhow::Error) {
    if broker::is_crossslot(e) {
        // Permanent, not a blip: the two keys never share a hash slot.
        error!(
            queue = %q,
            "redis cluster is not supported for the delayed path: {e}; \
             cauli:q:{{queue}} and cauli:delayed:{{queue}} do not share a hash \
             slot, so this queue's delayed and retried tasks can never reach \
             the stream on this deployment (PROTOCOL.md section 4.3)"
        );
    } else {
        warn!(queue = %q, "delayed mover failed: {e}");
    }
}

/// The trim is only a backstop, so this just bounds how long orphans linger.
const TRIM_INTERVAL_MS: u64 = 100;

/// §4.1 trim backstop. The batched XDEL in the flush (broker::AckBufs) is the
/// primary removal path; this reclaims what a flush torn between XACK and
/// XDEL leaves behind, `XTRIM MINID` up to `broker::acked_below`.
///
/// That boundary is the oldest pending id, so one slow task holds back every
/// entry behind it: never make this the primary path. Keeps running through
/// a §4.7 drain, since completions still flush then.
pub async fn trim_loop(ctx: Arc<Ctx>) {
    let mut conn = ctx.redis.clone();
    let mut tick = tokio::time::interval(Duration::from_millis(TRIM_INTERVAL_MS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // An unchanged boundary would make the XTRIM a no-op round trip.
    let mut trimmed_to: std::collections::HashMap<String, String> = Default::default();
    loop {
        tick.tick().await;
        for q in &ctx.queues {
            let boundary = match broker::acked_below(&mut conn, q).await {
                Ok(Some(b)) => b,
                Ok(None) => continue,
                Err(e) => {
                    warn!(queue = %q, "trim boundary probe failed: {e}");
                    continue;
                }
            };
            if trimmed_to.get(q).is_some_and(|prev| *prev == boundary) {
                continue;
            }
            match broker::trim_acked(&mut conn, q, &boundary).await {
                Ok(_) => {
                    trimmed_to.insert(q.clone(), boundary);
                }
                Err(e) => warn!(queue = %q, "XTRIM failed: {e}"),
            }
        }
    }
}

/// Reclaim margin on top of the execution backstop. The backstop counts from
/// submit, the PEL idle clock from delivery; the gap is dispatch overhead,
/// dominated by the idempotency round trip (up to `--redis-timeout`). Without
/// it a live entry becomes reclaimable before its own backstop fires.
fn reclaim_margin_ms(ctx: &Ctx) -> u64 {
    ctx.args
        .redis_timeout
        .saturating_mul(1000)
        .max(crate::exec::BACKSTOP_GRACE_MS)
}

/// XPENDING page size. Not `--batch`: that would cap redelivery after a
/// crash at `batch` entries per tick.
const RECOVERY_SCAN_BATCH: usize = 128;

/// §4.4 recovery: every visibility_timeout/2 per queue.
///
/// `--visibility-timeout` is a floor, not the threshold: an entry is
/// reclaimed only once idle >= max(visibility, envelope timeout + grace), or
/// a task longer than the visibility timeout (60s vs the 300s default) would
/// be reclaimed and run twice. Unparseable envelopes use the floor alone.
///
/// Each tick drains every eligible entry, round robin in pages behind the
/// fetch loop's admission gate, so a reclaimed backlog waits in redis, not in
/// memory. Every `CONSUMER_REAP_EVERY_TICKS` ticks it also reaps dead
/// consumers.
pub async fn recovery_loop(ctx: Arc<Ctx>) {
    let vt_ms = visibility_timeout_ms(ctx.args.visibility_timeout);
    let period = Duration::from_millis((vt_ms / 2).max(500));
    let mut conn = ctx.redis.clone();
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ticks: u64 = 0;
    loop {
        tick.tick().await;
        if ctx.shutting_down() {
            continue; // no new work during drain; mover/acks keep running
        }
        if ticks.is_multiple_of(CONSUMER_REAP_EVERY_TICKS) {
            reap_stale_consumers(&ctx, &mut conn, vt_ms).await;
        }
        ticks = ticks.wrapping_add(1);
        // None once a queue is drained for this tick.
        let mut cursors: Vec<(String, Option<String>)> = ctx
            .queues
            .iter()
            .map(|q| (q.clone(), Some("-".to_string())))
            .collect();
        'drain: while cursors.iter().any(|(_, c)| c.is_some()) && !ctx.shutting_down() {
            for (q, cursor) in cursors.iter_mut() {
                let Some(start) = cursor.take() else { continue };
                let Some(page) = admission_open(&ctx).await else {
                    break 'drain; // shutdown began while waiting for capacity
                };
                *cursor = recover_page(&ctx, &mut conn, q, vt_ms, &start, page).await;
            }
        }
    }
}

/// The fetch loop's admission gate, for recovery. Returns the page size to
/// reclaim, which is the free permit count: a larger page parks entries on
/// `io_sem`, and every reclaim bumps `delivery_count` toward a dead letter.
/// `None` if shutdown began while waiting.
async fn admission_open(ctx: &Arc<Ctx>) -> Option<usize> {
    loop {
        if ctx.shutting_down() {
            return None;
        }
        let permits = ctx.io_sem.available_permits();
        if permits > 0 && ctx.cpu_backlog() == 0 {
            return Some(permits.min(RECOVERY_SCAN_BATCH));
        }
        gate_wait(&ctx.io_sem, cpu_drained(ctx), 1, permits, GATE_POLL).await;
    }
}

fn cpu_drained(ctx: &Ctx) -> Option<&Notify> {
    ctx.cpu.get().map(|pool| pool.drained.as_ref())
}

/// Ceiling for the conditions no semaphore signals: cpu backlog, shutdown.
const GATE_POLL: Duration = Duration::from_millis(25);

/// Wait on whichever half of the gate is shut. With io permits free, waiting
/// on `io_sem` returns instantly and the caller spins on the closed cpu half,
/// so that case waits on the backlog drain signal instead.
async fn gate_wait(
    io_sem: &tokio::sync::Semaphore,
    cpu_drained: Option<&Notify>,
    want: usize,
    free: usize,
    poll: Duration,
) {
    if free >= want.max(1) {
        // The poll is only the ceiling, for shutdown and the second waiter
        // one notify_one misses; sleeping it out idles the cpu children.
        match cpu_drained {
            Some(drained) => {
                tokio::select! {
                    _ = drained.notified() => {}
                    _ = tokio::time::sleep(poll) => {}
                }
            }
            None => tokio::time::sleep(poll).await,
        }
    } else {
        await_capacity(io_sem, want, poll).await;
    }
}

/// Wake when `want` io permits are free, with `poll` as the ceiling. The
/// permits are only a signal and dropped at once (dispatch acquires its own),
/// so this never hoards a slot past one window.
async fn await_capacity(io_sem: &tokio::sync::Semaphore, want: usize, poll: Duration) {
    let want = want.max(1).min(u32::MAX as usize) as u32;
    tokio::select! {
        biased;
        res = io_sem.acquire_many(want) => {
            if res.is_err() {
                // Closed: acquire fails instantly, so sleep rather than spin.
                tokio::time::sleep(poll).await;
            }
        }
        _ = tokio::time::sleep(poll) => {}
    }
}

/// Recovery ticks between reaps: every ten minutes at the default 60s.
const CONSUMER_REAP_EVERY_TICKS: u64 = 20;

/// A paused fetch loop issues no XREADGROUP, so this must outlast any pause
/// that is not itself an incident.
const CONSUMER_REAP_MIN_IDLE_MS: u64 = 10 * 60 * 1000;

fn consumer_reap_idle_ms(vt_ms: u64) -> u64 {
    vt_ms.saturating_mul(4).max(CONSUMER_REAP_MIN_IDLE_MS)
}

/// `pending == 0` is the hard rule: DELCONSUMER drops the consumer's PEL and
/// strands its entries where no recovery reaches them. Idle is soft: a live
/// empty consumer is recreated by its next XREADGROUP.
fn consumer_is_reapable(
    name: &str,
    pending: usize,
    idle_ms: u64,
    self_name: &str,
    min_idle_ms: u64,
) -> bool {
    pending == 0 && name != self_name && idle_ms >= min_idle_ms
}

/// Every process start mints a new consumer name (hostname + pid) and redis
/// never deletes the old ones, so this does.
async fn reap_stale_consumers(
    ctx: &Arc<Ctx>,
    conn: &mut redis::aio::ConnectionManager,
    vt_ms: u64,
) {
    let min_idle_ms = consumer_reap_idle_ms(vt_ms);
    for q in &ctx.queues {
        let key = broker::q_key(q);
        let info: redis::streams::StreamInfoConsumersReply = match redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(&key)
            .arg("cauli")
            .query_async(conn)
            .await
        {
            Ok(i) => i,
            Err(e) => {
                warn!(queue = %q, "XINFO CONSUMERS failed, not reaping this tick: {e}");
                continue;
            }
        };
        for c in info.consumers {
            if !consumer_is_reapable(
                &c.name,
                c.pending,
                c.idle as u64,
                &ctx.consumer,
                min_idle_ms,
            ) {
                continue;
            }
            let deleted: redis::RedisResult<u64> = redis::cmd("XGROUP")
                .arg("DELCONSUMER")
                .arg(&key)
                .arg("cauli")
                .arg(&c.name)
                .query_async(conn)
                .await;
            match deleted {
                // Pending entries dropped with it; nonzero means it revived
                // after the XINFO read.
                Ok(0) => info!(
                    queue = %q, consumer = %c.name, idle_ms = c.idle,
                    "reaped stale stream consumer (no pending entries)"
                ),
                Ok(n) => warn!(
                    queue = %q, consumer = %c.name, pending = n,
                    "reaped a consumer that took {n} pending entries with it; those entries are \
                     no longer in any pending entries list and will not be redelivered"
                ),
                Err(e) => warn!(queue = %q, consumer = %c.name, "XGROUP DELCONSUMER failed: {e}"),
            }
        }
    }
}

/// One XPENDING page. Returns the next cursor, or None when the queue is done
/// for this tick (last page, or a redis error).
async fn recover_page(
    ctx: &Arc<Ctx>,
    conn: &mut redis::aio::ConnectionManager,
    queue: &str,
    vt_ms: u64,
    start: &str,
    page: usize,
) -> Option<String> {
    let margin_ms = reclaim_margin_ms(ctx);
    let pend = match broker::xpending_idle(conn, queue, vt_ms, start, page).await {
        Ok(p) => p,
        Err(e) => {
            warn!(queue = %queue, "XPENDING failed: {e}");
            return None;
        }
    };
    let page_len = pend.len();
    let next_cursor = match pend.last() {
        // Exclusive `(id`: skipped entries are not re-read, so no spin.
        Some(last) if page_len == page => Some(format!("({}", last.0)),
        _ => None,
    };

    let ids: Vec<String> = pend.iter().map(|p| p.0.clone()).collect();
    let peeked = match broker::peek_entries(conn, queue, &ids).await {
        Ok(p) => p,
        Err(e) => {
            warn!(queue = %queue, "peek (pipelined XRANGE) failed: {e}");
            return None;
        }
    };

    let mut eligible: Vec<(String, u64, u64, Option<Envelope>)> = Vec::new();
    for ((entry_id, _consumer, idle, delivery_count), peek) in pend.into_iter().zip(peeked) {
        let Some(raw_opt) = peek else {
            continue; // entry vanished (acked/claimed elsewhere) since XPENDING
        };
        let parsed = raw_opt
            .as_deref()
            .and_then(|r| serde_json::from_str::<Envelope>(r).ok());
        let required_idle_ms = match &parsed {
            Some(env) => vt_ms.max(
                env.timeout_ms
                    .saturating_add(crate::exec::BACKSTOP_GRACE_MS)
                    .saturating_add(margin_ms),
            ),
            None => vt_ms,
        };
        if idle < required_idle_ms {
            continue;
        }
        if ctx.holds_entry(queue, &entry_id) {
            // Still live here: reclaiming would run a second copy and burn a
            // delivery_count toward a dead letter.
            continue;
        }
        eligible.push((entry_id, idle, delivery_count, parsed));
    }
    if eligible.is_empty() {
        return next_cursor;
    }

    let claim_ids: Vec<String> = eligible.iter().map(|e| e.0.clone()).collect();
    let claimed = match broker::xclaim_entries(conn, queue, &ctx.consumer, vt_ms, &claim_ids).await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(queue = %queue, "pipelined XCLAIM failed: {e}");
            return None;
        }
    };
    for ((entry_id, idle, delivery_count, parsed), claim) in eligible.into_iter().zip(claimed) {
        let Some(raw) = claim else {
            continue; // another worker won the claim, or the entry vanished
        };
        let limit = redelivery_limit(parsed.as_ref());
        if delivery_count > limit {
            warn!(
                queue = %queue, entry = %entry_id, delivery_count, limit,
                "redelivery limit exceeded -> DLQ"
            );
            dlq_terminal(
                ctx,
                queue,
                &entry_id,
                raw.as_deref().unwrap_or(""),
                "redelivery_limit",
                None,
            )
            .await;
        } else {
            info!(
                queue = %queue, entry = %entry_id, idle, delivery_count,
                "claimed pending entry; re-executing"
            );
            // Retries are not incremented: this is a redelivery.
            spawn_dispatch(ctx.clone(), queue.to_string(), entry_id, raw);
        }
    }
    next_cursor
}

/// Period of the shim's `heartbeat` stamp on every embedded asyncio loop.
const HEARTBEAT_INTERVAL_MS: u64 = 5_000;

/// Shorter restarts the worker on survivable blocking calls (a 10s sync HTTP
/// request); longer keeps the async lane dead that long on a real wedge.
/// A restart costs about a second plus at least once redelivery.
const WEDGE_WINDOW_MS: u64 = 3 * HEARTBEAT_INTERVAL_MS;

/// Distinct from 0 graceful, 1 fatal startup, 101 panic, 130 forced.
pub const WEDGE_EXIT_CODE: i32 = 87;

/// Largest loop lag at the last watchdog tick (stats `loop_lag_ms`).
static LOOP_LAG_MS: AtomicU64 = AtomicU64::new(0);

/// `outstanding` is submitted minus completed, not queue depth: a wedged loop
/// usually shows an empty queue and Tasks it will never start.
struct LoopBeat {
    lag_ms: u64,
    outstanding: u64,
    completed: u64,
}

struct LoopWatch {
    completed: u64,
    progress_at: Instant,
}

/// Wedged event loop watchdog (docs/architecture.md). A blocking call in an
/// `async def` starves its loop thread forever and CPython cannot kill a
/// thread, so on a confirmed wedge the process exits for its supervisor to
/// restart it.
pub async fn wedge_loop(ctx: Arc<Ctx>) {
    let mut tick = tokio::time::interval(Duration::from_millis(HEARTBEAT_INTERVAL_MS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut watches: Vec<LoopWatch> = Vec::new();
    let mut rejected = ctx.pyrt.async_rejected();
    let mut rejected_at: Option<Instant> = None;
    let mut probe_warned = false;
    loop {
        tick.tick().await;
        if ctx.shutting_down() {
            continue; // a drain is not a wedge
        }
        // Awaited, so at most one probe waits on the GIL: a GIL that never
        // frees stalls here instead of parking a blocking thread per tick.
        let beats = match tokio::task::spawn_blocking(probe_loops).await {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => {
                if !probe_warned {
                    probe_warned = true;
                    warn!("event loop heartbeat unavailable, wedge detection is off: {e}");
                }
                continue;
            }
            Err(e) => {
                warn!("event loop heartbeat probe panicked: {e}");
                continue;
            }
        };
        let now = Instant::now();
        LOOP_LAG_MS.store(
            beats.iter().map(|b| b.lag_ms).max().unwrap_or(0),
            Ordering::Relaxed,
        );
        let seen = ctx.pyrt.async_rejected();
        if seen > rejected {
            rejected = seen;
            rejected_at = Some(now);
        }
        let since_rejection_ms = rejected_at.map(|t| ms_since(t, now));
        watches.resize_with(beats.len(), || LoopWatch {
            completed: 0,
            progress_at: now,
        });
        for (idx, beat) in beats.iter().enumerate() {
            let watch = &mut watches[idx];
            if beat.completed != watch.completed {
                watch.completed = beat.completed;
                watch.progress_at = now;
            }
            let since_progress_ms = ms_since(watch.progress_at, now);
            if !wedge_confirmed(
                beat.lag_ms,
                beat.outstanding,
                since_progress_ms,
                since_rejection_ms,
            ) {
                continue;
            }
            error!(
                loop_index = idx,
                lag_ms = beat.lag_ms,
                outstanding = beat.outstanding,
                since_progress_ms,
                async_rejected = rejected,
                "wedged async event loop confirmed: loop {idx} has not run a scheduled callback \
                 for {}ms and finished nothing while {} async tasks sat outstanding on it. A \
                 blocking call inside an async def (a synchronous HTTP request, time.sleep, a \
                 blocking database driver) starves that loop thread permanently and nothing in \
                 this process can recover it, so this worker is exiting with code {} for its \
                 supervisor to restart it. In flight tasks are redelivered (PROTOCOL.md \
                 section 4.4)",
                beat.lag_ms,
                beat.outstanding,
                WEDGE_EXIT_CODE
            );
            // Not process::exit: Python threads are still live here.
            crate::exit_now(WEDGE_EXIT_CODE);
        }
    }
}

/// Two signals, never the stale stamp alone: a GIL convoy from another lane
/// delays the stamp exactly like a wedge, and restarting on load is worse than
/// the brownout. The stamp counts only with outstanding work and no
/// completions for the window, or a recent `async_rejected` rise.
fn wedge_confirmed(
    lag_ms: u64,
    outstanding: u64,
    since_progress_ms: u64,
    since_rejection_ms: Option<u64>,
) -> bool {
    let stamp_stale = lag_ms >= WEDGE_WINDOW_MS;
    let nothing_finishing = outstanding > 0 && since_progress_ms >= WEDGE_WINDOW_MS;
    let rejecting = since_rejection_ms.is_some_and(|ms| ms <= WEDGE_WINDOW_MS);
    stamp_stale && (nothing_finishing || rejecting)
}

/// Takes the GIL: run on a blocking pool thread, never a tokio worker. The
/// module name must match the one pyrt.rs passes to `PyModule::from_code`; a
/// failed lookup only disables detection.
fn probe_loops() -> Result<Vec<LoopBeat>, String> {
    use pyo3::prelude::*;
    Python::attach(|py| {
        let raw: Vec<(u64, u64, u64)> = py
            .import("cauli_worker_shim")
            .and_then(|shim| shim.getattr("heartbeat"))
            .and_then(|f| f.call0())
            .and_then(|r| r.extract())
            .map_err(|e| e.to_string())?;
        Ok(raw
            .into_iter()
            .map(|(lag_ms, outstanding, completed)| LoopBeat {
                lag_ms,
                outstanding,
                completed,
            })
            .collect())
    })
}

fn ms_since(earlier: Instant, now: Instant) -> u64 {
    now.saturating_duration_since(earlier).as_millis() as u64
}

/// §7 stats line every --stats-interval seconds.
///
/// | field | meaning |
/// |---|---|
/// | `sync_live` | sync pool threads, replacements included, capped |
/// | `sync_abandoned` | cumulative hard timeout abandonments |
/// | `async_rejected` | submissions the shim's per loop queue refused at its cap |
/// | `cpu_backlog` | nonzero: fetching is paused for every lane |
/// | `oldest_ms` | age of the oldest outstanding work |
/// | `cpu_rss_mb` | summed RSS of the cpu children (`rss_mb` excludes them) |
/// | `loop_lag_ms` | largest event loop lag at the last watchdog tick |
pub async fn stats_loop(ctx: Arc<Ctx>) {
    let mut tick = tokio::time::interval(Duration::from_secs(ctx.args.stats_interval.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut conn = ctx.redis.clone();
    loop {
        tick.tick().await;
        let oldest = oldest_unacked_ms(&ctx, &mut conn).await;
        info!(
            "{} oldest_ms={} cpu_rss_mb={} sync_live={} sync_abandoned={} async_rejected={} cpu_backlog={} loop_lag_ms={}",
            ctx.counters.stats_line(),
            oldest,
            ctx.cpu_rss_mb(),
            ctx.sync_pool.live_threads.load(Ordering::Relaxed),
            ctx.sync_pool.abandoned.load(Ordering::Relaxed),
            ctx.pyrt.async_rejected(),
            ctx.cpu_overflow(),
            LOOP_LAG_MS.load(Ordering::Relaxed)
        );
    }
}

/// Age of the oldest outstanding work, 0 when none: the older of the oldest
/// pending entry and the oldest undelivered one. A broker probe, so it keeps
/// reporting while fetching is paused. Not the stream head: that can be an
/// acked entry awaiting its XDEL.
async fn oldest_unacked_ms(ctx: &Arc<Ctx>, conn: &mut redis::aio::ConnectionManager) -> u64 {
    let now = now_ms();
    let mut oldest = 0;
    for q in &ctx.queues {
        match broker::oldest_pending_id(conn, q).await {
            Ok(Some(id)) => oldest = oldest.max(stream_id_age_ms(&id, now)),
            Ok(None) => {}
            Err(e) => warn!(queue = %q, "oldest_ms probe (XPENDING) failed: {e}"),
        }
        match broker::oldest_undelivered_id(conn, q).await {
            Ok(Some(id)) => oldest = oldest.max(stream_id_age_ms(&id, now)),
            Ok(None) => {}
            Err(e) => warn!(queue = %q, "oldest_ms probe (backlog) failed: {e}"),
        }
    }
    oldest
}

/// 0 for an unparseable id or one ahead of `now_ms`, never a wrapped u64.
fn stream_id_age_ms(id: &str, now_ms: u64) -> u64 {
    id.split('-')
        .next()
        .and_then(|ms| ms.parse::<u64>().ok())
        .map_or(0, |ms| now_ms.saturating_sub(ms))
}

/// Saturating: release builds would wrap a plain multiply silently.
fn visibility_timeout_ms(visibility_timeout_s: u64) -> u64 {
    visibility_timeout_s.saturating_mul(1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visibility_timeout_ms_saturates_instead_of_wrapping() {
        assert_eq!(visibility_timeout_ms(60), 60_000);
        assert_eq!(visibility_timeout_ms(u64::MAX), u64::MAX);
    }

    #[test]
    fn stream_id_age_reads_the_millisecond_field_only() {
        assert_eq!(
            stream_id_age_ms("1700000000000-0", 1_700_000_005_000),
            5_000
        );
        assert_eq!(stream_id_age_ms("1700000000000-99", 1_700_000_000_250), 250);
        assert_eq!(stream_id_age_ms("1700000000000-0", 1_700_000_000_000), 0);
        assert_eq!(stream_id_age_ms("1700000009000-0", 1_700_000_000_000), 0);
        assert_eq!(stream_id_age_ms("garbage-1", 1_700_000_000_000), 0);
        assert_eq!(stream_id_age_ms("", 1_700_000_000_000), 0);
    }

    const OVER: u64 = WEDGE_WINDOW_MS + 1;
    const UNDER: u64 = WEDGE_WINDOW_MS - 1;

    #[test]
    fn a_wedged_loop_is_confirmed() {
        assert!(wedge_confirmed(OVER, 2, OVER, None));
        assert!(wedge_confirmed(OVER, 0, 0, Some(0)));
    }

    #[test]
    fn a_slow_loop_is_not_confirmed() {
        // Still completing: a GIL convoy, not a wedge.
        assert!(!wedge_confirmed(OVER, 2, 0, None));
        assert!(!wedge_confirmed(OVER, 2, UNDER, None));
        // A rejection from an older, cleared spell.
        assert!(!wedge_confirmed(OVER, 0, 0, Some(OVER)));
        assert!(!wedge_confirmed(UNDER, 2, OVER, Some(0)));
        // Wedged but idle: nothing is being lost yet.
        assert!(!wedge_confirmed(OVER, 0, OVER, None));
    }

    #[test]
    fn a_consumer_with_pending_entries_is_never_reaped() {
        let min = consumer_reap_idle_ms(60_000);
        assert!(!consumer_is_reapable(
            "host-a-1",
            1,
            u64::MAX,
            "host-b-2",
            min
        ));
        assert!(!consumer_is_reapable(
            "host-a-1",
            128,
            u64::MAX,
            "host-b-2",
            min
        ));
    }

    #[test]
    fn only_long_silent_foreign_consumers_are_reaped() {
        let min = consumer_reap_idle_ms(60_000);
        assert!(consumer_is_reapable("host-a-1", 0, min, "host-a-2", min));
        assert!(!consumer_is_reapable(
            "host-a-1",
            0,
            min - 1,
            "host-a-2",
            min
        ));
        // Never this process's own consumer.
        assert!(!consumer_is_reapable(
            "host-a-2",
            0,
            u64::MAX,
            "host-a-2",
            min
        ));
    }

    #[test]
    fn the_reap_threshold_clears_the_visibility_floor() {
        assert_eq!(consumer_reap_idle_ms(60_000), CONSUMER_REAP_MIN_IDLE_MS);
        assert_eq!(consumer_reap_idle_ms(3_600_000), 4 * 3_600_000);
        assert_eq!(consumer_reap_idle_ms(u64::MAX), u64::MAX);
    }

    /// `poll` is 30s, so passing means the semaphore woke the gate.
    #[tokio::test]
    async fn the_gate_wakes_on_a_freed_permit_not_on_the_poll() {
        let long_poll = Duration::from_secs(30);
        let sem = Arc::new(tokio::sync::Semaphore::new(4));
        let t0 = Instant::now();
        await_capacity(&sem, 2, long_poll).await;
        assert!(t0.elapsed() < Duration::from_secs(1), "already free");

        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        let freeing = sem.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            freeing.add_permits(2);
        });
        let t0 = Instant::now();
        await_capacity(&sem, 2, long_poll).await;
        assert!(t0.elapsed() < Duration::from_secs(5), "freed while waiting");
    }

    #[tokio::test]
    async fn the_gate_still_returns_at_the_poll_without_permits() {
        let poll = Duration::from_millis(30);
        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        let t0 = Instant::now();
        await_capacity(&sem, 1, poll).await;
        assert!(t0.elapsed() >= poll);

        let closed = Arc::new(tokio::sync::Semaphore::new(0));
        closed.close();
        let t0 = Instant::now();
        await_capacity(&closed, 1, poll).await;
        assert!(t0.elapsed() >= poll, "closed semaphore must not spin");
    }

    /// Shut on the cpu backlog alone, io permits free.
    #[tokio::test]
    async fn the_gate_does_not_spin_when_only_the_cpu_backlog_holds_it() {
        let poll = Duration::from_millis(30);
        let sem = Arc::new(tokio::sync::Semaphore::new(8));
        let t0 = Instant::now();
        gate_wait(&sem, None, 2, 8, poll).await;
        assert!(t0.elapsed() >= poll, "free permits must not short circuit");
        assert_eq!(sem.available_permits(), 8);
    }

    #[tokio::test]
    async fn the_gate_wakes_when_the_cpu_backlog_drains() {
        let sem = Arc::new(tokio::sync::Semaphore::new(8));
        let drained = Arc::new(Notify::new());
        let draining = drained.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            draining.notify_one();
        });
        let t0 = Instant::now();
        gate_wait(&sem, Some(&drained), 2, 8, Duration::from_secs(30)).await;
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "drained while waiting"
        );
    }

    #[tokio::test]
    async fn the_gate_waits_on_the_semaphore_when_io_is_the_blocker() {
        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        let freeing = sem.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            freeing.add_permits(2);
        });
        let t0 = Instant::now();
        gate_wait(&sem, None, 2, 0, Duration::from_secs(30)).await;
        assert!(t0.elapsed() < Duration::from_secs(5), "freed while waiting");
    }
}
