//! Long-running loops: fetch (XREADGROUP), delayed mover, recovery
//! (XPENDING/XCLAIM), stats. All spawned from main.

use crate::broker;
// The mover cutoff and the stream id age probe both compare against values
// redis itself wrote, so both read the redis anchored clock. See clock.rs.
use crate::clock::now_ms;
use crate::ctx::Ctx;
use crate::dispatch::{dlq_terminal, spawn_dispatch};
use crate::envelope::{redelivery_limit, Envelope};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Server side BLOCK window for XREADGROUP, in ms.
///
/// Clamped against `--redis-timeout` rather than used raw: the client side
/// response deadline applies to this very call, so a BLOCK window at or
/// above it turns every empty poll into a dead heat decided by scheduling
/// jitter. Each loss surfaces as an IoError, tears the fetch connection
/// down, logs, and backs off 500ms -- a permanent reconnect storm on an idle
/// queue, from a flag whose own help text reads as an endorsement of 1.
/// Halving keeps a comfortable gap at every value instead of failing the
/// deployment over it.
const FETCH_BLOCK_MS: u64 = 1_000;

fn fetch_block_ms(redis_timeout_s: u64) -> u64 {
    let half_deadline = redis_timeout_s.saturating_mul(1000) / 2;
    FETCH_BLOCK_MS.min(half_deadline).max(50)
}

/// §4 fetch loop. Gate: fetch only when io slots are free and no cpu dispatch
/// is blocked on a full backlog (bounded starvation, see ARCHITECTURE.md).
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
    // With N streams, one free slot is not enough to fetch safely (see the
    // per stream COUNT note below); the loop needs N. On a deployment whose
    // whole gate is smaller than its queue count, N is unreachable, so the
    // requirement is capped at the gate's own capacity or the loop would
    // never fetch at all.
    let min_permits = streams.min(ctx.io_concurrency.max(1));
    while !ctx.shutting_down() {
        // A full cpu backlog pauses EVERY lane here, not just cpu, and that
        // is correct, not a bug to "fix" by making this lane selective: an
        // entry's lane lives inside its envelope, and the envelope is not
        // parsed until after XREADGROUP returns it, so the gate cannot skip
        // just the cpu entries without first fetching (and then having
        // nowhere to safely put) io work it cannot dispatch either.
        // `cpu_backlog()` keeps this pause loud instead of silent (stats
        // line + a warn on the zero/nonzero edge); the coupling itself stays.
        let permits = ctx.io_sem.available_permits();
        if permits < min_permits || ctx.cpu_backlog() > 0 {
            gate_wait(&ctx.io_sem, min_permits, permits, GATE_POLL).await;
            continue;
        }
        // Fetch only what there is capacity to START, not a full --batch.
        // An entry's PEL idle clock starts at XREADGROUP delivery, while the
        // execution backstop is only armed after the io semaphore is
        // acquired, and under saturation that wait is unbounded. A fetched
        // entry parked on the semaphore past `timeout_ms + 2000` therefore
        // looks idle to the section 4.4 recovery loop and gets reclaimed
        // while its first attempt is still alive and has not started;
        // repeated parking inflates delivery_count until the entry is dead
        // lettered as redelivery_limit WITHOUT having executed once.
        // Bounding COUNT by free permits means fetched entries never park.
        //
        // COUNT is applied PER STREAM by redis, not across the read, so the
        // budget has to be divided by the stream count or a worker on
        // `-Q high,default,bulk` fetches up to 3x what it can start. The
        // surplus parked with its PEL idle clock running, which is the exact
        // state this bound exists to prevent: repeated parking inflates
        // delivery_count until the entry is dead lettered as
        // redelivery_limit without ever executing. The `min_permits` gate
        // above guarantees `permits >= streams` (or the gate is smaller than
        // the queue count and every slot is already free), so the worst case
        // -- every stream returning a full page -- still fits.
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
                    .and_then(|v| redis::from_redis_value::<String>(v).ok());
                spawn_dispatch(ctx.clone(), queue.clone(), entry.id.clone(), raw);
            }
        }
    }
    info!("fetch loop stopped (shutdown)");
}

/// Recreate the consumer groups after a NOGROUP, and say plainly what it
/// means. NOGROUP is not a connection blip and must never read like one: the
/// group (or the whole stream) is gone from a redis this worker is still
/// happily connected to, which is what a restart without persistence, an OOM
/// kill, a restore from backup or a FLUSHALL all look like from here.
///
/// Self healing rather than exiting, deliberately. Everything the group knew
/// is already lost at this point and no exit code can bring it back, while
/// the stream itself is where new work keeps arriving; a worker that dies
/// here turns a recoverable broker event into an outage on deployments with
/// no supervisor, and one that stays deaf is the failure this replaces. So
/// the entries list and the delayed set are called out as lost, once per
/// event, at error level, and consumption resumes on the next iteration.
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

/// EVALs the mover is allowed to run for one queue within a single tick.
///
/// The sweep repeats until a queue comes back short, so the per-EVAL
/// `--mover-limit` no longer caps the drain rate; this bounds how long one
/// queue may hold the tick when the delayed set is being refilled as fast as
/// it drains, so the other queues in `-Q a,b,c` still get swept.
const MOVER_ROUNDS_PER_TICK: usize = 32;

/// §4.3 delayed mover: every `--mover-interval` ms per queue, repeated
/// within the tick until the queue drains.
///
/// It used to be one fixed `LIMIT 128` EVAL per queue per 250ms tick with
/// the returned count discarded: a hard, unflaggable ceiling of 512 entries
/// per second per queue per process through which every retry, countdown,
/// eta and beat firing passes — 40x below the worker's own headline
/// throughput. A modest failure rate at full speed produced more retries per
/// second than the mover could move, and the delayed zset then grew without
/// bound while `oldest_ms`, which only watches the stream, reported nothing.
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
                    // Short page: this queue is drained for now, so the tick
                    // moves on instead of paying another round trip.
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
        // Not a blip: cauli:q:{queue} and cauli:delayed:{queue} never share
        // a hash slot, so this EVAL fails with CROSSSLOT on every future
        // tick too, not just this one. Name the real cause instead of
        // letting it read like an ordinary retryable error.
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

/// Extra idle margin the reclaim threshold carries ON TOP of an envelope's
/// own execution backstop.
///
/// The async lane's backstop is `timeout_ms + BACKSTOP_GRACE_MS` counted
/// from AFTER the submit, while the PEL idle clock starts at delivery. With
/// the identical expression on both sides an entry became reclaim eligible
/// while its own backstop had not fired yet -- zero margin, and the sync
/// lane's plain `timeout_ms` backstop was the only one that had any. The
/// difference is dispatch overhead: envelope parse, the idempotency round
/// trip (up to a full `--redis-timeout`) and the admission wait, so the
/// margin is sized on the one part of that which is configurable.
fn reclaim_margin_ms(ctx: &Ctx) -> u64 {
    ctx.args
        .redis_timeout
        .saturating_mul(1000)
        .max(crate::exec::BACKSTOP_GRACE_MS)
}

/// One XPENDING page while draining a queue's reclaim backlog. Deliberately
/// NOT `--batch` (the fetch loop's XREADGROUP admission knob): recovery used
/// to reuse it, which capped reclaim at `batch` entries per tick — after a
/// `kill -9` with a few hundred tasks in flight, redelivery trickled back at
/// 16 entries per `visibility_timeout/2` while a fresh worker sat idle.
const RECOVERY_SCAN_BATCH: usize = 128;

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
///
/// Throughput: each tick drains EVERY eligible entry, not one fixed-size
/// batch. Queues are scanned round-robin, one `RECOVERY_SCAN_BATCH` page at
/// a time (cursor-paged XPENDING; skipped still-running entries cannot make
/// the loop spin because the cursor only moves forward within a tick), with
/// the envelope peeks and the XCLAIMs pipelined per page. Page fetch honors
/// the same admission gate as the fetch loop (free io permits, no cpu
/// overflow) so a huge reclaimed backlog queues in Redis, not in worker
/// memory. A still-running long task skipped this tick is simply
/// re-examined next tick (the cursor resets to "-" every tick).
///
/// Every `CONSUMER_REAP_EVERY_TICKS` ticks the same loop also reaps the
/// group's dead consumers (see `reap_stale_consumers`), which nothing else
/// in the process ever removes.
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
        // Per-queue cursor: Some(start) = more pages may remain, None = done.
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

/// Mirror of the fetch loop's admission gate (see its comment for why a full
/// cpu backlog pauses every lane, not just cpu): wait until io slots are
/// free and no cpu dispatch is parked on a full backlog, so reclaiming a
/// large backlog cannot spawn an unbounded number of in-memory dispatch
/// tasks. `None` if shutdown began while waiting.
///
/// Returns the page size the caller may reclaim, which is the free permit
/// count, not `RECOVERY_SCAN_BATCH`. The gate used to prove one free slot
/// and then let `recover_page` XCLAIM and dispatch a full page of 128: with
/// one permit free that is 127 entries parked on `io_sem`, holding 127
/// parsed envelopes in memory, in exactly the state the fetch loop's own
/// bound exists to prevent. Reclaims carry no JUSTID, so every one of them
/// increments `delivery_count`, and `redelivery_limit` of those dead letters
/// a task as `RedeliveryLimitExceeded` that may never have run.
async fn admission_open(ctx: &Arc<Ctx>) -> Option<usize> {
    loop {
        if ctx.shutting_down() {
            return None;
        }
        let permits = ctx.io_sem.available_permits();
        if permits > 0 && ctx.cpu_backlog() == 0 {
            return Some(permits.min(RECOVERY_SCAN_BATCH));
        }
        gate_wait(&ctx.io_sem, 1, permits, GATE_POLL).await;
    }
}

/// Ceiling on how long either admission gate may sit on a condition it
/// cannot wait for (cpu backlog depth, shutdown). Not the io wait: that one
/// is event driven, see `await_capacity`.
const GATE_POLL: Duration = Duration::from_millis(25);

/// Wait on whichever half of the shut gate can actually be waited on.
///
/// The gate is a conjunction, and `free >= want` means the io half of it was
/// already open: the caller was stopped by the cpu backlog, which no
/// semaphore ever signals. Waiting on `io_sem` in that state returns
/// INSTANTLY, and the caller loops straight back into the same closed gate --
/// a full speed spin for as long as the backlog lasts, on the worker's
/// hottest loop. So the poll is the wait there, and the semaphore is the wait
/// only when the semaphore is the thing that is shut.
async fn gate_wait(io_sem: &tokio::sync::Semaphore, want: usize, free: usize, poll: Duration) {
    if free >= want.max(1) {
        tokio::time::sleep(poll).await;
    } else {
        await_capacity(io_sem, want, poll).await;
    }
}

/// Wait until the admission gate has a chance of being open again.
///
/// Both gates are a conjunction of one waitable condition (free io permits)
/// and two that are not (the cpu backlog depth, shutdown), so this waits on
/// the semaphore and keeps `poll` as the ceiling for the rest. It replaced a
/// blind `sleep(25ms)` on both sides: under saturation every slot freed just
/// inside a window idled until the next poll, which is task start latency
/// and nothing else.
///
/// The permits are taken as a SIGNAL and dropped immediately -- the dispatch
/// task that eventually runs acquires its own, exactly as before -- so this
/// changes when the loop wakes, never who owns a slot. Cancellation by the
/// `poll` arm returns anything already accumulated, so a `want` larger than
/// the free count cannot hoard permits from real dispatch for longer than
/// one window.
async fn await_capacity(io_sem: &tokio::sync::Semaphore, want: usize, poll: Duration) {
    let want = want.max(1).min(u32::MAX as usize) as u32;
    tokio::select! {
        biased;
        res = io_sem.acquire_many(want) => {
            if res.is_err() {
                // Closed semaphore: it will never hand out another permit,
                // and acquire returns instantly, so fall back to the poll
                // rather than spin the loop at full speed.
                tokio::time::sleep(poll).await;
            }
        }
        _ = tokio::time::sleep(poll) => {}
    }
}

/// Recovery ticks between two consumer reaps. The tick is
/// `visibility_timeout/2`, so at the default 60s this is one sweep every ten
/// minutes: the leak it drains is one dead consumer per process start, and
/// two XINFO calls per queue per ten minutes is the right price for it.
const CONSUMER_REAP_EVERY_TICKS: u64 = 20;

/// Floor on how long a consumer must have been idle before it is reaped, on
/// top of the visibility multiple. A worker whose fetch loop is paused (a
/// full cpu backlog pauses every lane) still issues no XREADGROUP, so the
/// floor has to be far longer than any pause that is not itself an incident.
const CONSUMER_REAP_MIN_IDLE_MS: u64 = 10 * 60 * 1000;

fn consumer_reap_idle_ms(vt_ms: u64) -> u64 {
    vt_ms.saturating_mul(4).max(CONSUMER_REAP_MIN_IDLE_MS)
}

/// Whether one XINFO CONSUMERS row is a corpse this worker may delete.
///
/// `pending == 0` is the hard rule and is never a heuristic: XGROUP
/// DELCONSUMER drops that consumer's pending entries list, so reaping a
/// consumer that still owns entries would strand them in the stream, in no
/// PEL and behind `last-delivered-id`, where no recovery path can ever reach
/// them again. Idle is the soft one: a false positive on a live but silent
/// consumer costs nothing, because an empty consumer is recreated by its
/// owner's next XREADGROUP.
fn consumer_is_reapable(
    name: &str,
    pending: usize,
    idle_ms: u64,
    self_name: &str,
    min_idle_ms: u64,
) -> bool {
    pending == 0 && name != self_name && idle_ms >= min_idle_ms
}

/// Delete the group's dead consumers, which nothing else ever does.
///
/// The consumer name is minted once per process start (hostname + pid), so
/// every restart, supervised child, rolling pod replacement and wedge self
/// exit leaves its old name in the group forever. Redis never reaps them:
/// they cost memory in the group, and they lengthen XINFO CONSUMERS and the
/// summary form of XPENDING for every operator and every worker that reads
/// them. Slow burn rather than an outage, and this is the drain.
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
                // The reply is how many pending entries went with it. XINFO
                // said zero and the consumer had been silent for
                // `min_idle_ms`, so anything else means it came back to life
                // inside that window and its entries are now unreachable.
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

/// Process one XPENDING page for `queue` starting at `start` (see §4.4 /
/// `recovery_loop` doc). Returns the cursor for the next page, or None when
/// this queue is drained for the tick (final page, or a redis error worth
/// backing off until next tick).
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
        // Exclusive-range resume (`(id`): never re-reads entries this tick,
        // including ones skipped as still-running below.
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

    // H1 eligibility per entry: idle must exceed the envelope's OWN timeout
    // (+ grace), not just the visibility floor.
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
            // Still within its own timeout budget: legitimately running,
            // not stuck. Do not reclaim yet.
            continue;
        }
        if ctx.holds_entry(queue, &entry_id) {
            // This very process is still holding the entry. Whatever the
            // idle clock says, reclaiming here would run a second copy
            // alongside the live first attempt and burn a delivery_count
            // against it -- four of which dead letter a task as
            // RedeliveryLimitExceeded that may not have executed once.
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
            // Same code path as a fresh delivery; retries NOT incremented.
            spawn_dispatch(ctx.clone(), queue.to_string(), entry_id, raw);
        }
    }
    next_cursor
}

/// How often the watchdog stamps every embedded asyncio loop through
/// `call_soon_threadsafe` (shim.py `heartbeat`).
const HEARTBEAT_INTERVAL_MS: u64 = 5_000;

/// Three heartbeat intervals: how long a loop must have been unresponsive,
/// and how long the corroborating signal must agree, before the wedge is
/// called. Fifteen seconds is chosen against the two costs. Too short and an
/// ordinary bad but survivable blocking call inside one `async def` (a
/// synchronous HTTP request with a ten second timeout) restarts the worker
/// every time it runs; too long and the async lane is dead for that whole
/// window on every real wedge. A restart costs about a second plus the
/// redelivery of in flight tasks, which at least once semantics already
/// promise (PROTOCOL.md section 4.4), so the bias is toward acting.
const WEDGE_WINDOW_MS: u64 = 3 * HEARTBEAT_INTERVAL_MS;

/// Exit code for a self exit on a confirmed wedge. Distinct from every other
/// code this binary produces (0 graceful, 1 fatal config or startup, 101
/// panic, 130 forced) so a supervisor, and the operator reading its log, can
/// tell this apart from a crash or a clean stop.
pub const WEDGE_EXIT_CODE: i32 = 87;

/// Largest loop lag measured at the last watchdog tick, in ms (stats:
/// `loop_lag_ms`). A process global rather than a `Ctx` field because there
/// is exactly one embedded interpreter per process, so this reading is a
/// process singleton like `stats::rss_mb`'s own, and the watchdog and the
/// stats loop stay decoupled.
static LOOP_LAG_MS: AtomicU64 = AtomicU64::new(0);

/// One loop's reading from shim.py's `heartbeat()`. `outstanding` is
/// submitted minus completed, not the queue depth: one drain turns a whole
/// batch into Tasks before the first of them can block the thread, so a
/// wedged loop typically shows an empty queue and several Tasks it will
/// never start.
struct LoopBeat {
    lag_ms: u64,
    outstanding: u64,
    completed: u64,
}

/// What the watchdog remembers about one loop between ticks.
struct LoopWatch {
    completed: u64,
    progress_at: Instant,
}

/// Wedged event loop watchdog (docs/decisions/process-model.md, question 3).
///
/// A blocking call inside one `async def` starves the loop thread it runs on
/// of every callback, including asyncio's own `wait_for` deadline, for the
/// life of the process. Nothing can recover that thread: CPython offers no
/// safe way to kill one. At the default `--io-loops 1` it therefore ends 100
/// percent of async throughput while the worker keeps fetching, fails every
/// async task at its full timeout, burns the retry schedule into the dead
/// letter queue, and reports healthy to every orchestrator.
///
/// So the process exits and lets its supervisor restart it, which
/// `supervisor.rs` does in about a second and PROTOCOL.md section 4.4 already
/// covers for in flight tasks. Replacing the wedged loop in place was
/// rejected for 1.0: it leaks a loop and its coroutines per wedge and the
/// round robin in the shim's `submit_async` would need health awareness.
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
            continue; // a drain is not a wedge, and it is already exiting
        }
        // The GIL is only ever taken on a dedicated thread or inside
        // spawn_blocking (pyrt.rs module doc). Exactly one probe is ever
        // outstanding, because this awaits it: if the interpreter stops
        // handing out the GIL entirely this stalls here instead of parking a
        // fresh blocking thread every tick.
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
            // Not process::exit: every asyncio loop thread, the sync pool and
            // any thread task code started are all still live here, which is
            // the exact condition exit_now exists for.
            crate::exit_now(WEDGE_EXIT_CODE);
        }
    }
}

/// Two signals, never one, and someone will want to simplify this to the
/// stale stamp alone: do not. A GIL convoy from the sync or cpu lane delays
/// the stamp exactly like a wedge does (a misclassified CPU heavy task on the
/// sync pool is the incident this instrument was added for), and restarting
/// the worker for a load spike is worse than the brownout the detector
/// exists to end. The stamp only counts once something independent agrees the
/// async lane has stopped producing: work outstanding at a loop that has
/// completed nothing for the same window, or `async_rejected` rising, which
/// is the shim's own per loop queue hitting its cap. A wedged loop with no
/// async work in the process corroborates neither and is left alone, since it
/// is costing nothing yet.
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

/// Call shim.py's `heartbeat()` under the GIL. MUST run on a blocking pool
/// thread, never a tokio worker.
///
/// The module is fetched from `sys.modules` under the name pyrt.rs built it
/// with (`PyModule::from_code(.., c"cauli_worker_shim")`, which publishes it
/// there via `PyImport_ExecCodeModuleEx`), because the runtime's own handle
/// on it is private. A failed lookup only disables detection: this returns an
/// error and the watchdog warns once, so a rename can cost the instrument but
/// never the process.
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

/// Whole milliseconds between two instants, saturating (`now` is always the
/// later one, but a clamp costs nothing and a wrap would read as a wedge).
fn ms_since(earlier: Instant, now: Instant) -> u64 {
    now.saturating_duration_since(earlier).as_millis() as u64
}

/// §7 stats line every --stats-interval seconds. `sync_live`/`sync_abandoned`
/// (H2) make sync-pool thread loss observable instead of a silent capacity
/// drip: sync_live is the pool's current thread count (initial + spawned
/// replacements, capped at a fixed multiple of --io-threads), sync_abandoned
/// is the cumulative count of hard timeout abandonments reported (each
/// spawns a replacement thread unless the pool is already at that cap).
/// `async_rejected` (MEM-5) is the field that moves during an event loop
/// wedge: the shim's own per loop queue rejects submissions past its cap
/// instead of growing forever, and this is the running count of those
/// rejections. It replaced `pending_async` (the pending completion map size),
/// whose own note here already conceded that this was the number that
/// actually moved while it stayed flat. `cpu_backlog` is the
/// live depth behind the fetch loop's admission gate (see its comment): a
/// nonzero reading means fetching is currently paused for every lane, not
/// just cpu, which `Counters::note_cpu_backlog` also logs on the
/// zero/nonzero edge so the pause is not only visible on a poll boundary.
/// `oldest_ms` is the backlog's leading indicator (see `oldest_unacked_ms`).
/// `cpu_rss_mb` is the summed resident memory of the cpu pool's children,
/// which `rss_mb` (this process alone) never included. `loop_lag_ms` is the
/// largest embedded event loop lag the wedge watchdog measured on its last
/// tick (see `wedge_loop`): the one field that moves when a misclassified CPU
/// heavy task on the sync pool starves the async loop's scheduling, which
/// every other field here reports as normal.
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

/// Age in ms of the oldest piece of outstanding work in any of this worker's
/// queues, or 0 when there is none. Deliberately a broker probe rather than a
/// sample taken as tasks run, because it keeps reporting while fetching is
/// paused: a paused fetch loop starts no tasks, so per task sampling goes
/// blind at exactly the moment the backlog is the only thing still moving.
///
/// Outstanding work is the older of two things, and each has to be asked for
/// separately:
///
///   * the oldest entry in the group's pending entries list (delivered,
///     still unacked), from `XPENDING`;
///   * the oldest entry the group has not delivered yet (pure backlog),
///     which is the first entry after the group's `last-delivered-id`.
///
/// This replaced a single `XRANGE q - + COUNT 1`, which read the oldest
/// entry in the STREAM regardless of its state. `add_ack_del` used to
/// pipeline XACK and XDEL unwrapped, so a connection drop between them left
/// an entry acked but present. Such an orphan is in no PEL and behind
/// `last-delivered-id`, so no recovery path can ever reach it and nothing
/// XTRIMs the stream, and the old probe reported its ever growing age
/// forever, across restarts, as though the queue were permanently wedged.
/// That pair is now one `MULTI`/`EXEC` over the single stream key, which
/// closes the window; reading the two real states keeps this leading
/// indicator correct regardless.
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

/// Age of a redis stream id, whose first dash separated field is the
/// millisecond timestamp the entry was written with. 0 for an unparseable
/// id, and 0 rather than a wrapped u64 for an id ahead of this worker's
/// clock (skew between the redis host and this one).
fn stream_id_age_ms(id: &str, now_ms: u64) -> u64 {
    id.split('-')
        .next()
        .and_then(|ms| ms.parse::<u64>().ok())
        .map_or(0, |ms| now_ms.saturating_sub(ms))
}

/// Seconds to milliseconds for the visibility timeout. Saturating, matching
/// the overflow safe style used elsewhere for hostile or just huge input
/// (dispatch.rs, envelope.rs, exec.rs, cpu.rs) and mirroring main.rs's own
/// `visibility_timeout_ms`: release builds have no overflow-checks, so a
/// plain multiply would wrap silently instead.
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

    /// A stream id is `<ms>-<seq>`: only the millisecond field is an age, the
    /// sequence must never leak into it, and nothing here may wrap.
    #[test]
    fn stream_id_age_reads_the_millisecond_field_only() {
        assert_eq!(
            stream_id_age_ms("1700000000000-0", 1_700_000_005_000),
            5_000
        );
        assert_eq!(stream_id_age_ms("1700000000000-99", 1_700_000_000_250), 250);
        assert_eq!(stream_id_age_ms("1700000000000-0", 1_700_000_000_000), 0);
        // Entry id ahead of this worker's clock: saturating, never a wrap to
        // a near u64::MAX age that would read as a catastrophic backlog.
        assert_eq!(stream_id_age_ms("1700000009000-0", 1_700_000_000_000), 0);
        assert_eq!(stream_id_age_ms("garbage-1", 1_700_000_000_000), 0);
        assert_eq!(stream_id_age_ms("", 1_700_000_000_000), 0);
    }

    const OVER: u64 = WEDGE_WINDOW_MS + 1;
    const UNDER: u64 = WEDGE_WINDOW_MS - 1;

    /// The property the exit is allowed to fire on: the loop stopped running
    /// scheduled callbacks AND stopped finishing the work already handed to
    /// it. Either corroborating signal is enough on its own.
    #[test]
    fn a_wedged_loop_is_confirmed() {
        // Work outstanding, nothing completed for the whole window.
        assert!(wedge_confirmed(OVER, 2, OVER, None));
        // Same stale stamp, corroborated instead by the shim's own per loop
        // queue rejecting past its cap, which is the late stage of the same
        // wedge (the queue took the whole window to fill).
        assert!(wedge_confirmed(OVER, 0, 0, Some(0)));
    }

    /// The property that keeps this from being a restart loop: a loop that is
    /// merely slow, or starved of the GIL by another lane, must never trigger
    /// an exit. One signal is never enough.
    #[test]
    fn a_slow_loop_is_not_confirmed() {
        // Late stamp, but tasks are still completing on it: a convoy, not a
        // wedge. This is the case a single check would get wrong.
        assert!(!wedge_confirmed(OVER, 2, 0, None));
        // Late stamp, work waiting, but progress within the window.
        assert!(!wedge_confirmed(OVER, 2, UNDER, None));
        // Late stamp and a rejection, but from an older spell that has since
        // cleared.
        assert!(!wedge_confirmed(OVER, 0, 0, Some(OVER)));
        // Stamp lag under the window: unresponsive for a while, not long
        // enough to call, whatever else agrees.
        assert!(!wedge_confirmed(UNDER, 2, OVER, Some(0)));
        // Wedged but idle: nothing was submitted, so nothing corroborates and
        // nothing is being lost yet.
        assert!(!wedge_confirmed(OVER, 0, OVER, None));
    }

    /// The one rule that can lose work: a consumer holding pending entries is
    /// never reaped, however dead it looks, because DELCONSUMER would drop
    /// its pending entries list and strand those entries.
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
        // The leak this drains: a previous incarnation of this very pod.
        assert!(consumer_is_reapable("host-a-1", 0, min, "host-a-2", min));
        // Idle but not long enough: a live worker between polls.
        assert!(!consumer_is_reapable(
            "host-a-1",
            0,
            min - 1,
            "host-a-2",
            min
        ));
        // This process's own consumer, which the recovery loop is using right
        // now to XCLAIM.
        assert!(!consumer_is_reapable(
            "host-a-2",
            0,
            u64::MAX,
            "host-a-2",
            min
        ));
    }

    /// The threshold is a multiple of the visibility timeout with a floor, so
    /// it stays far past any legitimate quiet spell at both ends of the
    /// configuration range, and never wraps.
    #[test]
    fn the_reap_threshold_clears_the_visibility_floor() {
        assert_eq!(consumer_reap_idle_ms(60_000), CONSUMER_REAP_MIN_IDLE_MS);
        assert_eq!(consumer_reap_idle_ms(3_600_000), 4 * 3_600_000);
        assert_eq!(consumer_reap_idle_ms(u64::MAX), u64::MAX);
    }

    /// Never poll: a freed slot must wake the gate immediately, or the wait
    /// is pure task start latency. `poll` is 30s here, so passing means the
    /// wake came from the semaphore and not from the timer.
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

    /// The other half of the gate (cpu backlog, shutdown) is not waitable, so
    /// a saturated semaphore must still return at the poll -- and a CLOSED
    /// one, whose acquire fails instantly, must not turn the loop into a spin.
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

    /// The gate shut on the cpu backlog alone, with io permits free. Nothing
    /// signals a draining backlog, so waiting on the semaphore here would be
    /// granted instantly and the caller would loop back into the same closed
    /// gate at full speed. The poll has to be the wait.
    #[tokio::test]
    async fn the_gate_does_not_spin_when_only_the_cpu_backlog_holds_it() {
        let poll = Duration::from_millis(30);
        let sem = Arc::new(tokio::sync::Semaphore::new(8));
        let t0 = Instant::now();
        gate_wait(&sem, 2, 8, poll).await;
        assert!(t0.elapsed() >= poll, "free permits must not short circuit");
        // The permits were never held: dispatch still sees all eight.
        assert_eq!(sem.available_permits(), 8);
    }

    /// ...and when the semaphore IS the shut half, the wait stays event
    /// driven: a freed slot wakes the loop long before the poll.
    #[tokio::test]
    async fn the_gate_waits_on_the_semaphore_when_io_is_the_blocker() {
        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        let freeing = sem.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            freeing.add_permits(2);
        });
        let t0 = Instant::now();
        gate_wait(&sem, 2, 0, Duration::from_secs(30)).await;
        assert!(t0.elapsed() < Duration::from_secs(5), "freed while waiting");
    }
}
