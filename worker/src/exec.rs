//! Per-class execution: sync io (thread pool), async io (embedded loops),
//! cpu (child processes). Each returns a normalized Outcome.

use crate::ctx::{parse_pyresp, Ctx, DecrGuard, Outcome};
use crate::envelope::{Envelope, ErrorJson};
use crate::pyrt::{SyncJob, TaskMeta};
use serde::Serialize;
use serde_json::value::RawValue;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, OwnedSemaphorePermit};
use tokio::time::timeout;
use tracing::warn;

/// The §5.1 cpu request line. args and kwargs are the caller's JSON text,
/// copied onto the line as is: the child decodes them, nothing here does.
#[derive(Serialize)]
struct CpuRequest<'a> {
    id: &'a str,
    task: &'a str,
    args: &'a RawValue,
    kwargs: &'a RawValue,
    soft_timeout_ms: Option<u64>,
}

/// Added to an envelope's `timeout_ms` for Rust side backstops: the async
/// completion wait and the recovery loop's reclaim decision.
pub const BACKSTOP_GRACE_MS: u64 = 2_000;

fn fail(type_: &str, msg: String) -> Outcome {
    Outcome::Failure {
        err: ErrorJson::new(type_, msg),
        retryable: true,
    }
}

/// Backs `cauli.current_task()` (py/cauli/_context.py) on the io lanes.
fn task_meta(env: &Envelope) -> TaskMeta {
    TaskMeta {
        id: env.id.clone(),
        retries: env.retries,
        max_retries: env.max_retries,
        queue: env.queue.clone(),
    }
}

/// Sync io task on the OS thread pool. The shim watchdog injects the soft
/// timeout. A hard timeout cannot kill a thread, so the task fails, the
/// thread result is abandoned and the pool spawns a replacement; otherwise
/// wedged tasks would slowly zero sync io capacity.
pub async fn run_sync_task(ctx: &Arc<Ctx>, env: &Envelope, _slot: OwnedSemaphorePermit) -> Outcome {
    ctx.counters.inflight_io.fetch_add(1, Ordering::Relaxed);
    let _dec = DecrGuard(&ctx.counters.inflight_io);

    let (tx, rx) = oneshot::channel();
    let (started_tx, started_rx) = oneshot::channel();
    if ctx
        .sync_pool
        .submit(SyncJob {
            name: env.task.clone(),
            args: env.args_raw().get().into(),
            kwargs: env.kwargs_raw().get().into(),
            soft_timeout_ms: env.soft_timeout_ms,
            meta: Some(Box::new(task_meta(env))),
            started: started_tx,
            resp: tx,
        })
        .is_err()
    {
        // Every pool thread is wedged and the bounded backlog is full.
        return fail(
            "WorkerLost",
            "sync pool queue full (io-threads wedged)".into(),
        );
    }

    // Two phases: `--io-threads` is smaller than the `--io-concurrency` gate,
    // so a job can queue with no thread behind it. That wait must not eat the
    // task's budget or be treated as a wedged thread (abandon and replace).
    // Dropping `rx` on timeout closes `job.resp`, so the pool skips the job.
    let queue_budget = Duration::from_millis(env.timeout_ms);
    match timeout(queue_budget, started_rx).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            return fail("WorkerLost", "sync executor thread vanished".into());
        }
        Err(_) => {
            warn!(
                task = %env.task, id = %env.id, waited_ms = env.timeout_ms,
                "sync task never reached a pool thread; every io thread is busy \
                 (--io-threads is below the --io-concurrency gate). Not a wedged \
                 thread: nothing was abandoned and no replacement is needed"
            );
            return fail(
                "WorkerLost",
                format!(
                    "sync pool queue wait exceeded {}ms with no free io thread",
                    env.timeout_ms
                ),
            );
        }
    }

    // Same latency span in all three lanes: excludes pool startup and
    // backlog parking, which have their own fields.
    let started = Instant::now();
    let outcome = match timeout(Duration::from_millis(env.timeout_ms), rx).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(_)) => fail("WorkerLost", "sync executor thread vanished".into()),
        Err(_) => {
            ctx.sync_pool.report_hard_timeout();
            warn!(
                task = %env.task, id = %env.id,
                "hard timeout after {}ms on sync thread task; abandoning thread result, spawning replacement",
                env.timeout_ms
            );
            fail(
                "TimeLimitExceeded",
                format!("hard timeout after {}ms (thread abandoned)", env.timeout_ms),
            )
        }
    };
    ctx.counters.lat_sync.record(started.elapsed());
    outcome
}

/// Async io task on an embedded asyncio loop. The shim enforces soft then
/// hard deadlines (§4.6); the Rust backstop (hard + grace) covers a wedged
/// loop thread that never answers.
pub async fn run_async_task(
    ctx: &Arc<Ctx>,
    env: &Envelope,
    _slot: OwnedSemaphorePermit,
) -> Outcome {
    ctx.counters.inflight_io.fetch_add(1, Ordering::Relaxed);
    let _dec = DecrGuard(&ctx.counters.inflight_io);

    // Both deadlines cross, not their min (envelope::async_timeouts_s).
    let (soft_s, hard_s) = env.async_timeouts_s();
    let (token, rx) = ctx.pyrt.queue_submit(
        &env.task,
        env.args_raw().get(),
        env.kwargs_raw().get(),
        hard_s,
        soft_s,
        Some(task_meta(env)),
    );
    // timeout_ms is caller controlled: near u64::MAX it must not wrap.
    let backstop_ms = env.timeout_ms.saturating_add(BACKSTOP_GRACE_MS);
    let started = Instant::now();
    let outcome = match timeout(Duration::from_millis(backstop_ms), rx).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(_)) => fail("WorkerLost", "async completion channel dropped".into()),
        Err(_) => {
            // Drop the pending slot too, or a wedged loop leaks the coroutine.
            ctx.pyrt.cancel(token);
            fail(
                "TimeLimitExceeded",
                format!(
                    "no completion within {}ms + grace (event loop unresponsive)",
                    env.timeout_ms
                ),
            )
        }
    };
    ctx.counters.lat_async.record(started.elapsed());
    outcome
}

/// Cpu task via the child pool (§5.1). When the bounded backlog is full this
/// task blocks on send and the fetch loop pauses on the overflow counter.
pub async fn run_cpu_task(ctx: &Arc<Ctx>, env: &Envelope) -> Outcome {
    // Children answer out of order, so responses match on this id. The
    // suffix keeps it unique if one envelope id is in flight twice (crafted
    // duplicates, §4.4 races). Result keys always use the envelope id.
    static CPU_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let wire_id = format!("{}.{:x}", env.id, CPU_SEQ.fetch_add(1, Ordering::Relaxed));
    let mut req = serde_json::to_string(&CpuRequest {
        id: &wire_id,
        task: &env.task,
        args: env.args_raw(),
        kwargs: env.kwargs_raw(),
        soft_timeout_ms: env.soft_timeout_ms,
    })
    .expect("cpu request serialize");
    // One request per line. A raw newline can only be whitespace between
    // JSON tokens (strings escape it), so a space keeps every value.
    if req.contains('\n') {
        req = req.replace('\n', " ");
    }
    let (tx, rx) = oneshot::channel();
    let job = crate::cpu::CpuJob {
        id: wire_id,
        req_line: req,
        timeout_ms: env.timeout_ms,
        resp: tx,
    };
    let cpu = ctx.cpu_pool().await;
    // Admission slot, held until the outcome. Without it entries park with
    // their PEL idle clocks running and get reclaimed, then dead lettered as
    // `RedeliveryLimitExceeded` unrun. Waiting counts into `overflow`, which
    // pauses both the fetch loop and the recovery loop.
    let _slot = match cpu.admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            cpu.overflow.fetch_add(1, Ordering::SeqCst);
            let permit = cpu.admission.clone().acquire_owned().await;
            if cpu.overflow.fetch_sub(1, Ordering::SeqCst) == 1 {
                cpu.drained.notify_one();
            }
            match permit {
                Ok(p) => p,
                Err(_) => return fail("WorkerLost", "cpu pool closed".into()),
            }
        }
    };
    match cpu.tx.try_send(job) {
        Ok(()) => {}
        Err(async_channel::TrySendError::Full(job)) => {
            cpu.overflow.fetch_add(1, Ordering::SeqCst);
            let r = cpu.tx.send(job).await;
            if cpu.overflow.fetch_sub(1, Ordering::SeqCst) == 1 {
                cpu.drained.notify_one();
            }
            if r.is_err() {
                return fail("WorkerLost", "cpu pool closed".into());
            }
        }
        Err(async_channel::TrySendError::Closed(_)) => {
            return fail("WorkerLost", "cpu pool closed".into());
        }
    }
    let started = Instant::now();
    let outcome = match rx.await {
        Ok(crate::cpu::CpuOutcome::Resp(line)) => parse_pyresp(&line, true),
        Ok(crate::cpu::CpuOutcome::Timeout) => fail(
            "TimeLimitExceeded",
            format!(
                "cpu child SIGKILLed after hard timeout {}ms",
                env.timeout_ms
            ),
        ),
        Ok(crate::cpu::CpuOutcome::Lost) => {
            ctx.counters.cpu_lost.fetch_add(1, Ordering::Relaxed);
            fail("WorkerLost", "cpu child died during task".into())
        }
        Err(_) => fail("WorkerLost", "cpu child dropped the task".into()),
    };
    ctx.counters.lat_cpu.record(started.elapsed());
    outcome
}
