//! Per-class execution: sync io (thread pool), async io (embedded loops),
//! cpu (child processes). Each returns a normalized Outcome.

use crate::ctx::{parse_pyresp, Ctx, DecrGuard, Outcome};
use crate::envelope::{Envelope, ErrorJson};
use crate::pyrt::SyncJob;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::warn;

/// Grace window added on top of an envelope's own `timeout_ms` for Rust-side
/// backstop timeouts (the async completion channel, and — H1 — the recovery
/// loop's reclaim decision). Matches the pre-existing async backstop value.
pub const BACKSTOP_GRACE_MS: u64 = 2_000;

fn fail(type_: &str, msg: String) -> Outcome {
    Outcome::Failure {
        err: ErrorJson::new(type_, msg),
        retryable: true,
    }
}

/// Sync io task on the dedicated OS thread pool. Soft timeout is injected by
/// the shim watchdog (PyThreadState_SetAsyncExc). Hard timeout cannot kill a
/// thread: we mark the task failed (retry path), abandon the thread result,
/// and ask the pool to spawn a replacement thread so capacity is restored
/// (audit H2 — without this, a slow drip of wedged tasks permanently zeroes
/// sync-io capacity over the worker's lifetime).
pub async fn run_sync_task(ctx: &Arc<Ctx>, env: &Envelope) -> Outcome {
    let _permit = ctx
        .io_sem
        .clone()
        .acquire_owned()
        .await
        .expect("io semaphore closed");
    ctx.counters.inflight_io.fetch_add(1, Ordering::Relaxed);
    let _dec = DecrGuard(&ctx.counters.inflight_io); // panic-safe: MEM-3

    let (tx, rx) = oneshot::channel();
    if ctx
        .sync_pool
        .submit(SyncJob {
            name: env.task.clone(),
            args_json: env.args_json(),
            kwargs_json: env.kwargs_json(),
            soft_timeout_ms: env.soft_timeout_ms,
            resp: tx,
        })
        .is_err()
    {
        // Bounded queue full (H2): every pool thread is wedged and the
        // backlog is already at capacity. Fail fast instead of growing memory
        // without bound.
        return fail(
            "WorkerLost",
            "sync pool queue full (io-threads wedged)".into(),
        );
    }

    match timeout(Duration::from_millis(env.timeout_ms), rx).await {
        Ok(Ok(s)) => parse_pyresp(&s, false),
        Ok(Err(_)) => fail("WorkerLost", "sync executor thread vanished".into()),
        Err(_) => {
            // `rx` is dropped right here (the timeout()'s inner future is
            // discarded once it loses the race), which flips job.resp closed
            // for the pool thread side. If the job is still queued (never
            // dequeued), the pool skips it instead of running it late as a
            // zombie (see pyrt::SyncPool's worker loop).
            ctx.sync_pool.report_hard_timeout();
            warn!(
                task = %env.task, id = %env.id,
                "hard timeout after {}ms on sync thread task; abandoning thread result, spawning replacement",
                env.timeout_ms
            );
            fail(
                "TimeoutError",
                format!("hard timeout after {}ms (thread abandoned)", env.timeout_ms),
            )
        }
    }
}

/// Async io task on an embedded asyncio loop; the shim wraps it in
/// asyncio.wait_for(effective_s) per §4.6. A Rust-side backstop timeout
/// (hard + grace) guards against a wedged loop thread.
pub async fn run_async_task(ctx: &Arc<Ctx>, env: &Envelope) -> Outcome {
    let _permit = ctx
        .io_sem
        .clone()
        .acquire_owned()
        .await
        .expect("io semaphore closed");
    ctx.counters.inflight_io.fetch_add(1, Ordering::Relaxed);
    let _dec = DecrGuard(&ctx.counters.inflight_io); // panic-safe: MEM-3

    let effective_s = env.effective_async_timeout_s();
    let (token, rx) = {
        let rt = ctx.pyrt.clone();
        let name = env.task.clone();
        let a = env.args_json();
        let k = env.kwargs_json();
        // GIL taken briefly off the tokio worker threads
        match tokio::task::spawn_blocking(move || rt.submit_async(&name, &a, &k, effective_s)).await
        {
            Ok(v) => v,
            Err(e) => {
                // A panic on the Python side (submit_async / pyo3 call) must
                // not become a worker-task panic (that would itself trip
                // MEM-3 territory); report it as a normal retryable failure.
                return fail("WorkerShimError", format!("submit_async panicked: {e}"));
            }
        }
    };
    // saturating_add: H3 — an attacker-chosen timeout_ms near u64::MAX must
    // not wrap this backstop to a near-zero duration (spurious TimeoutError).
    let backstop_ms = env.timeout_ms.saturating_add(BACKSTOP_GRACE_MS);
    match timeout(Duration::from_millis(backstop_ms), rx).await {
        Ok(Ok(s)) => parse_pyresp(&s, false),
        Ok(Err(_)) => fail("WorkerLost", "async completion channel dropped".into()),
        Err(_) => {
            // MEM-1: stop waiting AND drop the pending-completion slot, so a
            // wedged event-loop thread that never actually finishes this
            // coroutine cannot leak it (and the coroutine/args/kwargs it
            // holds) forever.
            ctx.pyrt.cancel(token);
            fail(
                "TimeoutError",
                format!(
                    "no completion within {}ms + grace (event loop unresponsive)",
                    env.timeout_ms
                ),
            )
        }
    }
}

/// Cpu task via the child pool (§5.1). Backlog is a bounded channel of
/// 2 * cpu_workers; when full, this dispatch task blocks on send while the
/// fetch loop pauses via the overflow counter.
pub async fn run_cpu_task(ctx: &Arc<Ctx>, env: &Envelope) -> Outcome {
    let req = serde_json::json!({
        "id": env.id,
        "task": env.task,
        // env.args/kwargs directly (Value), not a serialize-then-reparse
        // round trip through the *_json() string helpers (audit nit).
        "args": env.args_value(),
        "kwargs": env.kwargs_value(),
        "soft_timeout_ms": env.soft_timeout_ms,
    })
    .to_string();
    let (tx, rx) = oneshot::channel();
    let job = crate::cpu::CpuJob {
        req_line: req,
        timeout_ms: env.timeout_ms,
        resp: tx,
    };
    match ctx.cpu.tx.try_send(job) {
        Ok(()) => {}
        Err(async_channel::TrySendError::Full(job)) => {
            ctx.cpu.overflow.fetch_add(1, Ordering::SeqCst);
            let r = ctx.cpu.tx.send(job).await;
            ctx.cpu.overflow.fetch_sub(1, Ordering::SeqCst);
            if r.is_err() {
                return fail("WorkerLost", "cpu pool closed".into());
            }
        }
        Err(async_channel::TrySendError::Closed(_)) => {
            return fail("WorkerLost", "cpu pool closed".into());
        }
    }
    match rx.await {
        Ok(crate::cpu::CpuOutcome::Resp(line)) => parse_pyresp(&line, true),
        Ok(crate::cpu::CpuOutcome::Timeout) => fail(
            "TimeoutError",
            format!(
                "cpu child SIGKILLed after hard timeout {}ms",
                env.timeout_ms
            ),
        ),
        Ok(crate::cpu::CpuOutcome::Lost) => fail("WorkerLost", "cpu child died during task".into()),
        Err(_) => fail("WorkerLost", "cpu child dropped the task".into()),
    }
}
