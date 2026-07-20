//! Per-class execution: sync io (thread pool), async io (embedded loops),
//! cpu (child processes). Each returns a normalized Outcome.

use crate::ctx::{parse_pyresp, Ctx, Outcome};
use crate::envelope::{Envelope, ErrorJson};
use crate::pyrt::SyncJob;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::warn;

fn fail(type_: &str, msg: String) -> Outcome {
    Outcome::Failure {
        err: ErrorJson::new(type_, msg),
        retryable: true,
    }
}

/// Sync io task on the dedicated OS thread pool. Soft timeout is injected by
/// the shim watchdog (PyThreadState_SetAsyncExc). Hard timeout cannot kill a
/// thread: we mark the task failed (retry path) and abandon the thread result.
pub async fn run_sync_task(ctx: &Arc<Ctx>, env: &Envelope) -> Outcome {
    let _permit = ctx.io_sem.clone().acquire_owned().await.expect("io semaphore closed");
    ctx.counters.inflight_io.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    ctx.sync_pool.submit(SyncJob {
        name: env.task.clone(),
        args_json: env.args_json(),
        kwargs_json: env.kwargs_json(),
        soft_timeout_ms: env.soft_timeout_ms,
        resp: tx,
    });
    let out = match timeout(Duration::from_millis(env.timeout_ms), rx).await {
        Ok(Ok(s)) => parse_pyresp(&s, false),
        Ok(Err(_)) => fail("WorkerLost", "sync executor thread vanished".into()),
        Err(_) => {
            warn!(
                task = %env.task, id = %env.id,
                "hard timeout after {}ms on sync thread task; abandoning thread result",
                env.timeout_ms
            );
            fail(
                "TimeoutError",
                format!("hard timeout after {}ms (thread abandoned)", env.timeout_ms),
            )
        }
    };
    ctx.counters.inflight_io.fetch_sub(1, Ordering::Relaxed);
    out
}

/// Async io task on an embedded asyncio loop; the shim wraps it in
/// asyncio.wait_for(effective_s) per §4.6. A Rust-side backstop timeout
/// (hard + 2s) guards against a wedged loop thread.
pub async fn run_async_task(ctx: &Arc<Ctx>, env: &Envelope) -> Outcome {
    let _permit = ctx.io_sem.clone().acquire_owned().await.expect("io semaphore closed");
    ctx.counters.inflight_io.fetch_add(1, Ordering::Relaxed);
    let effective_s = env.effective_async_timeout_s();
    let rx = {
        let rt = ctx.pyrt.clone();
        let name = env.task.clone();
        let a = env.args_json();
        let k = env.kwargs_json();
        // GIL taken briefly off the tokio worker threads
        tokio::task::spawn_blocking(move || rt.submit_async(&name, &a, &k, effective_s))
            .await
            .expect("spawn_blocking panicked")
    };
    let out = match timeout(Duration::from_millis(env.timeout_ms + 2_000), rx).await {
        Ok(Ok(s)) => parse_pyresp(&s, false),
        Ok(Err(_)) => fail("WorkerLost", "async completion channel dropped".into()),
        Err(_) => fail(
            "TimeoutError",
            format!("no completion within {}ms + grace (event loop unresponsive)", env.timeout_ms),
        ),
    };
    ctx.counters.inflight_io.fetch_sub(1, Ordering::Relaxed);
    out
}

/// Cpu task via the child pool (§5.1). Backlog is a bounded channel of
/// 2 * cpu_workers; when full, this dispatch task blocks on send while the
/// fetch loop pauses via the overflow counter.
pub async fn run_cpu_task(ctx: &Arc<Ctx>, env: &Envelope) -> Outcome {
    let req = serde_json::json!({
        "id": env.id,
        "task": env.task,
        "args": serde_json::from_str::<serde_json::Value>(&env.args_json()).unwrap_or(serde_json::json!([])),
        "kwargs": serde_json::from_str::<serde_json::Value>(&env.kwargs_json()).unwrap_or(serde_json::json!({})),
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
            format!("cpu child SIGKILLed after hard timeout {}ms", env.timeout_ms),
        ),
        Ok(crate::cpu::CpuOutcome::Lost) => {
            fail("WorkerLost", "cpu child died during task".into())
        }
        Err(_) => fail("WorkerLost", "cpu child dropped the task".into()),
    }
}
