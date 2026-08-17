//! Embedded CPython integration.
//!
//! Rules honored here:
//! - `pyo3::prepare_freethreaded_python()` once at startup, before tokio.
//! - The GIL is only ever taken on dedicated OS threads (sync pool) or inside
//!   `spawn_blocking` (async submit). Tokio worker threads never hold the GIL
//!   while doing broker I/O.
//! - All Python-side complexity lives in the embedded shim (src/shim.py).
//! - Task arguments and outcomes cross as real Python objects, converted by
//!   src/pyjson.rs. The shim contains no per-task JSON codec: encoding and
//!   decoding there would run while holding the GIL that every in-process
//!   task shares, and so came straight out of io throughput.

use crate::ctx::Outcome;
use crate::envelope::ErrorJson;
use anyhow::{anyhow, Context, Result};
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyModule, PyTuple};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Registry entry parsed from shim `load_app` JSON (PROTOCOL §6 attributes).
/// Routing uses kind/is_async; timeout_ms feeds the H1 startup invariant
/// check (main.rs). shim.py's `load_app` sends several more attributes
/// (max_retries, backoff_*, jitter, store_result, queue) that only matter
/// per-envelope at execution time (§4.2/§4.6 math always uses the envelope's
/// own values, never the registry) — serde ignores those extra JSON fields
/// rather than the struct carrying dead fields nothing reads.
#[derive(Deserialize, Clone, Debug)]
pub struct TaskSpec {
    pub kind: String,
    pub is_async: bool,
    pub timeout_ms: u64,
}

#[derive(Deserialize, Debug)]
pub struct AppConfig {
    pub redis_url: String,
    pub default_queue: String,
    pub result_ttl: u64,
    pub idemp_ttl: u64,
    /// PROTOCOL §9.2: `{queue: seconds}` with `"*"` as the fallback key.
    /// `default` because an app object predating queue TTLs must still load.
    #[serde(default)]
    pub queue_ttl: HashMap<String, f64>,
    pub tasks: HashMap<String, TaskSpec>,
}

pub struct PyRuntime {
    shim: Py<PyModule>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Outcome>>>>,
    next_token: AtomicU64,
    /// Async submissions are queued here and drained by ONE dedicated
    /// submitter thread (see `init`), one GIL entry per batch. The previous
    /// design took the GIL from a fresh `spawn_blocking` closure per task;
    /// at high --io-concurrency that inflated tokio's blocking pool toward
    /// its 512-thread cap, all of them convoying on the GIL against the
    /// event-loop thread that was trying to run the tasks (measured: 527 OS
    /// threads and a 20 ms task body inflated to 92 ms p99 at 2048 in
    /// flight). A single submitter deletes the convoy and the pool usage;
    /// submission order becomes FIFO, which the racing closures never were.
    submit_tx: crossbeam_channel::Sender<SubmitJob>,
    /// MEM-5: cumulative submissions the shim rejected because a loop's own
    /// pending list was at cap (stats: `async_rejected`). Counted here
    /// rather than polled from Python: the GIL may only be taken on a
    /// dedicated thread or inside spawn_blocking (see the module doc above),
    /// and submit_batch_under_gil already holds it once per batch anyway.
    async_rejected: AtomicU64,
}

/// One queued async submission. Owns its data: the envelope's args/kwargs are
/// cloned at queue time exactly as the old spawn_blocking closure cloned them.
struct SubmitJob {
    token: u64,
    name: String,
    args: Value,
    kwargs: Value,
    timeout_s: f64,
}

/// Normalize the shim's outcome dict (a real Python object, not JSON text)
/// into an `Outcome`. Mirrors `ctx::parse_pyresp` field for field so the two
/// entry points cannot drift; the only difference is where the data comes
/// from. On a successful task exactly ONE traversal happens: the result
/// object into a `Value`.
fn outcome_from_py(obj: &Bound<'_, PyAny>) -> Outcome {
    let get = |k: &str| obj.get_item(k).ok();
    let flag = |k: &str| {
        get(k)
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false)
    };

    if flag("ok") {
        let result = match get("result") {
            Some(r) => match crate::pyjson::py_to_json(&r, 0) {
                Ok(v) => v,
                Err(e) => {
                    // Same classification the Python codec produced: a result
                    // that cannot be represented is terminal, not retryable
                    // (retrying re-runs the task to fail identically).
                    return Outcome::Failure {
                        err: ErrorJson::new(
                            "SerializationError",
                            format!("task result is not JSON serializable: {}", e.0),
                        ),
                        retryable: false,
                    };
                }
            },
            None => Value::Null,
        };
        return Outcome::Success(result);
    }

    let err = get("error")
        .and_then(|e| {
            let f = |k: &str| {
                e.get_item(k)
                    .ok()
                    .and_then(|v| v.extract::<String>().ok())
                    .unwrap_or_default()
            };
            let type_ = f("type");
            (!type_.is_empty()).then(|| ErrorJson {
                type_,
                message: f("message"),
                traceback: f("traceback"),
            })
        })
        .unwrap_or_else(|| {
            ErrorJson::new("UnknownError", "executor reported failure without error")
        });

    if flag("retry") {
        return Outcome::ForceRetry {
            countdown: get("countdown").and_then(|c| c.extract::<f64>().ok()),
            err,
        };
    }
    let retryable = get("retryable")
        .and_then(|r| r.extract::<bool>().ok())
        .unwrap_or(err.type_ != "SerializationError");
    Outcome::Failure { err, retryable }
}

/// `json_to_py`'s only failure mode is the argument tree nesting deeper than
/// `pyjson::MAX_DEPTH`: a structural property of the payload itself, not a
/// transient shim or environment problem, so retrying parses the identical
/// too deep tree again and fails identically every time. Classified the
/// same as the opposite direction's depth failure (`py_to_json`, handled in
/// `outcome_from_py` above) rather than left to fall into the generic
/// `WorkerShimError` catch all below: that type name reads as "the shim
/// itself misbehaved", which sends the next reader looking in the wrong
/// place, and worse, is retryable, which would burn the full backoff
/// schedule on a payload that can never succeed.
fn depth_failure_outcome(py: Python<'_>, e: &PyErr) -> Outcome {
    Outcome::Failure {
        err: ErrorJson::new(
            "SerializationError",
            format!(
                "task arguments are not JSON serializable: {}",
                pyerr_string(py, e)
            ),
        ),
        retryable: false,
    }
}

/// Matches the caps already enforced Python side (shim.py's `_MAX_TB`,
/// _exec.py's `_TRACEBACK_CAP`, both 8192): `pyerr_string` below feeds
/// `ErrorJson`, which is written into Redis result keys and DLQ entries
/// verbatim, so an unbounded exception message or traceback would land
/// there whole.
const MAX_PYERR_CHARS: usize = 8192;

fn pyerr_string(py: Python<'_>, e: &PyErr) -> String {
    let mut s = e.to_string();
    if let Some(tb) = e.traceback(py) {
        if let Ok(t) = tb.format() {
            s.push('\n');
            s.push_str(&t);
        }
    }
    crate::envelope::safe_truncate(&s, MAX_PYERR_CHARS).to_string()
}

impl PyRuntime {
    /// Initialize the interpreter, import the shim, load the app, register the
    /// completion callback and start the asyncio loop threads.
    /// Returns the runtime and the parsed app config.
    pub fn init(app_spec: &str, io_loops: usize) -> Result<(Arc<PyRuntime>, AppConfig)> {
        // Mandated entry point; pyo3 0.26 aliases it to Python::initialize.
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Outcome>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_cb = pending.clone();
        let app_spec = app_spec.to_string();

        let (shim, cfg_json) = Python::attach(|py| -> Result<(Py<PyModule>, String)> {
            let code = CString::new(include_str!("shim.py")).expect("shim.py contains NUL byte");
            let m = PyModule::from_code(py, code.as_c_str(), c"shim.py", c"cauli_worker_shim")
                .map_err(|e| anyhow!("failed to load embedded shim: {}", pyerr_string(py, &e)))?;

            let cfg: String = m
                .getattr("load_app")
                .and_then(|f| f.call1((app_spec.as_str(), "[]")))
                .and_then(|r| r.extract())
                .map_err(|e| {
                    anyhow!(
                        "failed to load app {:?}: {}",
                        app_spec,
                        pyerr_string(py, &e)
                    )
                })?;

            let cb = PyCFunction::new_closure(
                py,
                Some(c"cauli_complete"),
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<()> {
                    // Runs on the asyncio loop thread WITH THE GIL HELD. The
                    // outcome arrives as a real dict, so the shim no longer
                    // encodes JSON here and no string is copied across.
                    //
                    // Convert BEFORE taking the mutex: this lock is also taken
                    // (GIL-free) by submit_async and cancel, and blocking on it
                    // while holding the GIL would stall the whole event loop,
                    // not just this task.
                    let token: u64 = args.get_item(0)?.extract()?;
                    let outcome = outcome_from_py(&args.get_item(1)?);
                    if let Some(tx) = pending_cb.lock().unwrap().remove(&token) {
                        let _ = tx.send(outcome);
                    }
                    Ok(())
                },
            )
            .map_err(|e| {
                anyhow!(
                    "failed to build completion callback: {}",
                    pyerr_string(py, &e)
                )
            })?;

            m.getattr("set_callback")
                .and_then(|f| f.call1((cb,)))
                .map_err(|e| anyhow!("set_callback failed: {}", pyerr_string(py, &e)))?;
            m.getattr("start_loops")
                .and_then(|f| f.call1((io_loops,)))
                .map_err(|e| anyhow!("start_loops failed: {}", pyerr_string(py, &e)))?;

            Ok((m.unbind(), cfg))
        })?;

        // Do not interpolate cfg_json here: shim.py's load_app serializes
        // redis_url (with its password) and the full task registry into this
        // same string, and main.rs logs the full context chain on startup
        // failure ({e:#}). The wrapped serde_json error is appended to that
        // chain automatically and already names the parse failure (its type
        // and position) without echoing unrelated fields, so this stays
        // useful for debugging without the raw JSON.
        let cfg: AppConfig = serde_json::from_str(&cfg_json).with_context(|| {
            format!("shim load_app returned unparseable config for app {app_spec:?}")
        })?;

        let (submit_tx, submit_rx) = crossbeam_channel::unbounded::<SubmitJob>();
        let rt = Arc::new(PyRuntime {
            shim,
            pending,
            next_token: AtomicU64::new(1),
            submit_tx,
            async_rejected: AtomicU64::new(0),
        });

        // The async submitter thread. Blocks on the channel, then drains
        // whatever else is already queued and schedules the whole batch under
        // ONE GIL entry. Unbounded is safe: the io admission semaphore
        // (exec::run_async_task) caps queued jobs at --io-concurrency.
        //
        // Thread-state note (see the sync-pool landmine below): this thread
        // uses a plain Python::attach per batch, NOT a pinned thread state.
        // That is deliberate and safe HERE because nothing on this path
        // relies on threading.local surviving between batches -- the shim's
        // submit_async touches module globals only. The pinning requirement
        // applies to threads that EXECUTE task bodies, which cache things
        // like Django connections in thread locals. This thread never does.
        let rt_submit = Arc::clone(&rt);
        std::thread::Builder::new()
            .name("cauli-async-submit".into())
            .spawn(move || {
                const DRAIN_MAX: usize = 128;
                let mut batch: Vec<SubmitJob> = Vec::with_capacity(DRAIN_MAX);
                while let Ok(first) = submit_rx.recv() {
                    batch.push(first);
                    while batch.len() < DRAIN_MAX {
                        match submit_rx.try_recv() {
                            Ok(job) => batch.push(job),
                            Err(_) => break,
                        }
                    }
                    rt_submit.submit_batch_under_gil(&mut batch);
                }
                // Channel closed: PyRuntime dropped, worker is shutting down.
            })
            .expect("failed to spawn cauli-async-submit thread");

        Ok((rt, cfg))
    }

    /// Blocking sync task execution. MUST be called from a dedicated OS thread
    /// (the sync io pool), never from a tokio worker thread.
    ///
    /// Arguments cross as real Python objects and the outcome comes back as a
    /// real Python object: no JSON text is produced or parsed on this path in
    /// either direction. That matters because every instruction executed here
    /// holds the GIL, which all in-process tasks share -- the two `json.loads`
    /// calls and the encode that used to live in the shim were subtracted
    /// straight from total io throughput (see src/pyjson.rs).
    pub fn run_sync_blocking(
        &self,
        name: &str,
        args: &Value,
        kwargs: &Value,
        soft_timeout_ms: Option<u64>,
    ) -> Outcome {
        Python::attach(|py| {
            let call = (|| -> PyResult<Outcome> {
                let a = match crate::pyjson::json_to_py(py, args, 0) {
                    Ok(v) => v,
                    Err(e) => return Ok(depth_failure_outcome(py, &e)),
                };
                let k = match crate::pyjson::json_to_py(py, kwargs, 0) {
                    Ok(v) => v,
                    Err(e) => return Ok(depth_failure_outcome(py, &e)),
                };
                let out =
                    self.shim
                        .bind(py)
                        .getattr("run_sync")?
                        .call1((name, a, k, soft_timeout_ms))?;
                Ok(outcome_from_py(&out))
            })();
            call.unwrap_or_else(|e| Outcome::Failure {
                err: ErrorJson::new(
                    "WorkerShimError",
                    format!("run_sync failed: {}", pyerr_string(py, &e)),
                ),
                retryable: true,
            })
        })
    }

    /// Queue an async task for the submitter thread. Completion arrives
    /// push-style on the returned oneshot (via the registered Python
    /// callback). Takes no GIL and never blocks: call it directly from a
    /// tokio worker thread. Returns the token alongside the receiver so the
    /// caller can `cancel(token)` if it gives up waiting (MEM-1).
    pub fn queue_submit(
        &self,
        name: &str,
        args: &Value,
        kwargs: &Value,
        timeout_s: f64,
    ) -> (u64, oneshot::Receiver<Outcome>) {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        // Insert BEFORE send: once the job is on the channel the completion
        // callback can fire on the loop thread at any moment, and it must
        // find the slot.
        self.pending.lock().unwrap().insert(token, tx);

        let job = SubmitJob {
            token,
            name: name.to_string(),
            args: args.clone(),
            kwargs: kwargs.clone(),
            timeout_s,
        };
        if self.submit_tx.send(job).is_err() {
            // Submitter thread gone: only reachable during teardown.
            self.fail_pending(
                token,
                "queue_submit failed: submitter thread has exited".to_string(),
            );
        }
        (token, rx)
    }

    /// Schedule one drained batch on the event loops, under a single GIL
    /// entry. Runs ONLY on the cauli-async-submit thread. `submit_async` is
    /// resolved once per batch; the shim then batches its own loop wakeups on
    /// top (its per-loop pending queues), so a burst of N tasks costs one
    /// GIL entry here and one loop wakeup there instead of N of each.
    fn submit_batch_under_gil(&self, batch: &mut Vec<SubmitJob>) {
        // Depth failures are collected separately from generic shim failures:
        // they complete with a typed Outcome that is not retryable, directly,
        // rather than through `fail_pending`'s WorkerShimError/retryable bucket.
        let (failures, depth_failures) = Python::attach(|py| {
            let mut failed: Vec<(u64, String)> = Vec::new();
            let mut depth_failed: Vec<(u64, Outcome)> = Vec::new();
            let submit = match self.shim.bind(py).getattr("submit_async") {
                Ok(f) => f,
                Err(e) => {
                    // Shim itself broken: fail the whole batch, same
                    // retryable semantics as a per-task submit error.
                    let msg = pyerr_string(py, &e);
                    for job in batch.iter() {
                        failed.push((job.token, msg.clone()));
                    }
                    return (failed, depth_failed);
                }
            };
            // MEM-5: fetched once per batch, not cached on PyRuntime -- this
            // closure already holds the GIL and nothing else needs it.
            let queue_full_ty = self.shim.bind(py).getattr("AsyncQueueFull").ok();
            for job in batch.iter() {
                let a = match crate::pyjson::json_to_py(py, &job.args, 0) {
                    Ok(v) => v,
                    Err(e) => {
                        depth_failed.push((job.token, depth_failure_outcome(py, &e)));
                        continue;
                    }
                };
                let k = match crate::pyjson::json_to_py(py, &job.kwargs, 0) {
                    Ok(v) => v,
                    Err(e) => {
                        depth_failed.push((job.token, depth_failure_outcome(py, &e)));
                        continue;
                    }
                };
                if let Err(e) = submit.call1((job.token, job.name.as_str(), a, k, job.timeout_s)) {
                    if queue_full_ty
                        .as_ref()
                        .is_some_and(|ty| e.is_instance(py, ty))
                    {
                        self.async_rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    failed.push((job.token, pyerr_string(py, &e)));
                }
            }
            (failed, depth_failed)
        });
        for (token, msg) in failures {
            self.fail_pending(token, format!("submit_async failed: {msg}"));
        }
        for (token, outcome) in depth_failures {
            self.complete_pending(token, outcome);
        }
        batch.clear();
    }

    /// Complete a pending slot with the given outcome. Does nothing if the
    /// completion callback got there first.
    fn complete_pending(&self, token: u64, outcome: Outcome) {
        if let Some(tx) = self.pending.lock().unwrap().remove(&token) {
            let _ = tx.send(outcome);
        }
    }

    /// Complete a pending slot with a retryable shim failure (no-op if the
    /// completion callback got there first).
    fn fail_pending(&self, token: u64, msg: String) {
        self.complete_pending(
            token,
            Outcome::Failure {
                err: ErrorJson::new("WorkerShimError", msg),
                retryable: true,
            },
        );
    }

    /// MEM-1: remove (and drop) a pending completion slot. Called when the
    /// Rust-side backstop timeout gives up waiting on a submitted async task,
    /// so a wedged event-loop thread that never actually completes the
    /// coroutine (and so never calls the completion callback) cannot leak
    /// this slot -- and the Python-side coroutine/args/kwargs it was
    /// holding -- forever. A no-op if the callback already consumed it.
    pub fn cancel(&self, token: u64) {
        self.pending.lock().unwrap().remove(&token);
    }

    /// Cumulative submissions rejected because a loop's own pending list was
    /// at cap (stats: `async_rejected`, MEM-5). This is the counter that
    /// actually moves during an event loop wedge, which is why it replaced
    /// the pending completion map size on the stats line: see
    /// submit_batch_under_gil.
    pub fn async_rejected(&self) -> u64 {
        self.async_rejected.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Sync io pool: dedicated OS threads that acquire the GIL only around the shim
// run_sync call. CPython releases the GIL during blocking socket I/O, so this
// yields real io parallelism.
//
// H2 hardening: a hard timeout cannot kill a wedged OS thread (blocking call
// with no timeout, a C extension ignoring the async-exc injection). Rather
// than silently losing that pool slot forever:
//  - the queue is bounded (capacity = --io-concurrency) so a permanent global
//    wedge cannot grow memory without bound;
//  - each queued job's oneshot `resp` is the cancellation signal: if the
//    dispatcher already gave up (hard timeout elapsed) the receiver is
//    dropped and `resp.is_closed()` is true, so a worker thread skips running
//    it instead of executing a "zombie" job with unpredictable-timing side
//    effects;
//  - on a reported hard timeout, a replacement thread is spawned to restore
//    capacity, up to a fixed ceiling (SYNC_POOL_THREAD_CEILING_MULTIPLE
//    times the configured pool size); past that point spawn_worker logs a
//    warning and refuses instead of growing without bound. If the original
//    wedged thread ever does return, it just resumes serving as extra
//    headroom (we cannot safely kill a genuinely blocked OS thread from
//    here).
// ---------------------------------------------------------------------------

/// `report_hard_timeout` -> `spawn_worker` replaces a possibly wedged thread
/// with no way to know whether the original is ever coming back (H2). Left
/// unchecked, a hot loop of hard timeouts leaks one OS thread (an 8 MB stack
/// plus a pinned CPython thread state) per hit, forever, and no wedge is
/// even required to trigger it: an envelope with timeout_ms 0 alone makes
/// the dispatcher give up before a sync thread can ever answer (see
/// exec::run_sync_task). 4x gives real headroom for the legitimate case
/// (several genuinely wedged calls at once should not make the pool refuse
/// a replacement on the first one) while keeping the worst case a small
/// fixed multiple of the operator's own --io-threads choice instead of
/// unbounded growth: even at the largest auto derived --io-threads (512,
/// cli.rs) the ceiling (2048) is a small constant factor above the measured
/// sync knee (~1000 threads per proc, cli.rs), not the unbounded thousands
/// a leak like this used to reach within minutes.
const SYNC_POOL_THREAD_CEILING_MULTIPLE: i64 = 4;

/// Test only hook (gated like `CAULI_EXEC_CMD` in cpu.rs, a plain `cargo
/// build --release` carries none of this): a job queued with this exact
/// name makes the pool worker panic deliberately right after dequeuing it,
/// before touching Python. Lets a test reproduce the DecrGuard regression
/// (a panic inside a pool thread must still decrement live_threads) without
/// needing a real blocking call that hangs.
#[cfg(any(test, feature = "test-hooks"))]
const TEST_HOOK_PANIC_JOB: &str = "__cauli_test_hook_panic__";

pub struct SyncJob {
    pub name: String,
    /// Parsed args/kwargs, NOT JSON text: the conversion into Python objects
    /// happens on the pool thread under the GIL, and no encode/decode step
    /// exists on either side of the boundary any more (src/pyjson.rs).
    pub args: Value,
    pub kwargs: Value,
    pub soft_timeout_ms: Option<u64>,
    pub resp: oneshot::Sender<Outcome>,
}

pub struct SyncPool {
    tx: crossbeam_channel::Sender<SyncJob>,
    rx: crossbeam_channel::Receiver<SyncJob>,
    rt: Arc<PyRuntime>,
    next_idx: AtomicUsize,
    /// Fixed at construction: `threads * SYNC_POOL_THREAD_CEILING_MULTIPLE`.
    max_threads: i64,
    /// Hard-timeout abandonments reported so far (stats: `sync_abandoned`).
    pub abandoned: Arc<AtomicU64>,
    /// Worker threads currently alive (initial pool + replacements; stats:
    /// `sync_live`). Never decremented below the count that returns from a
    /// wedge, since we let those resume as extra headroom rather than race to
    /// retire them.
    pub live_threads: Arc<std::sync::atomic::AtomicI64>,
}

impl SyncPool {
    /// `queue_capacity` bounds the crossbeam channel; pass `--io-concurrency`
    /// (the same admission gate that already bounds outstanding submissions
    /// in the common case) so a fully-wedged pool cannot grow the backlog
    /// without bound (MEM-2 (c)).
    pub fn start(rt: Arc<PyRuntime>, threads: usize, queue_capacity: usize) -> SyncPool {
        let (tx, rx) = crossbeam_channel::bounded::<SyncJob>(queue_capacity.max(1));
        let threads = threads.max(1);
        let pool = SyncPool {
            tx,
            rx,
            rt,
            next_idx: AtomicUsize::new(0),
            max_threads: threads as i64 * SYNC_POOL_THREAD_CEILING_MULTIPLE,
            abandoned: Arc::new(AtomicU64::new(0)),
            live_threads: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        };
        for _ in 0..threads {
            pool.spawn_worker();
        }
        pool
    }

    fn spawn_worker(&self) {
        // Fail loud instead of leaking (H2 follow up): see
        // SYNC_POOL_THREAD_CEILING_MULTIPLE for why this number. sync_live
        // (stats_loop) will sit at the ceiling until an earlier wedged
        // thread returns on its own; nothing here can safely force that (see
        // the module doc above).
        if self.live_threads.load(Ordering::Relaxed) >= self.max_threads {
            tracing::warn!(
                max_threads = self.max_threads,
                "sync pool thread ceiling reached, not spawning a replacement; \
                 check for tasks with a tiny or zero timeout_ms and for a \
                 genuinely wedged blocking call in a sync task"
            );
            return;
        }
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
        let rx = self.rx.clone();
        let rt = self.rt.clone();
        let live = self.live_threads.clone();
        live.fetch_add(1, Ordering::Relaxed);
        std::thread::Builder::new()
            .name(format!("cauli-sync-{idx}"))
            .spawn(move || {
                // Panic safe (MEM-3 precedent, ctx::DecrGuard): this must be
                // the first thing in the closure so it covers the whole
                // thread body, including a panic inside run_sync_blocking.
                // The plain fetch_sub this replaced ran only after the recv
                // loop returned normally, so a panic here used to both lose
                // the thread AND leave sync_live over reporting forever --
                // exactly the failure mode that stat exists to catch.
                let _live_guard = crate::ctx::DecrGuard(&live);
                // Pin ONE persistent CPython thread state to this OS thread
                // for its whole lifetime. Without this, each `Python::attach`
                // below (a PyGILState_Ensure/Release pair on a non-Python
                // thread) creates and then DESTROYS a fresh thread state per
                // task, which wipes `threading.local` storage between tasks
                // — and everything the Python ecosystem builds on it: Django
                // caches its DB connection per thread (`CONN_MAX_AGE` is
                // meaningless if the cache dies with every task, and each
                // task then leaks a fresh connection until GC reaps the
                // orphaned one), as do requests.Session patterns, SQLAlchemy
                // scoped_session, etc. The initial Ensure registers the
                // state in CPython's gilstate TSS (never Released, so never
                // destroyed while the thread lives); SaveThread releases the
                // GIL before entering the blocking recv loop. Subsequent
                // attaches find and reuse the registered state.
                //
                // SAFETY: the interpreter is initialized (PyRuntime::init)
                // before any pool starts, and this pairs Ensure with an
                // immediate SaveThread so the GIL is not held around recv().
                unsafe {
                    pyo3::ffi::PyGILState_Ensure();
                    pyo3::ffi::PyEval_SaveThread();
                }
                while let Ok(job) = rx.recv() {
                    if job.resp.is_closed() {
                        // The dispatcher already abandoned this job (hard
                        // timeout while still queued): running it now would
                        // be zombie execution with no one listening.
                        continue;
                    }
                    #[cfg(any(test, feature = "test-hooks"))]
                    if job.name == TEST_HOOK_PANIC_JOB {
                        panic!("test-hooks: deliberate sync pool worker panic");
                    }
                    let out = rt.run_sync_blocking(
                        &job.name,
                        &job.args,
                        &job.kwargs,
                        job.soft_timeout_ms,
                    );
                    let _ = job.resp.send(out);
                }
            })
            .expect("failed to spawn sync pool thread");
    }

    /// Enqueue a job. `Err` means the bounded queue is full (every thread is
    /// wedged and the backlog is already at capacity); the caller should fail
    /// the task rather than block the async runtime.
    pub fn submit(&self, job: SyncJob) -> Result<(), SyncJob> {
        self.tx.try_send(job).map_err(|e| match e {
            crossbeam_channel::TrySendError::Full(j) => j,
            crossbeam_channel::TrySendError::Disconnected(j) => j,
        })
    }

    /// Called by the dispatcher when a sync task hard-times-out (exec.rs).
    /// The underlying OS thread may be permanently wedged; spawn a
    /// replacement so pool capacity is restored immediately.
    pub fn report_hard_timeout(&self) {
        self.abandoned.fetch_add(1, Ordering::Relaxed);
        self.spawn_worker();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Registers a synthetic module in `sys.modules` under `module_name` so
    /// shim.py's `importlib.import_module` finds it without touching the
    /// filesystem, and returns the "module:app" spec `PyRuntime::init`
    /// expects. `module_name` must be unique per test: `sys.modules` is
    /// process global and tests run concurrently.
    fn install_fake_app_module(py: Python<'_>, module_name: &str, src: &str) -> String {
        let code = CString::new(src).expect("test app source has no NUL bytes");
        let filename = CString::new(format!("{module_name}.py")).unwrap();
        let modname = CString::new(module_name).unwrap();
        let m = PyModule::from_code(py, code.as_c_str(), filename.as_c_str(), modname.as_c_str())
            .expect("failed to build synthetic test app module");
        py.import("sys")
            .expect("sys is always importable")
            .getattr("modules")
            .expect("sys.modules always exists")
            .set_item(module_name, m)
            .expect("sys.modules supports __setitem__");
        format!("{module_name}:app")
    }

    /// Startup regression: a config parse failure must never echo the raw
    /// shim JSON, which carries `redis_url` (password and all) verbatim.
    /// `result_ttl = -1` is one of several unvalidated fields that reach
    /// Rust's `u64` and fail to parse (see the docstring on AppConfig).
    #[test]
    fn unparseable_config_error_does_not_leak_redis_password() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(
                py,
                "cauli_test_secret_leak_app",
                r#"
class _App:
    redis_url = "redis://appuser:s3cr3tpw@example.com:6379/0"
    default_queue = "default"
    result_ttl = -1
    idemp_ttl = 86400
    queue_ttl = {}
    _tasks = {}

app = _App()
"#,
            )
        });
        let err = match PyRuntime::init(&app_spec, 1) {
            Ok(_) => panic!("expected init to fail: result_ttl=-1 cannot parse as u64"),
            Err(e) => e,
        };
        // main.rs prints startup errors with {e:#}, which walks the whole
        // anyhow context chain (see the with_context call above) -- this is
        // the exact text an operator would see in the log.
        let logged = format!("{err:#}");
        assert!(
            !logged.contains("s3cr3tpw"),
            "redis password leaked into startup error: {logged}"
        );
        assert!(
            !logged.contains("appuser"),
            "redis credentials leaked into startup error: {logged}"
        );
    }

    /// `pyerr_string` feeds `ErrorJson` (run_sync_blocking, fail_pending),
    /// which is written into Redis result keys and DLQ entries verbatim: an
    /// exception with a huge message or traceback must not carry all of it
    /// along, the same way shim.py's `_MAX_TB` and _exec.py's
    /// `_TRACEBACK_CAP` already bound the Python side equivalents.
    #[test]
    fn pyerr_string_is_capped_to_a_bounded_size() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let len = Python::attach(|py| {
            let huge = "x".repeat(50_000);
            let err = pyo3::exceptions::PyValueError::new_err(huge);
            pyerr_string(py, &err).len()
        });
        assert!(
            len <= MAX_PYERR_CHARS,
            "pyerr_string produced {len} bytes, expected at most {MAX_PYERR_CHARS}"
        );
    }

    /// A minimal app that parses cleanly: `_tasks` is empty because these
    /// tests drive `SyncPool` directly and never dispatch a real task.
    const MINIMAL_VALID_APP_SRC: &str = r#"
class _App:
    redis_url = "redis://localhost:6379/0"
    default_queue = "default"
    result_ttl = 3600
    idemp_ttl = 86400
    queue_ttl = {}
    _tasks = {}

app = _App()
"#;

    /// H2 follow up: `report_hard_timeout` must stop spawning once the pool
    /// hits its ceiling, instead of growing without bound.
    #[test]
    fn report_hard_timeout_stops_spawning_at_the_thread_ceiling() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_ceiling_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1).expect("valid app must init cleanly");
        let pool = SyncPool::start(rt, 1, 32); // threads=1 -> max_threads = 4
        assert_eq!(pool.live_threads.load(Ordering::Relaxed), 1);

        // Hammer well past the ceiling: an unbounded spawner would keep
        // growing live_threads by one per call.
        for _ in 0..20 {
            pool.report_hard_timeout();
        }

        assert_eq!(
            pool.abandoned.load(Ordering::Relaxed),
            20,
            "every hard timeout must still be counted, ceiling or not"
        );
        assert_eq!(
            pool.live_threads.load(Ordering::Relaxed),
            SYNC_POOL_THREAD_CEILING_MULTIPLE, // threads=1, so ceiling == multiple
            "sync pool grew past its thread ceiling"
        );
    }

    /// MEM-3 style regression for the sync pool specifically: a panic inside
    /// a pool thread (deliberately triggered via TEST_HOOK_PANIC_JOB) must
    /// still decrement live_threads, or the stat added to make thread loss
    /// observable silently stops telling the truth the moment it matters.
    #[test]
    fn sync_pool_thread_panic_does_not_overcount_live_threads() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_panic_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1).expect("valid app must init cleanly");
        let pool = SyncPool::start(rt, 1, 4);
        assert_eq!(pool.live_threads.load(Ordering::Relaxed), 1);

        let (resp_tx, resp_rx) = oneshot::channel();
        pool.submit(SyncJob {
            name: TEST_HOOK_PANIC_JOB.to_string(),
            args: Value::Null,
            kwargs: Value::Null,
            soft_timeout_ms: None,
            resp: resp_tx,
        })
        .ok()
        .expect("bounded queue has room for one job");

        // Poll instead of a fixed sleep: the decrement lands within
        // microseconds of the panic in practice, this just avoids a flaky
        // race on a slow host. resp_rx is kept alive so job.resp.is_closed()
        // is false when the worker thread dequeues the job.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while pool.live_threads.load(Ordering::Relaxed) != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "sync pool thread did not exit (and decrement live_threads) \
                 after a deliberate panic"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        drop(resp_rx);

        // The pool must still be usable afterward: report_hard_timeout can
        // spawn a fresh replacement, and the count reflects exactly that,
        // not an overcount left over from the panic.
        pool.report_hard_timeout();
        assert_eq!(pool.live_threads.load(Ordering::Relaxed), 1);
    }

    /// A payload nested deeper than `pyjson::MAX_DEPTH` can never succeed:
    /// retrying it parses the identical too deep tree again. Before the fix,
    /// `run_sync_blocking` bucketed this into "WorkerShimError" with
    /// `retryable: true`, burning the whole backoff schedule on a payload
    /// `--max-envelope-bytes` is far too coarse to catch.
    #[test]
    fn run_sync_blocking_depth_failure_is_not_retryable() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_depth_sync_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1).expect("valid app must init cleanly");

        // Deeper than MAX_DEPTH: the failure happens while converting `args`,
        // before the (unregistered) task name is ever looked up.
        let mut deep = serde_json::json!(0);
        for _ in 0..(crate::pyjson::MAX_DEPTH + 2) {
            deep = serde_json::json!([deep]);
        }
        match rt.run_sync_blocking("does.not.matter", &deep, &serde_json::json!({}), None) {
            Outcome::Failure { err, retryable } => {
                assert!(
                    !retryable,
                    "a nesting depth failure can never succeed on retry"
                );
                assert_eq!(err.type_, "SerializationError");
            }
            Outcome::Success(_) => panic!("expected a depth failure, got Success"),
            Outcome::ForceRetry { .. } => panic!("expected a depth failure, got ForceRetry"),
        }
    }

    /// Same as `run_sync_blocking_depth_failure_is_not_retryable` above, for
    /// the async submit lane (`queue_submit` -> `submit_batch_under_gil`).
    /// Before the fix this was also "WorkerShimError"/retryable true.
    #[tokio::test]
    async fn queue_submit_depth_failure_is_not_retryable() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_depth_async_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1).expect("valid app must init cleanly");

        let mut deep = serde_json::json!(0);
        for _ in 0..(crate::pyjson::MAX_DEPTH + 2) {
            deep = serde_json::json!([deep]);
        }
        let (_token, rx) = rt.queue_submit("does.not.matter", &deep, &serde_json::json!({}), 5.0);
        match rx.await.expect("submitter thread must respond") {
            Outcome::Failure { err, retryable } => {
                assert!(
                    !retryable,
                    "a nesting depth failure can never succeed on retry"
                );
                assert_eq!(err.type_, "SerializationError");
            }
            Outcome::Success(_) => panic!("expected a depth failure, got Success"),
            Outcome::ForceRetry { .. } => panic!("expected a depth failure, got ForceRetry"),
        }
    }

    /// shim.py's own `_registry` miss (sync lane, `_run_sync_inner`) must use
    /// the canonical "UnregisteredTask" string, the one PROTOCOL section 8
    /// documents and dispatch.rs's own registry gate uses, not the stale
    /// "Unregistered". In a real worker process dispatch.rs's registry
    /// check (built from this same `load_app()` snapshot) never lets an
    /// unregistered name reach here, but calling `run_sync_blocking`
    /// directly, as this test does, bypasses that gate the same way a
    /// future caller might.
    #[test]
    fn shim_sync_unregistered_task_error_type_is_canonical() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(
                py,
                "cauli_test_unregistered_sync_app",
                MINIMAL_VALID_APP_SRC,
            )
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1).expect("valid app must init cleanly");

        match rt.run_sync_blocking(
            "no.such.task",
            &serde_json::json!([]),
            &serde_json::json!({}),
            None,
        ) {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "UnregisteredTask");
                assert!(!retryable);
            }
            Outcome::Success(_) => panic!("expected a failure, got Success"),
            Outcome::ForceRetry { .. } => panic!("expected a failure, got ForceRetry"),
        }
    }

    /// Same as `shim_sync_unregistered_task_error_type_is_canonical` above,
    /// for the async lane (`_arun`).
    #[tokio::test]
    async fn shim_async_unregistered_task_error_type_is_canonical() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(
                py,
                "cauli_test_unregistered_async_app",
                MINIMAL_VALID_APP_SRC,
            )
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1).expect("valid app must init cleanly");

        let (_token, rx) = rt.queue_submit(
            "no.such.task",
            &serde_json::json!([]),
            &serde_json::json!({}),
            5.0,
        );
        match rx.await.expect("submitter thread must respond") {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "UnregisteredTask");
                assert!(!retryable);
            }
            Outcome::Success(_) => panic!("expected a failure, got Success"),
            Outcome::ForceRetry { .. } => panic!("expected a failure, got ForceRetry"),
        }
    }

    /// The shim's module body runs `from cauli import SoftTimeLimitExceeded`
    /// (top of shim.py) before `load_app` does any sys.path/VIRTUAL_ENV setup:
    /// `PyRuntime::init` above builds the shim module and calls `load_app` back
    /// to back with no gap between them. Installing the worker's wheel into the
    /// app's own venv (README "Install") links that venv's interpreter, so
    /// cauli is already on sys.path at that first import and this never bites.
    /// Building the worker from source (same doc, the very next section) links
    /// whatever interpreter the build found, which has no such site packages on
    /// its default sys.path, so that first import fails and the module falls
    /// back to its own local stand in class. Unless `load_app` rebinds once
    /// cauli actually becomes reachable, a user's own
    /// `except SoftTimeLimitExceeded:` then compares against a different class
    /// object and silently never matches.
    ///
    /// Reproduced directly rather than through a real venv. This process's
    /// embedded interpreter already stands in for the source built shape: no
    /// VIRTUAL_ENV, no cauli on its default sys.path, and every other test in
    /// this module runs with the fallback class. So the only thing left to
    /// control is exactly when cauli becomes importable relative to the shim's
    /// two calls. That is done through `sys.modules` directly rather than a
    /// real install, because mutating `VIRTUAL_ENV` or the process cwd is
    /// global state shared with every other test in this binary running at the
    /// same time. Both phases (cauli absent throughout, cauli appearing
    /// between the module body and `load_app`) run sequentially in this one
    /// test, each against its own shim instance, so neither can race the
    /// other's `sys.modules` mutation the way two separate `#[test]` functions
    /// executing in parallel could.
    #[test]
    fn load_app_rebinds_soft_time_limit_exceeded_once_cauli_becomes_importable() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        Python::attach(|py| {
            let sys_modules = py
                .import("sys")
                .expect("sys is always importable")
                .getattr("modules")
                .expect("sys.modules always exists");
            assert!(
                !sys_modules
                    .contains("cauli")
                    .expect("sys.modules supports __contains__"),
                "test setup invalid: cauli must not already be importable in this process"
            );
            let code = CString::new(include_str!("shim.py")).expect("shim.py contains NUL byte");

            // Phase 1: PROTOCOL section 4.2's documented supported case (and
            // how the entire fixture based e2e suite in this repo runs) is
            // cauli never installed at all. load_app must still succeed and
            // must not disturb the local fallback class: a working fallback
            // must not become an import error.
            let shim_no_cauli = PyModule::from_code(
                py,
                code.as_c_str(),
                c"shim.py",
                c"cauli_worker_shim_no_cauli_repro",
            )
            .expect("failed to load embedded shim");
            let fallback_before = shim_no_cauli
                .getattr("SoftTimeLimitExceeded")
                .expect("shim always exposes SoftTimeLimitExceeded");
            let app_spec_a =
                install_fake_app_module(py, "cauli_test_no_cauli_app", MINIMAL_VALID_APP_SRC);
            shim_no_cauli
                .getattr("load_app")
                .and_then(|f| f.call1((app_spec_a.as_str(), "[]")))
                .expect("load_app must succeed with no cauli installed at all");
            let fallback_after = shim_no_cauli
                .getattr("SoftTimeLimitExceeded")
                .expect("shim always exposes SoftTimeLimitExceeded");
            assert!(
                fallback_after.is(&fallback_before),
                "load_app must not disturb the local fallback class when cauli is absent"
            );

            // Phase 2, the actual bug: load a second, independent shim while
            // cauli is still not importable (its module body fails exactly
            // like phase 1's), then make cauli importable the way the venv
            // wheel or a source checkout would once load_app's own path
            // setup ran, and only then call load_app.
            let shim = PyModule::from_code(
                py,
                code.as_c_str(),
                c"shim.py",
                c"cauli_worker_shim_rebind_repro",
            )
            .expect("failed to load embedded shim");

            // A minimal stand in "cauli" package, built now but not yet
            // registered in sys.modules: this is what a real `import cauli`
            // resolves to once the venv's site packages (or a source
            // checkout) join sys.path.
            let fake_cauli_code =
                CString::new("class SoftTimeLimitExceeded(Exception):\n    pass\n")
                    .expect("fake cauli source has no NUL bytes");
            let fake_cauli = PyModule::from_code(
                py,
                fake_cauli_code.as_c_str(),
                c"cauli/__init__.py",
                c"cauli",
            )
            .expect("failed to build fake cauli module");
            let real_cls = fake_cauli
                .getattr("SoftTimeLimitExceeded")
                .expect("fake cauli exposes SoftTimeLimitExceeded");

            // Sanity check on the repro itself: before cauli is reachable at
            // all (sys.modules still has no "cauli" entry), this second
            // shim's module body must have landed on ITS OWN fallback class,
            // not on the fake cauli's, or this test would pass for a reason
            // that has nothing to do with the rebind. Each shim
            // instance defines its own distinct fallback class object (a
            // fresh `class SoftTimeLimitExceeded(Exception)` statement runs
            // every time the module body does), so this compares against
            // `real_cls`, not phase 1's `fallback_before`.
            let before = shim
                .getattr("SoftTimeLimitExceeded")
                .expect("shim always exposes SoftTimeLimitExceeded");
            assert!(
                !before.is(&real_cls),
                "test setup invalid: shim already matches cauli before cauli was even importable"
            );

            sys_modules
                .set_item("cauli", &fake_cauli)
                .expect("sys.modules supports __setitem__");

            let app_spec =
                install_fake_app_module(py, "cauli_test_rebind_app", MINIMAL_VALID_APP_SRC);
            shim.getattr("load_app")
                .and_then(|f| f.call1((app_spec.as_str(), "[]")))
                .expect("load_app must still succeed once cauli is importable");

            let after = shim
                .getattr("SoftTimeLimitExceeded")
                .expect("shim always exposes SoftTimeLimitExceeded");
            assert!(
                after.is(&real_cls),
                "load_app did not rebind SoftTimeLimitExceeded to the real cauli class \
                 once it became importable; a user's `except SoftTimeLimitExceeded:` \
                 would silently never match"
            );

            // Best effort: do not leave the fake package poisoning sys.modules
            // for every test that runs after this one in the same process.
            let _ = sys_modules.del_item("cauli");
        });
    }
}
