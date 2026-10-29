//! Embedded CPython integration.
//!
//! - The GIL is only taken on dedicated OS threads (sync pool, async
//!   submitter). Tokio worker threads never hold it.
//! - Python-side logic lives in the embedded shim (src/shim.py).
//! - Args and results cross as JSON text, decoded and encoded by msgspec, the
//!   codec the client and the cpu child use.

use crate::ctx::Outcome;
use crate::envelope::ErrorJson;
use anyhow::{anyhow, Context, Result};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCFunction, PyDict, PyList, PyModule, PyTuple};
use serde::Deserialize;
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Registry entry parsed from shim `load_app` JSON (PROTOCOL §6). The shim
/// sends more attributes; retry math always uses the envelope's own values, so
/// serde ignores them.
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
    #[serde(default)]
    pub queue_ttl: HashMap<String, f64>,
    pub tasks: HashMap<String, TaskSpec>,
}

pub struct PyRuntime {
    shim: Py<PyModule>,
    /// The shim's msgspec decoder and encoder, resolved once at init.
    decode: Py<PyAny>,
    encode: Py<PyAny>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Outcome>>>>,
    next_token: AtomicU64,
    /// Drained by one submitter thread (see `init`), one GIL entry per batch.
    /// A GIL entry per task from `spawn_blocking` convoys hundreds of blocking
    /// threads on the GIL against the event loop thread running the tasks.
    submit_tx: crossbeam_channel::Sender<SubmitJob>,
    /// Submissions the shim rejected because a loop's pending list was at cap
    /// (stats: `async_rejected`). Counted here, where the GIL is already held.
    async_rejected: AtomicU64,
}

/// What `cauli.current_task()` reads. Crosses into the shim as one
/// `(id, retries, max_retries, queue)` tuple. `id` is the envelope id the
/// caller holds as `AsyncResult.id`, not the cpu lane's suffixed wire id.
#[derive(Clone)]
pub struct TaskMeta {
    pub id: String,
    pub retries: u32,
    pub max_retries: u32,
    pub queue: String,
}

impl TaskMeta {
    fn to_py<'py>(&self, py: Python<'py>) -> Bound<'py, PyTuple> {
        (
            self.id.as_str(),
            self.retries,
            self.max_retries,
            self.queue.as_str(),
        )
            .into_pyobject(py)
            .expect("a tuple of str/u32 cannot fail to convert")
    }
}

/// One queued async submission; args/kwargs text is copied at queue time.
struct SubmitJob {
    token: u64,
    name: String,
    args: String,
    kwargs: String,
    /// Hard deadline (envelope `timeout_ms`), seconds.
    timeout_s: f64,
    /// Soft deadline, seconds, set only when it lands before the hard one.
    /// Kept separate so the shim raises `SoftTimeLimitExceeded` at the soft
    /// mark and keeps the hard deadline as the backstop.
    soft_timeout_s: Option<f64>,
    /// Backs `cauli.current_task()`. `None` only in unit tests.
    meta: Option<TaskMeta>,
}

/// Args JSON text into Python objects. msgspec gets a read only memoryview of
/// the Rust string: a PyBytes copy of a large payload costs more than the
/// decode. The view is released before returning.
fn decode_text<'py>(decode: &Bound<'py, PyAny>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    let py = decode.py();
    // Empty args or kwargs is the common case and this runs under the GIL.
    match text {
        "[]" => return Ok(PyList::empty(py).into_any()),
        "{}" => return Ok(PyDict::new(py).into_any()),
        _ => {}
    }
    // SAFETY: the view points at `text`, which outlives this call, is read
    // only (PyBUF_READ), and is released below before `text` can go away.
    let view = unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            pyo3::ffi::PyMemoryView_FromMemory(
                text.as_ptr() as *mut std::ffi::c_char,
                text.len() as pyo3::ffi::Py_ssize_t,
                pyo3::ffi::PyBUF_READ,
            ),
        )?
    };
    let decoded = decode.call1((&view,));
    view.call_method0("release")?;
    decoded
}

/// A task result as JSON text: the type gate, then msgspec. Either failing is
/// a terminal SerializationError, as in the cpu child.
fn encode_result(result: &Bound<'_, PyAny>, encode: &Bound<'_, PyAny>) -> Outcome {
    // None, written exactly as msgspec writes it, without calling the encoder.
    if result.is_none() {
        return Outcome::Success("null".to_owned());
    }
    let encoded = crate::pyjson::check_json(result, 0)
        .map_err(|e| e.0)
        .and_then(|()| {
            let bytes = encode.call1((result,)).map_err(|e| e.to_string())?;
            let bytes = bytes
                .downcast_into::<PyBytes>()
                .map_err(|e| e.to_string())?;
            String::from_utf8(bytes.as_bytes().to_vec()).map_err(|e| e.to_string())
        });
    match encoded {
        Ok(json) => Outcome::Success(json),
        Err(msg) => Outcome::Failure {
            err: ErrorJson::new(
                "SerializationError",
                format!("task result is not JSON serializable: {msg}"),
            ),
            retryable: false,
        },
    }
}

/// The shim's outcome dict into an `Outcome`. Mirrors `ctx::parse_pyresp`
/// field for field; keep the two in step.
fn outcome_from_py(obj: &Bound<'_, PyAny>, encode: &Bound<'_, PyAny>) -> Outcome {
    let get = |k: &str| obj.get_item(k).ok();
    let flag = |k: &str| {
        get(k)
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false)
    };

    if flag("ok") {
        return match get("result") {
            Some(r) => encode_result(&r, encode),
            None => Outcome::Success("null".to_owned()),
        };
    }

    let err = get("error")
        .and_then(|e| {
            let f = |k: &str| {
                // Lossy, not extract::<String>: one lone surrogate must not
                // blank the whole message or traceback.
                e.get_item(k)
                    .ok()
                    .and_then(|v| v.downcast_into::<pyo3::types::PyString>().ok())
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            };
            let type_ = f("type");
            (!type_.is_empty()).then(|| ErrorJson {
                type_,
                message: f("message"),
                traceback: f("traceback"),
                // Only the shim knows whether an exception left user code.
                // Absent stays empty so §8 omits it.
                origin: f("origin"),
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

/// msgspec refuses args only for a property of the payload (a float past f64
/// range, a lone surrogate), so a retry fails identically. Terminal, not the
/// retryable `WorkerShimError` that would burn the whole backoff schedule.
fn args_failure_outcome(py: Python<'_>, e: &PyErr) -> Outcome {
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

/// Bounds error text written verbatim into result keys and DLQ entries.
/// Matches shim.py's `_MAX_TB` and _exec.py's `_TRACEBACK_CAP`.
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

/// One `PyRuntime` per process, enforced. The shim's dispatch state lives in
/// module globals under one fixed `sys.modules` name, so a second `init`
/// reruns the shim in the same dict and takes over the completion callback:
/// the first runtime's in flight tasks would hang silently to their timeout.
static RUNTIME_BUILT: AtomicBool = AtomicBool::new(false);

/// Default for `--python`: an interpreter loading the same libpython this
/// worker embeds (`shim.cpu_child_python()`), or None for a static or frozen
/// build. `python3` off `PATH` or `sys.executable` can name a different
/// CPython that cannot import the app's C extensions.
pub fn interpreter_executable() -> Option<String> {
    Python::attach(|py| {
        let exe: String = py
            .import("cauli_worker_shim")
            .ok()?
            .getattr("cpu_child_python")
            .ok()?
            .call0()
            .ok()?
            .extract()
            .ok()?;
        (!exe.is_empty()).then_some(exe)
    })
}

impl PyRuntime {
    /// Initialize the interpreter, import the shim, load the app, register the
    /// completion callback and start the asyncio loop threads.
    pub fn init(
        app_spec: &str,
        io_loops: usize,
        io_concurrency: usize,
    ) -> Result<(Arc<PyRuntime>, AppConfig)> {
        // Never given back, even on failure: by then the shim body has run and
        // its loop threads exist.
        if RUNTIME_BUILT.swap(true, Ordering::SeqCst) {
            return Err(anyhow!(
                "PyRuntime::init was called twice in this process. The embedded shim \
                 is one Python module shared process wide, so a second runtime empties \
                 the first one's asyncio loop list and submission queues and takes over \
                 its completion callback: every task the first runtime still has in \
                 flight is then dropped silently, with no error raised anywhere. Build \
                 exactly one PyRuntime per process."
            ));
        }

        // Mandated entry point; pyo3 0.26 aliases it to Python::initialize.
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Outcome>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_cb = pending.clone();
        let app_spec = app_spec.to_string();

        type Loaded = (Py<PyModule>, Py<PyAny>, Py<PyAny>, String);
        let (shim, decode, encode, cfg_json) = Python::attach(|py| -> Result<Loaded> {
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
            // Bound by load_app, so only read after it returned.
            let decode = m.getattr("_decode")?.unbind();
            let encode = m.getattr("_encode")?.unbind();
            let encode_cb = encode.clone_ref(py);

            let cb = PyCFunction::new_closure(
                py,
                Some(c"cauli_complete"),
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<()> {
                    // Runs on the loop thread with the GIL held. Encode before
                    // taking the mutex: blocking on it under the GIL would stall
                    // the whole event loop.
                    let token: u64 = args.get_item(0)?.extract()?;
                    let outcome = outcome_from_py(&args.get_item(1)?, encode_cb.bind(args.py()));
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
            let loop_impl: String = m
                .getattr("start_loops")
                .and_then(|f| f.call1((io_loops, io_concurrency)))
                .and_then(|r| r.extract())
                .map_err(|e| anyhow!("start_loops failed: {}", pyerr_string(py, &e)))?;
            tracing::info!(loops = io_loops, implementation = %loop_impl, "asyncio loops started");

            Ok((m.unbind(), decode, encode, cfg))
        })?;

        // Never interpolate cfg_json: it carries redis_url with its password,
        // and startup failures are logged with the full context chain.
        let cfg: AppConfig = serde_json::from_str(&cfg_json).with_context(|| {
            format!("shim load_app returned unparseable config for app {app_spec:?}")
        })?;

        let (submit_tx, submit_rx) = crossbeam_channel::unbounded::<SubmitJob>();
        let rt = Arc::new(PyRuntime {
            shim,
            decode,
            encode,
            pending,
            next_token: AtomicU64::new(1),
            submit_tx,
            async_rejected: AtomicU64::new(0),
        });

        // The async submitter: drains the channel and schedules each batch
        // under one GIL entry. Unbounded is safe: the io admission semaphore
        // caps queued jobs at --io-concurrency. It pins one thread state for
        // cost only: an unpinned attach builds and destroys one per batch.
        let rt_submit = Arc::clone(&rt);
        std::thread::Builder::new()
            .name("cauli-async-submit".into())
            .spawn(move || {
                // SAFETY: the interpreter is initialized before this thread is
                // spawned, and Ensure is paired with an immediate SaveThread so
                // the GIL is not held around the blocking recv. The state is
                // never Released, so every later attach reuses it.
                unsafe {
                    pyo3::ffi::PyGILState_Ensure();
                    pyo3::ffi::PyEval_SaveThread();
                }
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

    /// Blocking sync task execution. Call only from a sync pool thread, never
    /// from a tokio worker thread. `args`/`kwargs` are the caller's JSON text.
    pub fn run_sync_blocking(
        &self,
        name: &str,
        args: &str,
        kwargs: &str,
        soft_timeout_ms: Option<u64>,
        meta: Option<&TaskMeta>,
    ) -> Outcome {
        Python::attach(|py| {
            let call = (|| -> PyResult<Outcome> {
                let decode = self.decode.bind(py);
                let a = match decode_text(decode, args) {
                    Ok(v) => v,
                    Err(e) => return Ok(args_failure_outcome(py, &e)),
                };
                let k = match decode_text(decode, kwargs) {
                    Ok(v) => v,
                    Err(e) => return Ok(args_failure_outcome(py, &e)),
                };
                let out = self.shim.bind(py).getattr("run_sync")?.call1((
                    name,
                    a,
                    k,
                    soft_timeout_ms,
                    meta.map(|m| m.to_py(py)),
                ))?;
                Ok(outcome_from_py(&out, self.encode.bind(py)))
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

    /// Queue an async task for the submitter thread; completion arrives on the
    /// returned oneshot. Takes no GIL and never blocks. The token lets the
    /// caller `cancel(token)` if it gives up waiting.
    pub fn queue_submit(
        &self,
        name: &str,
        args: &str,
        kwargs: &str,
        timeout_s: f64,
        soft_timeout_s: Option<f64>,
        meta: Option<TaskMeta>,
    ) -> (u64, oneshot::Receiver<Outcome>) {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        // Insert before send: the completion callback can fire as soon as the
        // job is on the channel, and must find the slot.
        self.pending.lock().unwrap().insert(token, tx);

        let job = SubmitJob {
            token,
            name: name.to_string(),
            args: args.to_owned(),
            kwargs: kwargs.to_owned(),
            timeout_s,
            soft_timeout_s,
            meta,
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

    /// Schedule one drained batch on the event loops under a single GIL entry.
    /// Runs only on the cauli-async-submit thread.
    fn submit_batch_under_gil(&self, batch: &mut Vec<SubmitJob>) {
        // Args failures are terminal, unlike `fail_pending`'s retryable bucket.
        let (failures, args_failures) = Python::attach(|py| {
            let mut failed: Vec<(u64, String)> = Vec::new();
            let mut args_failed: Vec<(u64, Outcome)> = Vec::new();
            let submit = match self.shim.bind(py).getattr("submit_async") {
                Ok(f) => f,
                Err(e) => {
                    // Shim itself broken: fail the whole batch, same
                    // retryable semantics as a per-task submit error.
                    let msg = pyerr_string(py, &e);
                    for job in batch.iter() {
                        failed.push((job.token, msg.clone()));
                    }
                    return (failed, args_failed);
                }
            };
            let queue_full_ty = self.shim.bind(py).getattr("AsyncQueueFull").ok();
            let decode = self.decode.bind(py);
            for job in batch.iter() {
                let a = match decode_text(decode, &job.args) {
                    Ok(v) => v,
                    Err(e) => {
                        args_failed.push((job.token, args_failure_outcome(py, &e)));
                        continue;
                    }
                };
                let k = match decode_text(decode, &job.kwargs) {
                    Ok(v) => v,
                    Err(e) => {
                        args_failed.push((job.token, args_failure_outcome(py, &e)));
                        continue;
                    }
                };
                if let Err(e) = submit.call1((
                    job.token,
                    job.name.as_str(),
                    a,
                    k,
                    job.timeout_s,
                    job.soft_timeout_s,
                    job.meta.as_ref().map(|m| m.to_py(py)),
                )) {
                    if queue_full_ty
                        .as_ref()
                        .is_some_and(|ty| e.is_instance(py, ty))
                    {
                        self.async_rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    failed.push((job.token, pyerr_string(py, &e)));
                }
            }
            (failed, args_failed)
        });
        for (token, msg) in failures {
            self.fail_pending(token, format!("submit_async failed: {msg}"));
        }
        for (token, outcome) in args_failures {
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

    /// Drop a pending completion slot when the backstop timeout gives up, so a
    /// wedged event loop that never calls back cannot leak it. A no-op if the
    /// callback already consumed it.
    pub fn cancel(&self, token: u64) {
        self.pending.lock().unwrap().remove(&token);
    }

    /// Submissions rejected because a loop's pending list was at cap (stats:
    /// `async_rejected`). This counter moves during an event loop wedge.
    pub fn async_rejected(&self) -> u64 {
        self.async_rejected.load(Ordering::Relaxed)
    }
}

// Sync io pool. CPython releases the GIL during blocking I/O, so these threads
// give real io parallelism. A hard timeout cannot kill a wedged OS thread, so:
// the queue is bounded at --io-concurrency; a job whose `resp` receiver is
// closed (dispatcher gave up) is skipped, never run as a zombie; each reported
// hard timeout spawns a replacement up to a fixed ceiling. A wedged thread that
// returns keeps serving as extra headroom.

/// Replacement ceiling as a multiple of --io-threads. Without it a stream of
/// hard timeouts (timeout_ms 0 is enough) leaks an OS thread and a thread
/// state per hit. 4x leaves room for several real wedges at once.
const SYNC_POOL_THREAD_CEILING_MULTIPLE: i64 = 4;

/// Test hook, absent from release builds: a job with this name makes the pool
/// thread panic right after dequeuing it, to test that DecrGuard still
/// decrements live_threads.
#[cfg(any(test, feature = "test-hooks"))]
const TEST_HOOK_PANIC_JOB: &str = "__cauli_test_hook_panic__";

pub struct SyncJob {
    pub name: String,
    /// The caller's args/kwargs JSON text, decoded on the pool thread.
    pub args: String,
    pub kwargs: String,
    pub soft_timeout_ms: Option<u64>,
    /// Backs `cauli.current_task()`. `None` only in unit tests. Boxed to keep
    /// the job small for `clippy::result_large_err`.
    pub meta: Option<Box<TaskMeta>>,
    /// Fired when a pool thread commits to running this job. The dispatcher
    /// starts `timeout_ms` here, not at submit: up to `io_concurrency -
    /// io_threads` jobs can wait in the queue with no thread behind them.
    pub started: oneshot::Sender<()>,
    pub resp: oneshot::Sender<Outcome>,
}

/// The pool's bounded job queue. A job leaves it only through a thread holding
/// the GIL, so a burst runs on one thread with no GIL handoff per job. A job
/// that blocks releases the GIL, and the standby thread takes the next one.
struct JobQueue {
    state: Mutex<QueueState>,
    ready: std::sync::Condvar,
    capacity: usize,
}

struct QueueState {
    jobs: std::collections::VecDeque<SyncJob>,
    /// Threads parked on `ready`.
    parked: usize,
    /// A woken thread has not yet reached the GIL. Only one standby at a time.
    standby: bool,
}

impl JobQueue {
    /// Park until there is a job, without taking it. A woken thread that
    /// finds the queue already emptied stands down, so the next job wakes
    /// a thread again.
    fn wait_for_job(&self) {
        let mut q = self.state.lock().unwrap();
        while q.jobs.is_empty() {
            q.parked += 1;
            q = self.ready.wait(q).unwrap();
            q.parked -= 1;
            if q.jobs.is_empty() {
                q.standby = false;
            }
        }
    }

    /// Take the next job, GIL held; `arrived` is true on the first take after
    /// acquiring it. Leaves a standby whenever work remains, so a blocking job
    /// does not hold up the one queued behind it.
    fn take(&self, arrived: bool) -> Option<SyncJob> {
        let mut q = self.state.lock().unwrap();
        if arrived {
            q.standby = false;
        }
        let job = q.jobs.pop_front();
        if !q.jobs.is_empty() {
            self.wake_standby(&mut q);
        }
        job
    }

    fn wake_standby(&self, q: &mut QueueState) {
        if !q.standby && q.parked > 0 {
            q.standby = true;
            self.ready.notify_one();
        }
    }
}

pub struct SyncPool {
    queue: Arc<JobQueue>,
    rt: Arc<PyRuntime>,
    next_idx: AtomicUsize,
    /// Fixed at construction: `threads * SYNC_POOL_THREAD_CEILING_MULTIPLE`.
    max_threads: i64,
    /// Hard-timeout abandonments reported so far (stats: `sync_abandoned`).
    pub abandoned: Arc<AtomicU64>,
    /// Threads alive, initial plus replacements (stats: `sync_live`).
    pub live_threads: Arc<std::sync::atomic::AtomicI64>,
}

fn run_sync_job(rt: &PyRuntime, job: SyncJob) {
    if job.resp.is_closed() {
        // Abandoned while queued: do not run a zombie.
        return;
    }
    #[cfg(any(test, feature = "test-hooks"))]
    if job.name == TEST_HOOK_PANIC_JOB {
        panic!("test-hooks: deliberate sync pool worker panic");
    }
    // From here the dispatcher charges timeout_ms. Err: it stopped waiting
    // since the check above.
    if job.started.send(()).is_err() {
        return;
    }
    let out = rt.run_sync_blocking(
        &job.name,
        &job.args,
        &job.kwargs,
        job.soft_timeout_ms,
        job.meta.as_deref(),
    );
    let _ = job.resp.send(out);
}

impl SyncPool {
    /// `queue_capacity` bounds the job queue (pass `--io-concurrency`) so a
    /// fully wedged pool cannot grow the backlog without bound.
    pub fn start(rt: Arc<PyRuntime>, threads: usize, queue_capacity: usize) -> SyncPool {
        let threads = threads.max(1);
        let pool = SyncPool {
            queue: Arc::new(JobQueue {
                state: Mutex::new(QueueState {
                    jobs: std::collections::VecDeque::new(),
                    parked: 0,
                    standby: false,
                }),
                ready: std::sync::Condvar::new(),
                capacity: queue_capacity.max(1),
            }),
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
        // Refuse loudly at the ceiling; sync_live stays there until a wedged
        // thread returns on its own.
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
        let queue = self.queue.clone();
        let rt = self.rt.clone();
        let live = self.live_threads.clone();
        live.fetch_add(1, Ordering::Relaxed);
        std::thread::Builder::new()
            .name(format!("cauli-sync-{idx}"))
            .spawn(move || {
                // First in the closure, so a panic anywhere in the thread body
                // still decrements sync_live.
                let _live_guard = crate::ctx::DecrGuard(&live);
                // Pin one thread state for the thread's lifetime. An unpinned
                // attach destroys it per task, wiping `threading.local`: Django
                // per thread DB connections, scoped sessions and the like.
                //
                // SAFETY: the interpreter is initialized before any pool
                // starts, and Ensure is paired with an immediate SaveThread so
                // the GIL is not held around recv(). Never Released.
                unsafe {
                    pyo3::ffi::PyGILState_Ensure();
                    pyo3::ffi::PyEval_SaveThread();
                }
                loop {
                    queue.wait_for_job();
                    // One GIL entry per burst, not per job: a GIL handoff per
                    // job costs several times the task itself.
                    Python::attach(|_py| {
                        let mut arrived = true;
                        while let Some(job) = queue.take(std::mem::take(&mut arrived)) {
                            run_sync_job(&rt, job);
                        }
                    });
                }
            })
            .expect("failed to spawn sync pool thread");
    }

    /// Enqueue a job. `Err` means the bounded queue is full; fail the task
    /// rather than block the runtime. Boxed for `clippy::result_large_err`.
    pub fn submit(&self, job: SyncJob) -> Result<(), Box<SyncJob>> {
        let mut q = self.queue.state.lock().unwrap();
        if q.jobs.len() >= self.queue.capacity {
            return Err(Box::new(job));
        }
        q.jobs.push_back(job);
        self.queue.wake_standby(&mut q);
        Ok(())
    }

    /// Called on a sync hard timeout: the thread may be wedged for good, so
    /// spawn a replacement.
    pub fn report_hard_timeout(&self) {
        self.abandoned.fetch_add(1, Ordering::Relaxed);
        self.spawn_worker();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes every test that builds a `PyRuntime` or mutates process wide
    /// interpreter state. A second runtime reruns the shim in the same module
    /// dict (see `RUNTIME_BUILT`), so overlapping tests hang or fail with
    /// WorkerShimError. Without the lock they still pass on an idle box.
    static SHIM_STATE_LOCK: Mutex<()> = Mutex::new(());

    /// Poison tolerant, so one panicking test does not fail all the others.
    fn shim_state_guard() -> std::sync::MutexGuard<'static, ()> {
        let guard = SHIM_STATE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Cleared only under the lock, so the guard stays armed inside each
        // test body.
        RUNTIME_BUILT.store(false, Ordering::SeqCst);
        // Mandated entry point; pyo3 0.26 aliases it to Python::initialize.
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        guard
    }

    /// Registers a synthetic module in `sys.modules` and returns its
    /// "module:app" spec. `module_name` must be unique per test.
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

    /// A config parse failure must never echo the raw shim JSON, which
    /// carries `redis_url` with its password.
    #[test]
    fn unparseable_config_error_does_not_leak_redis_password() {
        let _shim_state = shim_state_guard();
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
        let err = match PyRuntime::init(&app_spec, 1, 1) {
            Ok(_) => panic!("expected init to fail: result_ttl=-1 cannot parse as u64"),
            Err(e) => e,
        };
        // The exact text startup logs with {e:#}.
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

    /// A huge exception message or traceback is capped before it reaches
    /// result keys and DLQ entries.
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

    /// A minimal app with no tasks, for tests that drive `SyncPool` directly.
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

    /// A second `init` in one process fails immediately, also inside a test.
    #[test]
    fn a_second_pyruntime_in_one_process_fails_loudly() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_double_init_app", MINIMAL_VALID_APP_SRC)
        });
        let (_rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");

        let err = match PyRuntime::init(&app_spec, 1, 1) {
            Ok(_) => panic!("a second PyRuntime in one process must not be allowed"),
            Err(e) => format!("{e:#}"),
        };
        assert!(
            err.contains("called twice in this process"),
            "the second init failed, but for the wrong reason: {err}"
        );
        assert!(
            err.contains("dropped silently"),
            "the error must name the consequence, not just the rule: {err}"
        );
    }

    /// `report_hard_timeout` stops spawning at the ceiling.
    #[test]
    fn report_hard_timeout_stops_spawning_at_the_thread_ceiling() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_ceiling_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let pool = SyncPool::start(rt, 1, 32); // threads=1 -> max_threads = 4
        assert_eq!(pool.live_threads.load(Ordering::Relaxed), 1);

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

    /// A panic inside a pool thread still decrements live_threads.
    #[test]
    fn sync_pool_thread_panic_does_not_overcount_live_threads() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_panic_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let pool = SyncPool::start(rt, 1, 4);
        assert_eq!(pool.live_threads.load(Ordering::Relaxed), 1);

        let (resp_tx, resp_rx) = oneshot::channel();
        let (started_tx, _started_rx) = oneshot::channel();
        pool.submit(SyncJob {
            name: TEST_HOOK_PANIC_JOB.to_string(),
            args: String::new(),
            kwargs: String::new(),
            soft_timeout_ms: None,
            meta: None,
            started: started_tx,
            resp: resp_tx,
        })
        .ok()
        .expect("bounded queue has room for one job");

        // resp_rx stays alive so the pool thread does not skip the job.
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

        // Still usable, and the count has no leftover from the panic.
        pool.report_hard_timeout();
        assert_eq!(pool.live_threads.load(Ordering::Relaxed), 1);
    }

    /// Sync tasks that block (`time.sleep` here) and the thread that ran them.
    const SYNC_POOL_APP_SRC: &str = r#"
import threading
import time


class _Task:
    def __init__(self, fn):
        self.fn = fn
        self.kind = "io"
        self.is_async = False


def nap(seconds):
    if seconds:
        time.sleep(seconds)
    return threading.get_ident()


class _App:
    redis_url = "redis://localhost:6379/0"
    default_queue = "default"
    result_ttl = 3600
    idemp_ttl = 86400
    queue_ttl = {}
    _tasks = {"nap": _Task(nap)}


app = _App()
"#;

    /// The job's outcome and its `started` receiver, which must outlive the
    /// job: a dropped one reads as an abandoned job and the pool skips it.
    fn submit_nap(
        pool: &SyncPool,
        seconds: f64,
    ) -> (oneshot::Receiver<Outcome>, oneshot::Receiver<()>) {
        let (resp_tx, resp_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();
        pool.submit(SyncJob {
            name: "nap".to_string(),
            args: format!("[{seconds}]"),
            kwargs: "{}".to_string(),
            soft_timeout_ms: None,
            meta: None,
            started: started_tx,
            resp: resp_tx,
        })
        .ok()
        .expect("queue has room");
        (resp_rx, started_rx)
    }

    /// Blocking jobs overlap only if a standby is woken behind each one that
    /// starts: without it 8 naps of 300ms take 2.4s instead of about 0.3s.
    #[test]
    fn sync_pool_overlaps_blocking_jobs_queued_together() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_sync_overlap_app", SYNC_POOL_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let pool = SyncPool::start(rt, 8, 64);

        let started = std::time::Instant::now();
        let rxs: Vec<_> = (0..8).map(|_| submit_nap(&pool, 0.3)).collect();
        let mut threads = std::collections::HashSet::new();
        for (rx, _started) in rxs {
            match rx.blocking_recv().expect("pool must answer") {
                Outcome::Success(ident) => threads.insert(ident),
                other => panic!("nap failed: {}", outcome_name(&other)),
            };
        }
        let elapsed = started.elapsed();
        assert_eq!(threads.len(), 8, "every nap must run on its own thread");
        assert!(
            elapsed < std::time::Duration::from_millis(1200),
            "8 blocking jobs on 8 threads took {elapsed:?}, they ran serially"
        );
    }

    /// A job submitted while every thread is parked always wakes one, however
    /// the previous standby stood down. A stale standby flag hangs this test.
    #[test]
    fn sync_pool_never_strands_a_job_after_a_burst() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_sync_strand_app", SYNC_POOL_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let pool = SyncPool::start(rt, 4, 64);

        for round in 0..1000 {
            let burst: Vec<_> = (0..(round % 16 + 1))
                .map(|_| submit_nap(&pool, 0.0))
                .collect();
            for (rx, started) in burst {
                let (tx, done) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let _ = tx.send(rx.blocking_recv().is_ok());
                    drop(started);
                });
                let answered = done
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap_or(false);
                assert!(answered, "round {round}: a queued job was never run");
            }
        }
    }

    /// Args msgspec refuses are terminal, not a retryable WorkerShimError.
    /// `1e400` is valid JSON past f64 range, which serde_json carries as text.
    #[test]
    fn run_sync_blocking_args_decode_failure_is_not_retryable() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_depth_sync_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");

        // Decoding fails before the unregistered name is looked up.
        for (args, kwargs) in [("[1e400]", "{}"), ("[]", r#"{"k": -1e400}"#)] {
            match rt.run_sync_blocking("does.not.matter", args, kwargs, None, None) {
                Outcome::Failure { err, retryable } => {
                    assert!(!retryable, "{args} {kwargs} can never succeed on retry");
                    assert_eq!(err.type_, "SerializationError");
                }
                other => panic!("expected a decode failure, got {}", outcome_name(&other)),
            }
        }
    }

    /// Same, for the async submit lane.
    #[test]
    fn queue_submit_args_decode_failure_is_not_retryable() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_depth_async_app", MINIMAL_VALID_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");

        for (args, kwargs) in [("[1e400]", "{}"), ("[]", r#"{"k": -1e400}"#)] {
            let (_token, rx) = rt.queue_submit("does.not.matter", args, kwargs, 5.0, None, None);
            // blocking_recv: the std MutexGuard must not cross an await.
            match rx.blocking_recv().expect("submitter thread must respond") {
                Outcome::Failure { err, retryable } => {
                    assert!(!retryable, "{args} {kwargs} can never succeed on retry");
                    assert_eq!(err.type_, "SerializationError");
                }
                other => panic!("expected a decode failure, got {}", outcome_name(&other)),
            }
        }
    }

    /// Values msgspec alone would write (NaN as null, a set as an array) and
    /// values it refuses (a lone surrogate) are a terminal SerializationError;
    /// every JSON value comes back as the text the client's codec writes.
    #[test]
    fn result_encoding_matches_the_client_codec_and_rejects_non_json() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        Python::attach(|py| {
            let encode = py
                .import("msgspec.json")
                .and_then(|m| m.getattr("encode"))
                .expect("msgspec is importable in the test interpreter");
            let eval = |src: &str| py.eval(&CString::new(src).unwrap(), None, None).unwrap();
            let ok = "{'z': 1, 'a': [2**100, -0.0, 1e16, 'caf\\u00e9 \\U0001f600', (None, True)]}";
            match encode_result(&eval(ok), &encode) {
                Outcome::Success(json) => assert_eq!(
                    json,
                    r#"{"z":1,"a":[1267650600228229401496703205376,-0.0,1e16,"café 😀",[null,true]]}"#
                ),
                other => panic!("expected success, got {}", outcome_name(&other)),
            }
            for bad in ["float('nan')", "{1, 2}", "'bad \\udc80'", "{1: 'a'}"] {
                match encode_result(&eval(bad), &encode) {
                    Outcome::Failure { err, retryable } => {
                        assert_eq!(err.type_, "SerializationError", "{bad}");
                        assert!(!retryable, "{bad}");
                    }
                    other => panic!("{bad}: expected a failure, got {}", outcome_name(&other)),
                }
            }
        });
    }

    /// The shim's own registry miss (sync lane) reports "UnregisteredTask",
    /// as PROTOCOL §8 and dispatch.rs's gate do. That gate is bypassed here.
    #[test]
    fn shim_sync_unregistered_task_error_type_is_canonical() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(
                py,
                "cauli_test_unregistered_sync_app",
                MINIMAL_VALID_APP_SRC,
            )
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");

        match rt.run_sync_blocking("no.such.task", "[]", "{}", None, None) {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "UnregisteredTask");
                assert!(!retryable);
                // Synthesized by the shim, no task exception behind it.
                assert_eq!(err.origin, "worker");
            }
            Outcome::Success(_) => panic!("expected a failure, got Success"),
            Outcome::ForceRetry { .. } => panic!("expected a failure, got ForceRetry"),
        }
    }

    /// Same, for the async lane.
    #[test]
    fn shim_async_unregistered_task_error_type_is_canonical() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(
                py,
                "cauli_test_unregistered_async_app",
                MINIMAL_VALID_APP_SRC,
            )
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");

        let (_token, rx) = rt.queue_submit("no.such.task", "[]", "{}", 5.0, None, None);
        // blocking_recv: the std MutexGuard must not cross an await.
        match rx.blocking_recv().expect("submitter thread must respond") {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "UnregisteredTask");
                assert!(!retryable);
                // Synthesized by the shim, no task exception behind it.
                assert_eq!(err.origin, "worker");
            }
            Outcome::Success(_) => panic!("expected a failure, got Success"),
            Outcome::ForceRetry { .. } => panic!("expected a failure, got ForceRetry"),
        }
    }

    /// The shim body imports `SoftTimeLimitExceeded` before `load_app` sets up
    /// sys.path. A source built worker may not see cauli yet and falls back
    /// to a local class; `load_app` must rebind once cauli is importable, or
    /// a user's `except SoftTimeLimitExceeded:` never matches. Driven through
    /// `sys.modules`, since VIRTUAL_ENV and cwd are shared with other tests.
    #[test]
    fn load_app_rebinds_soft_time_limit_exceeded_once_cauli_becomes_importable() {
        let _shim_state = shim_state_guard();
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
                "cauli is importable in this test process: run cargo test with no virtualenv activated (embedded CPython finds one through PATH)"
            );
            let code = CString::new(include_str!("shim.py")).expect("shim.py contains NUL byte");

            // Phase 1: cauli never installed (supported, PROTOCOL §4.2).
            // load_app succeeds and keeps the fallback class.
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

            // Phase 2: cauli becomes importable between the module body and
            // load_app.
            let shim = PyModule::from_code(
                py,
                code.as_c_str(),
                c"shim.py",
                c"cauli_worker_shim_rebind_repro",
            )
            .expect("failed to load embedded shim");

            // A stand in "cauli" package, not yet in sys.modules.
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

            // Sanity check: this shim starts on its own fallback class, not
            // the fake cauli's, or the test would pass without the rebind.
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

            // Do not leave the fake package in sys.modules for later tests.
            let _ = sys_modules.del_item("cauli");
        });
    }
    /// `Outcome` has no `Debug`; names the variant for a failed assertion.
    fn outcome_name(o: &Outcome) -> &'static str {
        match o {
            Outcome::Success(_) => "Success",
            Outcome::Failure { .. } => "Failure",
            Outcome::ForceRetry { .. } => "ForceRetry",
        }
    }

    /// Three `async def` bodies that react differently to the soft mark.
    const SOFT_TIMEOUT_APP_SRC: &str = r#"
import asyncio


class _Task:
    def __init__(self, fn):
        self.fn = fn
        self.kind = "io"
        self.is_async = True


async def propagates(hold_s):
    await asyncio.sleep(hold_s)
    return "finished"


async def cleans_up(hold_s):
    try:
        await asyncio.sleep(hold_s)
    except asyncio.CancelledError:
        return "cleaned up"
    return "finished"


async def stubborn(hold_s):
    try:
        await asyncio.sleep(hold_s)
    except asyncio.CancelledError:
        await asyncio.sleep(hold_s)
    return "finished"


class _App:
    redis_url = "redis://localhost:6379/0"
    default_queue = "default"
    result_ttl = 3600
    idemp_ttl = 86400
    queue_ttl = {}
    _tasks = {
        "propagates": _Task(propagates),
        "cleans_up": _Task(cleans_up),
        "stubborn": _Task(stubborn),
    }


app = _App()
"#;

    /// The async lane keeps soft and hard deadlines separate: an `async def`
    /// task gets `SoftTimeLimitExceeded` at the soft mark and keeps its hard
    /// budget. One runtime for all cases, since only one may exist.
    #[test]
    fn async_soft_timeout_fires_before_the_hard_deadline() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_soft_timeout_app", SOFT_TIMEOUT_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");

        let submit = |task: &str, hold_s: f64, hard_s: f64, soft_s: Option<f64>| {
            let (_token, rx) =
                rt.queue_submit(task, &format!("[{hold_s}]"), "{}", hard_s, soft_s, None);
            rx.blocking_recv().expect("submitter thread must respond")
        };

        // 1. Soft fires and the body does not handle it: the reported error
        //    is SoftTimeLimitExceeded, not TimeLimitExceeded, and it arrives
        //    at the soft mark rather than at the hard one.
        let started = std::time::Instant::now();
        match submit("propagates", 10.0, 5.0, Some(0.2)) {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "SoftTimeLimitExceeded", "{err:?}");
                assert!(retryable);
            }
            other => panic!(
                "expected a soft timeout failure, got {}",
                outcome_name(&other)
            ),
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(4),
            "the soft deadline must fire at soft, not at hard"
        );

        // 2. The body's own cleanup path runs and its return value stands.
        match submit("cleans_up", 10.0, 5.0, Some(0.2)) {
            Outcome::Success(v) => assert_eq!(v, r#""cleaned up""#),
            other => panic!(
                "expected the cleanup return value, got {}",
                outcome_name(&other)
            ),
        }

        // 3. The hard budget survives the soft mark: this body ignores the
        //    stop and keeps working past `soft`.
        match submit("stubborn", 0.3, 5.0, Some(0.2)) {
            Outcome::Success(v) => assert_eq!(v, r#""finished""#),
            other => panic!(
                "expected the hard budget to apply, got {}",
                outcome_name(&other)
            ),
        }

        // 4. ...and the hard deadline is still a real backstop behind it.
        match submit("stubborn", 2.0, 1.0, Some(0.2)) {
            Outcome::Failure { err, retryable } => {
                assert_eq!(err.type_, "TimeLimitExceeded", "{err:?}");
                assert!(retryable);
            }
            other => panic!("expected a hard timeout, got {}", outcome_name(&other)),
        }

        // 5. No soft limit set: unchanged single-deadline behaviour.
        match submit("propagates", 0.05, 5.0, None) {
            Outcome::Success(v) => assert_eq!(v, r#""finished""#),
            other => panic!(
                "expected success with no soft limit, got {}",
                outcome_name(&other)
            ),
        }
    }

    /// Payload keys must stay mortal: on 3.12 and free threaded builds an
    /// interned string is immortal, so a decoder that interned every key
    /// would leak each distinct one (a uuid keyed map, say) forever.
    #[test]
    fn decoded_payload_keys_are_not_immortalized() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        Python::attach(|py| {
            let refs: u64 = py
                .eval(
                    c"__import__('sys').getrefcount(next(iter(__import__('msgspec.json').json.Decoder().decode(b'{\"rv_exec_unique_key_0b1c\": 1}'))))",
                    None,
                    None,
                )
                .unwrap()
                .extract()
                .unwrap();
            assert!(
                refs < 1 << 30,
                "payload key was immortalized (refcount {refs})"
            );
        });
    }

    /// A lone surrogate in an exception message (an undecodable filename
    /// joined into an f-string) must not blank the message and traceback.
    #[test]
    fn lone_surrogate_in_error_keeps_message_and_traceback() {
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();
        Python::attach(|py| {
            let obj = py
                .eval(
                    c"{'ok': False, 'retryable': True, 'error': {'type': 'ValueError', 'message': 'bad \\udc80 name', 'traceback': 'Traceback \\udc80', 'origin': 'task'}}",
                    None,
                    None,
                )
                .unwrap();
            // A failure never reaches the encoder.
            match outcome_from_py(&obj, &py.None().into_bound(py)) {
                Outcome::Failure { err, .. } => {
                    assert!(
                        err.message.starts_with("bad "),
                        "message lost: {:?}",
                        err.message
                    );
                    assert!(
                        err.traceback.starts_with("Traceback"),
                        "traceback lost: {:?}",
                        err.traceback
                    );
                }
                other => panic!("expected a failure, got {}", outcome_name(&other)),
            }
        });
    }

    const ASYNC_TIMEOUT_APP_SRC: &str = r#"
import asyncio


class _Task:
    def __init__(self, fn):
        self.fn = fn
        self.kind = "io"
        self.is_async = True


ran_past_hard = []


async def stubborn(hold_s):
    try:
        await asyncio.sleep(hold_s)
    except asyncio.CancelledError:
        await asyncio.sleep(hold_s)
    ran_past_hard.append(True)


async def own_timeout():
    await asyncio.wait_for(asyncio.sleep(5), 0.01)


class _App:
    redis_url = "redis://localhost:6379/0"
    default_queue = "default"
    result_ttl = 3600
    idemp_ttl = 86400
    queue_ttl = {}
    _tasks = {"stubborn": _Task(stubborn), "own_timeout": _Task(own_timeout)}


app = _App()
"#;

    /// The hard deadline behind a soft one must stop the body, not just
    /// report TimeLimitExceeded while the body runs on beside its retry.
    #[test]
    fn async_hard_deadline_after_soft_cancels_the_body() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_hard_cancel_app", ASYNC_TIMEOUT_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let (_token, rx) = rt.queue_submit("stubborn", "[0.6]", "{}", 0.4, Some(0.1), None);
        match rx.blocking_recv().expect("submitter thread must respond") {
            Outcome::Failure { err, .. } => assert_eq!(err.type_, "TimeLimitExceeded"),
            other => panic!("expected a hard timeout, got {}", outcome_name(&other)),
        }
        std::thread::sleep(std::time::Duration::from_millis(900));
        let ran: usize = Python::attach(|py| {
            py.import("cauli_test_hard_cancel_app")
                .and_then(|m| m.getattr("ran_past_hard"))
                .and_then(|l| l.len())
                .unwrap()
        });
        assert_eq!(
            ran, 0,
            "the body kept running after its hard deadline was reported"
        );
    }

    /// A TimeoutError the async body raises itself (its own inner wait_for)
    /// is the task's error, not the worker's TimeLimitExceeded.
    #[test]
    fn async_task_own_timeout_error_is_reported_as_the_tasks() {
        let _shim_state = shim_state_guard();
        let app_spec = Python::attach(|py| {
            install_fake_app_module(py, "cauli_test_own_timeout_app", ASYNC_TIMEOUT_APP_SRC)
        });
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let (_token, rx) = rt.queue_submit("own_timeout", "[]", "{}", 5.0, None, None);
        match rx.blocking_recv().expect("submitter thread must respond") {
            Outcome::Failure { err, .. } => {
                assert_eq!(err.type_, "TimeoutError", "{err:?}");
                assert_eq!(err.origin, "task");
                assert!(err.traceback.contains("own_timeout"), "{err:?}");
            }
            other => panic!(
                "expected the task's own TimeoutError, got {}",
                outcome_name(&other)
            ),
        }
    }

    /// The worker's own hard deadline must always read as TimeLimitExceeded.
    /// uvloop keeps time in whole milliseconds, so its timer fires at a time
    /// that can compare one float ulp below the deadline `_arun` computed.
    #[test]
    fn async_hard_deadline_is_never_reported_as_the_tasks_own_timeout() {
        let _shim_state = shim_state_guard();
        let src = ASYNC_TIMEOUT_APP_SRC.replace(
            "_tasks = {",
            "_tasks = {\"nap\": _Task(lambda: asyncio.sleep(10)), ",
        );
        let app_spec =
            Python::attach(|py| install_fake_app_module(py, "cauli_test_deadline_clock_app", &src));
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        // Whether a deadline lands an ulp low depends on the loop clock
        // reading it starts at, so spread starts over many milliseconds.
        let rxs: Vec<_> = (0..400)
            .map(|k| {
                if k % 8 == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                let timeout_s = (k % 50 + 1) as f64 / 1000.0;
                rt.queue_submit("nap", "[]", "{}", timeout_s, None, None).1
            })
            .collect();
        let mut wrong = Vec::new();
        for rx in rxs {
            match rx.blocking_recv().expect("submitter thread must respond") {
                Outcome::Failure { err, .. } if err.type_ == "TimeLimitExceeded" => {}
                Outcome::Failure { err, .. } => wrong.push(err.type_),
                other => wrong.push(outcome_name(&other).to_string()),
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of 400 misreported: {:?}",
            wrong.len(),
            &wrong[..wrong.len().min(3)]
        );
    }

    /// Every deadline on a loop shares one timer, moved only when a new
    /// deadline lands first. A short deadline queued behind a long one must
    /// still fire at its own time, not when the long one's timer comes due.
    #[test]
    fn async_short_deadline_behind_a_long_one_fires_on_time() {
        let _shim_state = shim_state_guard();
        let src = ASYNC_TIMEOUT_APP_SRC.replace(
            "_tasks = {",
            "_tasks = {\"nap\": _Task(lambda: asyncio.sleep(30)), ",
        );
        let app_spec =
            Python::attach(|py| install_fake_app_module(py, "cauli_test_deadline_order_app", &src));
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let (long_token, _long) = rt.queue_submit("nap", "[]", "{}", 20.0, None, None);
        std::thread::sleep(std::time::Duration::from_millis(50));
        let started = std::time::Instant::now();
        let (_token, short) = rt.queue_submit("nap", "[]", "{}", 0.05, None, None);
        match short
            .blocking_recv()
            .expect("submitter thread must respond")
        {
            Outcome::Failure { err, .. } => assert_eq!(err.type_, "TimeLimitExceeded"),
            other => panic!("expected TimeLimitExceeded, got {}", outcome_name(&other)),
        }
        let waited = started.elapsed();
        assert!(
            waited < std::time::Duration::from_secs(2),
            "a 50ms deadline behind a 20s one fired after {waited:?}"
        );
        rt.cancel(long_token);
    }

    /// Cancelled deadlines are only marked, so the heap is compacted once
    /// they are the majority: tasks that finish early must not pile up
    /// behind one that is still running, whatever the rate.
    #[test]
    fn async_deadline_heap_stays_bounded_by_live_deadlines() {
        let _shim_state = shim_state_guard();
        let src = ASYNC_TIMEOUT_APP_SRC.replace(
            "_tasks = {",
            "_tasks = {\"nap\": _Task(lambda: asyncio.sleep(30)), \
             \"quick\": _Task(lambda: asyncio.sleep(0)), ",
        );
        let app_spec =
            Python::attach(|py| install_fake_app_module(py, "cauli_test_deadline_heap_app", &src));
        let (rt, _cfg) = PyRuntime::init(&app_spec, 1, 1).expect("valid app must init cleanly");
        let (long_token, _long) = rt.queue_submit("nap", "[]", "{}", 300.0, None, None);
        let quick: Vec<_> = (0..3000)
            .map(|_| rt.queue_submit("quick", "[]", "{}", 300.0, None, None).1)
            .collect();
        for rx in quick {
            match rx.blocking_recv().expect("submitter thread must respond") {
                Outcome::Success(_) => {}
                other => panic!("quick task failed: {}", outcome_name(&other)),
            }
        }
        let heap_len: usize = Python::attach(|py| {
            py.import("cauli_worker_shim")
                .and_then(|m| m.getattr("_deadline_heaps"))
                .and_then(|h| h.get_item(0))
                .and_then(|h| h.getattr("_heap"))
                .and_then(|h| h.len())
                .expect("shim exposes its deadline heaps")
        });
        assert!(
            heap_len <= 102,
            "{heap_len} entries kept for 1 live deadline after 3000 finished ones"
        );
        rt.cancel(long_token);
    }
}
