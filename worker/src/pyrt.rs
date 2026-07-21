//! Embedded CPython integration.
//!
//! Rules honored here:
//! - `pyo3::prepare_freethreaded_python()` once at startup, before tokio.
//! - The GIL is only ever taken on dedicated OS threads (sync pool) or inside
//!   `spawn_blocking` (async submit). Tokio worker threads never hold the GIL
//!   while doing broker I/O.
//! - All Python-side complexity lives in the embedded shim (src/shim.py):
//!   the pyo3 surface is "call function, pass strings, get strings".

use anyhow::{anyhow, Context, Result};
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyModule, PyTuple};
use serde::Deserialize;
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
    pub tasks: HashMap<String, TaskSpec>,
}

pub struct PyRuntime {
    shim: Py<PyModule>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<String>>>>,
    next_token: AtomicU64,
}

fn synth_error_json(type_: &str, msg: &str) -> String {
    serde_json::json!({
        "ok": false,
        "retryable": true,
        "error": {"type": type_, "message": msg, "traceback": ""},
    })
    .to_string()
}

fn pyerr_string(py: Python<'_>, e: &PyErr) -> String {
    let mut s = e.to_string();
    if let Some(tb) = e.traceback(py) {
        if let Ok(t) = tb.format() {
            s.push('\n');
            s.push_str(&t);
        }
    }
    s
}

impl PyRuntime {
    /// Initialize the interpreter, import the shim, load the app, register the
    /// completion callback and start the asyncio loop threads.
    /// Returns the runtime and the parsed app config.
    pub fn init(app_spec: &str, io_loops: usize) -> Result<(Arc<PyRuntime>, AppConfig)> {
        // Mandated entry point; pyo3 0.26 aliases it to Python::initialize.
        #[allow(deprecated)]
        pyo3::prepare_freethreaded_python();

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<String>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_cb = pending.clone();
        let app_spec = app_spec.to_string();

        let (shim, cfg_json) = Python::attach(|py| -> Result<(Py<PyModule>, String)> {
            let code = CString::new(include_str!("shim.py")).expect("shim.py contains NUL byte");
            let m = PyModule::from_code(py, code.as_c_str(), c"shim.py", c"rupy_worker_shim")
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
                Some(c"rupy_complete"),
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<()> {
                    let token: u64 = args.get_item(0)?.extract()?;
                    let payload: String = args.get_item(1)?.extract()?;
                    if let Some(tx) = pending_cb.lock().unwrap().remove(&token) {
                        let _ = tx.send(payload);
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

        let cfg: AppConfig = serde_json::from_str(&cfg_json)
            .with_context(|| format!("shim load_app returned unparseable config: {cfg_json}"))?;

        Ok((
            Arc::new(PyRuntime {
                shim,
                pending,
                next_token: AtomicU64::new(1),
            }),
            cfg,
        ))
    }

    /// Blocking sync task execution. MUST be called from a dedicated OS thread
    /// (the sync io pool), never from a tokio worker thread.
    pub fn run_sync_blocking(
        &self,
        name: &str,
        args_json: &str,
        kwargs_json: &str,
        soft_timeout_ms: Option<u64>,
    ) -> String {
        Python::attach(|py| {
            let r = self
                .shim
                .bind(py)
                .getattr("run_sync")
                .and_then(|f| f.call1((name, args_json, kwargs_json, soft_timeout_ms)))
                .and_then(|v| v.extract::<String>());
            match r {
                Ok(s) => s,
                Err(e) => synth_error_json(
                    "WorkerShimError",
                    &format!("run_sync failed: {}", pyerr_string(py, &e)),
                ),
            }
        })
    }

    /// Submit an async task to an embedded asyncio loop. Completion arrives
    /// push-style on the returned oneshot (via the registered Python callback).
    /// Briefly takes the GIL to schedule; call from `spawn_blocking`. Returns
    /// the token alongside the receiver so the caller can `cancel(token)` if
    /// it gives up waiting (see `cancel`, MEM-1).
    pub fn submit_async(
        &self,
        name: &str,
        args_json: &str,
        kwargs_json: &str,
        timeout_s: f64,
    ) -> (u64, oneshot::Receiver<String>) {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(token, tx);

        let submit_err: Option<String> = Python::attach(|py| {
            match self
                .shim
                .bind(py)
                .getattr("submit_async")
                .and_then(|f| f.call1((token, name, args_json, kwargs_json, timeout_s)))
            {
                Ok(_) => None,
                Err(e) => Some(pyerr_string(py, &e)),
            }
        });

        if let Some(msg) = submit_err {
            if let Some(tx) = self.pending.lock().unwrap().remove(&token) {
                let _ = tx.send(synth_error_json(
                    "WorkerShimError",
                    &format!("submit_async failed: {msg}"),
                ));
            }
        }
        (token, rx)
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

    /// Current size of the pending-completion map (stats: `pending_async`).
    /// A number that only grows over time signals a wedged event-loop thread
    /// (MEM-1) even after `cancel` stops the Rust-side bookkeeping leak.
    pub fn pending_len(&self) -> usize {
        self.pending.lock().unwrap().len()
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
//  - on a reported hard timeout, a replacement thread is spawned immediately
//    to restore capacity. If the original wedged thread ever does return, it
//    just resumes serving as extra headroom (we cannot safely kill a
//    genuinely blocked OS thread from here).
// ---------------------------------------------------------------------------

pub struct SyncJob {
    pub name: String,
    pub args_json: String,
    pub kwargs_json: String,
    pub soft_timeout_ms: Option<u64>,
    pub resp: oneshot::Sender<String>,
}

pub struct SyncPool {
    tx: crossbeam_channel::Sender<SyncJob>,
    rx: crossbeam_channel::Receiver<SyncJob>,
    rt: Arc<PyRuntime>,
    next_idx: AtomicUsize,
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
        let pool = SyncPool {
            tx,
            rx,
            rt,
            next_idx: AtomicUsize::new(0),
            abandoned: Arc::new(AtomicU64::new(0)),
            live_threads: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        };
        for _ in 0..threads.max(1) {
            pool.spawn_worker();
        }
        pool
    }

    fn spawn_worker(&self) {
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
        let rx = self.rx.clone();
        let rt = self.rt.clone();
        let live = self.live_threads.clone();
        live.fetch_add(1, Ordering::Relaxed);
        std::thread::Builder::new()
            .name(format!("rupy-sync-{idx}"))
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    if job.resp.is_closed() {
                        // The dispatcher already abandoned this job (hard
                        // timeout while still queued): running it now would
                        // be zombie execution with no one listening.
                        continue;
                    }
                    let out = rt.run_sync_blocking(
                        &job.name,
                        &job.args_json,
                        &job.kwargs_json,
                        job.soft_timeout_ms,
                    );
                    let _ = job.resp.send(out);
                }
                live.fetch_sub(1, Ordering::Relaxed);
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
