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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Registry entry parsed from shim `load_app` JSON (PROTOCOL §6 attributes).
/// Routing uses kind/is_async; the rest is kept for introspection/log use
/// (envelope values drive retry/timeout math per §4.2/§4.6).
#[allow(dead_code)]
#[derive(Deserialize, Clone, Debug)]
pub struct TaskSpec {
    pub kind: String,
    pub is_async: bool,
    #[serde(default)]
    pub queue: Option<String>,
    pub max_retries: u32,
    pub timeout_ms: u64,
    #[serde(default)]
    pub soft_timeout_ms: Option<u64>,
    pub backoff_base_ms: u64,
    pub backoff_factor: f64,
    pub backoff_max_ms: u64,
    pub jitter: bool,
    pub store_result: bool,
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
            let code =
                CString::new(include_str!("shim.py")).expect("shim.py contains NUL byte");
            let m = PyModule::from_code(py, code.as_c_str(), c"shim.py", c"rupy_worker_shim")
                .map_err(|e| anyhow!("failed to load embedded shim: {}", pyerr_string(py, &e)))?;

            let cfg: String = m
                .getattr("load_app")
                .and_then(|f| f.call1((app_spec.as_str(), "[]")))
                .and_then(|r| r.extract())
                .map_err(|e| anyhow!("failed to load app {:?}: {}", app_spec, pyerr_string(py, &e)))?;

            let cb = PyCFunction::new_closure(
                py,
                Some(c"rupy_complete"),
                None,
                move |args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| -> PyResult<()> {
                    let token: u64 = args.get_item(0)?.extract()?;
                    let payload: String = args.get_item(1)?.extract()?;
                    if let Some(tx) = pending_cb.lock().unwrap().remove(&token) {
                        let _ = tx.send(payload);
                    }
                    Ok(())
                },
            )
            .map_err(|e| anyhow!("failed to build completion callback: {}", pyerr_string(py, &e)))?;

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
    /// Briefly takes the GIL to schedule; call from `spawn_blocking`.
    pub fn submit_async(
        &self,
        name: &str,
        args_json: &str,
        kwargs_json: &str,
        timeout_s: f64,
    ) -> oneshot::Receiver<String> {
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
        rx
    }
}

// ---------------------------------------------------------------------------
// Sync io pool: dedicated OS threads that acquire the GIL only around the shim
// run_sync call. CPython releases the GIL during blocking socket I/O, so this
// yields real io parallelism.
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
}

impl SyncPool {
    pub fn start(rt: Arc<PyRuntime>, threads: usize) -> SyncPool {
        let (tx, rx) = crossbeam_channel::unbounded::<SyncJob>();
        for i in 0..threads.max(1) {
            let rx = rx.clone();
            let rt = rt.clone();
            std::thread::Builder::new()
                .name(format!("rupy-sync-{i}"))
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        let out = rt.run_sync_blocking(
                            &job.name,
                            &job.args_json,
                            &job.kwargs_json,
                            job.soft_timeout_ms,
                        );
                        let _ = job.resp.send(out);
                    }
                })
                .expect("failed to spawn sync pool thread");
        }
        SyncPool { tx }
    }

    pub fn submit(&self, job: SyncJob) {
        // Unbounded queue: admission is bounded upstream by the io semaphore.
        let _ = self.tx.send(job);
    }
}
