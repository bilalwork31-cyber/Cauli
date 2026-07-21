//! CPU task pool: child processes speaking the PROTOCOL §5.1 line-delimited
//! JSON protocol (`{python} -m rupy._exec --app {spec}`), one in-flight
//! request per child, kill+respawn on hard timeout or child death.
//!
//! Test hook (documented, M5): if env var `RUPY_EXEC_CMD` is set, it is split
//! on whitespace and used verbatim as the child argv instead of
//! `{python} -m rupy._exec --app {spec}`. This lets the e2e suite run a
//! standalone stand-in child (tests/fixtures/fake_exec.py) without the real
//! rupy Python package installed. Compiled in only under `cfg(test)` or the
//! `test-hooks` feature -- a plain `cargo build --release` has no code path
//! that reads this env var at all, so `cargo test --features test-hooks` is
//! required to exercise it (see worker/Cargo.toml `[features]`).

use crate::stats::Counters;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{info, warn};

pub enum CpuOutcome {
    /// Raw response line from the child.
    Resp(String),
    /// Hard timeout: child was SIGKILLed and respawned.
    Timeout,
    /// Child died / pipe broke: respawned.
    Lost,
}

pub struct CpuJob {
    pub req_line: String,
    pub timeout_ms: u64,
    pub resp: oneshot::Sender<CpuOutcome>,
}

#[derive(Clone)]
pub struct CpuPool {
    pub tx: async_channel::Sender<CpuJob>,
    /// Number of dispatch tasks currently blocked on a full backlog; the fetch
    /// loop pauses while > 0 so the in-worker cpu backlog stays bounded at
    /// 2 * cpu_workers without starving io fetch indefinitely (children always
    /// make progress thanks to the hard-timeout SIGKILL).
    pub overflow: Arc<AtomicUsize>,
    pub child_pids: Arc<Mutex<Vec<u32>>>,
}

pub fn child_argv(python: &str, app_spec: &str) -> (String, Vec<String>) {
    // M5: the RUPY_EXEC_CMD override is a test-only hook (e2e uses it to run
    // tests/fixtures/fake_exec.py without the real rupy package). Compiled
    // out entirely for a normal `cargo build --release` so a production
    // binary has no env-driven way to replace the cpu child command; only
    // `cargo test` / `--features test-hooks` builds honor it.
    #[cfg(any(test, feature = "test-hooks"))]
    if let Ok(cmd) = std::env::var("RUPY_EXEC_CMD") {
        tracing::warn!("RUPY_EXEC_CMD test hook active: overriding cpu child command with {cmd:?}");
        let mut parts = cmd.split_whitespace().map(str::to_string);
        if let Some(prog) = parts.next() {
            return (prog, parts.collect());
        }
    }
    (
        python.to_string(),
        vec![
            "-m".into(),
            "rupy._exec".into(),
            "--app".into(),
            app_spec.into(),
        ],
    )
}

pub fn start(workers: usize, python: &str, app_spec: &str, counters: Arc<Counters>) -> CpuPool {
    let cap = (2 * workers).max(1);
    let (tx, rx) = async_channel::bounded::<CpuJob>(cap);
    let pool = CpuPool {
        tx,
        overflow: Arc::new(AtomicUsize::new(0)),
        child_pids: Arc::new(Mutex::new(Vec::new())),
    };
    let (prog, argv) = child_argv(python, app_spec);
    for i in 0..workers {
        tokio::spawn(child_loop(
            i,
            prog.clone(),
            argv.clone(),
            rx.clone(),
            counters.clone(),
            pool.child_pids.clone(),
        ));
    }
    pool
}

/// SIGKILL every live child (used on process exit paths; children also carry
/// PR_SET_PDEATHSIG so a SIGKILLed worker cannot leak them).
pub fn kill_children(pool: &CpuPool) {
    let pids = pool.child_pids.lock().unwrap().clone();
    for pid in pids {
        // L1: pid 0 is never a real child (Command::id() is Some right after
        // a successful spawn); `kill(0, SIGKILL)` signals this process's
        // ENTIRE process group (self-SIGKILL) rather than one child, so it
        // must never reach libc::kill even if tracking ever regresses.
        if pid == 0 {
            continue;
        }
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
    }
}

fn track_pid(pids: &Mutex<Vec<u32>>, pid: u32, add: bool) {
    let mut g = pids.lock().unwrap();
    if add {
        g.push(pid);
    } else {
        g.retain(|p| *p != pid);
    }
}

async fn child_loop(
    idx: usize,
    prog: String,
    argv: Vec<String>,
    rx: async_channel::Receiver<CpuJob>,
    counters: Arc<Counters>,
    pids: Arc<Mutex<Vec<u32>>>,
) {
    loop {
        let mut cmd = Command::new(&prog);
        cmd.args(&argv)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .env("PYTHONUNBUFFERED", "1")
            .kill_on_drop(true);
        unsafe {
            cmd.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                Ok(())
            });
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                warn!("cpu[{idx}]: failed to spawn {prog}: {e}; retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let pid = child.id().unwrap_or(0);
        track_pid(&pids, pid, true);

        let mut stdin = child.stdin.take().expect("child stdin piped");
        let mut lines = BufReader::new(child.stdout.take().expect("child stdout piped")).lines();

        // Ready line: {"ready": true, "pid": N}
        let ready_ok = match timeout(Duration::from_secs(30), lines.next_line()).await {
            Ok(Ok(Some(line))) => {
                let ok = serde_json::from_str::<serde_json::Value>(&line)
                    .map(|v| v["ready"] == serde_json::Value::Bool(true))
                    .unwrap_or(false);
                if !ok {
                    warn!("cpu[{idx}] pid={pid}: bad ready line: {line}");
                }
                ok
            }
            other => {
                warn!("cpu[{idx}] pid={pid}: no ready line ({other:?})");
                false
            }
        };
        if !ready_ok {
            let _ = child.start_kill();
            let _ = child.wait().await;
            track_pid(&pids, pid, false);
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        info!("cpu[{idx}]: child ready pid={pid}");

        let mut respawn = false;
        while !respawn {
            let job = match rx.recv().await {
                Ok(j) => j,
                Err(_) => {
                    // channel closed: shut this child down
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    track_pid(&pids, pid, false);
                    return;
                }
            };
            counters.inflight_cpu.fetch_add(1, Ordering::Relaxed);

            let mut line = job.req_line;
            line.push('\n');
            if let Err(e) = stdin.write_all(line.as_bytes()).await {
                warn!("cpu[{idx}] pid={pid}: write failed: {e}");
                let _ = job.resp.send(CpuOutcome::Lost);
                counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
                break; // exits while: child gets killed + respawned below
            }

            match timeout(Duration::from_millis(job.timeout_ms), lines.next_line()).await {
                Ok(Ok(Some(resp))) => {
                    let _ = job.resp.send(CpuOutcome::Resp(resp));
                }
                Ok(_) => {
                    // EOF or read error: child died mid-task
                    warn!("cpu[{idx}] pid={pid}: child died mid-task (WorkerLost)");
                    let _ = job.resp.send(CpuOutcome::Lost);
                    respawn = true;
                }
                Err(_) => {
                    warn!(
                        "cpu[{idx}] pid={pid}: hard timeout after {}ms; SIGKILL + respawn",
                        job.timeout_ms
                    );
                    let _ = job.resp.send(CpuOutcome::Timeout);
                    respawn = true;
                }
            }
            counters.inflight_cpu.fetch_sub(1, Ordering::Relaxed);
        }

        let _ = child.start_kill();
        let _ = child.wait().await;
        track_pid(&pids, pid, false);
    }
}
