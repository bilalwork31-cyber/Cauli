//! --procs N supervisor: the productized form of "run N workers on one box"
//! (bench3: 1→4 processes was +74% throughput at lower p99 — one GIL each).
//!
//! The parent stays Python-free: it resolves the -c derivation once, re-execs
//! itself `--procs 1` with every setting passed explicitly, and from then on
//! only spawns, restarts and forwards signals. Children join their own process
//! group (a tty Ctrl+C must not reach them directly, or the supervisor's
//! forwarded SIGTERM would look like a second signal and force-exit the drain)
//! and carry PR_SET_PDEATHSIG so a SIGKILLed supervisor cannot leak them.

use crate::cli;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info, warn};

/// Floor between two spawns of the same slot, so a child that dies at startup
/// (bad redis URL, import error) retries at 1/s instead of as fast as fork can.
const RESTART_DELAY: Duration = Duration::from_secs(1);

struct Slot {
    child: Option<Child>,
    spawned: Instant,
    respawn_at: Instant,
}

pub fn run(args: &cli::Args, resolved: &cli::Resolved) -> i32 {
    let exe: PathBuf = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            error!("cannot locate own executable to spawn worker procs: {e}");
            return 1;
        }
    };
    let argv = child_argv(args, resolved);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(supervise(&exe, &argv, resolved.procs))
}

async fn supervise(exe: &Path, argv: &[String], procs: usize) -> i32 {
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
    let now = Instant::now();
    let mut slots: Vec<Slot> = Vec::with_capacity(procs);
    for i in 0..procs {
        match spawn_child(exe, argv) {
            Ok(c) => {
                info!(
                    "worker proc {i}/{procs} started (pid {})",
                    c.id().unwrap_or(0)
                );
                slots.push(Slot {
                    child: Some(c),
                    spawned: now,
                    respawn_at: now,
                });
            }
            Err(e) => {
                error!("cannot spawn worker proc {i}: {e}");
                fan_out(&mut slots);
                for s in &mut slots {
                    if let Some(c) = &mut s.child {
                        let _ = c.wait().await;
                    }
                }
                return 1;
            }
        }
    }

    let mut shutting_down = false;
    let mut exit_code = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tokio::select! {
            _ = term.recv() => {
                info!("SIGTERM: forwarding to worker procs, draining");
                shutting_down = true;
                fan_out(&mut slots);
            }
            _ = int.recv() => {
                info!("SIGINT: forwarding to worker procs, draining");
                shutting_down = true;
                fan_out(&mut slots);
            }
            _ = tick.tick() => {
                let now = Instant::now();
                for (i, s) in slots.iter_mut().enumerate() {
                    if let Some(child) = &mut s.child {
                        match child.try_wait() {
                            Ok(Some(status)) => {
                                s.child = None;
                                if shutting_down {
                                    if !status.success() {
                                        exit_code = 1;
                                    }
                                    info!("worker proc {i} exited ({status})");
                                } else {
                                    warn!("worker proc {i} exited unexpectedly ({status}); restarting");
                                    s.respawn_at = now.max(s.spawned + RESTART_DELAY);
                                }
                            }
                            Ok(None) => {}
                            Err(e) => warn!("wait on worker proc {i} failed: {e}"),
                        }
                    } else if !shutting_down && now >= s.respawn_at {
                        match spawn_child(exe, argv) {
                            Ok(c) => {
                                info!("worker proc {i} restarted (pid {})", c.id().unwrap_or(0));
                                s.child = Some(c);
                                s.spawned = now;
                            }
                            Err(e) => {
                                warn!("respawn of worker proc {i} failed: {e}");
                                s.respawn_at = now + RESTART_DELAY;
                            }
                        }
                    }
                }
                if shutting_down && slots.iter().all(|s| s.child.is_none()) {
                    break;
                }
            }
        }
    }
    info!("all worker procs exited");
    exit_code
}

fn fan_out(slots: &mut [Slot]) {
    for s in slots {
        if let Some(child) = &s.child {
            if let Some(pid) = child.id() {
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }
        }
    }
}

fn spawn_child(exe: &Path, argv: &[String]) -> std::io::Result<Child> {
    let mut cmd = Command::new(exe);
    cmd.args(argv).kill_on_drop(false);
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
    cmd.spawn()
}

/// The child command line: every execution setting explicit, `--procs 1`, and
/// never `-c` — the derivation already happened here, and a child re-deriving
/// with its own procs divisor would compute different numbers.
fn child_argv(args: &cli::Args, r: &cli::Resolved) -> Vec<String> {
    let mut v: Vec<String> = vec![
        "--app".into(),
        args.app.clone(),
        "--procs".into(),
        "1".into(),
        "--io-threads".into(),
        r.io_threads.to_string(),
        "--io-concurrency".into(),
        r.io_concurrency.to_string(),
        "--cpu-workers".into(),
        r.cpu_workers.to_string(),
        "--io-loops".into(),
        args.io_loops.to_string(),
        "--cpu-child-threads".into(),
        args.cpu_child_threads.to_string(),
        "--cpu-prefetch".into(),
        args.cpu_prefetch.to_string(),
        "--batch".into(),
        args.batch.to_string(),
        "--visibility-timeout".into(),
        args.visibility_timeout.to_string(),
        "--max-envelope-bytes".into(),
        args.max_envelope_bytes.to_string(),
        "--drain-timeout".into(),
        args.drain_timeout.to_string(),
        "--python".into(),
        args.python.clone(),
        "--stats-interval".into(),
        args.stats_interval.to_string(),
        "--log-level".into(),
        args.log_level.clone(),
    ];
    if !args.queues.is_empty() {
        v.push("--queues".into());
        v.push(args.queues.join(","));
    }
    if let Some(url) = &args.redis_url {
        v.push("--redis-url".into());
        v.push(url.clone());
    }
    if args.no_fork_server {
        v.push("--no-fork-server".into());
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn child_argv_is_fully_resolved() {
        let args = cli::Args::try_parse_from([
            "cauli-worker",
            "-A",
            "m:app",
            "-c",
            "50",
            "-Q",
            "high,low",
            "--redis-url",
            "redis://127.0.0.1:6392/0",
            "--no-fork-server",
        ])
        .unwrap();
        let r = cli::resolve(&args, 6);
        let v = child_argv(&args, &r);
        // The child must not re-derive: no -c, procs pinned to 1, io/cpu
        // settings passed as the per-proc numbers the supervisor computed.
        assert!(!v.contains(&"-c".to_string()));
        assert!(!v.contains(&"--concurrency".to_string()));
        let at = |flag: &str| v.iter().position(|s| s == flag).unwrap() + 1;
        assert_eq!(v[at("--procs")], "1");
        assert_eq!(v[at("--io-threads")], r.io_threads.to_string());
        assert_eq!(v[at("--io-concurrency")], r.io_concurrency.to_string());
        assert_eq!(v[at("--cpu-workers")], r.cpu_workers.to_string());
        assert_eq!(v[at("--queues")], "high,low");
        assert_eq!(v[at("--redis-url")], "redis://127.0.0.1:6392/0");
        assert!(v.contains(&"--no-fork-server".to_string()));
        // Round-trips through the parser, and a child resolves to itself.
        let mut child_cmd = vec!["cauli-worker".to_string()];
        child_cmd.extend(v);
        let child = cli::Args::try_parse_from(&child_cmd).unwrap();
        assert_eq!(cli::resolve(&child, 6).procs, 1);
        assert_eq!(cli::resolve(&child, 6).io_threads, r.io_threads);
        assert_eq!(cli::resolve(&child, 6).io_concurrency, r.io_concurrency);
        assert_eq!(cli::resolve(&child, 6).cpu_workers, r.cpu_workers);
    }
}
