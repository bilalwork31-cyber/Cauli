//! --procs N supervisor: one GIL per process. The parent stays Python free,
//! resolves -c once, re-execs itself `--procs 1` with every setting explicit,
//! then only spawns, restarts and forwards signals. Children get their own
//! process group (a tty Ctrl+C plus the forwarded SIGTERM would read as a
//! second signal and force exit the drain) and PR_SET_PDEATHSIG (a SIGKILLed
//! supervisor cannot leak them).

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
    rt.block_on(supervise(
        &exe,
        &argv,
        args.redis_url.as_deref(),
        resolved.procs,
    ))
}

async fn supervise(exe: &Path, argv: &[String], redis_url: Option<&str>, procs: usize) -> i32 {
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
    let now = Instant::now();
    let mut slots: Vec<Slot> = Vec::with_capacity(procs);
    for i in 0..procs {
        match spawn_child(exe, argv, redis_url) {
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
                        match spawn_child(exe, argv, redis_url) {
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
                // SAFETY: kill(2) takes plain integers and touches no memory.
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }
        }
    }
}

fn spawn_child(exe: &Path, argv: &[String], redis_url: Option<&str>) -> std::io::Result<Child> {
    let mut cmd = Command::new(exe);
    cmd.args(argv).kill_on_drop(false);
    // In the environment, not argv, so the userinfo stays out of `ps aux`.
    if let Some(url) = redis_url {
        cmd.env("CAULI_REDIS_URL", url);
    }
    // SAFETY: the closure runs between fork and exec and calls only the
    // async signal safe setpgid(2) and prctl(2); it allocates nothing.
    unsafe {
        cmd.pre_exec(|| {
            libc::setpgid(0, 0);
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
    cmd.spawn()
}

/// Every setting explicit, `--procs 1`, never `-c`: a child re-deriving with
/// its own procs divisor would compute different numbers.
fn child_argv(args: &cli::Args, r: &cli::Resolved) -> Vec<String> {
    let mut v: Vec<String> = vec![
        "--app".into(),
        args.app_spec().to_string(),
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
        "--cpu-max-tasks-per-child".into(),
        args.cpu_max_tasks_per_child.to_string(),
        "--batch".into(),
        args.batch.to_string(),
        "--visibility-timeout".into(),
        args.visibility_timeout.to_string(),
        "--max-envelope-bytes".into(),
        args.max_envelope_bytes.to_string(),
        "--drain-timeout".into(),
        args.drain_timeout.to_string(),
        "--stats-interval".into(),
        args.stats_interval.to_string(),
        "--log-level".into(),
        args.log_level.clone(),
        "--redis-timeout".into(),
        args.redis_timeout.to_string(),
        "--mover-interval".into(),
        args.mover_interval.to_string(),
        "--mover-limit".into(),
        args.mover_limit.to_string(),
        "--ack-batch".into(),
        args.ack_batch.to_string(),
        "--ack-flush-ms".into(),
        args.ack_flush_ms.to_string(),
    ];
    // Only when set: without it each child resolves the same embedded
    // interpreter, and forwarding "python3" would force a PATH lookup.
    if let Some(python) = &args.python {
        v.push("--python".into());
        v.push(python.clone());
    }
    if !args.queues.is_empty() {
        v.push("--queues".into());
        v.push(args.queues.join(","));
    }
    // No `--redis-url`: its userinfo would be world readable in
    // /proc/<pid>/cmdline. Children get it as CAULI_REDIS_URL (spawn_child),
    // which has the flag's precedence.
    if args.no_fork_server {
        v.push("--no-fork-server".into());
    }
    if args.eager_cpu {
        v.push("--eager-cpu".into());
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    /// Flags a child must not get. Any other flag not forwarded is silently
    /// ignored whenever `-c` turns the supervisor on.
    const NOT_FORWARDED: &[(&str, &str)] = &[
        ("help", "clap builtin"),
        ("version", "clap builtin"),
        (
            "concurrency",
            "the derivation already happened here; a child re-deriving with \
             its own procs divisor would compute different numbers",
        ),
        (
            "procs",
            "forwarded as the literal 1, never as the operator's value",
        ),
        (
            "print-plan",
            "prints and exits; a child would print a second plan and never serve",
        ),
        (
            "redis-url",
            "passed through the child's CAULI_REDIS_URL instead, so the \
             userinfo does not land in /proc/<pid>/cmdline (spawn_child)",
        ),
    ];

    /// Enumerated from clap's definition, so a new flag fails here until it
    /// is forwarded or listed in NOT_FORWARDED.
    #[test]
    fn child_argv_covers_every_runtime_flag() {
        let args = cli::Args::try_parse_from([
            "cauli-worker",
            "-A",
            "m:app",
            "-c",
            "500",
            "-Q",
            "high,low",
            "--redis-url",
            "redis://127.0.0.1:6392/0",
            "--python",
            "/opt/app/venv/bin/python3",
            "--no-fork-server",
            "--eager-cpu",
        ])
        .unwrap();
        let resolved = cli::resolve(&args, 8);
        let argv = child_argv(&args, &resolved);

        for arg in cli::Args::command().get_arguments() {
            let Some(long) = arg.get_long() else { continue };
            if NOT_FORWARDED.iter().any(|(name, _)| *name == long) {
                continue;
            }
            assert!(
                argv.iter().any(|a| a == &format!("--{long}")),
                "--{long} is never passed to a supervised child, so it is \
                 silently ignored whenever --procs > 1 (which -c enables on \
                 its own). Add it to child_argv, or to NOT_FORWARDED with the \
                 reason it does not apply to a child."
            );
        }
    }

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
        assert!(!v.contains(&"-c".to_string()));
        assert!(!v.contains(&"--concurrency".to_string()));
        let at = |flag: &str| v.iter().position(|s| s == flag).unwrap() + 1;
        assert_eq!(v[at("--procs")], "1");
        assert_eq!(v[at("--io-threads")], r.io_threads.to_string());
        assert_eq!(v[at("--io-concurrency")], r.io_concurrency.to_string());
        assert_eq!(v[at("--cpu-workers")], r.cpu_workers.to_string());
        assert_eq!(v[at("--queues")], "high,low");
        assert!(!v.contains(&"--redis-url".to_string()));
        assert!(!v.iter().any(|s| s.contains("127.0.0.1:6392")));
        assert!(v.contains(&"--no-fork-server".to_string()));
        let mut child_cmd = vec!["cauli-worker".to_string()];
        child_cmd.extend(v);
        let child = cli::Args::try_parse_from(&child_cmd).unwrap();
        assert_eq!(cli::resolve(&child, 6).procs, 1);
        assert_eq!(cli::resolve(&child, 6).io_threads, r.io_threads);
        assert_eq!(cli::resolve(&child, 6).io_concurrency, r.io_concurrency);
        assert_eq!(cli::resolve(&child, 6).cpu_workers, r.cpu_workers);
    }
}
