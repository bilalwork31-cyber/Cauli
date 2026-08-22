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
    // Out of argv and into the environment: same value, same precedence in
    // main.rs, but not visible in `ps aux` (see `child_argv`).
    if let Some(url) = redis_url {
        cmd.env("CAULI_REDIS_URL", url);
    }
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
        // Was missing: `-c` alone turns the supervisor on whenever
        // `c / 64 > 1`, so an operator who raised --redis-timeout after an
        // incident on a noisy redis silently got the clap default of 5 in
        // every child, with nothing logged. `child_argv_covers_every_runtime_flag`
        // below is the guard against the next flag repeating this.
        "--redis-timeout".into(),
        args.redis_timeout.to_string(),
        "--mover-interval".into(),
        args.mover_interval.to_string(),
        "--mover-limit".into(),
        args.mover_limit.to_string(),
    ];
    // Only when the operator set one: with no flag each child resolves its
    // own embedded interpreter, which is the same binary and so the same
    // path, and forwarding a literal "python3" here would reinstate exactly
    // the PATH lookup the new default exists to avoid.
    if let Some(python) = &args.python {
        v.push("--python".into());
        v.push(python.clone());
    }
    if !args.queues.is_empty() {
        v.push("--queues".into());
        v.push(args.queues.join(","));
    }
    // `--redis-url` is deliberately NOT here: it carries userinfo, and argv
    // is world readable through `/proc/<pid>/cmdline` and `ps aux`. The
    // worker redacts the same URL in every log path, the python client
    // redacts it in `repr`, and pyrt refuses to interpolate it into a
    // startup error, so republishing it in plaintext once per child was the
    // one place it escaped. It reaches children through the child's own
    // `CAULI_REDIS_URL` instead (see `spawn_child`), which main.rs already
    // consults with exactly the precedence the flag had.
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

    /// Long flag names that legitimately do NOT belong on a child command
    /// line, each with the reason it is excluded. Everything else must be
    /// forwarded, or the flag is silently ignored whenever the supervisor is
    /// active -- which `-c` turns on without the operator asking for it.
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

    /// Every `Args` field with a runtime effect has to appear in
    /// `child_argv`, or it is silently dropped for every supervised worker.
    /// Enumerated from clap's own definition rather than a hand written list,
    /// so a newly added flag fails this test until it is either forwarded or
    /// explicitly excluded with a reason.
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
        // Credentials never reach argv; the URL travels in the child's env.
        assert!(!v.contains(&"--redis-url".to_string()));
        assert!(!v.iter().any(|s| s.contains("127.0.0.1:6392")));
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
