use clap::Parser;

const ADVANCED: &str = "Advanced tuning (derived from -c; see docs/CONFIGURATION.md)";

/// cauli-worker: Rust worker runtime for cauli Python task queues (PROTOCOL §7).
#[derive(Parser, Debug, Clone)]
#[command(name = "cauli-worker", version, about)]
pub struct Args {
    /// App location as module:attr (e.g. myproj.tasks:app). Not needed with
    /// --print-plan.
    #[arg(short = 'A', long, required_unless_present = "print_plan")]
    pub app: Option<String>,

    /// Comma separated queue names. Default: app.default_queue
    #[arg(short = 'Q', long, value_delimiter = ',')]
    pub queues: Vec<String>,

    /// Redis URL. Precedence: CLI > env CAULI_REDIS_URL > app.redis_url
    #[arg(long)]
    pub redis_url: Option<String>,

    /// Total concurrency: max tasks in flight across all worker processes.
    /// The one knob most deployments need (like celery -c). Turns --procs
    /// auto and derives the advanced flags below; any flag passed explicitly
    /// still wins. Unset, the worker keeps its standalone defaults.
    #[arg(short = 'c', long)]
    pub concurrency: Option<usize>,

    /// Worker processes, supervised by this binary (spawn, restart on death,
    /// signal fan-out). Default: 1, or with -c one process per ~64 slots up
    /// to all cores. -c is total and is divided across processes.
    #[arg(long)]
    pub procs: Option<usize>,

    /// Print the derived execution plan (processes, threads, slots, cpu
    /// children) and exit without starting anything
    #[arg(long, default_value_t = false)]
    pub print_plan: bool,

    /// Seconds before a dead worker's tasks are redelivered (PROTOCOL §4.4).
    /// At least 1.
    #[arg(long, default_value_t = 60)]
    pub visibility_timeout: u64,

    /// Graceful shutdown drain timeout in seconds
    #[arg(long, default_value_t = 30)]
    pub drain_timeout: u64,

    /// Seconds between stats log lines
    #[arg(long, default_value_t = 10)]
    pub stats_interval: u64,

    /// Log level (trace|debug|info|warn|error); RUST_LOG overrides
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Embedded asyncio event loop threads for async tasks. 1 won every
    /// measured sweep (extra loops contend for the one GIL); leave it alone
    #[arg(long, default_value_t = 1, help_heading = ADVANCED)]
    pub io_loops: usize,

    /// Python thread pool size for sync io tasks.
    /// Default: 64, or derived from -c as min(c, 512)/procs
    #[arg(long, help_heading = ADVANCED)]
    pub io_threads: Option<usize>,

    /// Max in flight io tasks (admission semaphore), sync and async together.
    /// Default: 256, or derived from -c as c/procs. Size the connection
    /// pools your tasks use to at least this value.
    #[arg(long, help_heading = ADVANCED)]
    pub io_concurrency: Option<usize>,

    /// Child processes for cpu tasks. Default: cores/procs
    #[arg(long, help_heading = ADVANCED)]
    pub cpu_workers: Option<usize>,

    /// Worker threads per cpu child (fork-server mode). M > 1 pipelines up
    /// to M requests per child; responses are matched by id (PROTOCOL §5.1).
    /// Must be within [1, 1024].
    #[arg(long, default_value_t = 1, help_heading = ADVANCED)]
    pub cpu_child_threads: usize,

    /// Extra cpu requests pre-staged in each child's socket buffer beyond the
    /// ones it is executing. Keeps a child from idling for a full IPC round
    /// trip between tasks; its next read returns immediately. 0 disables.
    ///
    /// Measured drain rate, 6 children on 6 cores: for ~0.5ms tasks depth 64
    /// is 4.1x depth 0; for ~2ms tasks depth 16 is 1.13x depth 3; for ~51ms
    /// tasks every depth is within noise (the task dwarfs the round trip).
    /// Deeper is not free: a child death fails everything staged behind it as
    /// retryable WorkerLost, and a staged task waits out the tasks ahead of
    /// it, so raise this for small tasks and leave it low for long ones.
    #[arg(long, default_value_t = 4, help_heading = ADVANCED)]
    pub cpu_prefetch: usize,

    /// Recycle a cpu child after it completes this many tasks, like Celery's
    /// maxtasksperchild. 0 never recycles. Staged work drains first, so no
    /// task is lost to a recycle
    #[arg(long, default_value_t = 10000, help_heading = ADVANCED)]
    pub cpu_max_tasks_per_child: usize,

    /// Start the cpu pool at boot instead of on the first cpu task. Costs
    /// resident children immediately; buys the first cpu task a warm start
    #[arg(long, default_value_t = false, help_heading = ADVANCED)]
    pub eager_cpu: bool,

    /// Disable the fork-server cpu child model: spawn each child directly
    /// over stdio, one request in flight per child (PROTOCOL §5.1 fallback
    /// mode). Also entered automatically if fork-server startup fails
    #[arg(long, default_value_t = false, help_heading = ADVANCED)]
    pub no_fork_server: bool,

    /// XREADGROUP COUNT per fetch. Must be at least 1. A fetch never takes
    /// more than the free io slots. For sub millisecond tasks a larger batch
    /// drains faster (noop tasks, one process: 128 was about 1.35x sync and
    /// 1.10x async; 256 with --cpu-prefetch 16 took noop cpu tasks from 85k/s
    /// to 134k/s), but fetched cpu entries wait for a cpu slot inside the
    /// worker with their recovery clock running, so raise it only for short
    /// tasks.
    #[arg(long, default_value_t = 64, help_heading = ADVANCED)]
    pub batch: usize,

    /// Max accepted envelope size in bytes. Larger entries are dead lettered
    /// as "malformed" before parsing. Keep it equal to the app's
    /// max_envelope_bytes.
    #[arg(long, default_value_t = 1_048_576, help_heading = ADVANCED)]
    pub max_envelope_bytes: usize,

    /// Python executable used to spawn cpu children.
    /// Default: this worker's own embedded interpreter (`sys.executable`)
    #[arg(long, help_heading = ADVANCED)]
    pub python: Option<String>,

    /// Connection and response timeout, in seconds, for every redis call.
    /// A timed out call is handled like any redis error: the task is
    /// redelivered, never lost. Keep it between 1 and half of
    /// --visibility-timeout.
    #[arg(long, default_value_t = 5, help_heading = ADVANCED)]
    pub redis_timeout: u64,

    /// Delayed and retry sweep interval in milliseconds (PROTOCOL §4.3).
    /// Countdowns, etas, retries and beat firings can start up to this late
    #[arg(long, default_value_t = 50, help_heading = ADVANCED)]
    pub mover_interval: u64,

    /// Entries the delayed and retry sweep moves per queue per round trip.
    /// The sweep repeats until a queue is drained, so this does not cap the
    /// rate.
    #[arg(long, default_value_t = 128, help_heading = ADVANCED)]
    pub mover_limit: usize,

    /// Completions buffered per queue before one XACK and XDEL flush
    /// (PROTOCOL §4.1). Values below 1 mean no batching.
    #[arg(long, default_value_t = 64, help_heading = ADVANCED)]
    pub ack_batch: usize,

    /// Max milliseconds the oldest buffered ack waits before a partial
    /// flush. A crash inside this window redelivers, and reruns, at most one
    /// buffer per queue. 0 flushes every completion immediately.
    #[arg(long, default_value_t = 2, help_heading = ADVANCED)]
    pub ack_flush_ms: u64,
}

/// Concrete per-process execution settings after applying the -c/--procs
/// derivation. One derivation site: the process that resolves this (the
/// supervisor, or a standalone worker) passes the values on explicitly, so a
/// supervised child never re-derives with a different procs divisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved {
    pub procs: usize,
    pub io_threads: usize,
    pub io_concurrency: usize,
    pub cpu_workers: usize,
}

/// Auto procs target: one worker process per this many concurrency slots,
/// never more than the cores. Small -c on a shared box (the Django + Redis +
/// worker colocation everyone actually runs) stays a single process; a large
/// -c on a dedicated box fans across every core, one GIL each (bench3 on 4
/// pinned cores: 1→4 procs was +74% throughput at lower p99). The 64 is a
/// defensible target, not yet a swept one — pin it when a wider box exists.
const SLOTS_PER_PROC: usize = 64;

/// Derivation rules (thresholds measured, bench2/bench3; docs/CONFIGURATION.md):
/// - procs: explicit, else min(cores, c/SLOTS_PER_PROC) with -c, else 1
///   (standalone behavior unchanged).
///
/// Every per-process division below FLOORS. `-c` is a ceiling on what the
/// whole worker will hold, not a per-process target: each supervised child
/// receives the same resolved flags, so a remainder cannot be handed to one
/// child alone, and rounding each child up overshot the number the operator
/// asked for -- `-c 65` derived 2 x 33 = 66, `-c 512` derived 6 x 86 = 516.
/// Overshoot is the dangerous direction: `-c` is what an operator sizes a
/// database connection pool against, so exceeding it can breach a limit that
/// lives outside this process. Undershooting by less than one slot per
/// process only leaves a little capacity on the table.
///
/// - io_concurrency: explicit, else c/procs with -c, else 256. The gate is
///   the bound for async tasks (a slot costs ~4 KB). The standalone 256 sits
///   above every band this repo has measured; the flag's own help carries the
///   numbers and the unresolved stall, so `--help` warns before a user tunes
///   past them.
/// - io_threads: explicit, else min(c, 512)/procs with -c, else 64. Capped at
///   the gate (a thread above it never receives work) and at 512 total: the
///   sync knee is ~1000 threads/proc and past it throughput and latency fall
///   together, so the derived default stays 1x the gate up to the cap rather
///   than oversubscribing (oversubscription trades task p99 for throughput —
///   an explicit choice, not a default).
/// - cpu_workers: explicit, else cores/procs but never more than -c's share:
///   more children than cores buys nothing, and more than c would make
///   `-c 8` on a pdf-convert queue mean something other than 8. Floored for
///   the same reason as the io lanes, and here the old div_ceil broke the
///   stated rule outright: at `-c 200` on 6 cores it derived 4 procs x 2
///   children = 8 children for 6 cores, the exact oversubscription the rule
///   exists to prevent.
pub fn resolve(args: &Args, cores: usize) -> Resolved {
    let cores = cores.max(1);
    let procs = args
        .procs
        .unwrap_or_else(|| match args.concurrency {
            Some(c) => cores.min(c.max(1).div_ceil(SLOTS_PER_PROC)),
            None => 1,
        })
        .max(1);
    let (io_threads, io_concurrency) = match args.concurrency {
        Some(c) => {
            let c = c.max(1);
            let gate = args.io_concurrency.unwrap_or(c / procs).max(1);
            let threads = args
                .io_threads
                .unwrap_or_else(|| (c.min(512) / procs).min(gate))
                .max(1);
            (threads, gate)
        }
        None => (
            args.io_threads.unwrap_or(64).max(1),
            args.io_concurrency.unwrap_or(256).max(1),
        ),
    };
    let cpu_workers = args
        .cpu_workers
        .unwrap_or_else(|| {
            let per_proc = cores / procs;
            match args.concurrency {
                Some(c) => per_proc.min(c.max(1) / procs),
                None => per_proc,
            }
        })
        .max(1);
    Resolved {
        procs,
        io_threads,
        io_concurrency,
        cpu_workers,
    }
}

impl Args {
    /// The app spec, for every consumer downstream of the --print-plan early
    /// return in main. clap keeps -A mandatory unless --print-plan is set,
    /// and that path exits before any caller here runs, so None at this
    /// point is a control flow bug rather than bad input.
    pub fn app_spec(&self) -> &str {
        self.app
            .as_deref()
            .expect("-A is required unless --print-plan, which exits earlier")
    }
}

pub fn valid_queue_name(q: &str) -> bool {
    !q.is_empty()
        && q.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(argv: &[&str]) -> Args {
        let mut full = vec!["cauli-worker", "--app", "m.tasks:app"];
        full.extend_from_slice(argv);
        Args::try_parse_from(full).unwrap()
    }

    #[test]
    fn parses_defaults() {
        let a = parse(&[]);
        assert_eq!(a.app.as_deref(), Some("m.tasks:app"));
        assert!(a.queues.is_empty());
        assert_eq!(a.redis_url, None);
        assert_eq!(a.concurrency, None);
        assert_eq!(a.procs, None);
        assert_eq!(a.io_loops, 1);
        assert_eq!(a.io_threads, None);
        assert_eq!(a.io_concurrency, None);
        assert_eq!(a.cpu_workers, None);
        assert_eq!(a.cpu_child_threads, 1);
        assert_eq!(a.cpu_max_tasks_per_child, 10000);
        assert!(!a.eager_cpu);
        assert!(!a.print_plan);
        assert!(!a.no_fork_server);
        assert_eq!(a.batch, 64);
        assert_eq!(a.visibility_timeout, 60);
        assert_eq!(a.max_envelope_bytes, 1_048_576);
        assert_eq!(a.drain_timeout, 30);
        assert_eq!(a.python, None);
        assert_eq!(a.mover_interval, 50);
        assert_eq!(a.mover_limit, 128);
        assert_eq!(a.stats_interval, 10);
        assert_eq!(a.log_level, "info");
        assert_eq!(a.redis_timeout, 5);
        assert_eq!(a.ack_batch, 64);
        assert_eq!(a.ack_flush_ms, 2);
    }

    /// Behaviour change: cpu children recycle by default now. 0 has to stay
    /// accepted, because it is the documented way to opt back out.
    #[test]
    fn cpu_recycle_defaults_to_10000_with_zero_as_the_opt_out() {
        assert_eq!(parse(&[]).cpu_max_tasks_per_child, 10_000);
        assert_eq!(
            parse(&["--cpu-max-tasks-per-child", "0"]).cpu_max_tasks_per_child,
            0
        );
        assert_eq!(
            parse(&["--cpu-max-tasks-per-child", "7"]).cpu_max_tasks_per_child,
            7
        );
    }

    #[test]
    fn parses_overrides_and_queue_list() {
        let a = Args::try_parse_from([
            "cauli-worker",
            "--app",
            "x:y",
            "--queues",
            "default,emails,bulk-2",
            "--redis-url",
            "redis://127.0.0.1:6392/0",
            "--io-loops",
            "2",
            "--io-threads",
            "8",
            "--io-concurrency",
            "32",
            "--cpu-workers",
            "3",
            "--batch",
            "4",
            "--visibility-timeout",
            "2",
            "--drain-timeout",
            "5",
            "--python",
            "python3.12",
            "--stats-interval",
            "1",
            "--log-level",
            "debug",
            "--redis-timeout",
            "7",
            "--ack-batch",
            "16",
            "--ack-flush-ms",
            "1",
        ])
        .unwrap();
        assert_eq!(a.queues, vec!["default", "emails", "bulk-2"]);
        assert_eq!(a.redis_url.as_deref(), Some("redis://127.0.0.1:6392/0"));
        assert_eq!(a.io_loops, 2);
        assert_eq!(a.io_threads, Some(8));
        assert_eq!(a.io_concurrency, Some(32));
        assert_eq!(a.cpu_workers, Some(3));
        assert_eq!(a.batch, 4);
        assert_eq!(a.visibility_timeout, 2);
        assert_eq!(a.drain_timeout, 5);
        assert_eq!(a.python.as_deref(), Some("python3.12"));
        assert_eq!(a.stats_interval, 1);
        assert_eq!(a.log_level, "debug");
        assert_eq!(a.redis_timeout, 7);
        assert_eq!(a.ack_batch, 16);
        assert_eq!(a.ack_flush_ms, 1);
    }

    /// docs/CONFIGURATION.md states --print-plan needs no app and no Redis.
    /// clap made -A unconditionally required, so the documented invocation
    /// exited 2 on a usage error instead of printing the plan.
    #[test]
    fn print_plan_alone_needs_no_app() {
        let a = Args::try_parse_from(["cauli-worker", "--print-plan"])
            .expect("--print-plan must parse without -A");
        assert!(a.print_plan);
        assert_eq!(a.app, None);
    }

    /// The other half of the same contract: -A stays mandatory for every
    /// invocation that will actually import an app and connect to Redis.
    #[test]
    fn app_is_still_required_without_print_plan() {
        assert!(Args::try_parse_from(["cauli-worker", "-c", "50"]).is_err());
    }

    #[test]
    fn short_flags_match_celery_muscle_memory() {
        let a = Args::try_parse_from(["cauli-worker", "-A", "m:app", "-c", "50", "-Q", "high,low"])
            .unwrap();
        assert_eq!(a.app.as_deref(), Some("m:app"));
        assert_eq!(a.concurrency, Some(50));
        assert_eq!(a.queues, vec!["high", "low"]);
    }

    #[test]
    fn missing_app_is_error() {
        assert!(Args::try_parse_from(["cauli-worker"]).is_err());
    }

    #[test]
    fn resolve_without_c_keeps_standalone_defaults() {
        let r = resolve(&parse(&[]), 6);
        assert_eq!(
            r,
            Resolved {
                procs: 1,
                io_threads: 64,
                io_concurrency: 256,
                cpu_workers: 6,
            }
        );
    }

    #[test]
    fn resolve_small_c_stays_one_proc() {
        // The shared box case (Django + Redis + worker colocated): a queue
        // worker at -c 50 must not fan out across the machine.
        let r = resolve(&parse(&["-c", "50"]), 6);
        assert_eq!(
            r,
            Resolved {
                procs: 1,
                io_threads: 50,
                io_concurrency: 50,
                cpu_workers: 6,
            }
        );
    }

    #[test]
    fn resolve_large_c_uses_every_core() {
        let r = resolve(&parse(&["-c", "4000"]), 6);
        assert_eq!(r.procs, 6);
        assert_eq!(r.io_concurrency, 666); // 6 x 666 = 3996, never above 4000
        assert_eq!(r.io_threads, 85); // 512/6, not 666: sync knee guard
        assert_eq!(r.cpu_workers, 1);
    }

    #[test]
    fn resolve_big_box_fans_wide() {
        let r = resolve(&parse(&["-c", "1000"]), 32);
        assert_eq!(
            r,
            Resolved {
                procs: 16, // 1000 / SLOTS_PER_PROC, under the 32 cores
                io_threads: 32,
                io_concurrency: 62, // 16 x 62 = 992; ceiling gave 1008 > 1000
                cpu_workers: 2,
            }
        );
    }

    /// The two invariants the per-process rounding exists to hold, swept
    /// rather than sampled. Both were violated by the old div_ceil rounding
    /// at points no single-value test happened to cover: `-c 65` on 6 cores
    /// derived 2 x 33 = 66 slots for a 65 slot budget, and `-c 200` derived
    /// 4 procs x 2 = 8 cpu children onto 6 cores.
    #[test]
    fn resolved_totals_never_exceed_c_or_the_cores() {
        for cores in [1usize, 2, 4, 6, 8, 16, 32, 64] {
            for c in [
                1usize, 2, 3, 5, 7, 8, 15, 16, 31, 32, 63, 64, 65, 100, 128, 129, 200, 255, 256,
                500, 512, 513, 1000, 1024, 4000, 10_000,
            ] {
                let r = resolve(&parse(&["-c", &c.to_string()]), cores);
                let slots = r.io_concurrency * r.procs;
                assert!(
                    slots <= c.max(r.procs),
                    "-c {c} on {cores} cores derived {} procs x {} slots = {slots}",
                    r.procs,
                    r.io_concurrency
                );
                let children = r.cpu_workers * r.procs;
                assert!(
                    children <= cores.max(r.procs),
                    "-c {c} on {cores} cores derived {} procs x {} cpu children \
                     = {children}",
                    r.procs,
                    r.cpu_workers
                );
            }
        }
    }

    #[test]
    fn resolve_c_caps_cpu_children() {
        // "-c 8 on a pdf-convert queue" must mean 8, even for the cpu lane.
        let r = resolve(&parse(&["-c", "8"]), 32);
        assert_eq!(r.procs, 1);
        assert_eq!(r.cpu_workers, 8);
        assert_eq!(r.io_concurrency, 8);
    }

    #[test]
    fn resolve_single_proc_ladder_shape() {
        let r = resolve(&parse(&["-c", "4000", "--procs", "1"]), 6);
        assert_eq!(
            r,
            Resolved {
                procs: 1,
                io_threads: 512,
                io_concurrency: 4000,
                cpu_workers: 6,
            }
        );
    }

    #[test]
    fn resolve_tiny_c_is_tiny_everywhere() {
        let r = resolve(&parse(&["-c", "2"]), 6);
        assert_eq!(r.procs, 1);
        assert_eq!(r.io_concurrency, 2);
        assert_eq!(r.io_threads, 2);
        assert_eq!(r.cpu_workers, 2);
    }

    #[test]
    fn resolve_explicit_flags_beat_derivation() {
        let r = resolve(
            &parse(&[
                "-c",
                "50",
                "--procs",
                "2",
                "--io-threads",
                "7",
                "--io-concurrency",
                "9",
                "--cpu-workers",
                "3",
            ]),
            6,
        );
        assert_eq!(
            r,
            Resolved {
                procs: 2,
                io_threads: 7,
                io_concurrency: 9,
                cpu_workers: 3,
            }
        );
    }

    #[test]
    fn resolve_explicit_gate_caps_derived_threads() {
        let r = resolve(
            &parse(&["-c", "100", "--procs", "1", "--io-concurrency", "8"]),
            6,
        );
        assert_eq!(r.io_concurrency, 8);
        assert_eq!(r.io_threads, 8);
    }

    #[test]
    fn resolve_procs_without_c_divides_cpu_workers_only() {
        let r = resolve(&parse(&["--procs", "3"]), 6);
        assert_eq!(
            r,
            Resolved {
                procs: 3,
                io_threads: 64,
                io_concurrency: 256,
                cpu_workers: 2,
            }
        );
    }

    #[test]
    fn queue_name_validation() {
        assert!(valid_queue_name("default"));
        assert!(valid_queue_name("a.b-c_9"));
        assert!(!valid_queue_name(""));
        assert!(!valid_queue_name("bad queue"));
        assert!(!valid_queue_name("q:colon"));
    }
}
