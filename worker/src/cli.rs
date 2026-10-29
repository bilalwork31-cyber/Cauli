use clap::Parser;

const ADVANCED: &str = "Advanced tuning (derived from -c; see docs/configuration.md)";

/// cauli-worker: Rust worker runtime for cauli Python task queues (PROTOCOL §7).
#[derive(Parser, Debug, Clone)]
#[command(
    name = "cauli-worker",
    version,
    about,
    after_help = "Every option, including tuning: cauli-worker --help"
)]
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
    /// The one knob most deployments need. Derives the process count and the
    /// tuning flags (see --help); a flag passed explicitly still wins.
    #[arg(short = 'c', long)]
    pub concurrency: Option<usize>,

    /// Worker processes, supervised by this binary (spawn, restart on death,
    /// signal fan-out). Default: 1, or with -c one process per ~64 slots up
    /// to all cores. -c is total and is divided across processes.
    #[arg(long, hide_short_help = true)]
    pub procs: Option<usize>,

    /// Print the derived execution plan (processes, threads, slots, cpu
    /// children) and exit without starting anything
    #[arg(long, default_value_t = false, hide_short_help = true)]
    pub print_plan: bool,

    /// Seconds before a dead worker's tasks are redelivered (PROTOCOL §4.4).
    /// At least 1.
    #[arg(long, default_value_t = 60, hide_short_help = true)]
    pub visibility_timeout: u64,

    /// Graceful shutdown drain timeout in seconds
    #[arg(long, default_value_t = 30, hide_short_help = true)]
    pub drain_timeout: u64,

    /// Seconds between stats log lines
    #[arg(long, default_value_t = 10, hide_short_help = true)]
    pub stats_interval: u64,

    /// Log level (trace|debug|info|warn|error); RUST_LOG overrides
    #[arg(short = 'l', long, default_value = "info")]
    pub log_level: String,

    /// Embedded asyncio event loop threads for async tasks. 1 won every
    /// measured sweep (extra loops contend for the one GIL); leave it alone
    #[arg(long, default_value_t = 1, help_heading = ADVANCED, hide_short_help = true)]
    pub io_loops: usize,

    /// Python thread pool size for sync io tasks.
    /// Default: 64, or derived from -c as min(c, 512)/procs
    #[arg(long, help_heading = ADVANCED, hide_short_help = true)]
    pub io_threads: Option<usize>,

    /// Max in flight io tasks (admission semaphore), sync and async together.
    /// Default: 256, or derived from -c as c/procs. Size the connection
    /// pools your tasks use to at least this value.
    #[arg(long, help_heading = ADVANCED, hide_short_help = true)]
    pub io_concurrency: Option<usize>,

    /// Child processes for cpu tasks. Default: cores/procs
    #[arg(long, help_heading = ADVANCED, hide_short_help = true)]
    pub cpu_workers: Option<usize>,

    /// Worker threads per cpu child (fork-server mode). M > 1 pipelines up
    /// to M requests per child; responses are matched by id (PROTOCOL §5.1).
    /// Must be within [1, 1024].
    #[arg(long, default_value_t = 1, help_heading = ADVANCED, hide_short_help = true)]
    pub cpu_child_threads: usize,

    /// Extra cpu requests staged in each child beyond the ones it is
    /// running, so a child never idles for an IPC round trip. 0 disables.
    /// Raise it for sub millisecond tasks; keep it low for long ones: a child
    /// death fails everything staged behind it as retryable WorkerLost, and a
    /// staged task waits out the tasks ahead of it.
    #[arg(long, default_value_t = 4, help_heading = ADVANCED, hide_short_help = true)]
    pub cpu_prefetch: usize,

    /// Recycle a cpu child after it completes this many tasks, like Celery's
    /// maxtasksperchild. 0 never recycles. Staged work drains first, so no
    /// task is lost to a recycle
    #[arg(long, default_value_t = 10000, help_heading = ADVANCED, hide_short_help = true)]
    pub cpu_max_tasks_per_child: usize,

    /// Start the cpu pool at boot instead of on the first cpu task. Costs
    /// resident children immediately; buys the first cpu task a warm start
    #[arg(long, default_value_t = false, help_heading = ADVANCED, hide_short_help = true)]
    pub eager_cpu: bool,

    /// Disable the fork-server cpu child model: spawn each child directly
    /// over stdio, one request in flight per child (PROTOCOL §5.1 fallback
    /// mode). Also entered automatically if fork-server startup fails
    #[arg(long, default_value_t = false, help_heading = ADVANCED, hide_short_help = true)]
    pub no_fork_server: bool,

    /// XREADGROUP COUNT per fetch, at least 1; a fetch never takes more than
    /// the free io slots. Raise it only for sub millisecond tasks: fetched cpu
    /// entries wait for a cpu slot inside the worker with their recovery clock
    /// running.
    #[arg(long, default_value_t = 64, help_heading = ADVANCED, hide_short_help = true)]
    pub batch: usize,

    /// Max accepted envelope size in bytes. Larger entries are dead lettered
    /// as "malformed" before parsing. Keep it equal to the app's
    /// max_envelope_bytes.
    #[arg(long, default_value_t = 1_048_576, help_heading = ADVANCED, hide_short_help = true)]
    pub max_envelope_bytes: usize,

    /// Python executable used to spawn cpu children.
    /// Default: this worker's own embedded interpreter (`sys.executable`)
    #[arg(long, help_heading = ADVANCED, hide_short_help = true)]
    pub python: Option<String>,

    /// Connection and response timeout, in seconds, for every redis call.
    /// A timed out call is handled like any redis error: the task is
    /// redelivered, never lost. Keep it between 1 and half of
    /// --visibility-timeout.
    #[arg(long, default_value_t = 5, help_heading = ADVANCED, hide_short_help = true)]
    pub redis_timeout: u64,

    /// Delayed and retry sweep interval in milliseconds (PROTOCOL §4.3).
    /// Countdowns, etas, retries and beat firings can start up to this late
    #[arg(long, default_value_t = 50, help_heading = ADVANCED, hide_short_help = true)]
    pub mover_interval: u64,

    /// Entries the delayed and retry sweep moves per queue per round trip.
    /// The sweep repeats until a queue is drained, so this does not cap the
    /// rate.
    #[arg(long, default_value_t = 128, help_heading = ADVANCED, hide_short_help = true)]
    pub mover_limit: usize,

    /// Completions buffered per queue before one XACK and XDEL flush
    /// (PROTOCOL §4.1). Values below 1 mean no batching.
    #[arg(long, default_value_t = 64, help_heading = ADVANCED, hide_short_help = true)]
    pub ack_batch: usize,

    /// Max milliseconds the oldest buffered ack waits before a partial
    /// flush. A crash inside this window redelivers, and reruns, at most one
    /// buffer per queue. 0 flushes every completion immediately.
    #[arg(long, default_value_t = 2, help_heading = ADVANCED, hide_short_help = true)]
    pub ack_flush_ms: u64,
}

/// Per process settings after the -c/--procs derivation, resolved once and
/// passed to children explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved {
    pub procs: usize,
    pub io_threads: usize,
    pub io_concurrency: usize,
    pub cpu_workers: usize,
}

/// One process per this many slots, capped at the cores: small -c on a
/// shared box stays one process, large -c fans out one GIL per core. 64 is a
/// chosen target, not a swept one.
const SLOTS_PER_PROC: usize = 64;

/// Derivation (docs/configuration.md). An explicit flag always wins.
/// - procs: min(cores, c/SLOTS_PER_PROC) with -c, else 1.
/// - io_concurrency: c/procs with -c, else 256.
/// - io_threads: min(c, 512)/procs with -c, capped at the gate (a thread
///   above it never gets work), else 64. Past about 1000 threads per process
///   sync throughput and latency both fall.
/// - cpu_workers: cores/procs, never more than c/procs.
///
/// Every division floors: `-c` is what operators size a database pool
/// against, so the totals must never exceed it (or the cores, for cpu).
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
    /// Only called after the --print-plan early return, where clap has made
    /// -A mandatory.
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

    /// 0 is the documented opt out of recycling.
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

    /// docs/configuration.md states --print-plan needs no app and no Redis.
    #[test]
    fn print_plan_alone_needs_no_app() {
        let a = Args::try_parse_from(["cauli-worker", "--print-plan"])
            .expect("--print-plan must parse without -A");
        assert!(a.print_plan);
        assert_eq!(a.app, None);
    }

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
        // A shared box at -c 50 must not fan out across the machine.
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
                io_concurrency: 62, // 16 x 62 = 992, never above 1000
                cpu_workers: 2,
            }
        );
    }

    /// Swept, not sampled: rounding up breaks these at points like `-c 65`
    /// and `-c 200` on 6 cores.
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
