use clap::Parser;

const ADVANCED: &str = "Advanced tuning (derived from -c; see docs/CONFIGURATION.md)";

/// cauli-worker: Rust worker runtime for cauli Python task queues (PROTOCOL §7).
#[derive(Parser, Debug, Clone)]
#[command(name = "cauli-worker", version, about)]
pub struct Args {
    /// App location as module:attr (e.g. myproj.tasks:app)
    ///
    /// Optional for --print-plan alone. That flag derives the plan from -c
    /// and the core count with no interpreter and no Redis, which is the
    /// contract docs/CONFIGURATION.md states, so demanding an importable app
    /// there would make it unusable for capacity planning off the host.
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

    /// Visibility timeout in seconds (crash recovery, PROTOCOL §4.4). Must be
    /// at least 1 (0 would make the recovery loop reclaim every
    /// currently-executing task on nearly every tick, audit M8) and must
    /// exceed your longest task timeout; enforced/warned in main.rs.
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
    /// Default: 256, or derived from -c as c/procs
    ///
    /// Not a free knob, and the measured band ends below the default. The
    /// single process async sweep (docs/AUDIT_LOG.md) has the gate binding
    /// up to about 64 per process and flat past it: 64 gave 25.3k tps, 128
    /// gave 27.2k, about 7% for twice the slots. The `--procs 6` sweep in
    /// bench/RESULTS.md peaked at 96 per process and then saw runs finish
    /// 91% to 99% and stall above 104, completing 11% at 512. That cliff is
    /// unresolved: the harness opened one redis connection per slot, so it
    /// may have measured its own connection count rather than cauli. Until
    /// it is re run with connections pinned, treat anything above 128 per
    /// process as untested rather than supported, and size your database
    /// pool for the gate (docs/CONFIGURATION.md, connection sizing).
    #[arg(long, help_heading = ADVANCED)]
    pub io_concurrency: Option<usize>,

    /// Child processes for cpu tasks. Default: cores/procs
    #[arg(long, help_heading = ADVANCED)]
    pub cpu_workers: Option<usize>,

    /// Worker threads per cpu child (fork-server mode). M > 1 pipelines up
    /// to M requests per child; responses are matched by id (PROTOCOL §5.1).
    /// Must be within [1, 1024] (FS-10 — enforced in main.rs after parsing).
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

    /// Recycle a cpu child after it completes this many tasks. THE DEFAULT
    /// IS 10000: children are recycled unless you say otherwise. Pass 0 to opt
    /// out and let a child live for the whole worker lifetime. This is the
    /// backstop for leaky C extensions and slowly dirtied copy on write
    /// pages, like Celery's maxtasksperchild, and nothing else in the worker
    /// bounds cpu child memory. Staged prefetch work always drains before the
    /// recycle fires, so no task is lost to it.
    ///
    /// It is a leak backstop, not a copy on write one: measured child private
    /// RSS plateaus at 2416kB by 5000 tasks and moves 4kB more out to 20000,
    /// so the old default of 1000 was re-forking mid ramp for no memory
    /// benefit while throwing away a warm child
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

    /// XREADGROUP COUNT per fetch. Must be >= 1: 0 would mean "unlimited" to
    /// Redis (audit M8); enforced in main.rs after parsing (exit 1).
    #[arg(long, default_value_t = 16, help_heading = ADVANCED)]
    pub batch: usize,

    /// Max accepted envelope size in bytes; oversize entries are DLQ'd as
    /// "malformed" before parsing (audit M2 — bounds the json::Value memory
    /// amplification and processing cost of an oversized/hostile payload).
    #[arg(long, default_value_t = 1_048_576, help_heading = ADVANCED)]
    pub max_envelope_bytes: usize,

    /// Python executable used to spawn cpu children.
    /// Default: this worker's own embedded interpreter (`sys.executable`)
    ///
    /// The old default was the bare string "python3", resolved through PATH
    /// by `Command::new`. A systemd `ExecStart=/opt/app/venv/bin/cauli-worker`
    /// or a Docker CMD never activates the venv, so it found the base
    /// interpreter with no `cauli` package installed: the fork server failed,
    /// the stdio fallback respawn looped, the cpu channel filled, and the
    /// fetch loop's cpu backlog gate then stopped fetching io work too. One
    /// misrouted interpreter took the whole worker offline at warn level.
    /// The embedded interpreter is by construction the one that imported the
    /// app, so it is the right default.
    #[arg(long, help_heading = ADVANCED)]
    pub python: Option<String>,

    /// Response and connection timeout, in seconds, for every redis round
    /// trip: fetch (XREADGROUP), the idempotency claim before a task body
    /// runs, the delayed mover, crash recovery, and task result writes.
    /// Unset in the underlying client, a redis that accepts the TCP
    /// connection but never answers (paused, swapping, or a network
    /// partition dropping packets rather than refusing them) hangs these
    /// calls forever. BLOCK on XREADGROUP is a server side wait, not a
    /// client side deadline, and does not substitute for this.
    ///
    /// This is new configuration surface, deliberately: the right value
    /// depends on this deployment's redis tail latency, which cannot be
    /// known in advance, so one hardcoded number would be wrong for either
    /// a shared noisy redis or a dedicated local one. Below roughly 1
    /// second, ordinary fork, fsync and network jitter risk a false trip
    /// (the delayed mover's Lua script alone can touch up to 128 items in
    /// one round trip). Past roughly half of visibility_timeout, a slow but
    /// genuinely alive redis is not caught meaningfully sooner than doing
    /// nothing.
    ///
    /// A trip is never a new failure mode: every affected call site already
    /// has a tested fallback for a redis error (finish() leaves the entry
    /// unacked for XCLAIM to redeliver; idemp_claim fails open and executes
    /// anyway), so this only reaches an existing safe outcome sooner, at the
    /// cost of one log line and at most one visibility timeout of added
    /// latency on that task. Never data loss.
    #[arg(long, default_value_t = 5, help_heading = ADVANCED)]
    pub redis_timeout: u64,

    /// Delayed and retry sweep interval in milliseconds (PROTOCOL §4.3).
    /// Every countdown, eta, retry and beat firing waits out at most one of
    /// these before it can run, so the tick is a floor on retry latency: at
    /// 250ms it added up to 250ms to a default first retry of 250-500ms, a
    /// +50% error on the backoff curve the user asked for
    #[arg(long, default_value_t = 50, help_heading = ADVANCED)]
    pub mover_interval: u64,

    /// Entries the delayed/retry sweep moves per queue per EVAL. The sweep
    /// now repeats within one tick until a queue comes back short, so this
    /// bounds one round trip, not the drain rate. It used to be a hard,
    /// unflaggable ceiling of `limit * (1000 / interval)` per queue per
    /// process -- 512 per second on the defaults, 40x below the headline
    /// throughput, with every retry, countdown and eta passing through it.
    #[arg(long, default_value_t = 128, help_heading = ADVANCED)]
    pub mover_limit: usize,

    /// Completions buffered per queue before they are flushed as ONE
    /// pipeline with ONE multi-id XACK and ONE multi-id XDEL (PROTOCOL
    /// §4.1). Batching acks is what keeps redis's single command-execution
    /// thread from being the throughput ceiling; 64 was measured as past
    /// the knee of that curve. Values below 1 are treated as 1 (no
    /// batching).
    #[arg(long, default_value_t = 64, help_heading = ADVANCED)]
    pub ack_batch: usize,

    /// Max milliseconds a completed task's ack may wait in the buffer
    /// before a partial batch is flushed anyway, measured from the OLDEST
    /// buffered completion. This bounds both result latency at low load and
    /// the §4.1 crash duplicate window (a worker killed mid-window loses at
    /// most one unflushed buffer per queue to redelivery). 0 flushes every
    /// completion immediately.
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
        assert_eq!(a.batch, 16);
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

    /// The standalone gate default of 256 is above every band this repo has
    /// measured, and bench/RESULTS.md asked for the mismatch to be fixed or
    /// loudly documented. It is documented, in the one place a tuning user
    /// looks: `--help`. Deleting that text is a regression, so pin it.
    #[test]
    fn io_concurrency_long_help_carries_the_measured_band() {
        use clap::CommandFactory;
        let help = Args::command().render_long_help().to_string();
        for token in ["--io-concurrency", "bench/RESULTS.md", "104", "128"] {
            assert!(
                help.contains(token),
                "--io-concurrency long help lost {token}"
            );
        }
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
