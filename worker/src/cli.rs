use clap::Parser;

const ADVANCED: &str = "Advanced tuning (derived from -c; see docs/CONFIGURATION.md)";

/// cauli-worker: Rust worker runtime for cauli Python task queues (PROTOCOL §7).
#[derive(Parser, Debug, Clone)]
#[command(name = "cauli-worker", version, about)]
pub struct Args {
    /// App location as module:attr (e.g. myproj.tasks:app)
    #[arg(short = 'A', long)]
    pub app: String,

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

    /// Recycle a cpu child after it completes this many tasks (0 = never).
    /// The backstop for leaky C extensions and slowly dirtied copy-on-write
    /// pages, like Celery's maxtasksperchild. Staged prefetch work always
    /// drains before the recycle fires, so no task is lost to it
    #[arg(long, default_value_t = 0, help_heading = ADVANCED)]
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

    /// Python executable used to spawn cpu children
    #[arg(long, default_value = "python3", help_heading = ADVANCED)]
    pub python: String,

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
/// - io_concurrency: explicit, else c/procs with -c, else 256. The gate is
///   the bound for async tasks (a slot costs ~4 KB).
/// - io_threads: explicit, else min(c, 512)/procs with -c, else 64. Capped at
///   the gate (a thread above it never receives work) and at 512 total: the
///   sync knee is ~1000 threads/proc and past it throughput and latency fall
///   together, so the derived default stays 1x the gate up to the cap rather
///   than oversubscribing (oversubscription trades task p99 for throughput —
///   an explicit choice, not a default).
/// - cpu_workers: explicit, else cores/procs but never more than -c's share:
///   more children than cores buys nothing, and more than c would make
///   `-c 8` on a pdf-convert queue mean something other than 8.
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
            let gate = args
                .io_concurrency
                .unwrap_or_else(|| c.div_ceil(procs))
                .max(1);
            let threads = args
                .io_threads
                .unwrap_or_else(|| c.min(512).div_ceil(procs).min(gate))
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
            let per_proc = cores.div_ceil(procs);
            match args.concurrency {
                Some(c) => per_proc.min(c.max(1).div_ceil(procs)),
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
        assert_eq!(a.app, "m.tasks:app");
        assert!(a.queues.is_empty());
        assert_eq!(a.redis_url, None);
        assert_eq!(a.concurrency, None);
        assert_eq!(a.procs, None);
        assert_eq!(a.io_loops, 1);
        assert_eq!(a.io_threads, None);
        assert_eq!(a.io_concurrency, None);
        assert_eq!(a.cpu_workers, None);
        assert_eq!(a.cpu_child_threads, 1);
        assert_eq!(a.cpu_max_tasks_per_child, 0);
        assert!(!a.eager_cpu);
        assert!(!a.print_plan);
        assert!(!a.no_fork_server);
        assert_eq!(a.batch, 16);
        assert_eq!(a.visibility_timeout, 60);
        assert_eq!(a.max_envelope_bytes, 1_048_576);
        assert_eq!(a.drain_timeout, 30);
        assert_eq!(a.python, "python3");
        assert_eq!(a.stats_interval, 10);
        assert_eq!(a.log_level, "info");
        assert_eq!(a.redis_timeout, 5);
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
        assert_eq!(a.python, "python3.12");
        assert_eq!(a.stats_interval, 1);
        assert_eq!(a.log_level, "debug");
        assert_eq!(a.redis_timeout, 7);
    }

    #[test]
    fn short_flags_match_celery_muscle_memory() {
        let a = Args::try_parse_from(["cauli-worker", "-A", "m:app", "-c", "50", "-Q", "high,low"])
            .unwrap();
        assert_eq!(a.app, "m:app");
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
        assert_eq!(r.io_concurrency, 667);
        assert_eq!(r.io_threads, 86); // 512/6, not 667: sync knee guard
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
                io_concurrency: 63,
                cpu_workers: 2,
            }
        );
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
