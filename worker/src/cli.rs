use clap::Parser;

/// rupy-worker: Rust worker runtime for rupy Python task queues (PROTOCOL §7).
#[derive(Parser, Debug, Clone)]
#[command(name = "rupy-worker", version, about)]
pub struct Args {
    /// App location as module:attr (e.g. myproj.tasks:app)
    #[arg(long)]
    pub app: String,

    /// Comma separated queue names. Default: app.default_queue
    #[arg(long, value_delimiter = ',')]
    pub queues: Vec<String>,

    /// Redis URL. Precedence: CLI > env RUPY_REDIS_URL > app.redis_url
    #[arg(long)]
    pub redis_url: Option<String>,

    /// Number of embedded asyncio event loop threads for async tasks
    #[arg(long, default_value_t = 1)]
    pub io_loops: usize,

    /// Python thread pool size for sync io tasks
    #[arg(long, default_value_t = 64)]
    pub io_threads: usize,

    /// Max in flight io tasks total (admission semaphore)
    #[arg(long, default_value_t = 256)]
    pub io_concurrency: usize,

    /// Child processes for cpu tasks. Default: number of cores
    #[arg(long)]
    pub cpu_workers: Option<usize>,

    /// XREADGROUP COUNT per fetch
    #[arg(long, default_value_t = 16)]
    pub batch: usize,

    /// Visibility timeout in seconds (crash recovery, PROTOCOL §4.4)
    #[arg(long, default_value_t = 60)]
    pub visibility_timeout: u64,

    /// Graceful shutdown drain timeout in seconds
    #[arg(long, default_value_t = 30)]
    pub drain_timeout: u64,

    /// Python executable used to spawn cpu children
    #[arg(long, default_value = "python3")]
    pub python: String,

    /// Seconds between stats log lines
    #[arg(long, default_value_t = 10)]
    pub stats_interval: u64,

    /// Log level (trace|debug|info|warn|error); RUST_LOG overrides
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

pub fn valid_queue_name(q: &str) -> bool {
    !q.is_empty()
        && q.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_defaults() {
        let a = Args::try_parse_from(["rupy-worker", "--app", "m.tasks:app"]).unwrap();
        assert_eq!(a.app, "m.tasks:app");
        assert!(a.queues.is_empty());
        assert_eq!(a.redis_url, None);
        assert_eq!(a.io_loops, 1);
        assert_eq!(a.io_threads, 64);
        assert_eq!(a.io_concurrency, 256);
        assert_eq!(a.cpu_workers, None);
        assert_eq!(a.batch, 16);
        assert_eq!(a.visibility_timeout, 60);
        assert_eq!(a.drain_timeout, 30);
        assert_eq!(a.python, "python3");
        assert_eq!(a.stats_interval, 10);
        assert_eq!(a.log_level, "info");
    }

    #[test]
    fn parses_overrides_and_queue_list() {
        let a = Args::try_parse_from([
            "rupy-worker",
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
        ])
        .unwrap();
        assert_eq!(a.queues, vec!["default", "emails", "bulk-2"]);
        assert_eq!(a.redis_url.as_deref(), Some("redis://127.0.0.1:6392/0"));
        assert_eq!(a.io_loops, 2);
        assert_eq!(a.io_threads, 8);
        assert_eq!(a.io_concurrency, 32);
        assert_eq!(a.cpu_workers, Some(3));
        assert_eq!(a.batch, 4);
        assert_eq!(a.visibility_timeout, 2);
        assert_eq!(a.drain_timeout, 5);
        assert_eq!(a.python, "python3.12");
        assert_eq!(a.stats_interval, 1);
        assert_eq!(a.log_level, "debug");
    }

    #[test]
    fn missing_app_is_error() {
        assert!(Args::try_parse_from(["rupy-worker"]).is_err());
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
