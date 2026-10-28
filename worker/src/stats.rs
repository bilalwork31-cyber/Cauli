use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use tracing::warn;

#[derive(Default)]
pub struct Counters {
    pub fetched: AtomicU64,
    pub ok: AtomicU64,
    pub failed: AtomicU64,
    pub retried: AtomicU64,
    pub dlq: AtomicU64,
    /// §4.5 executions suppressed by a held idempotency key, also counted in
    /// `ok`. Without it a constant `idempotency_key` suppresses every task
    /// while throughput looks normal.
    pub duplicate: AtomicU64,
    /// §9.1 entries discarded unrun past expiry or queue TTL, also counted in
    /// `dlq`: "queue cannot keep up" is a different signal from "work fails".
    pub expired: AtomicU64,
    /// Cpu children that died mid task (OOM kill, segfault): a pool health
    /// number an operator can alert on, separate from `failed`.
    pub cpu_lost: AtomicU64,
    pub inflight_io: AtomicI64,
    pub inflight_cpu: AtomicI64,
    /// Every dispatched entry from spawn to final broker write; drain waits on this.
    pub inflight_total: AtomicI64,
    /// True while the current full spell is logged: one line per edge.
    cpu_backlog_warned: AtomicBool,
    /// Start of the current full spell, for the cleared line's duration.
    cpu_backlog_since_ms: AtomicU64,
    /// Task body latency per execution lane, drained every stats tick.
    pub lat_sync: LatencyHist,
    pub lat_async: LatencyHist,
    pub lat_cpu: LatencyHist,
}

/// Hostname for the stats line, so a line from one of several supervised
/// processes can be traced to the process to restart. Sanitized because §7
/// makes the line logfmt: a space would corrupt every following key.
fn host() -> &'static str {
    static HOST: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    HOST.get_or_init(|| {
        let raw = gethostname::gethostname().to_string_lossy().into_owned();
        let safe: String = raw
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        if safe.is_empty() {
            "unknown".to_string()
        } else {
            safe
        }
    })
}

impl Counters {
    /// The §7 line. It drains the latency histograms, so call it once per
    /// tick or an interval splits across two lines.
    pub fn stats_line(&self) -> String {
        let (sync_p50, sync_p99) = self.lat_sync.take_p50_p99();
        let (async_p50, async_p99) = self.lat_async.take_p50_p99();
        let (cpu_p50, cpu_p99) = self.lat_cpu.take_p50_p99();
        format!(
            "stats: pid={} host={} fetched={} ok={} failed={} retried={} dlq={} expired={} duplicate={} cpu_lost={} inflight_io={} inflight_cpu={} rss_mb={} sync_p50={} sync_p99={} async_p50={} async_p99={} cpu_p50={} cpu_p99={}",
            std::process::id(),
            host(),
            self.fetched.load(Ordering::Relaxed),
            self.ok.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.retried.load(Ordering::Relaxed),
            self.dlq.load(Ordering::Relaxed),
            self.expired.load(Ordering::Relaxed),
            self.duplicate.load(Ordering::Relaxed),
            self.cpu_lost.load(Ordering::Relaxed),
            // Not clamped: a negative value is an accounting bug to expose.
            self.inflight_io.load(Ordering::Relaxed),
            self.inflight_cpu.load(Ordering::Relaxed),
            rss_mb(),
            sync_p50,
            sync_p99,
            async_p50,
            async_p99,
            cpu_p50,
            cpu_p99,
        )
    }

    /// Log the cpu backlog's zero/nonzero edge once, not once per poll.
    /// `depth` is `Ctx::cpu_overflow()`. A full cpu backlog pauses fetching
    /// for every lane, since the lane is unknown until the entry is parsed.
    pub fn note_cpu_backlog(&self, depth: usize, now_ms: u64) {
        if depth > 0 {
            if !self.cpu_backlog_warned.swap(true, Ordering::SeqCst) {
                self.cpu_backlog_since_ms.store(now_ms, Ordering::SeqCst);
                warn!(
                    depth,
                    "cpu backlog full, fetching paused for all lanes including io"
                );
            }
        } else if self.cpu_backlog_warned.swap(false, Ordering::SeqCst) {
            let since = self.cpu_backlog_since_ms.load(Ordering::SeqCst);
            warn!(
                duration_ms = now_ms.saturating_sub(since),
                "cpu backlog cleared, fetching resumed for all lanes"
            );
        }
    }
}

/// Log2 buckets: bucket `i` holds [2^i, 2^(i+1)) ms, bucket 0 everything
/// under 2ms, the top edge 2^24 ms (about 4.6 hours). 2x resolution is
/// enough to show a knee; precision belongs in bench/.
const LAT_BUCKETS: usize = 24;

/// Task body latency for one lane, interval scoped: a slow spell shows as a
/// spike on one line instead of dragging a lifetime aggregate.
pub struct LatencyHist {
    buckets: [AtomicU64; LAT_BUCKETS],
}

impl Default for LatencyHist {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl LatencyHist {
    pub fn record(&self, d: std::time::Duration) {
        self.buckets[lat_bucket(d.as_millis() as u64)].fetch_add(1, Ordering::Relaxed);
    }

    /// Drain the interval and return (p50, p99) in ms, 0 when empty.
    pub fn take_p50_p99(&self) -> (u64, u64) {
        let counts: [u64; LAT_BUCKETS] =
            std::array::from_fn(|i| self.buckets[i].swap(0, Ordering::Relaxed));
        (percentile(&counts, 50), percentile(&counts, 99))
    }
}

/// floor(log2(ms)) clamped to the top bucket; 0 folds into bucket 0.
fn lat_bucket(ms: u64) -> usize {
    let idx = 63 - ms.max(1).leading_zeros() as usize;
    idx.min(LAT_BUCKETS - 1)
}

fn lat_bucket_edges(i: usize) -> (u64, u64) {
    let lo = if i == 0 { 0 } else { 1u64 << i };
    (lo, 1u64 << (i + 1))
}

/// Linearly interpolated inside the bucket carrying the target rank.
fn percentile(counts: &[u64; LAT_BUCKETS], pct: u64) -> u64 {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return 0;
    }
    // Saturating: release builds have no overflow checks.
    let target = (total.saturating_mul(pct).saturating_add(99) / 100).max(1);
    let mut cum = 0u64;
    for (i, &c) in counts.iter().enumerate() {
        if c == 0 {
            continue;
        }
        if cum + c >= target {
            let (lo, hi) = lat_bucket_edges(i);
            return lo + (hi - lo) * (target - cum) / c;
        }
        cum += c;
    }
    // Unreachable: target <= total.
    lat_bucket_edges(LAT_BUCKETS - 1).1
}

pub fn rss_mb() -> u64 {
    rss_mb_at("/proc/self/status")
}

/// Summed RSS in MB of the cpu children; an exited pid contributes 0.
pub fn cpu_rss_mb(pids: &[u32]) -> u64 {
    pids.iter()
        .map(|pid| rss_mb_at(&format!("/proc/{pid}/status")))
        .sum()
}

/// VmRSS in MB from a /proc status file, 0 if unreadable.
fn rss_mb_at(status_path: &str) -> u64 {
    let Ok(s) = std::fs::read_to_string(status_path) else {
        return 0;
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0);
            return kb / 1024;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lat_bucket_maps_by_floor_log2_and_saturates() {
        assert_eq!(lat_bucket(0), 0);
        assert_eq!(lat_bucket(1), 0);
        assert_eq!(lat_bucket(2), 1);
        assert_eq!(lat_bucket(3), 1);
        assert_eq!(lat_bucket(4), 2);
        assert_eq!(lat_bucket(1_023), 9);
        assert_eq!(lat_bucket(1_024), 10);
        assert_eq!(lat_bucket(8_388_607), 22);
        assert_eq!(lat_bucket(8_388_608), LAT_BUCKETS - 1);
        assert_eq!(lat_bucket(u64::MAX), LAT_BUCKETS - 1);
        assert_eq!(lat_bucket_edges(0), (0, 2));
        assert_eq!(lat_bucket_edges(LAT_BUCKETS - 1), (8_388_608, 16_777_216));
    }

    #[test]
    fn record_puts_a_duration_in_its_log2_bucket() {
        let h = LatencyHist::default();
        h.record(std::time::Duration::from_micros(400));
        h.record(std::time::Duration::from_millis(20));
        assert_eq!(h.buckets[0].load(Ordering::Relaxed), 1);
        assert_eq!(h.buckets[4].load(Ordering::Relaxed), 1);
        assert_eq!(h.buckets[5].load(Ordering::Relaxed), 0);
    }

    #[test]
    fn percentile_of_an_empty_interval_is_zero() {
        let counts = [0u64; LAT_BUCKETS];
        assert_eq!(percentile(&counts, 50), 0);
        assert_eq!(percentile(&counts, 99), 0);
    }

    /// Bucket 3 is [8, 16): rank 50 of 100 at 8 + 8 * 50/100, rank 99 at 8 + 8 * 99/100.
    #[test]
    fn percentile_interpolates_inside_the_carrying_bucket() {
        let mut counts = [0u64; LAT_BUCKETS];
        counts[3] = 100;
        assert_eq!(percentile(&counts, 50), 12);
        assert_eq!(percentile(&counts, 99), 15);
    }

    /// 90 samples in [16, 32) and 10 in [1024, 2048): p99 = 1024 + 1024 * 9/10.
    #[test]
    fn percentile_separates_a_slow_tail_from_a_fast_median() {
        let mut counts = [0u64; LAT_BUCKETS];
        counts[4] = 90;
        counts[10] = 10;
        assert_eq!(percentile(&counts, 50), 24);
        assert_eq!(percentile(&counts, 99), 1_945);
    }

    #[test]
    fn percentile_of_one_sample_reports_its_bucket_top_edge() {
        let mut counts = [0u64; LAT_BUCKETS];
        counts[6] = 1;
        assert_eq!(percentile(&counts, 50), 128);
        assert_eq!(percentile(&counts, 99), 128);
    }

    #[test]
    fn take_p50_p99_drains_the_interval() {
        let h = LatencyHist::default();
        for _ in 0..100 {
            h.record(std::time::Duration::from_millis(8));
        }
        assert_eq!(h.take_p50_p99(), (12, 15));
        assert_eq!(h.take_p50_p99(), (0, 0));
    }

    /// Key order is the one PROTOCOL §7 documents.
    #[test]
    fn stats_line_carries_each_lane_percentile() {
        let counters = Counters::default();
        for _ in 0..100 {
            counters
                .lat_sync
                .record(std::time::Duration::from_millis(8));
            counters
                .lat_cpu
                .record(std::time::Duration::from_millis(2_000));
        }
        let line = counters.stats_line();
        assert!(
            line.contains(
                "sync_p50=12 sync_p99=15 async_p50=0 async_p99=0 cpu_p50=1536 cpu_p99=2037"
            ),
            "unexpected latency fields: {line}"
        );
        assert!(counters
            .stats_line()
            .contains("sync_p50=0 sync_p99=0 async_p50=0 async_p99=0 cpu_p50=0 cpu_p99=0"));
    }

    #[test]
    fn rss_mb_at_parses_vmrss_kb_and_tolerates_junk() {
        let dir = std::env::temp_dir().join(format!("cauli-rss-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let good = dir.join("good");
        std::fs::write(
            &good,
            "Name:	python3
VmRSS:	  204800 kB
Threads:	4
",
        )
        .unwrap();
        assert_eq!(rss_mb_at(good.to_str().unwrap()), 200);
        let no_field = dir.join("nofield");
        std::fs::write(
            &no_field,
            "Name:	python3
Threads:	4
",
        )
        .unwrap();
        assert_eq!(rss_mb_at(no_field.to_str().unwrap()), 0);
        assert_eq!(rss_mb_at(dir.join("missing").to_str().unwrap()), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Naming one live process twice doubles: a sum, not a max.
    #[test]
    fn cpu_rss_mb_sums_live_pids_and_skips_dead_ones() {
        assert_eq!(cpu_rss_mb(&[]), 0);
        assert_eq!(cpu_rss_mb(&[u32::MAX]), 0);
        let me = std::process::id();
        let mine = rss_mb();
        assert!(mine > 0, "this test process should have a readable VmRSS");
        // RSS can move between the two reads.
        assert!(cpu_rss_mb(&[me]).abs_diff(mine) <= 1);
        assert!(cpu_rss_mb(&[me, me]).abs_diff(2 * mine) <= 2);
    }

    #[test]
    fn stats_line_carries_cpu_lost() {
        let counters = Counters::default();
        assert!(counters.stats_line().contains(" cpu_lost=0 "));
        counters.cpu_lost.fetch_add(3, Ordering::Relaxed);
        assert!(counters.stats_line().contains(" cpu_lost=3 "));
    }

    #[test]
    fn note_cpu_backlog_flips_once_per_edge_not_per_poll() {
        let counters = Counters::default();
        assert!(!counters.cpu_backlog_warned.load(Ordering::SeqCst));

        counters.note_cpu_backlog(2, 1_000);
        assert!(counters.cpu_backlog_warned.load(Ordering::SeqCst));
        assert_eq!(counters.cpu_backlog_since_ms.load(Ordering::SeqCst), 1_000);

        counters.note_cpu_backlog(5, 1_500);
        assert!(counters.cpu_backlog_warned.load(Ordering::SeqCst));
        assert_eq!(counters.cpu_backlog_since_ms.load(Ordering::SeqCst), 1_000);

        counters.note_cpu_backlog(0, 4_000);
        assert!(!counters.cpu_backlog_warned.load(Ordering::SeqCst));

        counters.note_cpu_backlog(0, 4_100);
        assert!(!counters.cpu_backlog_warned.load(Ordering::SeqCst));
    }

    #[test]
    fn note_cpu_backlog_tracks_a_second_spell_independently() {
        let counters = Counters::default();
        counters.note_cpu_backlog(1, 100);
        counters.note_cpu_backlog(0, 200);
        counters.note_cpu_backlog(3, 900);
        assert!(counters.cpu_backlog_warned.load(Ordering::SeqCst));
        assert_eq!(counters.cpu_backlog_since_ms.load(Ordering::SeqCst), 900);
    }
}
