use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use tracing::warn;

#[derive(Default)]
pub struct Counters {
    pub fetched: AtomicU64,
    pub ok: AtomicU64,
    pub failed: AtomicU64,
    pub retried: AtomicU64,
    pub dlq: AtomicU64,
    /// §4.5 executions suppressed because their idempotency key was already
    /// held. Counted in `ok` too (the caller does get a result document);
    /// broken out for the same reason `expired` is, and because without it a
    /// constant or badly derived `idempotency_key` suppresses every task
    /// while the throughput graph climbs normally.
    pub duplicate: AtomicU64,
    /// §9.1 entries discarded unrun past their expiry / queue TTL. Counted in
    /// `dlq` too (they do get a DLQ entry); broken out because "work is being
    /// thrown away because the queue cannot keep up" is a different operational
    /// signal from "work is failing".
    pub expired: AtomicU64,
    /// Cpu children that died mid task (`CpuOutcome::Lost`). Broken out of
    /// `failed` because a child killed by the OOM killer or a segfault is a
    /// pool health problem, not a task problem, and folding it into a generic
    /// WorkerLost left repeated child death as a scrolling warning with no
    /// number an operator could alert on.
    pub cpu_lost: AtomicU64,
    pub inflight_io: AtomicI64,
    pub inflight_cpu: AtomicI64,
    /// Every dispatched entry from spawn to final broker write; drain waits on this.
    pub inflight_total: AtomicI64,
    /// True while `note_cpu_backlog` has already logged the current full
    /// spell. Lets the zero/nonzero edge be logged exactly once each way
    /// instead of once per caller per poll.
    cpu_backlog_warned: AtomicBool,
    /// `now_ms` at the instant `cpu_backlog_warned` last flipped to true, so
    /// the cleared line can report how long fetching was paused.
    cpu_backlog_since_ms: AtomicU64,
    /// Task body latency per execution lane, drained every stats tick.
    pub lat_sync: LatencyHist,
    pub lat_async: LatencyHist,
    pub lat_cpu: LatencyHist,
}

/// Hostname for the stats line, resolved once. PROTOCOL §7 makes this line
/// the entire operator interface, and `-c 500` on a six core box derives six
/// supervised processes each emitting an independent cumulative line with
/// identical field names: without an identity, the two alerting signals
/// docs/CONFIGURATION.md names cannot be traced back to a process to restart.
/// Sanitized because §7 makes the line a logfmt parsing contract: a
/// hostname carrying a space (or anything else that is not DNS-shaped)
/// would split into two pseudo-fields and corrupt every following key for
/// awk, promtail and vector alike. `pid` needs no such treatment.
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
    /// The periodic section 7 line. Draining the latency histograms is a
    /// side effect of building it, so call this exactly once per stats tick
    /// or an interval's samples get split across two lines.
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
            // Printed raw (no .max(0) clamp): a negative value here would be
            // an accounting bug, and clamping it would hide the signal.
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

    /// Log the cpu backlog's zero/nonzero transition, once per edge, never
    /// once per poll (which would flood the log for as long as the backlog
    /// stays full). `depth` is `Ctx::cpu_overflow()`: the count of dispatch
    /// tasks currently parked on a full cpu backlog channel. Warns when it
    /// fills, naming the actual cause plainly: the fetch loop cannot know a
    /// stream entry's lane before parsing it, so a full cpu backlog pauses
    /// fetching for every lane, not just cpu (see loops.rs's fetch_loop).
    /// Warns again with the stuck duration when it clears, so the pause is
    /// visible after the fact too, not only while it is still ongoing.
    /// `now_ms` is a parameter rather than read from the clock here so this
    /// stays unit testable with synthetic timestamps.
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

/// Number of log2 latency buckets. Bucket `i` holds samples in
/// [2^i, 2^(i+1)) ms, so bucket 0 absorbs everything under 2ms (sub
/// millisecond included) and bucket 23 everything from 2^23 ms up, putting
/// the top edge at 2^24 ms, near 4 hours. 24 of them cover the whole
/// plausible range of a task body in about 600 bytes per lane. The 2x worst
/// case resolution is deliberate: this line exists to show an operator a
/// knee, and anything that needs real precision belongs in bench/.
const LAT_BUCKETS: usize = 24;

/// Task body latency for one execution lane. Recording is a single relaxed
/// `fetch_add`, so the whole hot path cost is the two clock reads around the
/// body. Snapshot and reset together, which makes the reported percentiles
/// interval scoped: a slow spell shows up as a spike on one line instead of
/// permanently dragging a lifetime aggregate.
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

    /// Drain the interval and return its interpolated (p50, p99) in ms.
    /// Both are 0 when no task finished in the interval.
    pub fn take_p50_p99(&self) -> (u64, u64) {
        let counts: [u64; LAT_BUCKETS] =
            std::array::from_fn(|i| self.buckets[i].swap(0, Ordering::Relaxed));
        (percentile(&counts, 50), percentile(&counts, 99))
    }
}

/// Bucket index for a millisecond duration: floor(log2(ms)), clamped to the
/// top bucket. `ms.max(1)` folds 0 into bucket 0 and keeps `leading_zeros`
/// meaningful.
fn lat_bucket(ms: u64) -> usize {
    let idx = 63 - ms.max(1).leading_zeros() as usize;
    idx.min(LAT_BUCKETS - 1)
}

/// Inclusive lower and exclusive upper edge of bucket `i`, in ms. Bucket 0
/// starts at 0 rather than 1 because sub millisecond samples land in it.
fn lat_bucket_edges(i: usize) -> (u64, u64) {
    let lo = if i == 0 { 0 } else { 1u64 << i };
    (lo, 1u64 << (i + 1))
}

/// Percentile `pct` over one drained histogram, linearly interpolated inside
/// the bucket that carries the target rank. Resolution is the bucket width,
/// so the answer is within 2x of the true value by construction.
fn percentile(counts: &[u64; LAT_BUCKETS], pct: u64) -> u64 {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return 0;
    }
    // Ceiling of total * pct / 100, saturating so a huge interval count
    // cannot wrap in a release build (no overflow-checks there).
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
    // Not reachable: target is capped at `total`, so some bucket carries it.
    lat_bucket_edges(LAT_BUCKETS - 1).1
}

/// RSS in MB from this process's own /proc status.
pub fn rss_mb() -> u64 {
    rss_mb_at("/proc/self/status")
}

/// Summed RSS in MB over the cpu pool's live children, read at the stats
/// tick only so it costs the hot path nothing. A pid that has already exited
/// contributes 0 rather than failing the whole reading, since the pool
/// recycles and respawns children underneath this.
pub fn cpu_rss_mb(pids: &[u32]) -> u64 {
    pids.iter()
        .map(|pid| rss_mb_at(&format!("/proc/{pid}/status")))
        .sum()
}

/// RSS in MB from any /proc PID status file's VmRSS (kB). 0 if unreadable.
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

    /// Boundaries of the log2 mapping, including both ends: everything under
    /// 2ms folds into bucket 0, and anything past the 2^24 ms top edge
    /// saturates into the last bucket instead of indexing out of bounds.
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
        // Top edge is 2^24 ms, about 4.6 hours.
        assert_eq!(lat_bucket_edges(LAT_BUCKETS - 1), (8_388_608, 16_777_216));
    }

    /// A Duration must land in the bucket its millisecond value selects, and
    /// sub millisecond work must not escape bucket 0.
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

    /// Interpolation inside one bucket, checked against hand computed values
    /// rather than a range: bucket 3 spans [8, 16) ms, so with 100 samples
    /// rank 50 sits at 8 + 8 * 50/100 and rank 99 at 8 + 8 * 99/100.
    #[test]
    fn percentile_interpolates_inside_the_carrying_bucket() {
        let mut counts = [0u64; LAT_BUCKETS];
        counts[3] = 100;
        assert_eq!(percentile(&counts, 50), 12);
        assert_eq!(percentile(&counts, 99), 15);
    }

    /// The whole point of p99: a tail that a mean or a median would hide.
    /// 90 samples at 20ms (bucket 4, [16, 32)) and 10 at 2000ms (bucket 10,
    /// [1024, 2048)) must leave p50 in the fast bucket and p99 in the slow
    /// one, at the rank 99 offset 1024 + 1024 * 9/10.
    #[test]
    fn percentile_separates_a_slow_tail_from_a_fast_median() {
        let mut counts = [0u64; LAT_BUCKETS];
        counts[4] = 90;
        counts[10] = 10;
        assert_eq!(percentile(&counts, 50), 24);
        assert_eq!(percentile(&counts, 99), 1_945);
    }

    /// A single sample reports its bucket's top edge, the conservative read
    /// for a latency number: never under report the tail.
    #[test]
    fn percentile_of_one_sample_reports_its_bucket_top_edge() {
        let mut counts = [0u64; LAT_BUCKETS];
        counts[6] = 1;
        assert_eq!(percentile(&counts, 50), 128);
        assert_eq!(percentile(&counts, 99), 128);
    }

    /// Interval scoped, not cumulative: draining resets every bucket, so a
    /// slow spell cannot drag every later line with it.
    #[test]
    fn take_p50_p99_drains_the_interval() {
        let h = LatencyHist::default();
        for _ in 0..100 {
            h.record(std::time::Duration::from_millis(8));
        }
        assert_eq!(h.take_p50_p99(), (12, 15));
        assert_eq!(h.take_p50_p99(), (0, 0));
    }

    /// The six latency keys must carry the drained values, in the order
    /// PROTOCOL section 7 documents.
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
        // Drained by the previous call, so the next line reports a fresh
        // interval rather than repeating this one.
        assert!(counters
            .stats_line()
            .contains("sync_p50=0 sync_p99=0 async_p50=0 async_p99=0 cpu_p50=0 cpu_p99=0"));
    }

    /// VmRSS is reported in kB and must be read from an arbitrary /proc
    /// status file, not just this process's, so the cpu children can be
    /// summed the same way. Anything unreadable reads as 0 rather than
    /// poisoning the sum.
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

    /// The cpu pool figure is a sum over live children: no pids is 0, an
    /// exited pid contributes 0, and naming the same live process twice
    /// doubles, which is what proves this is a sum and not a max.
    #[test]
    fn cpu_rss_mb_sums_live_pids_and_skips_dead_ones() {
        assert_eq!(cpu_rss_mb(&[]), 0);
        assert_eq!(cpu_rss_mb(&[u32::MAX]), 0);
        let me = std::process::id();
        let mine = rss_mb();
        assert!(mine > 0, "this test process should have a readable VmRSS");
        // 1 MB of slack: RSS can genuinely move between the two reads.
        assert!(cpu_rss_mb(&[me]).abs_diff(mine) <= 1);
        assert!(cpu_rss_mb(&[me, me]).abs_diff(2 * mine) <= 2);
    }

    /// A dead cpu child must be countable, not just loggable.
    #[test]
    fn stats_line_carries_cpu_lost() {
        let counters = Counters::default();
        assert!(counters.stats_line().contains(" cpu_lost=0 "));
        counters.cpu_lost.fetch_add(3, Ordering::Relaxed);
        assert!(counters.stats_line().contains(" cpu_lost=3 "));
    }

    /// The flag must flip exactly once on entry and once on exit, not on
    /// every poll. That flip is what stands between a live incident and a
    /// flooded log, since callers hit this on every gate check while the
    /// backlog is full.
    #[test]
    fn note_cpu_backlog_flips_once_per_edge_not_per_poll() {
        let counters = Counters::default();
        assert!(!counters.cpu_backlog_warned.load(Ordering::SeqCst));

        counters.note_cpu_backlog(2, 1_000);
        assert!(counters.cpu_backlog_warned.load(Ordering::SeqCst));
        assert_eq!(counters.cpu_backlog_since_ms.load(Ordering::SeqCst), 1_000);

        // still nonzero on a later poll: does nothing, since_ms must not move
        counters.note_cpu_backlog(5, 1_500);
        assert!(counters.cpu_backlog_warned.load(Ordering::SeqCst));
        assert_eq!(counters.cpu_backlog_since_ms.load(Ordering::SeqCst), 1_000);

        counters.note_cpu_backlog(0, 4_000);
        assert!(!counters.cpu_backlog_warned.load(Ordering::SeqCst));

        // already clear: does nothing, must not panic or flip anything back on
        counters.note_cpu_backlog(0, 4_100);
        assert!(!counters.cpu_backlog_warned.load(Ordering::SeqCst));
    }

    /// A second full spell after a clear must be tracked from its own start,
    /// not the first spell's.
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
