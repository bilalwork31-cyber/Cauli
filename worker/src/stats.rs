use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use tracing::warn;

#[derive(Default)]
pub struct Counters {
    pub fetched: AtomicU64,
    pub ok: AtomicU64,
    pub failed: AtomicU64,
    pub retried: AtomicU64,
    pub dlq: AtomicU64,
    /// §9.1 entries discarded unrun past their expiry / queue TTL. Counted in
    /// `dlq` too (they do get a DLQ entry); broken out because "work is being
    /// thrown away because the queue cannot keep up" is a different operational
    /// signal from "work is failing".
    pub expired: AtomicU64,
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
}

impl Counters {
    pub fn stats_line(&self) -> String {
        format!(
            "stats: fetched={} ok={} failed={} retried={} dlq={} expired={} inflight_io={} inflight_cpu={} rss_mb={}",
            self.fetched.load(Ordering::Relaxed),
            self.ok.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.retried.load(Ordering::Relaxed),
            self.dlq.load(Ordering::Relaxed),
            self.expired.load(Ordering::Relaxed),
            // Printed raw (no .max(0) clamp): a negative value here would be
            // an accounting bug, and clamping it would hide the signal.
            self.inflight_io.load(Ordering::Relaxed),
            self.inflight_cpu.load(Ordering::Relaxed),
            rss_mb(),
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

/// RSS in MB from /proc/self/status VmRSS (kB). 0 if unreadable.
pub fn rss_mb() -> u64 {
    let Ok(s) = std::fs::read_to_string("/proc/self/status") else {
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
