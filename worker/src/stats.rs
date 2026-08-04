use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

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
