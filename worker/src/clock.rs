//! Redis anchored wall clock, sampled.
//!
//! Every ABSOLUTE instant this worker writes or compares has to mean the same
//! thing on every other worker, because they all read each other's writes: the
//! section 4.3 delayed set has several writers and a reader on each worker,
//! and the section 9.1 expiry check compares a worker's read against a
//! deadline the client stamped. A local `SystemTime` read does not have that
//! property. Since every worker runs the mover, the FASTEST local clock in the
//! fleet decides when delayed work fires, so one forward stepped worker fires
//! everything early and defeats backoff and eta; a forward step during
//! `schedule_retry` strands the entry with nothing to self heal it; and a
//! worker ahead of the client silently drops still valid work as expired.
//! Anchoring on redis, which every worker already agrees to talk to, removes
//! all three at once and needs no wire change.
//!
//! SAMPLED, not a `TIME` call per read, and this is the part someone will
//! want to "simplify". Reading `TIME` per call would add two SERIAL round
//! trips per task (the dispatch expiry check and the finish stamp), which
//! measured out at roughly double the broker command load. A timestamp is not
//! worth that. Between samples the reading extrapolates from a monotonic
//! `Instant`, so it never steps and never goes backwards, and quartz drift is
//! about 3ms per minute against the 250ms mover granularity.

use redis::aio::ConnectionManager;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Floor of the resample period. The pid spread below turns this into the 15
/// to 30 seconds the design calls for.
const SAMPLE_BASE: Duration = Duration::from_secs(15);

/// Width of the pid derived spread, in seconds, so a fleet that booted
/// together does not aim a synchronized `TIME` at redis every period.
const SAMPLE_SPREAD_S: u64 = 15;

/// How long a clock may go unsampled before it is called out. Three missed
/// resamples: at the drift above that is still only single digit ms of error,
/// so this warns about redis rather than about the accuracy of the reads.
const STALE_AFTER_MS: i64 = 90_000;

/// Skew worth naming at boot. Below this, redis and the local clock agree
/// closely enough that an operator has nothing to fix.
const SKEW_WARN_MS: i64 = 1_000;

static CLOCK: OnceLock<RedisClock> = OnceLock::new();

/// Edge flag for the sampler's log lines, so a redis outage costs one warn
/// and one recovery info rather than a line per resample for its duration.
static SAMPLING_WARNED: AtomicBool = AtomicBool::new(false);

pub struct RedisClock {
    /// Redis anchored epoch ms as of `base`. Resampling moves only this.
    anchor_ms: AtomicI64,
    /// Monotonic reference for every read. An `Instant` never runs backwards
    /// and never sees an NTP step, so between samples the clock only advances.
    base: Instant,
    /// `base.elapsed()` in ms at the last successful sample.
    sampled_at_ms: AtomicI64,
}

impl RedisClock {
    /// A clock already anchored on `redis_ms`, taken as of now.
    fn anchored(redis_ms: i64) -> Self {
        Self {
            anchor_ms: AtomicI64::new(redis_ms),
            base: Instant::now(),
            sampled_at_ms: AtomicI64::new(0),
        }
    }

    fn elapsed_ms(&self) -> i64 {
        self.base.elapsed().as_millis() as i64
    }

    pub fn now_ms(&self) -> u64 {
        // The anchor and the elapsed time are read separately, so a sample
        // landing between the two shifts the answer by the size of that one
        // correction (single digit ms). Harmless at every call site, and
        // cheaper than putting a lock on the hottest read in the worker.
        self.anchor_ms
            .load(Ordering::Relaxed)
            .saturating_add(self.elapsed_ms())
            .max(0) as u64
    }

    /// Re anchor on a fresh redis reading. A sample that moves the clock
    /// backwards is stored as is, deliberately: the correction is a few ms,
    /// every site that compares instants tolerates it, and smearing it would
    /// buy nothing but a second clock to reason about.
    fn apply_sample(&self, redis_ms: i64) {
        let elapsed = self.elapsed_ms();
        self.anchor_ms
            .store(redis_ms.saturating_sub(elapsed), Ordering::Relaxed);
        self.sampled_at_ms.store(elapsed, Ordering::Relaxed);
    }

    fn staleness_ms(&self) -> i64 {
        self.elapsed_ms() - self.sampled_at_ms.load(Ordering::Relaxed)
    }
}

/// Current wall clock in epoch ms, anchored on redis. Use this for every
/// absolute instant: delayed scores, expiry comparisons, result stamps. See
/// the module comment for why it is sampled rather than read from redis here.
pub fn now_ms() -> u64 {
    match CLOCK.get() {
        Some(clock) => clock.now_ms(),
        // Nothing to anchor on yet: the boot window before `init`, a redis
        // that refuses `TIME`, and unit tests that never call `init`.
        None => crate::ctx::now_ms(),
    }
}

/// Epoch ms from redis's `TIME` reply, which is a seconds and microseconds
/// pair.
fn time_reply_to_ms(secs: i64, micros: i64) -> i64 {
    secs.saturating_mul(1000).saturating_add(micros / 1000)
}

async fn sample(conn: &mut ConnectionManager) -> anyhow::Result<i64> {
    let (secs, micros): (i64, i64) = redis::cmd("TIME").query_async(conn).await?;
    Ok(time_reply_to_ms(secs, micros))
}

/// Install the first sample, or fold a later one into the installed clock.
fn record(redis_ms: i64) {
    if let Some(clock) = CLOCK.get() {
        clock.apply_sample(redis_ms);
        return;
    }
    // Publish a clock that is ALREADY anchored rather than one anchored a
    // moment after it becomes visible: a reader catching that gap would see
    // an epoch near zero, which every expiry check reads as "long expired".
    if CLOCK.set(RedisClock::anchored(redis_ms)).is_err() {
        if let Some(clock) = CLOCK.get() {
            clock.apply_sample(redis_ms);
        }
    }
}

/// Take the first sample, blocking, before any loop starts. Free here: the
/// worker has already had to reach redis for `ensure_groups`.
///
/// A failure is not fatal. Some managed redis deployments deny `TIME` by ACL,
/// and refusing to boot over a timestamp would turn a degraded clock into an
/// outage. The worker falls back to its own clock, says so plainly, and the
/// sampler keeps trying.
pub async fn init(conn: &mut ConnectionManager) {
    match sample(conn).await {
        Ok(redis_ms) => {
            let skew_ms = redis_ms - crate::ctx::now_ms() as i64;
            record(redis_ms);
            if skew_ms.abs() >= SKEW_WARN_MS {
                warn!(
                    skew_ms,
                    "this host's clock disagrees with redis by more than a second. cauli reads \
                     the redis clock, so scheduling is unaffected, but every log timestamp and \
                     anything enqueued from this host is off by that much: fix ntp here"
                );
            } else {
                info!(skew_ms, "clock anchored on redis TIME");
            }
        }
        Err(e) => warn!(
            "redis TIME failed at startup ({e}): falling back to this host's local clock until a \
             sample succeeds. Delayed, retried and expiring tasks will compare against a clock \
             the rest of the fleet does not share (PROTOCOL.md section 4)"
        ),
    }
}

/// 15 to 30 seconds, spread by pid. Deterministic per process rather than
/// random so a restart loop cannot land on the same phase every time.
fn sample_period() -> Duration {
    SAMPLE_BASE + Duration::from_secs(u64::from(std::process::id()) % SAMPLE_SPREAD_S)
}

/// Re anchor on redis for the life of the process.
pub async fn sampler_loop(mut conn: ConnectionManager) {
    let mut tick = tokio::time::interval(sample_period());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` completes its first tick immediately and `init` has already
    // taken that sample.
    tick.tick().await;
    loop {
        tick.tick().await;
        match sample(&mut conn).await {
            Ok(redis_ms) => {
                record(redis_ms);
                if SAMPLING_WARNED.swap(false, Ordering::Relaxed) {
                    info!("redis TIME is answering again: clock re anchored");
                }
            }
            Err(e) => {
                let anchored = CLOCK.get();
                let stale_ms = anchored.map_or(i64::MAX, RedisClock::staleness_ms);
                if stale_ms >= STALE_AFTER_MS && !SAMPLING_WARNED.swap(true, Ordering::Relaxed) {
                    match anchored {
                        Some(_) => warn!(
                            stale_ms,
                            "redis TIME has not answered for {stale_ms}ms ({e}). The clock is \
                             still extrapolating monotonically from the last sample and drifts \
                             about 3ms per minute, so reads stay usable; this is redis telling \
                             you something, not a clock fault"
                        ),
                        None => warn!(
                            "redis TIME is still failing ({e}): this worker's absolute \
                             timestamps come from its own system clock, which the rest of the \
                             fleet does not share (PROTOCOL.md section 4)"
                        ),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPOCH_2026: i64 = 1_770_000_000_000;

    #[test]
    fn now_ms_extrapolates_from_the_monotonic_base() {
        let clock = RedisClock::anchored(EPOCH_2026);
        let first = clock.now_ms();
        std::thread::sleep(Duration::from_millis(20));
        let second = clock.now_ms();
        assert!(second > first, "clock did not advance: {first} -> {second}");
        assert!(
            second - first >= 15 && second - first < 2_000,
            "advance looks wrong: {}ms",
            second - first
        );
    }

    #[test]
    fn a_sample_moves_the_anchor_not_the_monotonic_base() {
        // The whole point of the offset: a redis reading far from where this
        // clock currently sits is adopted in full on the next read.
        let clock = RedisClock::anchored(EPOCH_2026);
        clock.apply_sample(EPOCH_2026 + 3_600_000);
        let now = clock.now_ms() as i64;
        assert!(
            (now - (EPOCH_2026 + 3_600_000)).abs() < 1_000,
            "anchor not adopted: {now}"
        );
    }

    #[test]
    fn a_backward_sample_is_adopted_as_is_without_smearing() {
        // Small backward regressions at resample are harmless at every call
        // site, so there is deliberately no smearing to assert against.
        let clock = RedisClock::anchored(EPOCH_2026);
        clock.apply_sample(EPOCH_2026 - 40);
        let now = clock.now_ms() as i64;
        assert!(now < EPOCH_2026, "backward sample was not adopted: {now}");
    }

    #[test]
    fn staleness_is_measured_from_the_last_successful_sample() {
        let clock = RedisClock::anchored(EPOCH_2026);
        assert!(clock.staleness_ms() < 1_000);
        std::thread::sleep(Duration::from_millis(20));
        assert!(clock.staleness_ms() >= 15);
        clock.apply_sample(EPOCH_2026);
        assert!(
            clock.staleness_ms() < 1_000,
            "sample did not reset staleness"
        );
    }

    #[test]
    fn a_nonsense_anchor_degrades_to_zero_rather_than_wrapping() {
        // u64 is the wire type for every score and deadline, so a negative
        // epoch must clamp instead of wrapping to a huge future instant.
        let clock = RedisClock::anchored(i64::MIN);
        assert_eq!(clock.now_ms(), 0);
    }

    #[test]
    fn time_reply_converts_seconds_and_micros_to_ms() {
        assert_eq!(time_reply_to_ms(1_770_000_000, 500_000), EPOCH_2026 + 500);
        assert_eq!(time_reply_to_ms(1_770_000_000, 0), EPOCH_2026);
        assert_eq!(time_reply_to_ms(1_770_000_000, 999_999), EPOCH_2026 + 999);
        // Hostile reply: saturating rather than a release build wrap.
        assert_eq!(time_reply_to_ms(i64::MAX, i64::MAX), i64::MAX);
    }

    #[test]
    fn sample_period_stays_inside_the_designed_window() {
        let p = sample_period().as_secs();
        assert!((15..30).contains(&p), "period out of window: {p}s");
    }

    #[test]
    fn now_ms_falls_back_to_the_local_clock_before_the_first_sample() {
        // Nothing in this test binary calls `init`, so the global is unset and
        // the free function must still return a plausible epoch.
        assert!(now_ms() > 1_700_000_000_000, "fallback read looks wrong");
    }
}
