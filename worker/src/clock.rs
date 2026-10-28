//! Redis anchored wall clock, sampled.
//!
//! Absolute instants (delayed scores, PROTOCOL 9.1 expiry, result stamps) are
//! compared across workers. With local clocks the fastest host in the fleet
//! fires all delayed work early and a host ahead of the client drops valid
//! work as expired, so every worker reads one clock: redis `TIME`.
//!
//! Sampled, not read per call: per call `TIME` adds two serial round trips
//! per task. Between samples the reading extrapolates a monotonic `Instant`
//! (never steps backwards, drifts about 3ms per minute).

use redis::aio::ConnectionManager;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const SAMPLE_BASE: Duration = Duration::from_secs(15);

/// Pid spread, so a fleet booted together does not send `TIME` in lockstep.
const SAMPLE_SPREAD_S: u64 = 15;

/// Three missed resamples: still single digit ms of error, so this warns
/// about redis, not about accuracy.
const STALE_AFTER_MS: i64 = 90_000;

const SKEW_WARN_MS: i64 = 1_000;

static CLOCK: OnceLock<RedisClock> = OnceLock::new();

/// One warn and one recovery line per outage, not one per resample.
static SAMPLING_WARNED: AtomicBool = AtomicBool::new(false);

pub struct RedisClock {
    /// Redis epoch ms as of `base`. Resampling moves only this.
    anchor_ms: AtomicI64,
    base: Instant,
    /// `base.elapsed()` in ms at the last successful sample.
    sampled_at_ms: AtomicI64,
}

impl RedisClock {
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
        // Two unsynchronized loads: a racing sample shifts one read by a few
        // ms, which every caller tolerates. No lock on the hottest read.
        self.anchor_ms
            .load(Ordering::Relaxed)
            .saturating_add(self.elapsed_ms())
            .max(0) as u64
    }

    /// A backward correction is stored as is: it is a few ms and every
    /// comparison tolerates it, so no smearing.
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

/// Epoch ms anchored on redis. Use for every absolute instant.
pub fn now_ms() -> u64 {
    match CLOCK.get() {
        Some(clock) => clock.now_ms(),
        // Before `init`, when redis refuses `TIME`, and in unit tests.
        None => crate::ctx::now_ms(),
    }
}

fn time_reply_to_ms(secs: i64, micros: i64) -> i64 {
    secs.saturating_mul(1000).saturating_add(micros / 1000)
}

async fn sample(conn: &mut ConnectionManager) -> anyhow::Result<i64> {
    let (secs, micros): (i64, i64) = redis::cmd("TIME").query_async(conn).await?;
    Ok(time_reply_to_ms(secs, micros))
}

fn record(redis_ms: i64) {
    if let Some(clock) = CLOCK.get() {
        clock.apply_sample(redis_ms);
        return;
    }
    // Publish an already anchored clock: a reader seeing an unanchored one
    // would read an epoch near zero and expire everything.
    if CLOCK.set(RedisClock::anchored(redis_ms)).is_err() {
        if let Some(clock) = CLOCK.get() {
            clock.apply_sample(redis_ms);
        }
    }
}

/// First sample, before any loop starts. Not fatal on failure: some managed
/// redis deny `TIME` by ACL, so the worker falls back to its own clock, warns,
/// and the sampler keeps trying.
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

/// 15 to 30 seconds, spread by pid.
fn sample_period() -> Duration {
    SAMPLE_BASE + Duration::from_secs(u64::from(std::process::id()) % SAMPLE_SPREAD_S)
}

pub async fn sampler_loop(mut conn: ConnectionManager) {
    let mut tick = tokio::time::interval(sample_period());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The immediate first tick: `init` already took that sample.
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
        // A negative epoch must clamp, not wrap to a far future u64.
        let clock = RedisClock::anchored(i64::MIN);
        assert_eq!(clock.now_ms(), 0);
    }

    #[test]
    fn time_reply_converts_seconds_and_micros_to_ms() {
        assert_eq!(time_reply_to_ms(1_770_000_000, 500_000), EPOCH_2026 + 500);
        assert_eq!(time_reply_to_ms(1_770_000_000, 0), EPOCH_2026);
        assert_eq!(time_reply_to_ms(1_770_000_000, 999_999), EPOCH_2026 + 999);
        assert_eq!(time_reply_to_ms(i64::MAX, i64::MAX), i64::MAX);
    }

    #[test]
    fn sample_period_stays_inside_the_designed_window() {
        let p = sample_period().as_secs();
        assert!((15..30).contains(&p), "period out of window: {p}s");
    }

    #[test]
    fn now_ms_falls_back_to_the_local_clock_before_the_first_sample() {
        assert!(now_ms() > 1_700_000_000_000, "fallback read looks wrong");
    }
}
