use rand::Rng;

/// Deterministic part of PROTOCOL §4.2:
/// `d_ms = min(backoff_max_ms, backoff_base_ms * backoff_factor^(attempt-1))`
/// `attempt` is the NEW retries value (1-based).
pub fn base_backoff_ms(attempt: u32, base_ms: u64, factor: f64, max_ms: u64) -> f64 {
    let a = attempt.max(1);
    let d = (base_ms as f64) * factor.powi((a - 1) as i32);
    let maxf = max_ms as f64;
    if !d.is_finite() || d > maxf {
        maxf
    } else {
        d.max(0.0)
    }
}

/// Full §4.2 computation. If `jitter`: `d_ms = uniform(0.5 * d_ms, d_ms)`
/// (jitter applied AFTER the max clamp, exactly as the formula reads).
pub fn compute_backoff_ms(
    attempt: u32,
    base_ms: u64,
    factor: f64,
    max_ms: u64,
    jitter: bool,
) -> u64 {
    let d = base_backoff_ms(attempt, base_ms, factor, max_ms);
    let d = if jitter && d > 0.0 {
        rand::rng().random_range((0.5 * d)..=d)
    } else {
        d
    };
    d.round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_no_jitter_sequence() {
        // base 500ms, factor 2.0, max 60000
        assert_eq!(compute_backoff_ms(1, 500, 2.0, 60_000, false), 500);
        assert_eq!(compute_backoff_ms(2, 500, 2.0, 60_000, false), 1_000);
        assert_eq!(compute_backoff_ms(3, 500, 2.0, 60_000, false), 2_000);
        assert_eq!(compute_backoff_ms(4, 500, 2.0, 60_000, false), 4_000);
        assert_eq!(compute_backoff_ms(8, 500, 2.0, 60_000, false), 60_000); // 64000 clamped
    }

    #[test]
    fn non_integer_factor() {
        // 100 * 1.5^2 = 225
        assert_eq!(compute_backoff_ms(3, 100, 1.5, 60_000, false), 225);
    }

    #[test]
    fn clamp_applies_before_jitter() {
        // attempt high enough that raw value >> max; jitter range must be [0.5*max, max]
        for _ in 0..500 {
            let d = compute_backoff_ms(20, 500, 2.0, 10_000, true);
            assert!(d >= 5_000, "jittered {d} below 0.5*max");
            assert!(d <= 10_000, "jittered {d} above max");
        }
    }

    #[test]
    fn jitter_bounds_uniform_half_to_full() {
        let mut min_seen = u64::MAX;
        let mut max_seen = 0u64;
        for _ in 0..2_000 {
            let d = compute_backoff_ms(2, 1_000, 2.0, 60_000, true); // base d = 2000
            assert!(
                (1_000..=2_000).contains(&d),
                "jittered {d} out of [1000,2000]"
            );
            min_seen = min_seen.min(d);
            max_seen = max_seen.max(d);
        }
        // with 2000 samples we should see spread across the range
        assert!(
            min_seen < 1_200,
            "min_seen {min_seen} suggests jitter not spreading low"
        );
        assert!(
            max_seen > 1_800,
            "max_seen {max_seen} suggests jitter not spreading high"
        );
    }

    #[test]
    fn zero_base_is_zero() {
        assert_eq!(compute_backoff_ms(1, 0, 2.0, 60_000, true), 0);
        assert_eq!(compute_backoff_ms(1, 0, 2.0, 60_000, false), 0);
    }

    #[test]
    fn attempt_zero_treated_as_one() {
        assert_eq!(compute_backoff_ms(0, 500, 2.0, 60_000, false), 500);
    }
}
