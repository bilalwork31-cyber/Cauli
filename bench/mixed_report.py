"""Analyze a mixed_driver.py run: does `light` task latency spike inside a
poison-burst window vs outside one? An aggregate p99 can dilute or hide a
periodic stall if poison tasks are a small fraction of total traffic -- this
buckets each `light` sample as "near a poison burst" or "baseline" instead.

Usage: mixed_report.py [poison_burst_ms] [window_slack_ms]
"""

import sys

import redis
from hdrh.histogram import HdrHistogram

from common import REDIS_URL

LIGHT_KEY = "bench:latencies:light"
POISON_KEY = "bench:latencies:poison"
POISON_TIMES_KEY = "bench:poison_send_times"


def _load_pairs(r, key):
    raw = r.lrange(key, 0, -1)
    pairs = []
    for v in raw:
        ts_str, lat_str = v.decode().split(",")
        pairs.append((float(ts_str), float(lat_str)))
    return pairs


def _hist_from(latencies):
    hist = HdrHistogram(1, 60000, 3)
    for ms in latencies:
        hist.record_value(min(ms, 60000))
    return hist


def _print_hist(label, latencies):
    if not latencies:
        print(f"{label}: no samples")
        return
    hist = _hist_from(latencies)
    print(
        f"{label}: n={hist.get_total_count()} "
        f"p50={hist.get_value_at_percentile(50):.2f}ms "
        f"p95={hist.get_value_at_percentile(95):.2f}ms "
        f"p99={hist.get_value_at_percentile(99):.2f}ms "
        f"p99.9={hist.get_value_at_percentile(99.9):.2f}ms "
        f"max={max(latencies):.2f}ms"
    )


def main():
    burst_ms = float(sys.argv[1]) if len(sys.argv) > 1 else 50.0
    slack_ms = float(sys.argv[2]) if len(sys.argv) > 2 else 100.0
    window_s = (burst_ms + slack_ms) / 1000.0

    r = redis.Redis.from_url(REDIS_URL)
    light = _load_pairs(r, LIGHT_KEY)
    poison = _load_pairs(r, POISON_KEY)
    poison_send_times = [float(v) for v in r.lrange(POISON_TIMES_KEY, 0, -1)]

    if not light:
        print("no light samples", file=sys.stderr)
        raise SystemExit(1)

    print(f"light samples: {len(light)}, poison samples: {len(poison)}")
    print(f"poison bursts injected: {len(poison_send_times)}")
    print()

    near = []
    baseline = []
    for ts, lat in light:
        is_near = any(pt <= ts <= pt + window_s for pt in poison_send_times)
        (near if is_near else baseline).append(lat)

    _print_hist("light, NEAR a poison burst", near)
    _print_hist("light, baseline (no nearby poison)", baseline)
    print()
    _print_hist("poison task latency itself", [lat for _, lat in poison])

    if near and baseline:
        near_p99 = _hist_from(near).get_value_at_percentile(99)
        base_p99 = _hist_from(baseline).get_value_at_percentile(99)
        ratio = near_p99 / base_p99 if base_p99 > 0 else float("inf")
        print()
        print(f"near-burst p99 / baseline p99 = {ratio:.1f}x")


if __name__ == "__main__":
    main()
