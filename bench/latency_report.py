"""Analyze bench:latencies after a latency_producer run. Usage: latency_report.py"""

import sys

import redis
from hdrh.histogram import HdrHistogram

from common import REDIS_URL

LATENCY_KEY = "bench:latencies"
DRIFT_KEY = "bench:producer_max_drift_s"


def main():
    r = redis.Redis.from_url(REDIS_URL)
    raw = r.lrange(LATENCY_KEY, 0, -1)
    if not raw:
        print("no samples in bench:latencies", file=sys.stderr)
        raise SystemExit(1)

    hist = HdrHistogram(1, 60000, 3)
    max_ms = 0.0
    for v in raw:
        ms = float(v)
        hist.record_value(min(ms, 60000))
        max_ms = max(max_ms, ms)

    print(f"count: {hist.get_total_count()}")
    print(f"p50: {hist.get_value_at_percentile(50):.2f} ms")
    print(f"p95: {hist.get_value_at_percentile(95):.2f} ms")
    print(f"p99: {hist.get_value_at_percentile(99):.2f} ms")
    print(f"p99.9: {hist.get_value_at_percentile(99.9):.2f} ms")
    print(f"max: {max_ms:.2f} ms")

    drift_raw = r.get(DRIFT_KEY)
    drift_s = float(drift_raw) if drift_raw else 0.0
    behind = "yes" if drift_s > 0 else "no"
    print(f"producer fell behind schedule: {behind} (max drift: {drift_s * 1000:.1f} ms)")


if __name__ == "__main__":
    main()
