"""Open-loop latency load generator, framework-agnostic. Enqueues on a fixed
wall-clock schedule regardless of completion (no coordinated omission) -- pass
an `enqueue` callback that does the framework-specific send.

CLI: latency_producer.py <framework> <rate> <duration_s>
  framework: cauli_sync_latency (add more branches as lanes gain latency tasks)
"""

import sys
import time
from typing import Callable

import redis

from common import REDIS_URL

DRIFT_KEY = "bench:producer_max_drift_s"


def run(enqueue: Callable[[float], None], rate: float, duration_s: float) -> dict:
    """Call enqueue(scheduled_ts) once per tick for `duration_s` at `rate`/s.

    scheduled_ts is monotonic-clock-based (CLOCK_MONOTONIC on Linux, safely
    comparable across the producer and worker processes since both run on
    the same live kernel) -- the tick's intended send time, passed to
    enqueue so the task payload carries it, not the actual send time. NOT
    time.time(): a wall-clock correction mid-run (observed under WSL2)
    silently shifts every scheduled_ts computed after the jump, producing
    nonsense negative latencies downstream. Returns stats including how far
    the producer itself fell behind schedule.
    """
    period = 1.0 / rate
    n_ticks = int(duration_s * rate)
    t0_mono = time.monotonic()

    max_drift_s = 0.0
    sent = 0
    for i in range(n_ticks):
        target_mono = t0_mono + i * period
        now = time.monotonic()
        if now < target_mono:
            time.sleep(target_mono - now)
        else:
            max_drift_s = max(max_drift_s, now - target_mono)
        scheduled_ts = target_mono
        enqueue(scheduled_ts)
        sent += 1

    return {
        "sent": sent,
        "rate": rate,
        "duration_s": duration_s,
        "max_drift_s": max_drift_s,
    }


def main():
    framework = sys.argv[1]
    rate = float(sys.argv[2])
    duration_s = float(sys.argv[3])

    if framework == "cauli_sync_latency":
        from tasks_cauli_sync_latency import noop

        enqueue = lambda scheduled_ts: noop.delay(scheduled_ts)
    else:
        raise SystemExit(f"unknown framework {framework!r}")

    stats = run(enqueue, rate, duration_s)
    redis.Redis.from_url(REDIS_URL).set(DRIFT_KEY, stats["max_drift_s"])
    print(
        f"sent {stats['sent']} via {framework} at {rate}/s over {duration_s}s "
        f"(max producer drift: {stats['max_drift_s'] * 1000:.1f} ms)"
    )


if __name__ == "__main__":
    main()
