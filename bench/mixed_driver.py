"""Adversarial mixed workload driver (CLAIMS.md #4): runs a steady open-loop
`light` task stream (via latency_producer.run) on one thread, while a second
thread injects `poison` CPU-burst tasks on a fixed interval. `light` and
`poison` latencies land in separate Redis lists (bench:latencies:light,
bench:latencies:poison) tagged with each poison task's send time, so
mixed_report.py can check whether `light` latency spikes align with poison
bursts instead of just eyeballing an aggregate histogram.

CLI: mixed_driver.py <lane> <light_rate> <duration_s> [poison_interval_s]
  lane: cauli_async_mixed | cauli_async_mixed_fixed | taskiq_mixed | celery_mixed
"""

import asyncio
import sys
import threading
import time

import redis

from common import REDIS_URL
from latency_producer import run as producer_run

POISON_TIMES_KEY = "bench:poison_send_times"


class AsyncBridge:
    """Runs one persistent asyncio loop in a background thread and exposes a
    blocking `.call(coro_fn, *args)` -- lets a sync scheduling loop (like
    latency_producer.run) drive an async enqueue (taskiq's `.kiq()`) without
    paying a new-event-loop setup cost on every single call.
    """

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self._thread.start()

    def call(self, coro_fn, *args):
        fut = asyncio.run_coroutine_threadsafe(coro_fn(*args), self.loop)
        return fut.result()

    def close(self):
        self.loop.call_soon_threadsafe(self.loop.stop)


def make_enqueue_fns(lane):
    if lane == "cauli_async_mixed":
        from tasks_cauli_async_mixed import light, poison

        return (lambda ts: light.delay(ts)), (lambda ts: poison.delay(ts)), None
    if lane == "cauli_async_mixed_fixed":
        from tasks_cauli_async_mixed_fixed import light, poison

        return (lambda ts: light.delay(ts)), (lambda ts: poison.delay(ts)), None
    if lane == "celery_mixed":
        from tasks_celery_mixed import light, poison

        return (lambda ts: light.delay(ts)), (lambda ts: poison.delay(ts)), None
    if lane == "taskiq_mixed":
        from tasks_taskiq_mixed import broker, light, poison

        bridge = AsyncBridge()
        bridge.call(broker.startup)
        light_fn = lambda ts: bridge.call(light.kiq, ts)
        poison_fn = lambda ts: bridge.call(poison.kiq, ts)
        return light_fn, poison_fn, bridge
    if lane == "arq_mixed":
        from arq.connections import create_pool

        from tasks_arq_mixed import redis_settings

        bridge = AsyncBridge()
        pool = bridge.call(create_pool, redis_settings)
        light_fn = lambda ts: bridge.call(pool.enqueue_job, "light", ts)
        poison_fn = lambda ts: bridge.call(pool.enqueue_job, "poison", ts)
        return light_fn, poison_fn, bridge
    if lane == "dramatiq_mixed":
        from tasks_dramatiq_mixed import light, poison

        return (lambda ts: light.send(ts)), (lambda ts: poison.send(ts)), None
    raise SystemExit(f"unknown lane {lane!r}")


def poison_loop(poison_fn, interval_s, duration_s, r):
    t_end = time.monotonic() + duration_s
    while time.monotonic() < t_end:
        ts = time.monotonic()
        r.rpush(POISON_TIMES_KEY, ts)
        poison_fn(ts)
        time.sleep(interval_s)


def main():
    lane = sys.argv[1]
    light_rate = float(sys.argv[2])
    duration_s = float(sys.argv[3])
    poison_interval_s = float(sys.argv[4]) if len(sys.argv) > 4 else 3.0

    light_fn, poison_fn, bridge = make_enqueue_fns(lane)
    r = redis.Redis.from_url(REDIS_URL)

    poison_thread = threading.Thread(
        target=poison_loop, args=(poison_fn, poison_interval_s, duration_s, r)
    )
    poison_thread.start()
    stats = producer_run(light_fn, light_rate, duration_s)
    poison_thread.join()

    if bridge:
        bridge.close()

    print(
        f"lane={lane} light: sent {stats['sent']} at {light_rate}/s, "
        f"max producer drift {stats['max_drift_s'] * 1000:.1f}ms; "
        f"poison: every {poison_interval_s}s over {duration_s}s"
    )


if __name__ == "__main__":
    main()
