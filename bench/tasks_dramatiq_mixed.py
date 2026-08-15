"""Adversarial mixed workload, Dramatiq: native multicore via --processes,
same structural class as Celery prefork -- expected to isolate a poison
task's CPU burst to the one process handling it. See CLAIMS.md #4.

Run with: python3 -m dramatiq tasks_dramatiq_mixed --processes ... --threads ...
"""

import time

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

from common import REDIS_URL
from workloads import POISON_BURST_MS, cpu_burn

broker = RedisBroker(url=REDIS_URL)
dramatiq.set_broker(broker)
_r = redis.Redis.from_url(REDIS_URL)


@dramatiq.actor
def light(scheduled_ts):
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    _r.rpush("bench:latencies:light", f"{scheduled_ts},{latency_ms}")


@dramatiq.actor
def poison(scheduled_ts):
    cpu_burn(POISON_BURST_MS)
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    _r.rpush("bench:latencies:poison", f"{scheduled_ts},{latency_ms}")
