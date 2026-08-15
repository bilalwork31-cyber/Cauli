"""Adversarial mixed workload, arq: single asyncio loop per worker process,
same architecture class as cauli's async lane and taskiq -- does poison's
CPU burst stall every other in-flight task? See CLAIMS.md #4.

Run with: arq tasks_arq_mixed.WorkerSettings
"""

import time

import redis.asyncio as aredis
from arq.connections import RedisSettings

from common import REDIS_URL
from workloads import POISON_BURST_MS, cpu_burn

_r = aredis.Redis.from_url(REDIS_URL)
redis_settings = RedisSettings.from_dsn(REDIS_URL)


async def light(ctx, scheduled_ts):
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    await _r.rpush("bench:latencies:light", f"{scheduled_ts},{latency_ms}")


async def poison(ctx, scheduled_ts):
    cpu_burn(POISON_BURST_MS)
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    await _r.rpush("bench:latencies:poison", f"{scheduled_ts},{latency_ms}")


class WorkerSettings:
    functions = [light, poison]
    redis_settings = redis_settings
    # See tasks_arq.py: default poll_delay=0.5s/max_jobs=10 would swamp the
    # latency measurement with its own polling granularity.
    poll_delay = 0.01
    max_jobs = 200
