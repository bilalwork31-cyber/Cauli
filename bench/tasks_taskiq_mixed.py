"""Adversarial mixed workload, taskiq: same shared-single-asyncio-loop
architecture class as cauli's async lane, so the same question applies --
does one task's CPU burst stall every other in-flight task? See CLAIMS.md #4
and tasks_cauli_async_mixed.py.

Run with: taskiq worker tasks_taskiq_mixed:broker --workers ... --max-async-tasks ...
"""

import time

import redis.asyncio as aredis
from taskiq_redis import ListQueueBroker

from common import REDIS_URL
from workloads import POISON_BURST_MS, cpu_burn

broker = ListQueueBroker(url=REDIS_URL, queue_name="fwbench_mixed")
_r = aredis.Redis.from_url(REDIS_URL)


@broker.task
async def light(scheduled_ts):
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    await _r.rpush("bench:latencies:light", f"{scheduled_ts},{latency_ms}")


@broker.task
async def poison(scheduled_ts):
    cpu_burn(POISON_BURST_MS)
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    await _r.rpush("bench:latencies:poison", f"{scheduled_ts},{latency_ms}")
