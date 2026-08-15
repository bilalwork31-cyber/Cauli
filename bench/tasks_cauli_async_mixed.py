"""Adversarial mixed workload, cauli async, NAIVE version: `poison` runs its
CPU burst directly inside the async task, on the same embedded asyncio loop
as every `light` task in this process. If cauli's single-process model has
a shared-fate problem, this is where it shows up -- `light` task latency
should spike for the duration of each `poison` burst. See CLAIMS.md #4 and
tasks_cauli_async_mixed_fixed.py (the same workload routed correctly).

Run with: cauli-worker -A tasks_cauli_async_mixed:app -c ...
"""

import time

import redis.asyncio as aredis
from cauli import Cauli

from common import REDIS_URL
from workloads import POISON_BURST_MS, cpu_burn

app = Cauli(redis_url=REDIS_URL)
_r = aredis.Redis.from_url(REDIS_URL)


@app.task(store_result=False, max_retries=0)
async def light(scheduled_ts):
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    await _r.rpush("bench:latencies:light", f"{scheduled_ts},{latency_ms}")


@app.task(store_result=False, max_retries=0)
async def poison(scheduled_ts):
    cpu_burn(POISON_BURST_MS)  # holds the GIL on the shared loop thread
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    await _r.rpush("bench:latencies:poison", f"{scheduled_ts},{latency_ms}")
