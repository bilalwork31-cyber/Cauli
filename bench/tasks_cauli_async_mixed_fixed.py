"""Adversarial mixed workload, cauli async, FIXED version: `poison`'s CPU
burst is kind="cpu" -- routed to a forked child, off the shared asyncio
loop entirely. Contrast against tasks_cauli_async_mixed.py: if `light`
latency stops spiking here, the naive version's stall was a routing
mistake, not an unfixable architectural flaw. See CLAIMS.md #4.

Run with: cauli-worker -A tasks_cauli_async_mixed_fixed:app -c ...
"""

import time

import redis
import redis.asyncio as aredis
from cauli import Cauli

from common import REDIS_URL
from workloads import POISON_BURST_MS, cpu_burn

app = Cauli(redis_url=REDIS_URL)
_r = aredis.Redis.from_url(REDIS_URL)
# Lazy, not module-level-opened: a kind="cpu" child is forked from a warmed
# parent (README: gc.freeze() + copy-on-write fork), so a connection opened
# before fork would be shared across every child -- same fork-safety
# footgun as tasks_celery_pg_prefork.py, opened here on first use instead.
_sync_r = None


@app.task(store_result=False, max_retries=0)
async def light(scheduled_ts):
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    await _r.rpush("bench:latencies:light", f"{scheduled_ts},{latency_ms}")


@app.task(kind="cpu", store_result=False, max_retries=0)
def poison(scheduled_ts):
    global _sync_r
    cpu_burn(POISON_BURST_MS)  # forked child -- does not touch the shared GIL
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    if _sync_r is None:
        _sync_r = redis.Redis.from_url(REDIS_URL)
    _sync_r.rpush("bench:latencies:poison", f"{scheduled_ts},{latency_ms}")
