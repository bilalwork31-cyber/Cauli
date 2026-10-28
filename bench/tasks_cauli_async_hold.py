"""Long-sleeping task, cauli async: stays in-flight so RSS/PSS can be
measured at a known steady concurrency. Each task counts itself into
STARTED_KEY first, so memory_campaign.py knows when all N are in flight.
Run with: cauli-worker -A tasks_cauli_async_hold:app -c ...
"""

import asyncio

import redis.asyncio as aredis
from cauli import Cauli

from common import REDIS_POOL_MAX, REDIS_URL, STARTED_KEY

app = Cauli(redis_url=REDIS_URL)
_r = None


@app.task(store_result=False, max_retries=0, timeout=3600)
async def hold():
    global _r
    if _r is None:
        _r = aredis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)
    await _r.incr(STARTED_KEY)
    await asyncio.sleep(3600)
