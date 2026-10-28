"""Long-sleeping task, taskiq: stays in-flight so RSS/PSS can be measured at
a known steady concurrency. Each task counts itself into STARTED_KEY first.
Run with: taskiq worker tasks_taskiq_hold:broker --max-async-tasks ...
"""

import asyncio

import redis.asyncio as aredis
from taskiq_redis import ListQueueBroker

from common import REDIS_POOL_MAX, REDIS_URL, STARTED_KEY

broker = ListQueueBroker(url=REDIS_URL, queue_name="fwbench")
_r = aredis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)


@broker.task
async def hold():
    await _r.incr(STARTED_KEY)
    await asyncio.sleep(3600)
