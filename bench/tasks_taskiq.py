"""No-op async task, taskiq. Run with: taskiq worker bench.tasks_taskiq:broker ..."""

import redis.asyncio as aredis
from taskiq_redis import ListQueueBroker

from common import DONE_KEY, REDIS_URL

broker = ListQueueBroker(url=REDIS_URL, queue_name="fwbench")
_r = aredis.Redis.from_url(REDIS_URL)


@broker.task
async def noop():
    await _r.incr(DONE_KEY)
