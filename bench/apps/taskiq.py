"""Every taskiq task the benchmarks run. Worker: taskiq worker bench.apps.taskiq:broker"""

import asyncio

import redis.asyncio as aredis
from taskiq_redis import ListQueueBroker

from bench import DONE_KEY, HOLD_S, REDIS_POOL_MAX, REDIS_URL, STARTED_KEY

broker = ListQueueBroker(url=REDIS_URL, queue_name="bench")
_r = aredis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)
# Sends in flight at once while enqueueing; one send per pooled connection.
ENQUEUE_WINDOW = 500


@broker.task
async def noop():
    await _r.incr(DONE_KEY)


@broker.task
async def hold():
    await _r.incr(STARTED_KEY)
    await asyncio.sleep(HOLD_S)


async def _enqueue(jobs):
    tasks = globals()
    await broker.startup()
    for start in range(0, len(jobs), ENQUEUE_WINDOW):
        window = jobs[start : start + ENQUEUE_WINDOW]
        await asyncio.gather(*(tasks[name].kiq(*args) for name, args in window))
    await broker.shutdown()


def enqueue(jobs):
    asyncio.run(_enqueue(jobs))
