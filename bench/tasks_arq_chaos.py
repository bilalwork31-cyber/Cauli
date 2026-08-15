"""Chaos-test task, arq: sleeps briefly (long enough to likely be in-flight
when the worker is kill -9'd), then records its tag. arq's delivery
semantics under a hard crash are exactly what this test measures -- not
assumed. Run with: arq tasks_arq_chaos.WorkerSettings
"""

import asyncio

import redis.asyncio as aredis
from arq.connections import RedisSettings

from common import REDIS_URL

EXEC_KEY = "bench:chaos:executions"

_r = aredis.Redis.from_url(REDIS_URL)
redis_settings = RedisSettings.from_dsn(REDIS_URL)


async def chaos(ctx, tag):
    await asyncio.sleep(0.5)
    await _r.rpush(EXEC_KEY, tag)


class WorkerSettings:
    functions = [chaos]
    redis_settings = redis_settings
    # See tasks_arq.py: default poll_delay=0.5s/max_jobs=10.
    poll_delay = 0.01
    max_jobs = 200
