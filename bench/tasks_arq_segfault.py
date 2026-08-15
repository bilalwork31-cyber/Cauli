"""Segfault blast-radius test, arq: single asyncio loop per worker process
-- a segfault in `segfault` should crash that whole process, taking every
in-flight `hold` task with it, same architecture class as cauli's naive
async lane. See CLAIMS.md #4.

Run with: arq tasks_arq_segfault.WorkerSettings
"""

import asyncio
import ctypes

import redis.asyncio as aredis
from arq.connections import RedisSettings

from common import REDIS_URL

_r = aredis.Redis.from_url(REDIS_URL)
redis_settings = RedisSettings.from_dsn(REDIS_URL)


async def hold(ctx):
    await asyncio.sleep(3600)


async def segfault(ctx):
    ctypes.string_at(0)  # reads from a null pointer -- reliably segfaults


class WorkerSettings:
    functions = [hold, segfault]
    redis_settings = redis_settings
    # See tasks_arq.py: default poll_delay=0.5s/max_jobs=10.
    poll_delay = 0.01
    max_jobs = 200
