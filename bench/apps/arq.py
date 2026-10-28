"""Every arq job the benchmarks run. arq pins redis<6, so this module runs from
its own venv (requirements-arq.txt). Worker: arq bench.apps.arq.WorkerSettings"""

import asyncio
import ctypes

import redis.asyncio as aredis
from arq.connections import RedisSettings, create_pool

from bench import DONE_KEY, EXEC_KEY, HOLD_S, REDIS_POOL_MAX, REDIS_URL

_r = aredis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)


async def noop(ctx):
    await _r.incr(DONE_KEY)


async def hold(ctx):
    await asyncio.sleep(HOLD_S)


async def chaos(ctx, tag):
    await asyncio.sleep(0.5)
    await _r.rpush(EXEC_KEY, tag)


async def segfault(ctx):
    ctypes.string_at(0)


class WorkerSettings:
    functions = [noop, hold, chaos, segfault]
    redis_settings = RedisSettings.from_dsn(REDIS_URL)
    # The defaults (poll_delay 0.5 s, max_jobs 10) cap a no-op near 27/s.
    # arq has no CLI flag for either.
    poll_delay = 0.01
    max_jobs = 200


async def _enqueue(jobs):
    pool = await create_pool(WorkerSettings.redis_settings)
    for name, args in jobs:
        await pool.enqueue_job(name, *args)
    await pool.aclose()


def enqueue(jobs):
    asyncio.run(_enqueue(jobs))
