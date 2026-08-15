"""CPU-bound task, arq. arq has no built-in cpu-pool concept (single asyncio
loop per worker process) -- offloaded to a ProcessPoolExecutor via
run_in_executor, the standard way arq users get real multicore for
GIL-holding work. Run with: arq tasks_arq_cpu:WorkerSettings
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor

import redis.asyncio as aredis
from arq.connections import RedisSettings

from common import DONE_KEY, REDIS_URL
from workloads import cpu_burn

_r = aredis.Redis.from_url(REDIS_URL)
redis_settings = RedisSettings.from_dsn(REDIS_URL)
_pool = ProcessPoolExecutor()


async def burn(ctx, ms):
    await asyncio.get_running_loop().run_in_executor(_pool, cpu_burn, ms)
    await _r.incr(DONE_KEY)


class WorkerSettings:
    functions = [burn]
    redis_settings = redis_settings
