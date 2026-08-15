"""CPU-bound task, arq. arq has no built-in cpu-pool concept (single asyncio
loop per worker process) -- offloaded to a ProcessPoolExecutor via
run_in_executor, the standard way arq users get real multicore for
GIL-holding work. Run with: arq tasks_arq_cpu:WorkerSettings
"""

import asyncio
import os
from concurrent.futures import ProcessPoolExecutor

import redis.asyncio as aredis
from arq.connections import RedisSettings

from common import DONE_KEY, REDIS_URL
from workloads import cpu_burn

_r = aredis.Redis.from_url(REDIS_URL)
redis_settings = RedisSettings.from_dsn(REDIS_URL)
# arq has no CLI concurrency knob for this (it's this file's own executor,
# not arq's), so the pool size is an env var instead, to sweep it the same
# way every other framework's process count is swept.
_pool = ProcessPoolExecutor(max_workers=int(os.environ.get("ARQ_CPU_POOL_SIZE", os.cpu_count())))


async def burn(ctx, ms):
    await asyncio.get_running_loop().run_in_executor(_pool, cpu_burn, ms)
    await _r.incr(DONE_KEY)


class WorkerSettings:
    functions = [burn]
    redis_settings = redis_settings
    # See tasks_arq.py: default poll_delay=0.5s/max_jobs=10 caps throughput
    # far below what the CPU pool itself can do.
    poll_delay = 0.01
    max_jobs = 200
