"""Every cauli task the benchmarks run. Worker: cauli-worker -A bench.apps.cauli:app"""

import asyncio
import ctypes
import time

import redis
import redis.asyncio as aredis
from cauli import Cauli

from bench import (
    DONE_KEY,
    EXEC_KEY,
    HOLD_S,
    REDIS_POOL_MAX,
    REDIS_URL,
    STARTED_KEY,
    cpu_burn,
)

app = Cauli(redis_url=REDIS_URL)
_sync = redis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)
_async = aredis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)
# The memory benchmark holds 10,000 tasks at once: a small blocking pool, or
# they fail inside the task body (pool exhausted) or exceed Redis's default
# maxclients of 10,000 across processes, and read as never in flight.
_hold = aredis.Redis.from_pool(
    aredis.BlockingConnectionPool.from_url(REDIS_URL, max_connections=64, timeout=None)
)
# About 2 ms of pure Python per call. The free threaded comparison needs a fixed
# amount of work: cpu_burn stops at a wall clock deadline, so a thread starved
# of CPU would still finish on time and hide the contention being measured.
PURE_PYTHON_LOOPS = 40_000


def pure_python_burn() -> int:
    x = 0
    for i in range(PURE_PYTHON_LOOPS):
        x += i * i
    return x


@app.task(store_result=False, max_retries=0)
def noop_sync():
    _sync.incr(DONE_KEY)


@app.task(store_result=False, max_retries=0)
async def noop_async():
    await _async.incr(DONE_KEY)


@app.task(kind="cpu", store_result=False, max_retries=0)
def burn(ms):
    cpu_burn(ms)
    _sync.incr(DONE_KEY)


@app.task(store_result=False, max_retries=0)
def burn_thread():
    pure_python_burn()
    _sync.incr(DONE_KEY)


@app.task(kind="cpu", store_result=False, max_retries=0)
def burn_process():
    pure_python_burn()
    _sync.incr(DONE_KEY)


@app.task(store_result=False, max_retries=0, timeout=HOLD_S)
async def hold():
    await _hold.incr(STARTED_KEY)
    await asyncio.sleep(HOLD_S)


# A crashed worker's task is redelivered once it has been idle for
# max(--visibility-timeout, timeout + grace), so the timeout is sized to the
# 0.5 s body: a 30 s timeout alone would hold every redelivery for 35 s.
@app.task(store_result=False, max_retries=5, timeout=2)
def chaos(tag):
    time.sleep(0.5)
    _sync.rpush(EXEC_KEY, tag)


@app.task(store_result=False, max_retries=0)
async def segfault():
    ctypes.string_at(0)


@app.task(kind="cpu", store_result=False, max_retries=0)
def segfault_cpu():
    ctypes.string_at(0)


def enqueue(jobs):
    tasks = globals()
    app.enqueue_many([(tasks[name], tuple(args)) for name, args in jobs])
