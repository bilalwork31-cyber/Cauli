"""Every Dramatiq actor the benchmarks run. Worker: dramatiq bench.apps.dramatiq"""

import ctypes
import time

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

from bench import DONE_KEY, EXEC_KEY, HOLD_S, REDIS_POOL_MAX, REDIS_URL

dramatiq.set_broker(RedisBroker(url=REDIS_URL))
_r = redis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)


@dramatiq.actor
def noop():
    _r.incr(DONE_KEY)


@dramatiq.actor
def hold():
    time.sleep(HOLD_S)


@dramatiq.actor(max_retries=5)
def chaos(tag):
    time.sleep(0.5)
    _r.rpush(EXEC_KEY, tag)


@dramatiq.actor
def segfault():
    ctypes.string_at(0)


def enqueue(jobs):
    tasks = globals()
    for name, args in jobs:
        tasks[name].send(*args)
