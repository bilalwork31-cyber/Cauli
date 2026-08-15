"""Segfault blast-radius test, Dramatiq: expected to isolate the crash to
the one process handling `segfault`, same structural class as Celery
prefork. See CLAIMS.md #4.

Run with: python3 -m dramatiq tasks_dramatiq_segfault --processes ... --threads ...
"""

import ctypes
import time

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

from common import REDIS_URL

broker = RedisBroker(url=REDIS_URL)
dramatiq.set_broker(broker)
_r = redis.Redis.from_url(REDIS_URL)


@dramatiq.actor
def hold():
    time.sleep(3600)


@dramatiq.actor
def segfault():
    ctypes.string_at(0)  # reads from a null pointer -- reliably segfaults
