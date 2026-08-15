"""Chaos-test task, Dramatiq: sleeps briefly (long enough to likely be
in-flight when the worker is kill -9'd), then records its tag.

Run with: python3 -m dramatiq tasks_dramatiq_chaos --processes ... --threads ...
"""

import time

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

from common import REDIS_URL

EXEC_KEY = "bench:chaos:executions"

broker = RedisBroker(url=REDIS_URL)
dramatiq.set_broker(broker)
_r = redis.Redis.from_url(REDIS_URL)


@dramatiq.actor(max_retries=5)
def chaos(tag):
    time.sleep(0.5)
    _r.rpush(EXEC_KEY, tag)
