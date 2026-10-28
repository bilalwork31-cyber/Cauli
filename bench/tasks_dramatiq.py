"""No-op sync task, dramatiq. Run with: python3 -m dramatiq tasks_dramatiq"""

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

from common import DONE_KEY, REDIS_POOL_MAX, REDIS_URL

broker = RedisBroker(url=REDIS_URL)
dramatiq.set_broker(broker)
_r = redis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)


@dramatiq.actor
def noop():
    _r.incr(DONE_KEY)
