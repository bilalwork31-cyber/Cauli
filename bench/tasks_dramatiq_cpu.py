"""CPU-bound task, dramatiq. Native multicore via --processes at run time
(one OS process per worker, like Celery prefork). Run with:
python3 -m dramatiq tasks_dramatiq_cpu --processes ... --threads ...
"""

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

from common import DONE_KEY, REDIS_URL
from workloads import cpu_burn

broker = RedisBroker(url=REDIS_URL)
dramatiq.set_broker(broker)
_r = redis.Redis.from_url(REDIS_URL)


@dramatiq.actor
def burn(ms):
    cpu_burn(ms)
    _r.incr(DONE_KEY)
