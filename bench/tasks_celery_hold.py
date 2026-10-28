"""Long-sleeping task, Celery: stays in-flight so RSS/PSS can be measured
at a known steady concurrency. Each task counts itself into STARTED_KEY
first, so memory_campaign.py knows when all N are in flight. Run with:
celery -A tasks_celery_hold worker -P prefork -c ...  (or -P gevent)
"""

import time

import redis
from celery import Celery

from common import REDIS_POOL_MAX, REDIS_URL, STARTED_KEY

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)


@app.task(ignore_result=True)
def hold():
    _r.incr(STARTED_KEY)
    time.sleep(3600)
