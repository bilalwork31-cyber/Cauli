"""CPU-bound task, Celery, prefork (true multicore via OS processes).
Run with: celery -A tasks_celery_cpu worker -P prefork -c ...
"""

import redis
from celery import Celery

from common import DONE_KEY, REDIS_URL
from workloads import cpu_burn

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)


@app.task(ignore_result=True)
def burn(ms):
    cpu_burn(ms)
    _r.incr(DONE_KEY)
