"""No-op sync task, Celery. Run with: celery -A bench.tasks_celery worker -c ..."""

import redis
from celery import Celery

from common import DONE_KEY, REDIS_URL

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)


@app.task(ignore_result=True)
def noop():
    _r.incr(DONE_KEY)
