"""Chaos-test task, Celery's plain default: acks_early (the message is acked
the moment the worker RECEIVES it, before execution even starts). A kill -9
mid-execution loses the task outright -- it was already removed from the
queue. This is what most Celery deployments actually run unless someone
explicitly opts into acks_late; comparing cauli's at-least-once against
THIS instead of tasks_celery_chaos_acks_late.py would be comparing across
delivery guarantee levels, which is the apples-to-oranges cheat this suite
is explicitly trying not to make. Kept and labeled honestly, not hidden.

Run with: celery -A tasks_celery_chaos_default worker -P prefork -c ...
"""

import time

import redis
from celery import Celery

from common import REDIS_URL

EXEC_KEY = "bench:chaos:executions"

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)


@app.task(ignore_result=True)
def chaos(tag):
    time.sleep(0.5)
    _r.rpush(EXEC_KEY, tag)
