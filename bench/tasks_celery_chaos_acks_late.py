"""Chaos-test task, Celery configured for at-least-once: task_acks_late=True
means the message is NOT acked until after the task returns, so a kill -9
mid-execution leaves it unacked and eligible for redelivery once the
broker's visibility_timeout expires. reject_on_worker_lost=True means a
worker dying mid-task explicitly requeues it rather than leaving it stuck.
This is the fair comparison against cauli's (always-on) at-least-once
delivery -- see tasks_celery_chaos_default.py for what Celery's plain
default (acks_early) actually does under the same crash.

Run with: celery -A tasks_celery_chaos_acks_late worker -P prefork -c ...
"""

import time

import redis
from celery import Celery

from common import REDIS_URL

EXEC_KEY = "bench:chaos:executions"

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
app.conf.broker_transport_options = {"visibility_timeout": 5}
_r = redis.Redis.from_url(REDIS_URL)


@app.task(ignore_result=True, acks_late=True, reject_on_worker_lost=True, max_retries=5)
def chaos(tag):
    time.sleep(0.5)
    _r.rpush(EXEC_KEY, tag)
