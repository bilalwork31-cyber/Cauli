"""Every Celery task the benchmarks run. Worker: celery -A bench.apps.celery worker

`acks_late_app` is Celery configured for at least once delivery, the fair
match for cauli in the crash test; `app` is Celery's default, which acks a
message when it is received.
"""

import ctypes
import time

import redis
from celery import Celery

from bench import (
    DONE_KEY,
    EXEC_KEY,
    HOLD_S,
    REDIS_POOL_MAX,
    REDIS_URL,
    STARTED_KEY,
    cpu_burn,
)

app = Celery("bench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
acks_late_app = Celery("bench_acks_late", broker=REDIS_URL, backend=None)
acks_late_app.conf.task_ignore_result = True
# Redelivery of an unacked message waits this long; matches the cauli side.
acks_late_app.conf.broker_transport_options = {"visibility_timeout": 5}
_r = redis.Redis.from_url(REDIS_URL, max_connections=REDIS_POOL_MAX)


@app.task
def noop():
    _r.incr(DONE_KEY)


@app.task
def burn(ms):
    cpu_burn(ms)
    _r.incr(DONE_KEY)


@app.task
def hold():
    _r.incr(STARTED_KEY)
    time.sleep(HOLD_S)


@app.task
def chaos(tag):
    time.sleep(0.5)
    _r.rpush(EXEC_KEY, tag)


@acks_late_app.task(acks_late=True, reject_on_worker_lost=True, max_retries=5)
def chaos_acks_late(tag):
    time.sleep(0.5)
    _r.rpush(EXEC_KEY, tag)


@app.task
def segfault():
    ctypes.string_at(0)


def enqueue(jobs):
    tasks = globals()
    for name, args in jobs:
        tasks[name].delay(*args)
