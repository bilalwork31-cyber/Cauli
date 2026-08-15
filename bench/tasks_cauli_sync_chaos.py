"""Chaos-test task, cauli sync: sleeps briefly (long enough to likely be
in-flight when the worker is kill -9'd), then records its tag. cauli's
delivery is at-least-once by design (Redis Streams consumer group +
visibility-timeout reclaim) -- no acks_late knob needed, unlike Celery.
Run with: cauli-worker -A tasks_cauli_sync_chaos:app -c ... --visibility-timeout 5
"""

import time

import redis
from cauli import Cauli

from common import REDIS_URL

EXEC_KEY = "bench:chaos:executions"

app = Cauli(redis_url=REDIS_URL)
_r = redis.Redis.from_url(REDIS_URL)


@app.task(store_result=False, max_retries=5, timeout=30)
def chaos(tag):
    time.sleep(0.5)
    _r.rpush(EXEC_KEY, tag)
