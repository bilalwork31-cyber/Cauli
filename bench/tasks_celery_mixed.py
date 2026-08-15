"""Adversarial mixed workload, Celery prefork: the structurally-immune
baseline -- one OS process per in-flight task, so a `poison` task's CPU
burst cannot touch a `light` task running in a different process. Expected
result: `light` latency does NOT spike. See CLAIMS.md #4.

Run with: celery -A tasks_celery_mixed worker -P prefork -c ...
"""

import time

import redis
from celery import Celery

from common import REDIS_URL
from workloads import POISON_BURST_MS, cpu_burn

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)


@app.task(ignore_result=True)
def light(scheduled_ts):
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    _r.rpush("bench:latencies:light", f"{scheduled_ts},{latency_ms}")


@app.task(ignore_result=True)
def poison(scheduled_ts):
    cpu_burn(POISON_BURST_MS)
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    _r.rpush("bench:latencies:poison", f"{scheduled_ts},{latency_ms}")
