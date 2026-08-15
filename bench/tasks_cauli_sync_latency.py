"""Latency-tagged sync task, cauli. Run with: cauli-worker -A bench.tasks_cauli_sync_latency:app -c ..."""

import time

import redis
from cauli import Cauli

from common import REDIS_URL

LATENCY_KEY = "bench:latencies"

app = Cauli(redis_url=REDIS_URL)
_r = redis.Redis.from_url(REDIS_URL)


@app.task(store_result=False, max_retries=0)
def noop(scheduled_ts: float):
    latency_ms = (time.monotonic() - scheduled_ts) * 1000
    _r.rpush(LATENCY_KEY, latency_ms)
