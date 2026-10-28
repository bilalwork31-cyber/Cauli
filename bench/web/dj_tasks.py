"""The cauli app and tasks for the Django side, laid out the way
cauli.contrib.django documents it: django_app() first, then the task module
imports its models."""

import os
import time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "web.dj_settings")

import redis  # noqa: E402
from cauli.contrib.django import django_app  # noqa: E402

from common import REDIS_URL  # noqa: E402

app = django_app(redis_url=REDIS_URL)

from django.db.models import F  # noqa: E402

from djapp.models import WebJob  # noqa: E402
from web.common import LATENCY_KEY  # noqa: E402

_r = redis.Redis.from_url(REDIS_URL)
_ar = None


@app.task(store_result=False, max_retries=0)
def process_job(job_id, t0_ns):
    # A missing row means the task ran before the request's transaction
    # committed; raising makes that visible as a dead letter, not a no-op.
    if not WebJob.objects.filter(id=job_id).update(processed=F("processed") + 1):
        raise LookupError(f"web_job {job_id} not visible to the worker")
    _r.rpush(LATENCY_KEY, time.time_ns() - t0_ns)


@app.task(store_result=False, max_retries=0)
async def aprocess_job(job_id, t0_ns):
    global _ar
    if not await WebJob.objects.filter(id=job_id).aupdate(processed=F("processed") + 1):
        raise LookupError(f"web_job {job_id} not visible to the worker")
    if _ar is None:
        import redis.asyncio as aredis

        _ar = aredis.Redis.from_url(REDIS_URL)
    await _ar.rpush(LATENCY_KEY, time.time_ns() - t0_ns)
