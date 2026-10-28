"""The Django project for the web benchmark in one module: settings, model,
cauli tasks, views, URLs and the WSGI and ASGI callables.

It is its own settings module and its own Django app, so the settings come
first: django_app() runs django.setup(), which reads them from this module
while it is still importing. The model and everything that uses it follow.
"""

import os
import time
from urllib.parse import urlparse

from bench import PG_DSN, REDIS_URL, WEB_LATENCY_KEY, WEB_PAYLOAD

os.environ.setdefault("DJANGO_SETTINGS_MODULE", __name__)
_dsn = urlparse(PG_DSN)

SECRET_KEY = "bench-only-not-a-secret"
DEBUG = False
ALLOWED_HOSTS = ["*"]
ROOT_URLCONF = __name__
MIDDLEWARE: list[str] = []
USE_TZ = True
INSTALLED_APPS = [__name__]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _dsn.path.lstrip("/"),
        "USER": _dsn.username,
        "PASSWORD": _dsn.password,
        "HOST": _dsn.hostname,
        "PORT": _dsn.port or 5432,
        # Under ASGI each request's sync ORM work runs on a fresh thread, so a
        # persistent connection is never reused: with 60 the server reached
        # 385 of 400 Postgres connections in 15 s. The ASGI arm passes 0.
        "CONN_MAX_AGE": int(os.environ.get("BENCH_DJ_CONN_MAX_AGE", "60")),
    }
}

import redis  # noqa: E402
import redis.asyncio as aredis  # noqa: E402
from cauli.contrib.django import django_app  # noqa: E402

app = django_app(redis_url=REDIS_URL)

from celery import Celery  # noqa: E402
from django.core.asgi import get_asgi_application  # noqa: E402
from django.core.wsgi import get_wsgi_application  # noqa: E402
from django.db import models, transaction  # noqa: E402
from django.db.models import F  # noqa: E402
from django.http import HttpResponse, JsonResponse  # noqa: E402
from django.urls import path  # noqa: E402


class WebJob(models.Model):
    """One row per request. The task increments ``processed``, so after a
    drain 0 means lost and 2 or more means duplicated."""

    id = models.BigAutoField(primary_key=True)
    payload = models.TextField()
    processed = models.IntegerField(default=0)

    class Meta:
        app_label = __name__.rpartition(".")[2]
        db_table = "web_job"
        managed = False


_r = redis.Redis.from_url(REDIS_URL)
_ar = aredis.Redis.from_url(REDIS_URL)
# Producer only: no Celery worker runs, so its arm measures what .delay()
# costs inside the same request and nothing else.
celery_app = Celery(broker=REDIS_URL)


@app.task(store_result=False, max_retries=0)
def process_job(job_id, t0_ns):
    # A missing row means the task ran before the request's transaction
    # committed; raising makes that a dead letter instead of a silent no-op.
    if not WebJob.objects.filter(id=job_id).update(processed=F("processed") + 1):
        raise LookupError(f"web_job {job_id} not visible to the worker")
    _r.rpush(WEB_LATENCY_KEY, time.time_ns() - t0_ns)


@app.task(store_result=False, max_retries=0)
async def aprocess_job(job_id, t0_ns):
    if not await WebJob.objects.filter(id=job_id).aupdate(processed=F("processed") + 1):
        raise LookupError(f"web_job {job_id} not visible to the worker")
    await _ar.rpush(WEB_LATENCY_KEY, time.time_ns() - t0_ns)


@celery_app.task(name="web.process_job")
def celery_process_job(job_id, t0_ns):
    raise NotImplementedError("producer only arm, never executed")


def health(request):
    return HttpResponse("ok")


def insert(request):
    # Same transaction as the enqueue arms, so the difference is the enqueue.
    with transaction.atomic():
        job = WebJob.objects.create(payload=WEB_PAYLOAD)
    return JsonResponse({"id": job.id})


def cauli_on_commit(request):
    t0 = time.time_ns()
    with transaction.atomic():
        job = WebJob.objects.create(payload=WEB_PAYLOAD)
        process_job.delay_on_commit(job.id, t0)
    return JsonResponse({"id": job.id})


def cauli_async_task(request):
    t0 = time.time_ns()
    with transaction.atomic():
        job = WebJob.objects.create(payload=WEB_PAYLOAD)
        aprocess_job.delay_on_commit(job.id, t0)
    return JsonResponse({"id": job.id})


def celery_on_commit(request):
    t0 = time.time_ns()
    with transaction.atomic():
        job = WebJob.objects.create(payload=WEB_PAYLOAD)
        transaction.on_commit(lambda: celery_process_job.delay(job.id, t0))
    return JsonResponse({"id": job.id})


async def ainsert(request):
    job = await WebJob.objects.acreate(payload=WEB_PAYLOAD)
    return JsonResponse({"id": job.id})


async def acauli(request):
    t0 = time.time_ns()
    job = await WebJob.objects.acreate(payload=WEB_PAYLOAD)
    await process_job.adelay(job.id, t0)
    return JsonResponse({"id": job.id})


urlpatterns = [
    path("health", health),
    path("insert", insert),
    path("cauli", cauli_on_commit),
    path("cauli_async_task", cauli_async_task),
    path("celery", celery_on_commit),
    path("ainsert", ainsert),
    path("acauli", acauli),
]
wsgi = get_wsgi_application()
asgi = get_asgi_application()
