"""URLs and views for the Django side. Each view writes one row through the
ORM; the enqueue arms then hand its id to a task, the way a real signup or
order view does."""

import time

from celery import Celery
from django.db import transaction
from django.http import HttpResponse, JsonResponse
from django.urls import path

from common import REDIS_URL
from djapp.models import WebJob
from web.common import PAYLOAD
from web.dj_tasks import aprocess_job, process_job

# Producer only reference arm: no Celery worker runs, so this measures what a
# Celery .delay() costs inside the same request, nothing else.
celery_app = Celery(broker=REDIS_URL)


@celery_app.task(name="web.process_job")
def celery_process_job(job_id, t0_ns):
    raise NotImplementedError("producer only arm, never executed")


def health(request):
    return HttpResponse("ok")


def insert(request):
    # Same transaction shape as the enqueue arms, so the difference between
    # this arm and theirs is the enqueue alone.
    with transaction.atomic():
        job = WebJob.objects.create(payload=PAYLOAD)
    return JsonResponse({"id": job.id})


def cauli_on_commit(request):
    t0 = time.time_ns()
    with transaction.atomic():
        job = WebJob.objects.create(payload=PAYLOAD)
        process_job.delay_on_commit(job.id, t0)
    return JsonResponse({"id": job.id})


def cauli_async_task(request):
    t0 = time.time_ns()
    with transaction.atomic():
        job = WebJob.objects.create(payload=PAYLOAD)
        aprocess_job.delay_on_commit(job.id, t0)
    return JsonResponse({"id": job.id})


def celery_on_commit(request):
    t0 = time.time_ns()
    with transaction.atomic():
        job = WebJob.objects.create(payload=PAYLOAD)
        transaction.on_commit(lambda: celery_process_job.delay(job.id, t0))
    return JsonResponse({"id": job.id})


async def ainsert(request):
    job = await WebJob.objects.acreate(payload=PAYLOAD)
    return JsonResponse({"id": job.id})


async def acauli(request):
    t0 = time.time_ns()
    job = await WebJob.objects.acreate(payload=PAYLOAD)
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
