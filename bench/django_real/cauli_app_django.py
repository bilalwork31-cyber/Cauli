"""cauli app for the django_real benchmark: ONE worker process consuming ALL
queues (campaign + persist + bg fill). django.setup() runs once, before task
definitions, via django_boot - the whole Django app is paid for exactly once.

All tasks are sync kind="io" (Django ORM is synchronous) and run on cauli's
worker thread pool; send concurrency inside a batch comes from the shared
process-global send pool (DJ_SEND_POOL) + the same per-page semaphores as
Celery.

DJ_CAULI_KIND=cpu (symmetric-topology benchmark) registers every task
kind="cpu" instead: all work runs on the fork-server child pool
(--cpu-workers N x --cpu-child-threads M), i.e. N frozen forked GILs with M
in-flight tasks each - the same process/thread budget shape as Celery
prefork -c N with its per-batch thread pool. Registry kind is authoritative
worker-side (PROTOCOL §2), so the worker's env decides the lane.
"""

import os

import django_boot  # noqa: F401  (django.setup() BEFORE task definitions)

from cauli import Cauli

import tasks_shared as ts
import store as redis_store

_PORT = int(os.environ.get("BENCH_REDIS_PORT", "6396"))
_KIND = os.environ.get("DJ_CAULI_KIND", "io").strip().lower()

app = Cauli(
    redis_url=f"redis://127.0.0.1:{_PORT}/0",
    default_queue="default",
    result_ttl=3600,
    idemp_ttl=86400,
)


@app.task(
    name="campaign.dispatch",
    kind=_KIND,
    queue="dispatch",
    max_retries=0,
    timeout=120.0,
    store_result=False,
)
def dispatch(campaign_id):
    tick = ts.dispatch_tick(campaign_id)
    for batch in tick["batches"]:
        send_batch.apply_async(args=(campaign_id, batch), queue=tick["queue"])
    return {"claimed": tick["claimed"], "queue": tick["queue"]}


@app.task(
    name="campaign.send_batch",
    kind=_KIND,
    max_retries=0,
    timeout=900.0,
    store_result=False,
)
def send_batch(campaign_id, batch):
    return ts.send_batch(campaign_id, batch)


@app.task(
    name="persist.drain",
    kind=_KIND,
    queue="persist",
    max_retries=0,
    timeout=300.0,
    store_result=False,
)
def persist_drain():
    def reenqueue(countdown):
        persist_drain.apply_async(queue="persist", countdown=countdown)

    return ts.persist_drain_and_chain(reenqueue)


@app.task(
    name="bgfill.ghost_job",
    kind=_KIND,
    queue="backfill_heavy",
    max_retries=0,
    timeout=300.0,
    store_result=False,
)
def ghost_job():
    n = ts.ghost_job()
    if redis_store.bg_active():
        ghost_job.apply_async(queue="backfill_heavy")
    return n


@app.task(
    name="webhook.drain",
    kind=_KIND,
    queue="webhook_ingest",
    max_retries=0,
    timeout=120.0,
    store_result=False,
)
def webhook_drain():
    return ts.webhook_drain_tick()
