"""Scenario C Celery app: production topology tasks + stage-2 persister.

Same broker/backend/conf as campaign_celery (additive file, own app object).
Extra queue `persist` is served by an additional prefork worker (-c 2) inside
the same 1G scope (see runner_c.sh).
"""

import os

from celery import Celery

import bgfill_common
import campaign_common_c
import persist_common
import store

_PORT = int(os.environ.get("BENCH_REDIS_PORT", "6390"))

app = Celery(
    "campaign_c",
    broker=f"redis://127.0.0.1:{_PORT}/0",
    backend=f"redis://127.0.0.1:{_PORT}/1",
)

app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_ignore_result=True,
    result_expires=3600,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    enable_utc=True,
    timezone="UTC",
    broker_connection_retry_on_startup=True,
    task_routes={
        "campaign.dispatch": {"queue": "dispatch"},
        "persist.drain": {"queue": "persist"},
        "bgfill.ghost_job": {"queue": "backfill_heavy"},
        "webhook.drain": {"queue": "webhook_ingest"},
    },
)


@app.task(name="campaign.dispatch")
def dispatch(campaign_id):
    tick = campaign_common_c.dispatch_tick(campaign_id)
    for batch in tick["batches"]:
        send_batch.apply_async(args=[campaign_id, batch], queue=tick["queue"])
    return {"claimed": tick["claimed"], "queue": tick["queue"]}


@app.task(name="campaign.send_batch")
def send_batch(campaign_id, batch):
    return campaign_common_c.send_batch_sync_c(campaign_id, batch)


@app.task(name="persist.drain")
def persist_drain():
    def reenqueue(countdown):
        persist_drain.apply_async(queue="persist", countdown=countdown)

    return persist_common.drain_and_chain(reenqueue)


@app.task(name="bgfill.ghost_job")
def ghost_job():
    n = bgfill_common.ghost_job()
    if store.bg_active():
        ghost_job.apply_async(queue="backfill_heavy")
    return n


@app.task(name="webhook.drain")
def webhook_drain():
    return bgfill_common.webhook_drain_tick()
