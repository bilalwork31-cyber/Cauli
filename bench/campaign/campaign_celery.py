"""Celery stack wrapper (production chatsx topology, bench-scaled).

Broker db 0, backend db 1 on the suite redis; acks_late=True, prefetch=1,
json. Measurement is store-based (db 3), so task results are ignored on both
stacks (cauli matches with store_result=False).

Queues -> workers (started by runner_campaign.sh inside ONE 1G scope):
  celery,backfill_heavy,webhook_ingest   default worker  -c 2
  campaign_long                          -c 4 --max-tasks-per-child=1000
  campaign_short                         -c 2
  dispatch                               --pool=solo
"""

import os

from celery import Celery

import bgfill_common
import campaign_common
import store

_PORT = int(os.environ.get("BENCH_REDIS_PORT", "6390"))

app = Celery(
    "campaign",
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
        "bgfill.ghost_job": {"queue": "backfill_heavy"},
        "webhook.drain": {"queue": "webhook_ingest"},
    },
)


@app.task(name="campaign.dispatch")
def dispatch(campaign_id):
    tick = campaign_common.dispatch_tick(campaign_id)
    for batch in tick["batches"]:
        send_batch.apply_async(args=[campaign_id, batch], queue=tick["queue"])
    return {"claimed": tick["claimed"], "queue": tick["queue"]}


@app.task(name="campaign.send_batch")
def send_batch(campaign_id, batch):
    return campaign_common.send_batch_sync(campaign_id, batch)


@app.task(name="bgfill.ghost_job")
def ghost_job():
    n = bgfill_common.ghost_job()
    if store.bg_active():
        ghost_job.apply_async(queue="backfill_heavy")
    return n


@app.task(name="webhook.drain")
def webhook_drain():
    return bgfill_common.webhook_drain_tick()
