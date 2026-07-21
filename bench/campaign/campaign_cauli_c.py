"""Scenario C cauli app: per PROTOCOL section 6, additive file.

C runs the ASYNC send variant by default (CAULI_VARIANT defaults to "async"
here; env can still force sync for debugging). persist.drain is a sync io
task (psycopg2 blocks; runs on the worker thread pool) that self-chains via
apply_async(countdown=...).
"""

import os

from cauli import Cauli

import bgfill_common
import campaign_common_c
import persist_common
import store

_PORT = int(os.environ.get("BENCH_REDIS_PORT", "6390"))

app = Cauli(
    redis_url=f"redis://127.0.0.1:{_PORT}/0",
    default_queue="default",
    result_ttl=3600,
    idemp_ttl=86400,
)


@app.task(
    name="campaign.dispatch",
    kind="io",
    queue="dispatch",
    max_retries=0,
    timeout=120.0,
    store_result=False,
)
def dispatch(campaign_id):
    tick = campaign_common_c.dispatch_tick(campaign_id)
    variant = os.environ.get("CAULI_VARIANT", "async")
    target = send_batch_sync if variant == "sync" else send_batch_async
    for batch in tick["batches"]:
        target.apply_async(args=(campaign_id, batch), queue=tick["queue"])
    return {"claimed": tick["claimed"], "queue": tick["queue"]}


@app.task(
    name="campaign.send_batch_sync",
    kind="io",
    max_retries=0,
    timeout=900.0,
    store_result=False,
)
def send_batch_sync(campaign_id, batch):
    return campaign_common_c.send_batch_sync_c(campaign_id, batch)


@app.task(
    name="campaign.send_batch_async",
    kind="io",
    max_retries=0,
    timeout=900.0,
    store_result=False,
)
async def send_batch_async(campaign_id, batch):
    return await campaign_common_c.send_batch_async_c(campaign_id, batch)


@app.task(
    name="persist.drain",
    kind="io",
    queue="persist",
    max_retries=0,
    timeout=300.0,
    store_result=False,
)
def persist_drain():
    def reenqueue(countdown):
        persist_drain.apply_async(queue="persist", countdown=countdown)

    return persist_common.drain_and_chain(reenqueue)


@app.task(
    name="bgfill.ghost_job",
    kind="io",
    queue="backfill_heavy",
    max_retries=0,
    timeout=300.0,
    store_result=False,
)
def ghost_job():
    n = bgfill_common.ghost_job()
    if store.bg_active():
        ghost_job.apply_async(queue="backfill_heavy")
    return n


@app.task(
    name="webhook.drain",
    kind="io",
    queue="webhook_ingest",
    max_retries=0,
    timeout=120.0,
    store_result=False,
)
def webhook_drain():
    return bgfill_common.webhook_drain_tick()
