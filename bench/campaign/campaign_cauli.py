"""cauli stack wrapper, per PROTOCOL.md section 6.

Same queues as the Celery topology, one worker process (runner starts it with
--queues default,dispatch,campaign_short,campaign_long,backfill_heavy,webhook_ingest).

CAULI_VARIANT env (read by dispatch AT CALL TIME inside the worker) selects
which send task the dispatcher enqueues:
  sync  (default) -> campaign.send_batch_sync  (worker thread pool; calls the
                     IDENTICAL campaign_common.send_batch_sync Celery runs)
  async           -> campaign.send_batch_async (embedded asyncio loop)

max_retries=0 everywhere: the campaign has its own application-level retry
ladder (status=retry rows), so runtime-level task retries must not double it.
store_result=False matches celery task_ignore_result=True (measurement is
store-based).
"""

import os

from cauli import Cauli

import bgfill_common
import campaign_common
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
    tick = campaign_common.dispatch_tick(campaign_id)
    variant = os.environ.get("CAULI_VARIANT", "sync")
    target = send_batch_async if variant == "async" else send_batch_sync
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
    return campaign_common.send_batch_sync(campaign_id, batch)


@app.task(
    name="campaign.send_batch_async",
    kind="io",
    max_retries=0,
    timeout=900.0,
    store_result=False,
)
async def send_batch_async(campaign_id, batch):
    return await campaign_common.send_batch_async(campaign_id, batch)


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
