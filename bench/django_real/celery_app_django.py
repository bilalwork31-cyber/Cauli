"""Celery app for the django_real benchmark: production topology tasks over
the real Django app. Same conf as campaign_celery_c (acks_late, prefetch 1,
json, redis broker db0/backend db1) on the throwaway redis 6396.

Every worker process imports this module -> django.setup() -> full app import
cost per fork, exactly like production.
"""

import os

import django_boot  # noqa: F401  (django.setup() BEFORE anything else)

from celery import Celery
from celery.signals import worker_process_init

from django.db import connections

import tasks_shared as ts
import store as redis_store

_PORT = int(os.environ.get("BENCH_REDIS_PORT", "6396"))

app = Celery(
    "django_real",
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


@worker_process_init.connect
def _reset_db_connections(**kwargs):
    """Never reuse a Postgres connection inherited across fork()."""
    connections.close_all()


# Symmetric-topology benchmark hook (env CAULI_BENCH_GC_FREEZE=1): give Celery
# the same copy-on-write treatment cauli's fork-server parent applies (§5.1).
# This module is imported by the prefork MASTER before it forks its children,
# so: warm every lazily-imported module the tasks touch at runtime, make sure
# NO live DB connection crosses fork(), then gc.collect() + gc.freeze() so the
# warmed import image moves to the permanent GC generation and children's GC
# never dirties those pages.
if os.environ.get("CAULI_BENCH_GC_FREEZE") == "1":
    import gc
    import json as _json  # noqa: F401
    import random as _random  # noqa: F401
    import concurrent.futures as _cf  # noqa: F401

    from django.db import connection as _conn

    _conn.ensure_connection()  # warm the psycopg backend import path...
    connections.close_all()  # ...but no live connection may cross fork()
    gc.collect()
    gc.freeze()


@app.task(name="campaign.dispatch")
def dispatch(campaign_id):
    tick = ts.dispatch_tick(campaign_id)
    for batch in tick["batches"]:
        send_batch.apply_async(args=[campaign_id, batch], queue=tick["queue"])
    return {"claimed": tick["claimed"], "queue": tick["queue"]}


@app.task(name="campaign.send_batch")
def send_batch(campaign_id, batch):
    return ts.send_batch(campaign_id, batch)


@app.task(name="persist.drain")
def persist_drain():
    def reenqueue(countdown):
        persist_drain.apply_async(queue="persist", countdown=countdown)

    return ts.persist_drain_and_chain(reenqueue)


@app.task(name="bgfill.ghost_job")
def ghost_job():
    n = ts.ghost_job()
    if redis_store.bg_active():
        ghost_job.apply_async(queue="backfill_heavy")
    return n


@app.task(name="webhook.drain")
def webhook_drain():
    return ts.webhook_drain_tick()
