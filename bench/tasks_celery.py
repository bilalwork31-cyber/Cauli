"""Celery app under test. Broker db 0, backend db 1 on the bench redis.

celeryconfig fairness block (production fair, matched to rupy semantics):
- task_acks_late=True: message is acked after execution, like rupy's
  XACK after completion (at least once delivery on both stacks).
- worker_prefetch_multiplier=1: no prefetch hoarding; each slot takes one
  message at a time, matching rupy's admission gate. This is the standard
  production setting for long tasks.
- task_ignore_result=False + result backend: both stacks pay one result
  write per task (rupy always writes rupy:result:{id} when store_result).
- result_extended=False: store the minimal meta. date_done is stored
  regardless and is what the driver uses for completion timestamps.
- result_expires=3600 matches rupy result_ttl=3600.
- json serializer both directions (rupy envelopes and results are JSON).
- No task retries configured: benchmark tasks either succeed or fail once,
  matching tasks_rupy.py max_retries=0.
- Everything else stays at Celery defaults (gossip, mingle, heartbeat) so
  Celery runs the way it ships in production.

Port comes from BENCH_REDIS_PORT (default 6390) so verification runs can use
an alternate throwaway redis without touching this file.
"""
import os

from celery import Celery

import common

_PORT = int(os.environ.get("BENCH_REDIS_PORT", "6390"))

app = Celery(
    "bench",
    broker=f"redis://127.0.0.1:{_PORT}/0",
    backend=f"redis://127.0.0.1:{_PORT}/1",
)

app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_ignore_result=False,
    result_extended=False,
    result_expires=3600,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    enable_utc=True,
    timezone="UTC",
    broker_connection_retry_on_startup=True,
)


@app.task(name="bench.io_task")
def io_task() -> int:
    return common.io_call()


@app.task(name="bench.cpu_task")
def cpu_task() -> str:
    return common.cpu_call()
