"""cauli app under test, per PROTOCOL.md section 6.

Same throwaway redis as Celery (port from BENCH_REDIS_PORT, default 6390).
cauli keeps all its keys under cauli:* in db 0; Celery uses db 0 (broker) and
db 1 (backend). Runs are sequential with FLUSHALL between scenarios, so there
is no interference.

Fairness, matched to tasks_celery.py:
- store_result=True on all tasks (both stacks pay one result write per task).
- result_ttl=3600 matches celery result_expires=3600.
- max_retries=0: Celery tasks do not auto retry on exception, so cauli must
  not either (a failure is final on both stacks).
- Generous timeouts so the benchmark never trips them.
"""

import os

from cauli import Cauli

import common

_PORT = int(os.environ.get("BENCH_REDIS_PORT", "6390"))

app = Cauli(
    redis_url=f"redis://127.0.0.1:{_PORT}/0",
    default_queue="default",
    result_ttl=3600,
    idemp_ttl=86400,
)


@app.task(name="bench.io_task", kind="io", max_retries=0, timeout=120.0)
def io_task() -> int:
    """Sync io task: runs on the worker's Python thread pool."""
    return common.io_call()


@app.task(name="bench.io_task_async", kind="io", max_retries=0, timeout=120.0)
async def io_task_async() -> int:
    """Async io task: runs on the worker's embedded asyncio loop."""
    return await common.io_call_async()


@app.task(name="bench.cpu_task", kind="cpu", max_retries=0, timeout=300.0)
def cpu_task() -> str:
    """cpu task: runs in a cauli child process (python3 -m cauli._exec)."""
    return common.cpu_call()
