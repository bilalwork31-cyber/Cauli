"""Integration test app: real cauli package driven by the real cauli-worker binary."""

import asyncio
import os

from cauli import Cauli, Retry, SoftTimeLimitExceeded  # noqa: F401

app = Cauli(
    redis_url=os.environ.get("CAULI_REDIS_URL", "redis://127.0.0.1:6394/0"),
    default_queue="default",
    result_ttl=600,
    # PROTOCOL section 9.3: re-route by pattern without touching task code.
    task_routes={"*.routed_task": "routed"},
    # PROTOCOL section 9.2: anything sitting on `shortlived` more than 1s is
    # no longer worth running.
    queue_ttl={"shortlived": 1.0},
)


@app.task()
def echo(x):
    return {"echo": x}


@app.task()
async def aecho(x):
    await asyncio.sleep(0.05)
    return {"aecho": x}


@app.task(kind="cpu")
def cpu_math(n):
    return n * 2


@app.task(
    max_retries=5, backoff_base=0.05, backoff_factor=1.0, backoff_max=0.1, jitter=False
)
def flaky(path, fail_times):
    with open(path, "a+") as f:
        f.seek(0)
        n = len(f.read())
        f.write("x")
    if n < fail_times:
        raise ValueError(f"attempt {n} fails")
    return {"succeeded_on_attempt": n}


@app.task(
    max_retries=1, backoff_base=0.05, backoff_factor=1.0, backoff_max=0.1, jitter=False
)
def always_fail():
    raise RuntimeError("nope")


@app.task()
def counted(path):
    with open(path, "a") as f:
        f.write("x")
    return "counted"


@app.task(
    max_retries=5, backoff_base=0.05, backoff_factor=1.0, backoff_max=0.1, jitter=False
)
def flaky_idemp(path, fail_times):
    # Used with an idempotency_key: the retry must run, not resolve as a
    # duplicate of its own claim.
    with open(path, "a+") as f:
        f.seek(0)
        n = len(f.read())
        f.write("x")
    if n < fail_times:
        raise ValueError(f"attempt {n} fails")
    return {"succeeded_on_attempt": n}


@app.task(timeout=4, max_retries=0)
def slow_idemp(path, seconds):
    """Sleeps so a worker can be killed mid task (crash redelivery test).

    Appends to `path` on every attempt, counting how often the body ran.
    """
    with open(path, "a") as f:
        f.write("x")
    import time

    time.sleep(seconds)
    return {"slow_idemp_done": True}


@app.task(max_retries=0, timeout=0.5)
def redelivery_doomed(x):
    """Never runs: the redelivery limit test dead letters it on sight."""
    return x


@app.task(kind="cpu", soft_timeout=0.3, timeout=10, max_retries=0)
def slow_cpu():
    import time

    time.sleep(5)
    return "should not get here"


@app.task()
def marker(path):
    """Writes a file. Used to prove an EXPIRED task never actually ran."""
    with open(path, "a") as f:
        f.write("ran")
    return "ran"


@app.task()
def routed_task(x):
    return {"routed": x}


def _transit(args, kwargs):
    # repr shows exactly what arrived (key order, -0.0, big ints, unicode);
    # the values themselves go back out through the result path.
    return [repr(args), repr(kwargs), list(args), kwargs]


@app.task(max_retries=0)
def transit_sync(*args, **kwargs):
    return _transit(args, kwargs)


@app.task(max_retries=0)
async def transit_async(*args, **kwargs):
    return _transit(args, kwargs)


@app.task(kind="cpu", max_retries=0)
def transit_cpu(*args, **kwargs):
    return _transit(args, kwargs)
