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
    # Same shape as `flaky`, used with an idempotency_key (C1 regression):
    # the retry must actually execute, not resolve as "duplicate" against
    # its own earlier claim.
    with open(path, "a+") as f:
        f.seek(0)
        n = len(f.read())
        f.write("x")
    if n < fail_times:
        raise ValueError(f"attempt {n} fails")
    return {"succeeded_on_attempt": n}


@app.task(timeout=4, max_retries=0)
def slow_idemp(path, seconds):
    """Sleeps so a killed worker can be caught mid task; paired with an
    idempotency_key in the crash redelivery test (the MineAgain half of the
    C1 fix, as opposed to `flaky_idemp`'s scheduled retry half). Appends to
    `path` on every invocation attempt, so the test can tell exactly how
    many times the body was entered.
    """
    with open(path, "a") as f:
        f.write("x")
    import time

    time.sleep(seconds)
    return {"slow_idemp_done": True}


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
