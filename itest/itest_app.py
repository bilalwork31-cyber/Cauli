"""Integration test app: real rupy package driven by the real rupy-worker binary."""
import asyncio
import os

from rupy import Rupy, Retry, SoftTimeLimitExceeded  # noqa: F401

app = Rupy(
    redis_url=os.environ.get("RUPY_REDIS_URL", "redis://127.0.0.1:6394/0"),
    default_queue="default",
    result_ttl=600,
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


@app.task(max_retries=5, backoff_base=0.05, backoff_factor=1.0, backoff_max=0.1, jitter=False)
def flaky(path, fail_times):
    with open(path, "a+") as f:
        f.seek(0)
        n = len(f.read())
        f.write("x")
    if n < fail_times:
        raise ValueError(f"attempt {n} fails")
    return {"succeeded_on_attempt": n}


@app.task(max_retries=1, backoff_base=0.05, backoff_factor=1.0, backoff_max=0.1, jitter=False)
def always_fail():
    raise RuntimeError("nope")


@app.task()
def counted(path):
    with open(path, "a") as f:
        f.write("x")
    return "counted"


@app.task(kind="cpu", soft_timeout=0.3, timeout=10, max_retries=0)
def slow_cpu():
    import time
    time.sleep(5)
    return "should not get here"
