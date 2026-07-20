"""Duck-typed fixture app for worker e2e tests.

Plain classes exposing exactly the PROTOCOL §6 introspection attributes;
no rupy package import. The worker reads everything via getattr.
"""
import asyncio
import os
import time


class Retry(Exception):
    """Recognized by the worker shim via type name 'Retry' + .countdown."""

    def __init__(self, countdown=None):
        super().__init__("retry requested")
        self.countdown = countdown


class TaskDef:
    def __init__(self, name, fn, is_async=False, kind="io", queue=None,
                 max_retries=3, timeout_ms=300000, soft_timeout_ms=None,
                 backoff_base_ms=500, backoff_factor=2.0, backoff_max_ms=60000,
                 jitter=True, store_result=True):
        self.name = name
        self.fn = fn
        self.is_async = is_async
        self.kind = kind
        self.queue = queue
        self.max_retries = max_retries
        self.timeout_ms = timeout_ms
        self.soft_timeout_ms = soft_timeout_ms
        self.backoff_base_ms = backoff_base_ms
        self.backoff_factor = backoff_factor
        self.backoff_max_ms = backoff_max_ms
        self.jitter = jitter
        self.store_result = store_result


def echo(*args, **kwargs):
    return {"args": list(args), "kwargs": kwargs}


async def aecho(*args, **kwargs):
    await asyncio.sleep(0.05)
    return {"args": list(args), "kwargs": kwargs}


def fail(msg="boom"):
    raise ValueError(msg)


def flaky(counter_file, fail_times):
    n = 0
    if os.path.exists(counter_file):
        with open(counter_file) as f:
            n = int(f.read().strip() or 0)
    n += 1
    with open(counter_file, "w") as f:
        f.write(str(n))
    if n <= int(fail_times):
        raise ValueError("flaky attempt %d" % n)
    return n


def retry_once(counter_file):
    if not os.path.exists(counter_file):
        with open(counter_file, "w") as f:
            f.write("1")
        raise Retry(countdown=0.2)
    return "after-retry"


def slow(seconds=30.0):
    time.sleep(float(seconds))
    return "slow-done"


async def aslow(seconds=30.0):
    await asyncio.sleep(float(seconds))
    return "aslow-done"


def soft_slow(total=5.0):
    # Sleep in small slices so PyThreadState_SetAsyncExc lands promptly.
    end = time.monotonic() + float(total)
    while time.monotonic() < end:
        time.sleep(0.05)
    return "soft-done"


def bad_return():
    return {1, 2, 3}  # a set: not JSON serializable


def cpu_echo(*args, **kwargs):  # executed by fake_exec.py, never in-process
    return {"args": list(args)}


def cpu_slow(seconds=30.0):
    time.sleep(float(seconds))
    return "slow-done"


class App:
    def __init__(self):
        self.redis_url = "redis://127.0.0.1:6392/0"
        self.default_queue = "default"
        self.result_ttl = 600
        self.idemp_ttl = 600
        self._tasks = {}

    def add(self, td):
        self._tasks[td.name] = td


app = App()
app.add(TaskDef("fx.echo", echo))
app.add(TaskDef("fx.aecho", aecho, is_async=True))
app.add(TaskDef("fx.fail", fail))
app.add(TaskDef("fx.flaky", flaky))
app.add(TaskDef("fx.retry_once", retry_once))
app.add(TaskDef("fx.slow", slow))
app.add(TaskDef("fx.aslow", aslow, is_async=True))
app.add(TaskDef("fx.soft_slow", soft_slow))
app.add(TaskDef("fx.bad_return", bad_return))
app.add(TaskDef("fx.cpu_echo", cpu_echo, kind="cpu"))
app.add(TaskDef("fx.cpu_slow", cpu_slow, kind="cpu"))
