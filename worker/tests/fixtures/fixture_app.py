"""Duck-typed fixture app for worker e2e tests.

Plain classes exposing exactly the PROTOCOL §6 introspection attributes;
no cauli package import. The worker reads everything via getattr.
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
    def __init__(
        self,
        name,
        fn,
        is_async=False,
        kind="io",
        queue=None,
        max_retries=3,
        timeout_ms=300000,
        soft_timeout_ms=None,
        backoff_base_ms=500,
        backoff_factor=2.0,
        backoff_max_ms=60000,
        jitter=True,
        store_result=True,
    ):
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


def slow_counted(counter_file, seconds=30.0):
    """Records one invocation (append-only counter) BEFORE sleeping, so a test
    can detect duplicate concurrent execution even while the task is still
    in flight (used by the H1 visibility-timeout regression test)."""
    n = 0
    if os.path.exists(counter_file):
        with open(counter_file) as f:
            n = int(f.read().strip() or 0)
    n += 1
    with open(counter_file, "w") as f:
        f.write(str(n))
    time.sleep(float(seconds))
    return n


async def aslow(seconds=30.0):
    await asyncio.sleep(float(seconds))
    return "aslow-done"


async def async_block(seconds=3.0):
    """A coroutine that blocks synchronously instead of awaiting.

    Wedges the ONE event loop thread it runs on: with no `await`, asyncio
    cannot run any other callback on that thread -- including its own
    `wait_for` timeout check -- until this call returns. Used to force the
    Rust-side backstop timeout path (MEM-1 regression: without cancel(), that
    path used to leak a pending-completion map entry forever)."""
    time.sleep(float(seconds))
    return "unreachable-in-time"


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


def cpu_slow_pid(seconds=1.0):  # fake_exec: sleep then report pid/tid
    time.sleep(float(seconds))
    return {}


def cpu_soft_slow(total=5.0):  # fake_exec: sliced sleep, soft-timeout target
    time.sleep(float(total))
    return "soft-done"


def cpu_die_once(counter_file):  # fake_exec: os._exit(9) once, then "revived"
    return "revived"


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
app.add(TaskDef("fx.slow_counted", slow_counted))
app.add(TaskDef("fx.aslow", aslow, is_async=True))
app.add(TaskDef("fx.async_block", async_block, is_async=True))
app.add(TaskDef("fx.soft_slow", soft_slow))
app.add(TaskDef("fx.bad_return", bad_return))
app.add(TaskDef("fx.cpu_echo", cpu_echo, kind="cpu"))
app.add(TaskDef("fx.cpu_slow", cpu_slow, kind="cpu"))
app.add(TaskDef("fx.cpu_slow_pid", cpu_slow_pid, kind="cpu"))
app.add(TaskDef("fx.cpu_soft_slow", cpu_soft_slow, kind="cpu"))
app.add(TaskDef("fx.cpu_die_once", cpu_die_once, kind="cpu"))
