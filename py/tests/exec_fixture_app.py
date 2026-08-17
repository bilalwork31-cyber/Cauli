"""Fixture app imported by the cauli._exec child in subprocess tests.

The child is spawned with cwd = this directory, so `--app exec_fixture_app:app`
resolves via the CWD sys.path entry. The redis URL points at a dead port on
purpose: _exec must never touch redis.
"""

import asyncio
import os
import time

from cauli import Retry, Cauli

app = Cauli(redis_url="redis://127.0.0.1:1/0")

# Env-gated lifecycle hooks: test_hooks.py points CAULI_TEST_HOOKLOG at a file
# and asserts the _exec child runs process_init once at startup and
# before/after around each request (PROTOCOL.md section 4.8).
_hooklog = os.environ.get("CAULI_TEST_HOOKLOG")
if _hooklog:

    def _mark(phase):
        with open(_hooklog, "a") as f:
            f.write(f"{phase} {os.getpid()}\n")

    app.process_init(lambda: _mark("process_init"))
    app.before_task(lambda: _mark("before"))
    app.after_task(lambda: _mark("after"))


@app.task(name="add", kind="cpu")
def add(a, b):
    return a + b


@app.task(name="boom", kind="cpu")
def boom():
    raise ValueError("kaboom")


@app.task(name="bigfail", kind="cpu")
def bigfail():
    raise ValueError("x" * 20000)


@app.task(name="sleepy", kind="cpu")
def sleepy(seconds):
    time.sleep(seconds)
    return "done"


@app.task(name="sleepy_slices", kind="cpu")
def sleepy_slices(seconds):
    # Sliced sleep: an injected async exception (threaded soft timeout via
    # PyThreadState_SetAsyncExc) only lands between bytecodes, never inside
    # one long C-level time.sleep call.
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        time.sleep(0.02)
    return "done"


@app.task(name="pidinfo", kind="cpu")
def pidinfo():
    import os
    import threading

    return {"pid": os.getpid(), "tid": threading.get_ident()}


@app.task(name="selfsignal", kind="cpu")
def selfsignal(sig):
    # Kills THIS child with a real signal (e.g. SIGSEGV), standing in for a
    # segfault or an OOM kill so the fork-server parent's reaper has a
    # WIFSIGNALED exit status to log.
    os.kill(os.getpid(), sig)
    return "unreachable"


@app.task(name="freeze_count", kind="cpu")
def freeze_count():
    # In a fork-server child this must be > 0: the parent froze its warmed
    # import image before forking (gc.freeze -> permanent generation).
    import gc

    return gc.get_freeze_count()


@app.task(name="retryme", kind="cpu")
def retryme(countdown=None):
    raise Retry(countdown)


class _DuckRetry(Exception):
    """A duck-typed lookalike of cauli.Retry that does NOT subclass it.

    Used to regression-test M6: `_exec.py` must recognize a forced retry by
    class NAME + `.countdown` (matching worker/src/shim.py's rule), not by
    `isinstance(exc, cauli.exceptions.Retry)` -- the old isinstance-based
    check would silently miss this and treat it as a plain failure. Renamed
    (rather than defined as `class Retry`) so it does not shadow the real
    `Retry` imported above, which `retryme` still needs.
    """

    def __init__(self, countdown=None):
        super().__init__("duck retry")
        self.countdown = countdown


_DuckRetry.__name__ = "Retry"


@app.task(name="duck_retryme", kind="cpu")
def duck_retryme(countdown=None):
    raise _DuckRetry(countdown)


@app.task(name="unser", kind="cpu")
def unser():
    return {1, 2, 3}  # a set is not JSON serializable


@app.task(name="noisy", kind="cpu")
def noisy():
    print("this print must NOT corrupt the pipe protocol")
    return "quiet"


@app.task(name="aadd", kind="cpu")
async def aadd(a, b):
    await asyncio.sleep(0)
    return a + b
