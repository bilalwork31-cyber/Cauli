"""MEM-5 repro + regression: worker/src/shim.py's per loop async submission
queue (`_pending[idx]`) used to have no ceiling. A blocking call inside an
async task (sync HTTP, time.sleep, a blocking DB driver) starves that loop's
own callback processing forever; `_drain` then never runs again and
`submit_async` kept appending real args/kwargs objects to `_pending[idx]`
without bound, for as long as the wedge lasted. The Rust side `pending_async`
stat cannot see this: it is a separate map that self heals on its own
backstop timer regardless of Python health (see worker/src/pyrt.rs MEM-1 and
worker/src/loops.rs stats_loop).

This drives shim.py's submit path directly instead of a full worker + redis
run: the bug and its fix live entirely in this one pure Python module, so a
direct import is both faster and more deterministic than an end to end test.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

SHIM_PATH = Path(__file__).resolve().parents[2] / "worker" / "src" / "shim.py"


def _load_shim():
    # A fresh module per test: _loops/_pending/_registry are module globals,
    # so reusing one import across tests would leak daemon threads and state
    # between them.
    name = f"cauli_shim_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, SHIM_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def shim():
    return _load_shim()


def _wedge_one_loop(shim):
    """Start 1 loop and jam it with a task that blocks the OS thread instead
    of awaiting, then wait until that task is actually running. Returns the
    Event to set to release it."""
    entered = threading.Event()
    release = threading.Event()

    async def wedge():
        entered.set()
        # A blocking call inside an async def, not an await: this starves
        # the loop of its own event loop turns exactly like a synchronous
        # HTTP request or time.sleep would in a real task body.
        release.wait(timeout=30)
        return "released"

    shim._registry["wedge"] = SimpleNamespace(fn=wedge)
    shim.start_loops(1)
    shim.submit_async(1, "wedge", (), {}, 30.0)
    assert entered.wait(timeout=5), "wedge task never started running"
    return release


def test_wedged_loop_pending_queue_is_capped_and_rejects_fast(shim, capsys):
    async def fast(n):
        return n

    shim._registry["fast"] = SimpleNamespace(fn=fast)
    outcomes = []
    shim.set_callback(lambda token, out: outcomes.append((token, out)))
    release = _wedge_one_loop(shim)

    try:
        cap = shim._PENDING_CAP
        flood = cap + 200
        rejected = []
        started = time.monotonic()
        for i in range(flood):
            try:
                shim.submit_async(100 + i, "fast", (i,), {}, 5.0)
            except shim.AsyncQueueFull as e:
                rejected.append(e)
        elapsed = time.monotonic() - started

        pending_len = len(shim._pending[0])

        # The actual bug: without a cap this reaches `flood`, not `cap`.
        assert pending_len == cap, f"expected queue capped at {cap}, got {pending_len}"
        assert len(rejected) == flood - cap

        # Rejecting must be immediate, not blocked on anything: the whole
        # flood (thousands of calls) has to finish in well under a second,
        # not hang until some timeout.
        assert elapsed < 2.0, (
            f"submissions past the cap took {elapsed:.3f}s, expected near instant"
        )

        # Exactly one warning, not one per rejection (that would flood the log).
        err = capsys.readouterr().err
        assert err.count("queue hit its cap") == 1
    finally:
        release.set()

    # Once the wedge clears, the loop drains normally and new submissions
    # succeed again: the cap is a ceiling, not a permanent latch.
    deadline = time.monotonic() + 5
    while len(outcomes) < cap + 1 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(outcomes) == cap + 1  # 1 wedge task + `cap` fast tasks drained

    recovered = threading.Event()
    shim.set_callback(lambda token, out: recovered.set())
    shim.submit_async(999999, "fast", (1,), {}, 5.0)
    assert recovered.wait(timeout=5), (
        "loop did not resume accepting/running tasks after the wedge cleared"
    )


def test_healthy_loop_is_unaffected_by_the_cap(shim):
    """Guard against the cap check itself breaking ordinary submissions."""

    async def fast(n):
        return n * 2

    shim._registry["fast"] = SimpleNamespace(fn=fast)
    done = threading.Event()
    results = []

    def cb(token, out):
        results.append((token, out))
        done.set()

    shim.set_callback(cb)
    shim.start_loops(1)

    for i in range(20):
        done.clear()
        shim.submit_async(i, "fast", (i,), {}, 5.0)
        assert done.wait(timeout=5)

    assert [out["result"] for _, out in results] == [i * 2 for i in range(20)]
