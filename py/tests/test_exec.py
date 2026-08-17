"""Test 6: cauli._exec subprocess end to end over the line pipe protocol (PROTOCOL.md 5.1)."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from helpers import ExecChild

TESTS_DIR = Path(__file__).resolve().parent


@pytest.fixture()
def child():
    c = ExecChild(TESTS_DIR)
    ready = c.read_json(timeout=20)
    assert ready == {"ready": True, "pid": c.proc.pid}
    yield c
    c.terminate()


def test_ready_line_and_roundtrip(child):
    resp = child.request(
        {
            "id": "r1",
            "task": "add",
            "args": [2, 3],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "r1", "ok": True, "result": 5}

    # kwargs and sequential requests on the same child
    resp = child.request(
        {
            "id": "r2",
            "task": "add",
            "args": [1],
            "kwargs": {"b": 10},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "r2", "ok": True, "result": 11}


def test_task_exception_reported_not_fatal(child):
    resp = child.request(
        {"id": "e1", "task": "boom", "args": [], "kwargs": {}, "soft_timeout_ms": None}
    )
    assert resp["id"] == "e1"
    assert resp["ok"] is False
    assert "retry" not in resp
    err = resp["error"]
    assert err["type"] == "ValueError"
    assert err["message"] == "kaboom"
    assert "ValueError: kaboom" in err["traceback"]

    # the child survived and keeps serving
    resp = child.request(
        {
            "id": "e2",
            "task": "add",
            "args": [4, 4],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "e2", "ok": True, "result": 8}


def test_traceback_truncated_to_8kb(child):
    resp = child.request(
        {
            "id": "b1",
            "task": "bigfail",
            "args": [],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp["ok"] is False
    assert resp["error"]["type"] == "ValueError"
    assert len(resp["error"]["traceback"]) <= 8192
    assert resp["error"]["message"] == "x" * 20000  # message itself is not truncated


def test_soft_timeout_interrupts_sleep(child):
    t0 = time.monotonic()
    resp = child.request(
        {
            "id": "s1",
            "task": "sleepy",
            "args": [2],
            "kwargs": {},
            "soft_timeout_ms": 200,
        },
        timeout=10,
    )
    elapsed = time.monotonic() - t0
    assert elapsed < 1.5, f"soft timeout response took {elapsed:.2f}s, expected ~0.2s"
    assert resp["id"] == "s1"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "SoftTimeLimitExceeded"

    # timer is disarmed afterwards: a slow-ish task with no soft timeout completes
    resp = child.request(
        {
            "id": "s2",
            "task": "sleepy",
            "args": [0.3],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "s2", "ok": True, "result": "done"}


def test_retry_response_with_countdown(child):
    resp = child.request(
        {
            "id": "t1",
            "task": "retryme",
            "args": [2.5],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp["id"] == "t1"
    assert resp["ok"] is False
    assert resp["retry"] is True
    assert resp["countdown"] == 2.5
    assert resp["error"]["type"] == "Retry"


def test_retry_response_without_countdown(child):
    resp = child.request(
        {
            "id": "t2",
            "task": "retryme",
            "args": [],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp["ok"] is False
    assert resp["retry"] is True
    assert resp["countdown"] is None
    assert resp["error"]["type"] == "Retry"


def test_retry_recognized_by_duck_type_not_isinstance(child):
    # M6 regression: a "Retry" class that does NOT subclass cauli.exceptions.Retry
    # must still be recognized (name + .countdown duck typing), matching
    # worker/src/shim.py's rule for io tasks.
    resp = child.request(
        {
            "id": "t3",
            "task": "duck_retryme",
            "args": [1.5],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp["ok"] is False
    assert resp["retry"] is True
    assert resp["countdown"] == 1.5
    assert resp["error"]["type"] == "Retry"


def test_non_serializable_result_is_serialization_error(child):
    resp = child.request(
        {"id": "u1", "task": "unser", "args": [], "kwargs": {}, "soft_timeout_ms": None}
    )
    assert resp["id"] == "u1"
    assert resp["ok"] is False
    assert resp["error"]["type"] == "SerializationError"

    resp = child.request(
        {
            "id": "u2",
            "task": "add",
            "args": [1, 1],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "u2", "ok": True, "result": 2}


def test_async_task_runs_via_asyncio(child):
    resp = child.request(
        {
            "id": "a1",
            "task": "aadd",
            "args": [3, 4],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp == {"id": "a1", "ok": True, "result": 7}


def test_task_prints_do_not_corrupt_protocol(child):
    resp = child.request(
        {"id": "n1", "task": "noisy", "args": [], "kwargs": {}, "soft_timeout_ms": None}
    )
    assert resp == {"id": "n1", "ok": True, "result": "quiet"}


def test_unknown_task_is_reported(child):
    resp = child.request(
        {"id": "x1", "task": "nope", "args": [], "kwargs": {}, "soft_timeout_ms": None}
    )
    assert resp["id"] == "x1"
    assert resp["ok"] is False
    # Matches the worker's own pre dispatch registry check (PROTOCOL.md
    # section 8, worker/src/dispatch.rs): same error.type string, and
    # non retryable so a caller matching the documented sentinel actually
    # sees this path instead of it being retried under "max_retries".
    assert resp["error"]["type"] == "UnregisteredTask"
    assert resp["retryable"] is False


def test_eof_exits_zero(child):
    resp = child.request(
        {
            "id": "z1",
            "task": "add",
            "args": [0, 0],
            "kwargs": {},
            "soft_timeout_ms": None,
        }
    )
    assert resp["ok"] is True
    rc = child.close(timeout=10)
    assert rc == 0
    child.drain_eof()
