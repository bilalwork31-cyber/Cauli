"""Test 4: AsyncResult against result JSONs written directly to redis (PROTOCOL.md section 8)."""

from __future__ import annotations

import json
import threading
import time
import uuid

import pytest

from rupy import AsyncResult, TaskFailedError


def _write_result(redis_client, task_id, doc):
    redis_client.set(f"rupy:result:{task_id}", json.dumps(doc))


def _ar(app):
    return AsyncResult(uuid.uuid4().hex, app)


def test_pending_when_key_absent(app):
    ar = _ar(app)
    assert ar.status() == "pending"


def test_success(app, redis_client):
    ar = _ar(app)
    _write_result(
        redis_client,
        ar.id,
        {
            "status": "success",
            "result": {"n": 5, "ok": [1, 2]},
            "error": None,
            "finished_at": 123,
        },
    )
    assert ar.status() == "success"
    assert ar.get(timeout=1) == {"n": 5, "ok": [1, 2]}
    assert ar.duplicate is False


def test_failure_raises_taskfailederror_with_attrs(app, redis_client):
    ar = _ar(app)
    _write_result(
        redis_client,
        ar.id,
        {
            "status": "failure",
            "result": None,
            "error": {
                "type": "ValueError",
                "message": "bad input",
                "traceback": "Traceback (most recent call last):\n...",
            },
            "finished_at": 123,
        },
    )
    assert ar.status() == "failure"
    with pytest.raises(TaskFailedError) as excinfo:
        ar.get(timeout=1)
    err = excinfo.value
    assert err.type == "ValueError"
    assert err.message == "bad input"
    assert err.traceback.startswith("Traceback")
    assert "ValueError" in str(err) and "bad input" in str(err)


def test_duplicate_returns_none_and_sets_flag(app, redis_client):
    ar = _ar(app)
    _write_result(
        redis_client,
        ar.id,
        {"status": "duplicate", "result": None, "error": None, "finished_at": 123},
    )
    assert ar.duplicate is False
    assert ar.status() == "duplicate"
    assert ar.get(timeout=1) is None
    assert ar.duplicate is True


def test_pending_timeout_raises_timeouterror(app):
    ar = _ar(app)
    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        ar.get(timeout=0.3)
    assert time.monotonic() - t0 >= 0.25


def test_get_polls_until_late_result_arrives(app, redis_client):
    ar = _ar(app)

    def write_later():
        time.sleep(0.4)
        _write_result(
            redis_client,
            ar.id,
            {"status": "success", "result": "late", "error": None, "finished_at": 1},
        )

    writer = threading.Thread(target=write_later)
    writer.start()
    t0 = time.monotonic()
    try:
        assert ar.get(timeout=5) == "late"
    finally:
        writer.join()
    assert time.monotonic() - t0 >= 0.3, (
        "get() must actually have waited for the result"
    )
