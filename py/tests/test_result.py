"""Test 4: AsyncResult against result JSONs written directly to redis (PROTOCOL.md section 8)."""

from __future__ import annotations

import json
import threading
import time
import uuid

import pytest

from cauli import AsyncResult, TaskFailedError


def _write_result(redis_client, task_id, doc):
    redis_client.set(f"cauli:result:{task_id}", json.dumps(doc))


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


def test_origin_is_exposed_and_absent_reads_as_unknown(app, redis_client):
    # Section 8 `origin`, read through as one attribute. A worker predating
    # the field sends no origin at all, which must surface as None rather
    # than be guessed at.
    for sent, expected in (("task", "task"), ("worker", "worker"), (None, None)):
        ar = _ar(app)
        error = {"type": "ValueError", "message": "boom", "traceback": ""}
        if sent is not None:
            error["origin"] = sent
        _write_result(
            redis_client,
            ar.id,
            {"status": "failure", "result": None, "error": error, "finished_at": 1},
        )
        with pytest.raises(TaskFailedError) as excinfo:
            ar.get(timeout=1)
        assert excinfo.value.origin == expected


def test_client_synthesized_errors_carry_origin_client(app, redis_client):
    # InvalidResult never crosses the wire: this package mints it locally,
    # so it is the one error whose origin is "client".
    ar = _ar(app)
    redis_client.set(f"cauli:result:{ar.id}", b"{not json")
    with pytest.raises(TaskFailedError) as excinfo:
        ar.get(timeout=1)
    assert excinfo.value.type == "InvalidResult"
    assert excinfo.value.origin == "client"

    ar = _ar(app)
    _write_result(redis_client, ar.id, {"status": "sideways", "finished_at": 1})
    with pytest.raises(TaskFailedError) as excinfo:
        ar.get(timeout=1)
    assert excinfo.value.type == "InvalidResult"
    assert excinfo.value.origin == "client"


def test_expired_carries_worker_origin(app, redis_client):
    ar = _ar(app)
    _write_result(
        redis_client,
        ar.id,
        {
            "status": "expired",
            "result": None,
            "error": {
                "type": "Expired",
                "message": "gone",
                "traceback": "",
                "origin": "worker",
            },
            "finished_at": 1,
        },
    )
    with pytest.raises(TaskFailedError) as excinfo:
        ar.get(timeout=1)
    assert excinfo.value.type == "Expired"
    assert excinfo.value.origin == "worker"


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


def test_pending_timeout_message_does_not_claim_the_task_is_pending(app):
    # A task that ran and succeeded reads exactly the same (no result key)
    # once result_ttl has elapsed, so the message must not assert "pending"
    # as if that were a known fact; it must name the task id and state
    # plainly that no result key is present.
    ar = _ar(app)
    with pytest.raises(TimeoutError) as excinfo:
        ar.get(timeout=0.3)
    message = str(excinfo.value)
    assert "pending" not in message
    assert ar.id in message


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


def test_status_and_get_raise_named_error_for_undecodable_bytes(app, redis_client):
    ar = _ar(app)
    redis_client.set(f"cauli:result:{ar.id}", b"not json at all {{{")

    with pytest.raises(TaskFailedError) as excinfo:
        ar.status()
    assert excinfo.value.type == "InvalidResult"
    assert ar.id in str(excinfo.value)

    with pytest.raises(TaskFailedError) as excinfo:
        ar.get(timeout=1)
    assert excinfo.value.type == "InvalidResult"
    assert ar.id in str(excinfo.value)


def test_status_and_get_raise_named_error_for_a_json_array_document(app, redis_client):
    ar = _ar(app)
    redis_client.set(f"cauli:result:{ar.id}", json.dumps([1, 2, 3]))

    with pytest.raises(TaskFailedError) as excinfo:
        ar.status()
    assert excinfo.value.type == "InvalidResult"
    assert ar.id in str(excinfo.value)

    with pytest.raises(TaskFailedError) as excinfo:
        ar.get(timeout=1)
    assert excinfo.value.type == "InvalidResult"


def test_status_matches_get_for_a_dict_missing_the_status_field(app, redis_client):
    ar = _ar(app)
    _write_result(redis_client, ar.id, {"result": 1, "error": None, "finished_at": 1})

    # Existing good behaviour to match: get() already classifies this as
    # InvalidResult via its own fallthrough for an unrecognized status.
    with pytest.raises(TaskFailedError) as excinfo:
        ar.get(timeout=1)
    assert excinfo.value.type == "InvalidResult"

    # The inconsistency this test guards against: status() used to silently
    # return "pending" for this exact document instead of matching get().
    with pytest.raises(TaskFailedError) as excinfo:
        ar.status()
    assert excinfo.value.type == "InvalidResult"


def test_duplicate_exposes_the_claimant_and_its_result(app, redis_client):
    # PROTOCOL.md section 4.5: a claim is never released, so the only way a
    # suppressed caller can learn whether the work actually succeeded is the
    # claimant id carried on the duplicate result.
    ar = _ar(app)
    claimant_id = uuid.uuid4().hex
    _write_result(
        redis_client,
        ar.id,
        {
            "status": "duplicate",
            "result": None,
            "error": None,
            "claimant_id": claimant_id,
            "finished_at": 123,
        },
    )
    _write_result(
        redis_client,
        claimant_id,
        {
            "status": "success",
            "result": "did the work",
            "error": None,
            "finished_at": 1,
        },
    )

    assert ar.claimant() is None, "nothing known before get() resolves the document"
    assert ar.get(timeout=1) is None
    assert ar.duplicate is True
    assert ar.claimant_id == claimant_id

    claimant = ar.claimant()
    assert claimant is not None
    assert claimant.id == claimant_id
    assert claimant.get(timeout=1) == "did the work"


def test_duplicate_with_null_claimant_id_stays_none(app, redis_client):
    # Section 4.5 race: the claim key expired between the failed SET and the
    # GET of its holder, so the worker writes claimant_id null. That must not
    # turn into an AsyncResult for a task id of "None".
    ar = _ar(app)
    _write_result(
        redis_client,
        ar.id,
        {
            "status": "duplicate",
            "result": None,
            "error": None,
            "claimant_id": None,
            "finished_at": 123,
        },
    )
    assert ar.get(timeout=1) is None
    assert ar.claimant_id is None
    assert ar.claimant() is None


def test_status_reads_both_as_an_attribute_and_as_a_call(app, redis_client):
    # `if r.status == "success":` is the Celery idiom and used to compare a
    # bound method to a string, which is False forever and raises nothing.
    # `r.status()` is cauli's own spelling (PROTOCOL.md section 12) and every
    # existing call site uses it. Both have to be correct.
    ar = _ar(app)
    assert ar.status == "pending"
    assert ar.status() == "pending"

    _write_result(
        redis_client,
        ar.id,
        {"status": "success", "result": 7, "error": None, "finished_at": 1},
    )
    assert ar.status == "success"
    assert ar.status() == "success"
    assert isinstance(ar.status, str)
    assert f"{ar.status}" == "success"
    assert ar.status != "failure"


def test_calling_the_status_value_does_not_re_read_redis(app, redis_client):
    # `r.status()` has to stay one round trip, as it was when status was a
    # plain method: the read happens when the property is evaluated, and
    # calling the value just hands it back.
    ar = _ar(app)
    _write_result(
        redis_client,
        ar.id,
        {"status": "success", "result": 1, "error": None, "finished_at": 1},
    )
    value = ar.status
    redis_client.delete(f"cauli:result:{ar.id}")
    assert value() == "success"
    assert ar.status == "pending"
