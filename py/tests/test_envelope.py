"""Test 1: delay() XADDs a single-field 'e' envelope matching PROTOCOL.md section 2 exactly."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

import pytest

from helpers import ENVELOPE_KEYS, assert_default_option_fields, now_ms

from cauli import AsyncResult


def test_delay_xadds_exact_envelope(app, redis_client):
    @app.task()
    def add(a, b, scale=1):
        return (a + b) * scale

    t0 = now_ms()
    res = add.delay(1, 2, scale=3)
    t1 = now_ms()

    assert isinstance(res, AsyncResult)

    entries = redis_client.xrange("cauli:q:default")
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert set(fields.keys()) == {b"e"}, "stream entry must have exactly one field 'e'"

    env = json.loads(fields[b"e"])
    assert set(env.keys()) == ENVELOPE_KEYS, (
        "envelope must have exactly the 19 spec fields"
    )

    assert env["v"] == 1
    assert isinstance(env["id"], str) and re.fullmatch(r"[0-9a-f]{32}", env["id"])
    assert env["id"] == res.id
    assert env["task"] == f"{add.fn.__module__}.{add.fn.__qualname__}"
    assert env["args"] == [1, 2]
    assert env["kwargs"] == {"scale": 3}
    assert env["queue"] == "default"
    assert_default_option_fields(env)
    assert isinstance(env["enqueued_at"], int)
    assert t0 <= env["enqueued_at"] <= t1
    assert env["not_before"] is None

    # nothing leaked into the delayed zset
    assert redis_client.exists("cauli:delayed:default") == 0


def test_each_delay_gets_unique_id_and_entry(app, redis_client):
    @app.task()
    def ping():
        return "pong"

    r1 = ping.delay()
    r2 = ping.delay()
    assert r1.id != r2.id

    entries = redis_client.xrange("cauli:q:default")
    assert len(entries) == 2
    ids = {json.loads(fields[b"e"])["id"] for _sid, fields in entries}
    assert ids == {r1.id, r2.id}


@pytest.mark.parametrize(
    "options",
    [
        {"idempotency_key": 42},
        {"expires": datetime(1960, 1, 1, tzinfo=timezone.utc)},
        {"expires": -2e12},
        {"expires": 1e17},
    ],
    ids=[
        "int_idempotency_key",
        "expires_before_epoch",
        "negative_expires",
        "huge_expires",
    ],
)
def test_options_the_worker_cannot_parse_are_refused_at_the_call_site(
    app, redis_client, options
):
    # The worker parses these fields as a string and an unsigned 64 bit
    # integer. Anything else fails the whole parse, the task id cannot be
    # recovered, no result is ever written, and get() waits forever.
    @app.task()
    def t():
        return None

    with pytest.raises((TypeError, ValueError)):
        t.apply_async(**options)
    assert redis_client.xlen("cauli:q:default") == 0


def test_arguments_nested_past_the_worker_parse_limit_are_refused(app, redis_client):
    @app.task()
    def t(x):
        return None

    def nest(depth):
        value = 1
        for _ in range(depth):
            value = [value]
        return value

    t.delay(nest(125))
    with pytest.raises(ValueError, match="nested"):
        t.delay(nest(126))
    assert redis_client.xlen("cauli:q:default") == 1
