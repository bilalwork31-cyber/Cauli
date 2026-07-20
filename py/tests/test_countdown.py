"""Test 2: countdown enqueues to the delayed zset (no XADD), score = now + countdown*1000."""
from __future__ import annotations

import json

from helpers import ENVELOPE_KEYS, assert_default_option_fields, now_ms


def test_countdown_zadds_delayed_not_stream(app, redis_client):
    @app.task()
    def ping(x):
        return x

    t0 = now_ms()
    res = ping.apply_async(args=(7,), countdown=5)
    t1 = now_ms()

    # No XADD happened: the ready stream must not even exist.
    assert redis_client.exists("rupy:q:default") == 0

    members = redis_client.zrange("rupy:delayed:default", 0, -1, withscores=True)
    assert len(members) == 1
    raw, score = members[0]
    env = json.loads(raw)

    assert t0 + 5000 <= score <= t1 + 5000
    assert env["not_before"] == int(score), "not_before must equal the zset score"

    # Envelope otherwise identical to a normal enqueue.
    assert set(env.keys()) == ENVELOPE_KEYS
    assert env["id"] == res.id
    assert env["task"] == f"{ping.fn.__module__}.{ping.fn.__qualname__}"
    assert env["args"] == [7]
    assert env["kwargs"] == {}
    assert env["queue"] == "default"
    assert_default_option_fields(env)
    assert t0 <= env["enqueued_at"] <= t1


def test_fractional_countdown(app, redis_client):
    @app.task()
    def ping():
        return None

    t0 = now_ms()
    ping.apply_async(countdown=0.5)
    t1 = now_ms()

    members = redis_client.zrange("rupy:delayed:default", 0, -1, withscores=True)
    assert len(members) == 1
    raw, score = members[0]
    env = json.loads(raw)
    assert t0 + 500 <= score <= t1 + 500
    assert env["not_before"] == int(score)
    assert redis_client.exists("rupy:q:default") == 0
