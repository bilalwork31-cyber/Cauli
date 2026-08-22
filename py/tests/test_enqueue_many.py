"""``enqueue_many`` / ``aenqueue_many``: N calls, ONE pipelined round trip.

The batch must produce envelopes byte-identical in shape to ``.delay()`` (there
is one wire protocol, not two), and it must validate the whole batch before it
writes anything.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from cauli import AsyncCauli, AsyncResult
from helpers import ENVELOPE_KEYS, assert_default_option_fields


def _stream(client, queue="default"):
    return [
        json.loads(fields[b"e"]) for _sid, fields in client.xrange(f"cauli:q:{queue}")
    ]


def _delayed(client, queue="default"):
    return [
        (json.loads(raw), score)
        for raw, score in client.zrange(
            f"cauli:delayed:{queue}", 0, -1, withscores=True
        )
    ]


def test_batch_writes_one_envelope_per_call_in_order(app, redis_client):
    @app.task(name="add")
    def add(a, b=0):
        return a + b

    results = app.enqueue_many(
        [
            (add, (1,)),
            (add, (2,), {"b": 5}),
            (add, (3, 4)),
        ]
    )

    assert [type(r) for r in results] == [AsyncResult] * 3
    envelopes = _stream(redis_client)
    assert len(envelopes) == 3
    assert [e["id"] for e in envelopes] == [r.id for r in results]
    assert [(e["args"], e["kwargs"]) for e in envelopes] == [
        ([1], {}),
        ([2], {"b": 5}),
        ([3, 4], {}),
    ]
    for env in envelopes:
        assert set(env.keys()) == ENVELOPE_KEYS
        assert env["task"] == "add"
        assert env["queue"] == "default"
        assert_default_option_fields(env)


def test_a_bare_task_means_no_arguments(app, redis_client):
    @app.task(name="ping")
    def ping():
        return "pong"

    results = app.enqueue_many([ping, ping])
    envelopes = _stream(redis_client)
    assert len(envelopes) == 2
    assert {e["id"] for e in envelopes} == {r.id for r in results}
    assert all(e["args"] == [] and e["kwargs"] == {} for e in envelopes)


def test_options_and_delayed_calls_may_be_mixed_into_one_batch(app, redis_client):
    @app.task(name="mix")
    def mix(n):
        return n

    app.enqueue_many(
        [
            (mix, (1,)),
            (mix, (2,), {}, {"queue": "other"}),
            (mix, (3,), {}, {"countdown": 60, "idempotency_key": "k3"}),
        ]
    )

    assert [e["args"] for e in _stream(redis_client)] == [[1]]
    assert [e["args"] for e in _stream(redis_client, "other")] == [[2]]
    delayed = _delayed(redis_client)
    assert len(delayed) == 1
    env, score = delayed[0]
    assert env["args"] == [3]
    assert env["idempotency_key"] == "k3"
    assert env["not_before"] == int(score)


def test_the_batch_is_one_pipeline_not_one_write_per_call(app, redis_client):
    @app.task(name="counted")
    def counted(n):
        return n

    client = app._get_redis()
    seen: list[dict] = []
    real_pipeline = client.pipeline

    def spy(*args, **kwargs):
        seen.append(kwargs)
        return real_pipeline(*args, **kwargs)

    client.pipeline = spy
    try:
        app.enqueue_many([(counted, (i,)) for i in range(5)])
    finally:
        del client.pipeline

    assert len(seen) == 1, "the whole batch must leave in a single pipeline"
    assert seen[0]["transaction"] is False, "a batch is not a transaction"
    assert len(_stream(redis_client)) == 5


def test_an_empty_batch_writes_nothing_and_returns_nothing(app, redis_client):
    assert app.enqueue_many([]) == []
    assert redis_client.exists("cauli:q:default") == 0


def test_a_bad_call_aborts_the_whole_batch_before_any_write(app, redis_client):
    @app.task(name="strict")
    def strict(n):
        return n

    with pytest.raises(TypeError, match="strict"):
        app.enqueue_many([(strict, (1,)), (strict, (), {"nope": 1})])

    assert redis_client.exists("cauli:q:default") == 0, (
        "the valid first call must not be published when a later one is invalid"
    )


def test_an_oversize_call_aborts_the_whole_batch(app, redis_client):
    @app.task(name="heavy")
    def heavy(blob):
        return blob

    app.max_envelope_bytes = 512
    with pytest.raises(ValueError, match="byte limit"):
        app.enqueue_many([(heavy, ("ok",)), (heavy, ("x" * 4096,))])

    assert redis_client.exists("cauli:q:default") == 0


def test_unknown_option_is_rejected_by_name(app, redis_client):
    @app.task(name="opt")
    def opt():
        return None

    with pytest.raises(TypeError, match=r"unknown enqueue_many option\(s\) \['ttl'\]"):
        app.enqueue_many([(opt, (), {}, {"ttl": 5})])
    assert redis_client.exists("cauli:q:default") == 0


@pytest.mark.parametrize(
    "call, exc",
    [
        ("not-a-task", TypeError),
        ((), TypeError),
        (("name", (1,)), TypeError),
        ((None, (), {}, {}, "extra"), ValueError),
    ],
)
def test_malformed_elements_are_refused(app, call, exc):
    with pytest.raises(exc):
        app.enqueue_many([call])


def test_a_list_element_works_like_a_tuple(app, redis_client):
    @app.task(name="listy")
    def listy(n):
        return n

    app.enqueue_many([[listy, (7,)]])
    assert [e["args"] for e in _stream(redis_client)] == [[7]]


def test_the_batch_envelope_is_what_delay_would_have_written(app, redis_client):
    @app.task(name="same")
    def same(a, b=1):
        return a + b

    same.delay(1, b=2)
    app.enqueue_many([(same, (1,), {"b": 2})])
    one, two = _stream(redis_client)
    volatile = {"id", "enqueued_at"}
    assert {k: v for k, v in one.items() if k not in volatile} == {
        k: v for k, v in two.items() if k not in volatile
    }


def test_aenqueue_many_matches_the_blocking_batch(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="a.batch")
    async def batch(n):
        return n

    async def main():
        try:
            return await app.aenqueue_many(
                [(batch, (1,)), (batch, (2,), {}, {"countdown": 30})]
            )
        finally:
            await app.aclose()

    results = asyncio.run(main())
    assert len(results) == 2
    envelopes = _stream(redis_client)
    assert [e["args"] for e in envelopes] == [[1]]
    assert envelopes[0]["id"] == results[0].id
    delayed = _delayed(redis_client)
    assert [e["args"] for e, _score in delayed] == [[2]]
    assert delayed[0][0]["id"] == results[1].id


def test_aenqueue_many_on_an_empty_batch_opens_no_pipeline(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url)

    async def main():
        try:
            return await app.aenqueue_many([])
        finally:
            await app.aclose()

    assert asyncio.run(main()) == []
    assert redis_client.exists("cauli:q:default") == 0
