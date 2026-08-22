"""AsyncCauli (awaitable enqueue/result) and the redis_client injection point.

The async half must produce the SAME envelope in the SAME keys as the blocking
half -- there is one wire protocol, not two.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import redis as redis_lib

from cauli import AsyncCauli, Cauli
from cauli import beat as beat_module
from helpers import ENVELOPE_KEYS


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


def _run(app, coro_factory):
    """Run one coroutine on a fresh loop and close the app's asyncio pool.

    redis-py binds its connections to the loop that first uses them, so each
    test gets its own AsyncCauli and closes it before the loop goes away.
    """

    async def main():
        try:
            return await coro_factory()
        finally:
            await app.aclose()

    return asyncio.run(main())


# ------------------------------------------------------------- adelay


def test_adelay_writes_the_expected_envelope(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="t")
    def t(x, y=0):
        return x + y

    result = _run(app, lambda: t.adelay(3, y=4))

    env = _stream(redis_client)[0]
    assert set(env) == ENVELOPE_KEYS
    assert env["task"] == "t"
    assert env["args"] == [3]
    assert env["kwargs"] == {"y": 4}
    assert env["id"] == result.id


def test_adelay_and_delay_agree_field_for_field(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="t", queue="q1", max_retries=7)
    def t(x):
        return x

    t.delay(1)
    _run(app, lambda: t.adelay(1))

    sync_env, async_env = _stream(redis_client, "q1")
    volatile = {"id", "enqueued_at"}
    assert {k: v for k, v in sync_env.items() if k not in volatile} == {
        k: v for k, v in async_env.items() if k not in volatile
    }


def test_aapply_async_countdown_lands_in_the_delayed_zset(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="t")
    def t():
        return None

    _run(app, lambda: t.aapply_async(countdown=120, queue="later"))

    assert redis_client.xlen("cauli:q:later") == 0
    delayed = _delayed(redis_client, "later")
    assert len(delayed) == 1
    env, score = delayed[0]
    assert env["not_before"] == int(score)
    assert env["queue"] == "later"


def test_aapply_async_validates_the_signature_before_touching_redis(
    redis_url, redis_client
):
    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="t")
    def t(x):
        return x

    with pytest.raises(TypeError):
        _run(app, lambda: t.aapply_async(kwargs={"nope": 1}))
    assert redis_client.xlen("cauli:q:default") == 0


def test_adelay_on_a_plain_app_says_what_to_do(redis_url, redis_client):
    app = Cauli(redis_url=redis_url)

    @app.task(name="t")
    def t():
        return None

    with pytest.raises(TypeError, match="AsyncCauli"):
        asyncio.run(t.adelay())
    assert redis_client.xlen("cauli:q:default") == 0


# ------------------------------------------------------------- aget/astatus


def test_astatus_and_aget_resolve_a_success_document(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="t")
    def t():
        return None

    async def go():
        handle = await t.adelay()
        redis_client.set(
            f"cauli:result:{handle.id}",
            json.dumps({"status": "success", "result": 41}),
        )
        assert await handle.astatus() == "success"
        return await handle.aget(timeout=2)

    assert _run(app, go) == 41


def test_aget_raises_on_failure(redis_url, redis_client):
    from cauli.exceptions import TaskFailedError

    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="t")
    def t():
        return None

    async def go():
        handle = await t.adelay()
        redis_client.set(
            f"cauli:result:{handle.id}",
            json.dumps(
                {"status": "failure", "error": {"type": "ValueError", "message": "x"}}
            ),
        )
        return await handle.aget(timeout=2)

    with pytest.raises(TaskFailedError) as exc:
        _run(app, go)
    assert exc.value.type == "ValueError"


def test_aget_times_out_without_a_result_key(redis_url):
    app = AsyncCauli(redis_url=redis_url)

    @app.task(name="t")
    def t():
        return None

    async def go():
        handle = await t.adelay()
        assert await handle.astatus() == "pending"
        return await handle.aget(timeout=0.2, poll_interval=0.02)

    with pytest.raises(TimeoutError):
        _run(app, go)


def test_aget_on_a_plain_app_says_what_to_do(redis_url):
    app = Cauli(redis_url=redis_url)

    @app.task(name="t")
    def t():
        return None

    handle = t.delay()
    with pytest.raises(TypeError, match="AsyncCauli"):
        asyncio.run(handle.aget(timeout=1))


# ------------------------------------------------------------- redis_client


def test_injected_client_instance_is_used_instead_of_the_url(redis_url, redis_client):
    injected = redis_lib.Redis.from_url(redis_url)
    # A dead port: nothing can connect through the URL, so a successful
    # enqueue proves the injected client is the one being used.
    app = Cauli(redis_url="redis://127.0.0.1:1/0", redis_client=injected)

    @app.task(name="t")
    def t():
        return None

    t.delay()
    assert redis_client.xlen("cauli:q:default") == 1
    injected.close()


def test_injected_factory_is_lazy_and_called_once(redis_url, redis_client):
    calls = []

    def factory():
        calls.append(1)
        return redis_lib.Redis.from_url(redis_url)

    app = Cauli(redis_url="redis://127.0.0.1:1/0", redis_client=factory)

    @app.task(name="t")
    def t():
        return None

    # Declaring tasks must not resolve a master: that is the whole reason the
    # factory form exists for Sentinel.
    assert calls == []
    t.delay()
    t.delay()
    assert calls == [1]
    assert redis_client.xlen("cauli:q:default") == 2


def test_async_client_injection(redis_url, redis_client):
    from redis import asyncio as aioredis

    calls = []

    def factory():
        calls.append(1)
        return aioredis.Redis.from_url(redis_url)

    app = AsyncCauli(redis_url="redis://127.0.0.1:1/0", async_redis_client=factory)

    @app.task(name="t")
    def t():
        return None

    assert calls == []
    _run(app, lambda: t.adelay())
    assert calls == [1]
    assert redis_client.xlen("cauli:q:default") == 1


def test_beat_connects_through_the_injected_client(
    redis_url, redis_client, monkeypatch
):
    from cauli.schedules import interval

    injected = redis_lib.Redis.from_url(redis_url)
    app = Cauli(redis_url="redis://127.0.0.1:1/0", redis_client=injected)
    app.add_periodic_task("live", "app.a", interval(60))
    monkeypatch.setattr(beat_module, "load_app", lambda spec: app)

    assert beat_module.main(["--app", "x:app", "--once"]) == 0
    assert redis_client.hexists("cauli:beat:schedule", "live")
    injected.close()


def test_beat_refuses_redis_url_against_an_injected_client(
    redis_url, redis_client, monkeypatch
):
    injected = redis_lib.Redis.from_url(redis_url)
    app = Cauli(redis_url="redis://127.0.0.1:1/0", redis_client=injected)
    monkeypatch.setattr(beat_module, "load_app", lambda spec: app)

    assert beat_module.main(["--app", "x:app", "--redis-url", redis_url, "--once"]) == 1
    injected.close()
