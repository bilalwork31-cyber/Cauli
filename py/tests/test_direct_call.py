"""Test 5: decorated tasks stay directly callable without a broker."""

from __future__ import annotations

import asyncio
import inspect

from rupy import Rupy


def _offline_app() -> Rupy:
    # Dead port on purpose: direct calls must never touch redis.
    return Rupy(redis_url="redis://127.0.0.1:1/0")


def test_sync_task_direct_call():
    app = _offline_app()

    @app.task()
    def add(a, b):
        return a + b

    assert add(2, 3) == 5
    assert add.fn(2, 3) == 5
    assert add.is_async is False


def test_async_task_direct_call_returns_coroutine():
    app = _offline_app()

    @app.task()
    async def amul(a, b):
        await asyncio.sleep(0)
        return a * b

    coro = amul(2, 4)
    assert inspect.iscoroutine(coro)
    assert asyncio.run(coro) == 8
    assert amul.is_async is True


def test_direct_call_exceptions_propagate():
    app = _offline_app()

    @app.task()
    def boom():
        raise RuntimeError("inline failure")

    try:
        boom()
    except RuntimeError as exc:
        assert str(exc) == "inline failure"
    else:
        raise AssertionError("expected RuntimeError")
