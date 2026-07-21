"""Test 7: is_async detection, kind validation, name defaults, unit conversions."""

from __future__ import annotations

import pytest

from cauli import Cauli, TaskDef


def _offline_app() -> Cauli:
    return Cauli(redis_url="redis://127.0.0.1:1/0")


def test_is_async_detection():
    app = _offline_app()

    @app.task()
    def sync_task():
        return 1

    @app.task()
    async def async_task():
        return 1

    assert sync_task.is_async is False
    assert async_task.is_async is True


def test_kind_defaults_and_validation():
    app = _offline_app()

    @app.task()
    def default_kind():
        return None

    @app.task(kind="io")
    def io_kind():
        return None

    @app.task(kind="cpu")
    def cpu_kind():
        return None

    assert default_kind.kind == "io"
    assert io_kind.kind == "io"
    assert cpu_kind.kind == "cpu"

    with pytest.raises(ValueError):

        @app.task(kind="gpu")
        def bad_kind():
            return None

    assert not any(name.endswith("bad_kind") for name in app._tasks)


def test_default_name_and_registration():
    app = _offline_app()

    @app.task()
    def foo():
        return None

    assert foo.name == f"{foo.fn.__module__}.{foo.fn.__qualname__}"
    assert app._tasks[foo.name] is foo
    assert isinstance(foo, TaskDef)


def test_bare_decorator_form():
    app = _offline_app()

    @app.task
    def bare():
        return 41

    assert isinstance(bare, TaskDef)
    assert bare() == 41
    assert bare.kind == "io"


def test_seconds_to_ms_conversions():
    app = _offline_app()

    @app.task(timeout=1.5, soft_timeout=0.25, backoff_base=0.1, backoff_max=2.5)
    def t():
        return None

    assert t.timeout_ms == 1500
    assert t.soft_timeout_ms == 250
    assert t.backoff_base_ms == 100
    assert t.backoff_max_ms == 2500


def test_soft_timeout_must_be_less_than_timeout():
    app = _offline_app()

    with pytest.raises(ValueError):

        @app.task(timeout=5, soft_timeout=5)
        def equal_limits():
            return None
