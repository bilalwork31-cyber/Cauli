"""cauli.contrib.django unit tests (no broker, no worker, sqlite in-memory).

The end-to-end proof (real worker + real Postgres + restart) lives in
itest/test_django.py; this file covers the contrib API surface: settings
driven config, DB hook mechanics, autodiscovery, and on-commit enqueueing.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile

import pytest

django = pytest.importorskip("django")

from django.conf import settings  # noqa: E402

from cauli import Cauli  # noqa: E402
from cauli.contrib.django import (  # noqa: E402
    autodiscover_tasks,
    django_app,
    install_db_hooks,
)


@pytest.fixture(scope="module", autouse=True)
def django_configured():
    if not settings.configured:
        settings.configure(
            USE_TZ=True,
            INSTALLED_APPS=[],
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    # File-backed on purpose: Django's sqlite backend silently
                    # IGNORES close() for in-memory databases (closing would
                    # destroy them), which would fake out every hook test
                    # below that asserts a connection actually closed.
                    "NAME": os.path.join(
                        tempfile.gettempdir(), f"cauli-contrib-test-{os.getpid()}.db"
                    ),
                    # 0 = "close when old" fires on every close_old_connections
                    # call, which is exactly what the hook tests below assert.
                    "CONN_MAX_AGE": 0,
                }
            },
            CAULI_REDIS_URL="redis://127.0.0.1:1/7",
            CAULI_DEFAULT_QUEUE="dj-queue",
            CAULI_RESULT_TTL=123,
            CAULI_IDEMP_TTL=456,
        )
        django.setup()
    yield


def _app_no_redis() -> Cauli:
    return Cauli(redis_url="redis://127.0.0.1:1/0")


def test_django_app_reads_cauli_settings():
    app = django_app()
    assert app.redis_url == "redis://127.0.0.1:1/7"
    assert app.default_queue == "dj-queue"
    assert app.result_ttl == 123
    assert app.idemp_ttl == 456
    # DB lifecycle hooks come pre-registered (Celery-fixup parity).
    assert len(app._before_task_hooks) == 1
    assert len(app._after_task_hooks) == 1
    assert len(app._process_init_hooks) == 1


def test_django_app_kwargs_override_settings():
    app = django_app(default_queue="explicit", result_ttl=9)
    assert app.default_queue == "explicit"
    assert app.result_ttl == 9
    assert app.redis_url == "redis://127.0.0.1:1/7"  # still from settings


def test_before_hook_closes_expired_connection_sync_path():
    from django.db import connection

    app = install_db_hooks(_app_no_redis())
    connection.ensure_connection()
    assert connection.connection is not None
    app._before_task_hooks[0]()  # CONN_MAX_AGE=0 -> must close it
    assert connection.connection is None


def test_process_init_hook_closes_all_connections():
    from django.db import connection

    app = install_db_hooks(_app_no_redis())
    connection.ensure_connection()
    assert connection.connection is not None
    app._process_init_hooks[0]()  # connections.close_all
    assert connection.connection is None


def test_hook_on_event_loop_returns_awaitable_closing_executor_thread_conn():
    """On the async path the hook must clean up in asgiref's thread-sensitive
    executor — the thread Django's async ORM runs sync DB code in — not on
    the event loop thread itself."""
    from asgiref.sync import sync_to_async
    from django.db import connection

    app = install_db_hooks(_app_no_redis())
    hook = app._before_task_hooks[0]

    async def scenario():
        # The lambda matters: `connection` is a per-thread proxy, so the
        # attribute lookup must happen INSIDE the executor thread (a bound
        # method created on the loop thread would target the loop thread's
        # wrapper instead — exactly the trap the hook itself avoids by
        # running close_old_connections wholly inside sync_to_async).
        await sync_to_async(
            lambda: connection.ensure_connection(), thread_sensitive=True
        )()
        assert (
            await sync_to_async(lambda: connection.connection, thread_sensitive=True)()
            is not None
        )
        r = hook()
        assert r is not None and hasattr(r, "__await__"), (
            "on a loop thread the hook must hand back an awaitable "
            "for the shim to await"
        )
        await r
        assert (
            await sync_to_async(lambda: connection.connection, thread_sensitive=True)()
            is None
        )

    asyncio.run(scenario())


def _write_pkg(tmp_path, name, tasks_body=None):
    pkg = tmp_path / name
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    if tasks_body is not None:
        (pkg / "tasks.py").write_text(tasks_body)


def test_autodiscover_imports_tasks_modules(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    _write_pkg(tmp_path, "pkg_with", "DISCOVERED = True\n")
    _write_pkg(tmp_path, "pkg_without")  # no tasks module: skipped silently

    app = _app_no_redis()
    imported = autodiscover_tasks(app, packages=["pkg_with", "pkg_without"])
    assert imported == ["pkg_with.tasks"]
    assert sys.modules["pkg_with.tasks"].DISCOVERED is True


def test_autodiscover_propagates_broken_tasks_module(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    _write_pkg(tmp_path, "pkg_broken", "raise RuntimeError('broken tasks module')\n")
    with pytest.raises(RuntimeError, match="broken tasks module"):
        autodiscover_tasks(_app_no_redis(), packages=["pkg_broken"])


def test_autodiscover_defaults_to_installed_apps():
    # INSTALLED_APPS is empty in this configuration: nothing to discover,
    # and crucially no error.
    assert autodiscover_tasks(_app_no_redis()) == []


class _Boom(Exception):
    pass


def _captured_app(monkeypatch):
    app = _app_no_redis()

    @app.task(name="t1")
    def t1(x):
        return x

    sent = []
    monkeypatch.setattr(
        app,
        "_enqueue",
        lambda task, args, kwargs, **kw: sent.append((task.name, args, kwargs, kw)),
    )
    return t1, sent


def test_delay_on_commit_defers_until_commit(monkeypatch):
    from django.db import transaction

    t1, sent = _captured_app(monkeypatch)
    with transaction.atomic():
        assert t1.delay_on_commit(41, flag=True) is None
        assert sent == [], "must not publish inside the transaction"
    assert sent == [
        (
            "t1",
            (41,),
            {"flag": True},
            {
                "countdown": None,
                "queue": None,
                "idempotency_key": None,
                "eta": None,
                "expires": None,
            },
        )
    ]


def test_delay_on_commit_never_publishes_on_rollback(monkeypatch):
    from django.db import transaction

    t1, sent = _captured_app(monkeypatch)
    with pytest.raises(_Boom):
        with transaction.atomic():
            t1.delay_on_commit(1)
            raise _Boom()
    assert sent == [], "a rolled-back transaction must not publish"


def test_apply_async_on_commit_forwards_options(monkeypatch):
    from django.db import transaction

    t1, sent = _captured_app(monkeypatch)
    with transaction.atomic():
        t1.apply_async_on_commit(
            args=(1,),
            kwargs={"a": 2},
            countdown=3.5,
            queue="q2",
            idempotency_key="k",
            expires=60.0,
        )
    assert sent == [
        (
            "t1",
            (1,),
            {"a": 2},
            {
                "countdown": 3.5,
                "queue": "q2",
                "idempotency_key": "k",
                "eta": None,
                "expires": 60.0,
            },
        )
    ]


def test_delay_on_commit_outside_atomic_publishes_immediately(monkeypatch):
    t1, sent = _captured_app(monkeypatch)
    t1.delay_on_commit(7)
    assert [s[0:2] for s in sent] == [("t1", (7,))]
