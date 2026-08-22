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
import threading

import pytest

django = pytest.importorskip("django")

from django.conf import settings  # noqa: E402

from cauli import Cauli  # noqa: E402
from cauli.contrib.django import (  # noqa: E402
    autodiscover_tasks,
    django_app,
    install_db_hooks,
    install_orm_executors,
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
    # Lifecycle hooks come pre-registered: the sticky-executor assignment
    # FIRST, then close_old_connections (Celery-fixup parity). The order is
    # load-bearing -- the before-task connection health check must run on the
    # sticky thread the task is about to use, so the executor assignment has
    # to be in place before it.
    names = [h.__name__ for h in app._before_task_hooks]
    assert names == [
        "cauli_assign_orm_executor",
        "cauli_close_old_connections",
    ]
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


def test_hook_on_event_loop_skips_executor_hop_until_a_connection_exists(monkeypatch):
    """Before anything opens a connection the async hook must do nothing.

    close_old_connections has nothing to close until a connection exists, so
    the sync_to_async round trip is two thread hand-offs of pure overhead on
    every task. It has to come straight back once a connection does exist --
    skipping the cleanup then would leak the connection the task opened.
    """
    from asgiref.sync import sync_to_async
    from django.db import connection

    from cauli.contrib import django as contrib_django

    app = install_db_hooks(_app_no_redis())
    hook = app._before_task_hooks[0]
    monkeypatch.setattr(contrib_django, "_connection_opened", False)

    async def scenario():
        assert hook() is None, (
            "nothing has opened a connection yet, so the hook must not pay "
            "the thread-sensitive executor hop"
        )
        await sync_to_async(
            lambda: connection.ensure_connection(), thread_sensitive=True
        )()
        assert contrib_django._connection_opened, (
            "connection_created must latch the flag"
        )
        awaitable = hook()
        assert awaitable is not None and hasattr(awaitable, "__await__"), (
            "once a connection exists the hook must go back to closing it"
        )
        await awaitable
        assert (
            await sync_to_async(lambda: connection.connection, thread_sensitive=True)()
            is None
        )

    asyncio.run(scenario())


def _fresh_executor_state(monkeypatch, workers):
    """Reset the module-singleton pool so each test installs its own size."""
    from cauli.contrib import django as contrib_django

    monkeypatch.setattr(contrib_django, "_orm_executors", [])
    monkeypatch.setattr(contrib_django, "_orm_tokens", [])
    monkeypatch.setattr(contrib_django, "_orm_rr", 0)
    return install_orm_executors(_app_no_redis(), workers=workers)


def test_sticky_executor_pins_a_task_to_one_thread_across_awaits(monkeypatch):
    """All thread_sensitive work of one task must land on ONE executor thread.

    That is the property that lets Django reuse the same cached connection
    across every await in the task; if two calls land on two threads the task
    is back to one-connection-per-call and CONN_MAX_AGE means nothing.
    """
    from asgiref.sync import sync_to_async

    app = _fresh_executor_state(monkeypatch, workers=4)
    hook = app._before_task_hooks[0]

    def thread_name():
        return threading.current_thread().name

    async def one_task():
        hook()  # what the shim does before the task body runs
        first = await sync_to_async(thread_name, thread_sensitive=True)()
        await asyncio.sleep(0)  # a real suspension between the two calls
        second = await sync_to_async(thread_name, thread_sensitive=True)()
        assert first == second, "task hopped executor threads across an await"
        assert first.startswith("cauli-orm-"), (
            f"thread_sensitive work ran on {first!r}, not a sticky executor"
        )
        return first

    asyncio.run(one_task())


def test_sticky_executors_spread_tasks_and_bound_thread_count(monkeypatch):
    """N concurrent tasks must use more than one thread, and at most M.

    More than one is the parallelism that fixed the measured 348 ms pileup on
    asgiref's single global executor; at most M is the bound that keeps the
    process at M cached database connections.
    """
    from asgiref.sync import sync_to_async

    workers = 3
    app = _fresh_executor_state(monkeypatch, workers=workers)
    hook = app._before_task_hooks[0]

    async def one_task():
        hook()
        return await sync_to_async(
            lambda: threading.current_thread().name, thread_sensitive=True
        )()

    async def scenario():
        return await asyncio.gather(*[one_task() for _ in range(workers * 4)])

    names = set(asyncio.run(scenario()))
    assert len(names) == workers, (
        f"expected exactly {workers} sticky threads in use, saw {names}"
    )


def test_orm_executor_hook_is_inert_off_the_event_loop(monkeypatch):
    """On a sync-pool thread the hook must do nothing: that thread IS the
    task's thread, and stamping a context there would leak an executor
    assignment into unrelated asgiref use."""
    from asgiref.sync import SyncToAsync

    app = _fresh_executor_state(monkeypatch, workers=2)
    hook = app._before_task_hooks[0]
    hook()  # no running loop here
    assert SyncToAsync.thread_sensitive_context.get(None) is None


def test_orm_executors_zero_disables(monkeypatch):
    app = _fresh_executor_state(monkeypatch, workers=0)
    assert app._before_task_hooks == []


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
    def t1(*args, **kwargs):
        # *args/**kwargs on purpose: these tests exercise option FORWARDING
        # through a monkeypatched _enqueue below, with example args/kwargs
        # picked per test and never actually passed to this body, so the
        # fixture task must accept anything rather than assert a real shape.
        return args

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


def test_on_commit_rejects_a_bad_signature_inside_the_transaction(monkeypatch):
    """A mistyped call must raise INSIDE the atomic block, not at COMMIT.

    Deferring the check past the commit is the whole footgun: by the time the
    enqueue fails the row is already durable, so the view 500s with committed
    state and no task at all. Raising here lets the transaction roll back.
    """
    from django.db import transaction

    t1, sent = _captured_app(monkeypatch)

    @t1.app.task(name="t2")
    def t2(a, b):
        return a + b

    with transaction.atomic():
        with pytest.raises(TypeError, match="t2"):
            t2.delay_on_commit(1, bee=2)
    assert sent == [], "a call that fails validation must never be published"


def test_on_commit_rejects_an_unencodable_argument_inside_the_transaction(monkeypatch):
    """The encode dry run must fire at the call site too.

    The audit's exact case: a model with a UUID primary key. The codec refuses
    a UUID, and without the dry run that refusal lands at COMMIT, after the
    row is written.
    """
    import uuid

    from django.db import transaction

    t1, sent = _captured_app(monkeypatch)
    with transaction.atomic():
        with pytest.raises(TypeError, match="not JSON encodable"):
            t1.apply_async_on_commit(args=(uuid.uuid4(),))
        with pytest.raises(TypeError, match="not JSON encodable"):
            t1.delay_on_commit(pk=uuid.uuid4())
    assert sent == []


def test_on_commit_docstrings_warn_about_django_testcase():
    """The TestCase wall has to be documented where the user reads it.

    ``django.test.TestCase`` rolls its atomic block back, so an on_commit
    enqueue is discarded and the task is silently never published. That is
    indistinguishable from a broken integration, and the only signal is a
    failing assertion, so both docstrings must name the two escapes.
    """
    from cauli.task import TaskDef

    both = (TaskDef.delay_on_commit.__doc__ or "") + (
        TaskDef.apply_async_on_commit.__doc__ or ""
    )
    assert "TestCase" in both
    assert "captureOnCommitCallbacks(execute=True)" in both
    assert "TransactionTestCase" in both
