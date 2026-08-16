"""cauli.contrib.fastapi unit tests (no broker, no worker, no real Postgres).

Every hook below runs against a real SQLAlchemy ``AsyncEngine`` pointed at
``127.0.0.1:1``, a reserved port nothing listens on: engine/sessionmaker/
session construction and closing a session that never ran a query need no
I/O, so the suite proves the hook wiring and the ContextVar bookkeeping
without a database, exactly the way ``test_contrib_django.py`` proves its
hooks against a file backed sqlite database instead of a real Postgres
server. The real worker, real Postgres proof (concurrent load through the
actual ``cauli-worker`` binary, connections counted via ``pg_stat_activity``
before and during the run) is a manual verification built on
``bench/sqla_models.py``'s scaffolding, not a committed test.
"""

from __future__ import annotations

import asyncio

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # noqa: E402

from cauli import Cauli  # noqa: E402
from cauli.contrib import fastapi as contrib_fastapi  # noqa: E402
from cauli.contrib.fastapi import (  # noqa: E402
    fastapi_app,
    get_session,
    install_sqlalchemy_session,
)

# Port 1 is a reserved port nothing listens on: connecting to it fails fast,
# which is what lets the tests below prove certain calls need no I/O at all
# without ever touching a real database or a real redis.
_FAKE_REDIS_URL = "redis://127.0.0.1:1/0"
_FAKE_DATABASE_URL = "postgresql+psycopg://u:p@127.0.0.1:1/db"


@pytest.fixture(autouse=True)
def _reset_session_var():
    """Every test starts and ends with no session active.

    Belt and suspenders on top of each test cleaning up after itself: the
    ContextVar is process wide module state, and a test that left a session
    behind would silently hand it to whichever test ran next -- exactly the
    leak this whole module exists to prevent, so the test suite for it must
    not be able to hide one.
    """
    token = contrib_fastapi._session_var.set(None)
    yield
    contrib_fastapi._session_var.reset(token)


def _app_no_redis() -> Cauli:
    return Cauli(redis_url=_FAKE_REDIS_URL)


def _engine():
    return create_async_engine(_FAKE_DATABASE_URL)


def test_fastapi_app_builds_engine_and_wires_hooks():
    app = fastapi_app(_FAKE_DATABASE_URL, redis_url=_FAKE_REDIS_URL, default_queue="q")
    assert app.redis_url == _FAKE_REDIS_URL
    assert app.default_queue == "q"
    assert [h.__name__ for h in app._before_task_hooks] == [
        "cauli_open_sqlalchemy_session"
    ]
    assert [h.__name__ for h in app._after_task_hooks] == [
        "cauli_close_sqlalchemy_session"
    ]
    assert [h.__name__ for h in app._process_init_hooks] == [
        "cauli_dispose_sqlalchemy_engine"
    ]


def test_get_session_raises_outside_any_task():
    with pytest.raises(LookupError, match="no AsyncSession is active"):
        get_session()


def test_before_hook_is_a_no_op_without_a_running_loop():
    app = install_sqlalchemy_session(_app_no_redis(), _engine())
    before = app._before_task_hooks[0]
    assert before() is None  # this test function itself has no running loop
    with pytest.raises(LookupError):
        get_session()


def test_session_opened_before_and_closed_after_success(monkeypatch):
    app = install_sqlalchemy_session(_app_no_redis(), _engine())
    before, after = app._before_task_hooks[0], app._after_task_hooks[0]

    async def scenario():
        before()
        session = get_session()
        assert isinstance(session, AsyncSession)

        closed = []
        orig_close = session.close

        async def spy_close():
            closed.append(True)
            await orig_close()

        monkeypatch.setattr(session, "close", spy_close)

        awaitable = after()
        assert awaitable is not None and hasattr(awaitable, "__await__"), (
            "on a loop thread the after hook must hand back an awaitable "
            "for the shim to await, exactly like django.py's DB hook"
        )
        await awaitable
        assert closed == [True]
        with pytest.raises(LookupError):
            get_session()

    asyncio.run(scenario())


def test_session_still_closed_when_the_task_raises(monkeypatch):
    app = install_sqlalchemy_session(_app_no_redis(), _engine())
    before, after = app._before_task_hooks[0], app._after_task_hooks[0]
    closed = []

    class Boom(Exception):
        pass

    async def scenario():
        before()
        session = get_session()
        orig_close = session.close

        async def spy_close():
            closed.append(True)
            await orig_close()

        monkeypatch.setattr(session, "close", spy_close)
        try:
            raise Boom("task blew up")
        finally:
            # Exactly the worker's contract (PROTOCOL.md 4.8): after task
            # hooks run in the outer finally, on every outcome path.
            await after()

    with pytest.raises(Boom):
        asyncio.run(scenario())
    assert closed == [True]
    with pytest.raises(LookupError):
        get_session()


def test_context_var_does_not_leak_between_concurrent_tasks_same_thread():
    """The failure this whole design exists to prevent: two tasks in flight
    at once on one event loop thread must never see each other's session."""
    app = install_sqlalchemy_session(_app_no_redis(), _engine())
    before, after = app._before_task_hooks[0], app._after_task_hooks[0]

    async def one_task():
        before()
        session = get_session()
        await asyncio.sleep(0)  # yield here so sibling tasks interleave
        assert get_session() is session, "lost its own session across an await"
        await after()
        return id(session)

    async def scenario():
        return await asyncio.gather(*[one_task() for _ in range(5)])

    ids = asyncio.run(scenario())
    assert len(set(ids)) == 5, "two concurrent tasks shared one session"


def test_sequential_tasks_on_the_same_thread_get_fresh_sessions():
    app = install_sqlalchemy_session(_app_no_redis(), _engine())
    before, after = app._before_task_hooks[0], app._after_task_hooks[0]

    async def scenario():
        before()
        first = get_session()
        await after()
        with pytest.raises(LookupError):
            get_session()  # nothing must linger between tasks

        before()
        second = get_session()
        await after()
        return first, second

    first, second = asyncio.run(scenario())
    assert first is not second


def test_process_init_disposes_engine(monkeypatch):
    # AsyncEngine is a slotted proxy: instance level monkeypatching of a
    # single engine's `dispose` is rejected ("attribute is read only"), so
    # the fake is installed on the class instead.
    engine = _engine()
    disposed = []

    async def fake_dispose(self):
        disposed.append(True)

    monkeypatch.setattr(type(engine), "dispose", fake_dispose)
    app = install_sqlalchemy_session(_app_no_redis(), engine)
    app._process_init_hooks[0]()  # no running loop here, same as the real call sites
    assert disposed == [True]


def test_process_init_dispose_hook_runs_without_error_on_real_engine():
    app = install_sqlalchemy_session(_app_no_redis(), _engine())
    app._process_init_hooks[0]()  # must not hang and must not raise
