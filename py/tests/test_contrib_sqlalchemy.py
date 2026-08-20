"""cauli.contrib.sqlalchemy unit tests (no broker, no worker, no real Postgres).

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
import os
import time
import uuid

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
psycopg = pytest.importorskip("psycopg")

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # noqa: E402

from cauli import Cauli  # noqa: E402
from cauli.contrib import sqlalchemy as contrib_sqlalchemy  # noqa: E402
from cauli.contrib.sqlalchemy import (  # noqa: E402
    get_session,
    install_sqlalchemy_session,
    sqlalchemy_app,
)

# Port 1 is a reserved port nothing listens on: connecting to it fails fast,
# which is what lets the tests below prove certain calls need no I/O at all
# without ever touching a real database or a real redis.
_FAKE_REDIS_URL = "redis://127.0.0.1:1/0"
_FAKE_DATABASE_URL = "postgresql+psycopg://u:p@127.0.0.1:1/db"

# The audit's throwaway Postgres, role and database both "bench", the same
# instance bench/sqla_models.py points at. Only the one dispose test below
# that needs a real, populated pool uses this; it skips itself when nothing
# answers rather than failing the file in an environment without Postgres.
_BENCH_PG_DSN = os.environ.get(
    "BENCH_PG_DSN", "postgresql://bench:bench@127.0.0.1:5432/bench"
)


@pytest.fixture(autouse=True)
def _reset_session_var():
    """Every test starts and ends with no session active.

    Belt and suspenders on top of each test cleaning up after itself: the
    ContextVar is process wide module state, and a test that left a session
    behind would silently hand it to whichever test ran next -- exactly the
    leak this whole module exists to prevent, so the test suite for it must
    not be able to hide one.
    """
    token = contrib_sqlalchemy._session_var.set(None)
    yield
    contrib_sqlalchemy._session_var.reset(token)


def _app_no_redis() -> Cauli:
    return Cauli(redis_url=_FAKE_REDIS_URL)


def _engine():
    return create_async_engine(_FAKE_DATABASE_URL)


def test_sqlalchemy_app_builds_engine_and_wires_hooks():
    app = sqlalchemy_app(
        _FAKE_DATABASE_URL, redis_url=_FAKE_REDIS_URL, default_queue="q"
    )
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
    """Unlike test_process_init_disposes_engine above, which replaces
    dispose() itself with a fake and so never touches the pool, this test
    checks a real connection out of a real pool and back in before the hook
    runs, a populated pool being the fork safety scenario the hook exists
    for (module docstring's process init bullet and the install_sqlalchemy_
    session process init paragraph); a hook that only ever ran against an
    empty, never used pool never exercised that path. The assertion reads
    Postgres's own pg_stat_activity rather than "no exception raised", so it
    cannot pass by accident if the dispose call were ever dropped from the
    hook: an empty hook body still runs without error, but it would leave
    the backend counted below.
    """
    marker = f"cauli-sqlalchemy-dispose-itest-{uuid.uuid4().hex}"
    async_dsn = _BENCH_PG_DSN.replace("postgresql://", "postgresql+psycopg://", 1)
    engine = create_async_engine(
        async_dsn,
        pool_size=1,
        max_overflow=0,
        connect_args={"application_name": marker},
    )

    def backends_for_marker() -> int:
        with psycopg.connect(_BENCH_PG_DSN, autocommit=True) as conn:
            row = conn.execute(
                "select count(*) from pg_stat_activity where application_name = %s",
                (marker,),
            ).fetchone()
            return row[0]

    async def checkout_and_release() -> None:
        async with engine.connect() as conn:
            await conn.execute(sqlalchemy.text("select 1"))

    try:
        asyncio.run(checkout_and_release())
        checked_in = backends_for_marker()
    except Exception as exc:
        pytest.skip(f"bench Postgres not reachable at {_BENCH_PG_DSN}: {exc}")

    try:
        assert checked_in == 1, (
            "checkout/release did not leave a real pooled backend for "
            "dispose to close; the test would be vacuous"
        )

        app = install_sqlalchemy_session(_app_no_redis(), engine)
        app._process_init_hooks[0]()  # must not hang and must not raise

        deadline = time.monotonic() + 5
        n = backends_for_marker()
        while n and time.monotonic() < deadline:
            time.sleep(0.1)
            n = backends_for_marker()
        assert n == 0, "dispose() left the real backend connection open"
    finally:
        asyncio.run(engine.dispose())  # belt and suspenders: never leak a real backend


# The module was called cauli.contrib.fastapi until the 1.0 API freeze. That
# name survives as a reexport alias, and these two tests are the only thing
# standing between it and a future edit that quietly duplicates the
# implementation there instead of reexporting it.


def test_old_fastapi_import_path_still_works():
    """Code written against the pre rename name keeps importing and keeps
    working. Identity is what is asserted, not mere importability: an alias
    that copied the implementation would import exactly as cleanly and then
    own a second ContextVar and a second engine.
    """
    from cauli.contrib import fastapi as contrib_fastapi
    from cauli.contrib.fastapi import (
        fastapi_app,
        get_session as aliased_get_session,
        install_sqlalchemy_session as aliased_install,
    )

    assert fastapi_app is sqlalchemy_app
    assert aliased_get_session is get_session
    assert aliased_install is install_sqlalchemy_session
    assert contrib_fastapi.sqlalchemy_app is sqlalchemy_app


def test_alias_and_new_module_share_one_session():
    """The behavioural half of the alias contract: a session opened by hooks
    wired through the old import path is the very session the new module's
    get_session hands back, one ContextVar behind both names."""
    from cauli.contrib.fastapi import fastapi_app

    app = fastapi_app(_FAKE_DATABASE_URL, redis_url=_FAKE_REDIS_URL)
    before, after = app._before_task_hooks[0], app._after_task_hooks[0]

    async def scenario():
        before()
        session = get_session()
        assert isinstance(session, AsyncSession)
        await after()

    asyncio.run(scenario())
