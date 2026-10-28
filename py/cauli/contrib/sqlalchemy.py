"""Async SQLAlchemy session per task for cauli. Framework agnostic: it serves
FastAPI, Starlette, Litestar or bare asyncio apps. ``cauli.contrib.fastapi``
is an alias.

Usage::

    # myproj/cauli.py
    from cauli.contrib.sqlalchemy import sqlalchemy_app, get_session

    app = sqlalchemy_app("postgresql+psycopg://user:pass@host/db")

    # myproj/store/tasks.py
    from myproj.cauli import app, get_session

    @app.task()
    async def refresh_prices(sku_id):
        session = get_session()
        session.add(...)
        await session.commit()

    # run the worker
    cauli-worker --app myproj.cauli:app

What it wires: one engine and sessionmaker built eagerly (no I/O until first
checkout), ``engine.dispose()`` as a process init hook so no pooled
connection is inherited across a fork, and one ``AsyncSession`` per task,
opened by a before task hook, closed by an after task hook, read with
:func:`get_session`.

Sizing: the engine gets ``pool_size=15, max_overflow=0``. ``--io-concurrency``
(default 256, see ``docs/configuration.md``) is not tied to it, so tasks
beyond 15 wait for a slot up to ``pool_timeout`` (30 s) and then raise
``QueuePool limit ... reached``. Either raise the pool to match
``--io-concurrency`` or lower ``--io-concurrency`` to the pool size. Total
connections are ``procs`` times that ceiling; put pgbouncer (transaction
mode) in front once that nears Postgres ``max_connections``.

Transactions are yours: commit explicitly. An uncommitted write is
discarded silently when the session closes.

Hazards, none detectable at import time:

- **Keep ``--io-loops 1``.** An async pool binds each connection to the loop
  that first checked it out; more than one loop hands connections across
  loops and the driver fails or misbehaves.
- **No cpu lane.** ``kind="cpu"`` tasks are unsupported; use a synchronous
  engine there. An ``async def`` cpu task runs in its own loop, started after
  the hooks ran, so :func:`get_session` raises there.
- **A soft timeout cancels the client, not the query.** The server keeps
  running it and holding its locks. Set Postgres ``statement_timeout`` if a
  query must stop server side.
- **Do not leak the session past the task body.** ``AsyncSession`` reopens
  itself on next use, so a background task still holding it after the after
  task hook ran does untracked work on a connection that never returns to
  the pool. Await such work before the task returns.
"""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from cauli.app import AsyncCauli, Cauli

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


#: None means no session is active for the current task.
_session_var: ContextVar[Any] = ContextVar("cauli_sqlalchemy_session", default=None)


def get_session() -> AsyncSession:
    """Return the ``AsyncSession`` opened for the current task.

    Raises ``LookupError`` outside a task, on the sync thread pool, or in a
    ``kind="cpu"`` task. It never opens a session itself: a loop check here
    would pass inside an ``async def`` cpu task, which is unsupported.
    """
    session = _session_var.get()
    if session is None:
        raise LookupError(
            "cauli.contrib.sqlalchemy.get_session(): no AsyncSession is "
            "active on this task. This accessor only works inside an async "
            "io lane task whose app was wired by install_sqlalchemy_session() "
            "or sqlalchemy_app(); it is inert by design on the sync thread "
            'pool and for kind="cpu" tasks (use a plain synchronous '
            "SQLAlchemy engine there instead)."
        )
    return session


#: With max_overflow=0 every connection stays pooled; overflow ones reconnect per task.
DEFAULT_POOL_SIZE = 15


def sqlalchemy_app(database_url: str, **overrides: Any) -> AsyncCauli:
    """Build an :class:`cauli.AsyncCauli` with a session per task engine.

    ``database_url`` goes to ``create_async_engine`` and must name an async
    driver (``postgresql+psycopg://``, ``postgresql+asyncpg://``).
    ``overrides`` go to ``AsyncCauli(...)`` unchanged.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        database_url, pool_size=DEFAULT_POOL_SIZE, max_overflow=0
    )
    app = AsyncCauli(**overrides)
    return install_sqlalchemy_session(app, engine)


def install_sqlalchemy_session(app: Cauli, engine: AsyncEngine) -> Cauli:
    """Register the session per task hooks for ``engine`` on ``app``.

    Use it when you build the app or engine yourself. The sessionmaker uses
    ``expire_on_commit=False`` because async SQLAlchemy raises on the
    implicit refresh that reading a committed row would otherwise trigger.
    The after task hook closes the session on every outcome and never
    commits or rolls back. The process init hook disposes the engine in
    every process, including a forked cpu child.
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker

    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    def cauli_open_sqlalchemy_session() -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        _session_var.set(session_factory())

    def cauli_close_sqlalchemy_session() -> Any:
        session = _session_var.get()
        if session is None:
            return None
        _session_var.set(None)
        return session.close()  # shim awaits the returned coroutine (PROTOCOL.md 4.8)

    def cauli_dispose_sqlalchemy_engine() -> None:
        asyncio.run(engine.dispose())

    app.before_task(cauli_open_sqlalchemy_session)
    app.after_task(cauli_close_sqlalchemy_session)
    app.process_init(cauli_dispose_sqlalchemy_engine)
    return app
