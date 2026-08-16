"""FastAPI / async SQLAlchemy integration for cauli (optional; cauli core
stays framework agnostic).

Typical project layout::

    # myproj/cauli.py
    from cauli.contrib.fastapi import fastapi_app, get_session

    app = fastapi_app("postgresql+psycopg://user:pass@host/db")

    # myproj/store/tasks.py
    from myproj.cauli import app, get_session

    @app.task()
    async def refresh_prices(sku_id):
        session = get_session()
        session.add(...)
        await session.commit()

    # run the worker
    cauli-worker --app myproj.cauli:app

What :func:`fastapi_app` gives you on top of a plain ``Cauli()``:

- ``create_async_engine()`` and ``async_sessionmaker()`` built ONCE, eagerly,
  at call time (see :func:`install_sqlalchemy_session`), not lazily on first
  task. Building an engine does no I/O and needs no running event loop, a
  connection is only opened on first checkout, so eager construction is
  safe. A lazily built per task engine buys nothing and invites exactly the
  double checked locking bugs Django's ORM never has to think about, because
  Django owns one implicit global connection registry. SQLAlchemy
  deliberately has none, so this module owns the one engine instead.
- ``engine.dispose()`` registered as a process init hook: the Django analog
  of ``connections.close_all()``, for the same reason, a pooled connection
  must not be inherited across a fork (PROTOCOL.md section 4.8 covers the
  fork server child and stdio child timings this hook runs at).
- One task scoped ``AsyncSession``, opened by a before task hook and closed
  by an after task hook, reachable from task code through :func:`get_session`,
  the smallest surface that works given SQLAlchemy's lack of a Django style
  implicit registry: cauli calls tasks as ``fn(*args, **kwargs)`` straight
  off the wire, with no per task argument injection anywhere in the
  protocol, so something has to carry the session from the hook to the task
  body, and a ``ContextVar`` plus one accessor is the minimum that does it.

This module never commits or rolls back for you. Managing the transaction,
``await session.commit()``, or letting an exception propagate to roll back,
is task code's job, the same division ``cauli.contrib.django`` keeps between
managing connections (its job) and managing transactions (yours).

Two hazards this module deliberately leaves for you to respect, neither
detectable at import time:

- **One io loop, or the pool is not really pooled.** An async connection
  pool binds each connection to whichever event loop first checks it out.
  cauli's ``--io-loops`` defaults to 1 and this module assumes that default;
  running with ``--io-loops`` above 1 hands the same pool to more than one
  loop thread, and a connection born on loop A handed to a task on loop B
  fails, or worse, misbehaves quietly, depending on the driver. Nothing here
  enforces ``--io-loops 1`` in code, leave it at the default.
- **The cpu lane is out of scope, and "no running loop" does not prove you
  are safe from it.** ``kind="cpu"`` tasks are not supported by this engine;
  use a plain synchronous SQLAlchemy engine there instead, the same way
  ``cauli.contrib.django`` leaves the cpu lane to Django's ordinary
  synchronous ORM path, no special casing needed. The before task hook's
  guard is ``asyncio.get_running_loop()`` raising, and that guard is
  necessary but on its own it is not sufficient reasoning: a ``kind="cpu"``
  task declared ``async def`` DOES run inside a real event loop, one
  ``asyncio.run()`` per call, started by ``cauli._exec.py`` only after the
  before/after hooks for that request have already run outside of it. The
  hook is excluded only because it runs before that loop exists, an
  accident of call order in ``_exec.py``, not a property of cpu lane async
  tasks themselves; inside the task body itself
  ``asyncio.get_running_loop()`` succeeds just fine. That is precisely why
  :func:`get_session` performs no loop check of its own and never opens a
  session lazily: the only place a session is allowed to come from is the
  before task hook, at the one call site documented above, and a loop check
  inside the accessor would be fooled by exactly the case this paragraph
  describes.
"""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from cauli.app import Cauli

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


#: None means "no session active for the current task". A plain default
#: rather than an unset ContextVar so every reader (the accessor, the after
#: task hook, the tests) can use one uniform `.get()` with no try/except.
_session_var: ContextVar[Any] = ContextVar("cauli_fastapi_session", default=None)


def get_session() -> AsyncSession:
    """Return the :class:`~sqlalchemy.ext.asyncio.AsyncSession` opened for the
    task currently executing on this coroutine.

    Only reads the ``ContextVar`` the before task hook set; performs no guard
    of its own and never opens a session lazily, see the module docstring's
    cpu lane paragraph for why a loop check here would not be trustworthy.
    Raises ``LookupError`` when nothing is set: outside a task, on the sync
    thread pool, or from a ``kind="cpu"`` task, which this integration does
    not support.
    """
    session = _session_var.get()
    if session is None:
        raise LookupError(
            "cauli.contrib.fastapi.get_session(): no AsyncSession is active "
            "on this task. This accessor only works inside an async io lane "
            "task whose app was wired by install_sqlalchemy_session() or "
            "fastapi_app(); it is inert by design on the sync thread pool "
            'and for kind="cpu" tasks (use a plain synchronous SQLAlchemy '
            "engine there instead)."
        )
    return session


def fastapi_app(database_url: str, **overrides: Any) -> Cauli:
    """Build a :class:`cauli.Cauli` app wired with a session per task async
    SQLAlchemy engine.

    ``database_url`` is passed straight to ``create_async_engine`` (an async
    driver dialect such as ``postgresql+psycopg://`` or
    ``postgresql+asyncpg://``; this module does not choose one for you).
    Keyword ``overrides`` go to ``Cauli(...)`` unchanged (``redis_url=...``
    etc). Registers the session hooks via :func:`install_sqlalchemy_session`.
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    app = Cauli(**overrides)
    return install_sqlalchemy_session(app, engine)


def install_sqlalchemy_session(app: Cauli, engine: AsyncEngine) -> Cauli:
    """Register the session per task lifecycle hooks on ``app`` for ``engine``.

    Builds one ``async_sessionmaker(engine, expire_on_commit=False)`` right
    now, not lazily on first task, building a sessionmaker does no I/O
    either. ``expire_on_commit=False`` is fixed, not configurable here: after
    an ``await session.commit()`` a task very often keeps using attributes on
    the rows it just committed, and SQLAlchemy's default of expiring them on
    commit would turn that ordinary next line into an implicit I/O refresh,
    which async SQLAlchemy raises on rather than allows outside an active
    greenlet context.

    - before task: opens an ``AsyncSession`` and stores it in the
      ``ContextVar`` :func:`get_session` reads. Does nothing when
      ``asyncio.get_running_loop()`` raises, the same "not my lane" guard
      ``cauli.contrib.django.install_orm_executors`` uses; see the module
      docstring for the one case, ``kind="cpu"`` async tasks, where that
      guard's exclusion is a call order accident and not a property of the
      task, and why that is exactly the reasoning this hook, not the
      accessor, is the only place allowed to open a session.
    - after task: unconditionally awaits ``session.close()`` and clears the
      ContextVar, on every outcome the hook runs for (success, a raised
      exception, a soft timeout), so a session never survives past its task
      and the ContextVar never hands a closed, stale session to whatever
      runs next on the same thread. Never commits or rolls back; that
      decision belongs to task code, not this module (see the module
      docstring).
    - process init: disposes ``engine`` (via ``asyncio.run()``, since
      process init hooks fire before any loop exists in every context they
      run in). Must run in every process that imported this engine, most
      importantly a forked cpu child, so a pooled connection opened before
      the fork can never end up split between parent and child.

    Called for you by :func:`fastapi_app`; call directly when you already
    have a ``Cauli`` app and/or built the engine yourself.
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker

    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    def cauli_open_sqlalchemy_session() -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return  # sync pool thread or cpu child: not this integration's lane
        _session_var.set(session_factory())

    def cauli_close_sqlalchemy_session() -> Any:
        session = _session_var.get()
        if session is None:
            return None  # before task hook never opened one on this path
        _session_var.set(None)
        return session.close()  # shim awaits the returned coroutine (PROTOCOL.md 4.8)

    def cauli_dispose_sqlalchemy_engine() -> None:
        asyncio.run(engine.dispose())

    app.before_task(cauli_open_sqlalchemy_session)
    app.after_task(cauli_close_sqlalchemy_session)
    app.process_init(cauli_dispose_sqlalchemy_engine)
    return app
