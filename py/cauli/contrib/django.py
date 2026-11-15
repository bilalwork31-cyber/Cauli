"""Django integration for cauli.

Usage, mirroring the Celery layout::

    # myproj/cauli.py
    from cauli.contrib.django import autodiscover_tasks, django_app

    app = django_app()          # or django_app("myproj.settings")
    autodiscover_tasks(app)     # imports <app>.tasks for each INSTALLED_APP

    # myproj/store/tasks.py     (any INSTALLED_APPS package)
    from myproj.cauli import app

    @app.task()
    def refresh_prices(sku_id): ...

    # run the worker
    cauli-worker --app myproj.cauli:app

:func:`django_app` runs ``django.setup()`` when needed, reads
``CAULI_REDIS_URL``, ``CAULI_BROKER_URL``, ``CAULI_DEFAULT_QUEUE``,
``CAULI_RESULT_TTL`` and ``CAULI_IDEMP_TTL`` from settings, and installs
:func:`install_orm_executors` and :func:`install_db_hooks`.

Connections: Postgres backends are about ``procs * io_threads`` plus
``procs * min(CAULI_ORM_EXECUTORS, io_concurrency)`` plus
``procs * cpu_workers``. Put pgbouncer (transaction mode) in front once that
nears ``max_connections`` (100 by default); see ``docs/configuration.md``.

``TaskDef.delay_on_commit`` and ``apply_async_on_commit`` defer publishing
until the current transaction commits.
"""

from __future__ import annotations

import contextvars
import importlib
import importlib.util
import os
import threading
from typing import Any, Iterable

from cauli.app import AsyncCauli, Cauli

_SETTINGS_MAP = (
    ("CAULI_REDIS_URL", "redis_url"),
    ("CAULI_BROKER_URL", "broker_url"),
    ("CAULI_DEFAULT_QUEUE", "default_queue"),
    ("CAULI_RESULT_TTL", "result_ttl"),
    ("CAULI_IDEMP_TTL", "idemp_ttl"),
)


def django_app(settings_module: str | None = None, **overrides: Any) -> AsyncCauli:
    """Build a :class:`cauli.AsyncCauli` app wired for a Django project.

    ``settings_module`` (e.g. ``"myproj.settings"``) is the
    ``DJANGO_SETTINGS_MODULE`` default; omit it when the variable is set.
    ``overrides`` win over Django settings, which win over ``Cauli``
    defaults. ``orm_executors`` (or ``CAULI_ORM_EXECUTORS``) sizes
    :func:`install_orm_executors`.
    """
    if settings_module:
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)

    import django
    from django.apps import apps
    from django.conf import settings

    if not apps.ready:
        django.setup()

    kwargs: dict[str, Any] = {}
    for setting_name, kwarg in _SETTINGS_MAP:
        if hasattr(settings, setting_name):
            kwargs[kwarg] = getattr(settings, setting_name)
    orm_executors = overrides.pop(
        "orm_executors",
        getattr(settings, "CAULI_ORM_EXECUTORS", DEFAULT_ORM_EXECUTORS),
    )
    kwargs.update(overrides)

    app = AsyncCauli(**kwargs)
    # The executor hook must run before close_old_connections, so the health
    # check runs on the thread whose connection the task will use.
    install_orm_executors(app, workers=orm_executors)
    install_db_hooks(app)
    return app


#: Process wide: several apps in one process must share one answer.
_connection_opened = False
_CONNECTION_SIGNAL_UID = "cauli.contrib.django.connection_opened"


def _note_connection_opened(**_kwargs: Any) -> None:
    global _connection_opened
    _connection_opened = True


#: Measured knee: 64 concurrent 5 ms ORM calls took 348 ms on one thread, 48 ms on 8.
DEFAULT_ORM_EXECUTORS = 8

_orm_executors: list[Any] = []
_orm_tokens: list[Any] = []
_orm_rr = 0
_orm_lock = threading.Lock()
#: Tasks in flight per executor token, and whether the current task is one.
_orm_in_flight: dict[Any, int] = {}
_orm_counted: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "cauli_orm_counted", default=False
)


def _shares_its_executor() -> bool:
    """True when another task is running on the current task's ORM executor."""
    from asgiref.sync import SyncToAsync

    token = SyncToAsync.thread_sensitive_context.get(None)
    with _orm_lock:
        others = _orm_in_flight.get(token, 0) - _orm_counted.get()
    return others > 0


def _close_broken_connections() -> None:
    """``close_old_connections`` for a thread other tasks are using.

    Closes only a connection that has already failed, which no task can be
    relying on. Closing one merely past ``CONN_MAX_AGE`` would pull it from
    under a neighbour that is mid query; that waits until the executor has a
    single task.
    """
    from django.db import connections

    for conn in connections.all(initialized_only=True):
        if conn.errors_occurred and not conn.is_usable():
            conn.close()


def install_orm_executors(app: Cauli, workers: int = DEFAULT_ORM_EXECUTORS) -> Cauli:
    """Give async tasks ``workers`` sticky threads for Django's async ORM.

    Otherwise every ``sync_to_async(thread_sensitive=True)`` call, which
    Django's ``aget``/``acreate`` use, serialises on asgiref's one global
    thread and ``CONN_MAX_AGE`` is ignored. A before task hook assigns each
    async task one executor round robin through asgiref's
    ``thread_sensitive_context``, so one task keeps one thread and one
    connection, and the process holds at most ``workers`` connections.
    Tasks on one executor share its connection: do not hold a transaction
    open across an ``await``.

    Sync pool and cpu tasks are untouched. ``workers=0`` disables it. The
    pool is process wide and the first caller sets its size.
    """
    if workers <= 0:
        return app
    try:
        from asgiref.sync import SyncToAsync, ThreadSensitiveContext
    except ImportError:
        return app
    if not hasattr(SyncToAsync, "thread_sensitive_context") or not hasattr(
        SyncToAsync, "context_to_thread_executor"
    ):
        import warnings

        warnings.warn(
            "cauli.contrib.django: this asgiref version lacks "
            "thread_sensitive_context/context_to_thread_executor; sticky ORM "
            "executors disabled, async ORM work will serialise on asgiref's "
            "global executor thread",
            RuntimeWarning,
            stacklevel=2,
        )
        return app

    from concurrent.futures import ThreadPoolExecutor

    with _orm_lock:
        if not _orm_executors:
            for i in range(int(workers)):
                ex = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix=f"cauli-orm-{i}"
                )
                # asgiref reads attributes off whatever the ContextVar holds
                # (3.13 added ._exited), so hold its own type, never entered.
                # The module list keeps the WeakKeyDictionary key alive.
                token = ThreadSensitiveContext()
                SyncToAsync.context_to_thread_executor[token] = ex
                _orm_executors.append(ex)
                _orm_tokens.append(token)

    def cauli_assign_orm_executor() -> None:
        global _orm_rr
        try:
            import asyncio

            asyncio.get_running_loop()
        except RuntimeError:
            return
        with _orm_lock:
            token = _orm_tokens[_orm_rr % len(_orm_tokens)]
            _orm_rr += 1
            _orm_in_flight[token] = _orm_in_flight.get(token, 0) + 1
        # Hooks run inside the task's context, so this is per task.
        SyncToAsync.thread_sensitive_context.set(token)
        _orm_counted.set(True)

    def cauli_release_orm_executor() -> None:
        if not _orm_counted.get():
            return
        _orm_counted.set(False)
        token = SyncToAsync.thread_sensitive_context.get(None)
        with _orm_lock:
            _orm_in_flight[token] = _orm_in_flight.get(token, 1) - 1

    app.before_task(cauli_assign_orm_executor)
    app.after_task(cauli_release_orm_executor)
    return app


def install_db_hooks(app: Cauli) -> Cauli:
    """Register Django connection lifecycle hooks on ``app``, as Celery does.

    - before and after every task: ``close_old_connections``, so
      ``CONN_MAX_AGE`` and ``CONN_HEALTH_CHECKS`` apply and a connection
      stale after a database restart is replaced.
    - process init: ``connections.close_all``, so no connection opened at
      import survives into forked cpu children.

    Async tasks run the cleanup through ``sync_to_async(thread_sensitive=True)``,
    the thread that holds their connections. While another task shares that
    thread only a connection that has already failed is closed. The hop is skipped until the
    process opens its first connection (latched on ``connection_created``;
    the loop thread cannot see the executor's connections), saving 21% of
    worker CPU on an async HTTP workload.
    """
    from django.db import close_old_connections, connections
    from django.db.backends.signals import connection_created

    connection_created.connect(
        _note_connection_opened,
        weak=False,
        dispatch_uid=_CONNECTION_SIGNAL_UID,
    )

    def cauli_close_old_connections() -> Any:
        try:
            import asyncio

            asyncio.get_running_loop()
        except RuntimeError:
            close_old_connections()
            return None
        if not _connection_opened:
            return None
        # The shim awaits the returned coroutine (PROTOCOL.md section 4.8).
        from asgiref.sync import sync_to_async

        cleanup = (
            _close_broken_connections
            if _shares_its_executor()
            else close_old_connections
        )
        return sync_to_async(cleanup, thread_sensitive=True)()

    def cauli_close_all_connections() -> None:
        global _connection_opened
        connections.close_all()
        # A forked child inherits the flag but close_all just dropped the
        # connections, so it starts clean.
        _connection_opened = False

    app.before_task(cauli_close_old_connections)
    app.after_task(cauli_close_old_connections)
    app.process_init(cauli_close_all_connections)
    return app


def autodiscover_tasks(
    app: Cauli,
    packages: Iterable[str] | None = None,
    related_name: str = "tasks",
) -> list[str]:
    """Import ``<pkg>.<related_name>`` for each Django app, or each of ``packages``.

    A package without that module is skipped; an import failure raises.
    Returns the imported module names. ``app`` only documents intent.
    """
    del app
    if packages is None:
        from django.apps import apps as django_apps

        packages = [cfg.name for cfg in django_apps.get_app_configs()]

    imported: list[str] = []
    for pkg in packages:
        if not hasattr(importlib.import_module(pkg), "__path__"):
            continue  # a plain module app has no submodules
        module_name = f"{pkg}.{related_name}"
        if importlib.util.find_spec(module_name) is None:
            continue
        importlib.import_module(module_name)
        imported.append(module_name)
    return imported
