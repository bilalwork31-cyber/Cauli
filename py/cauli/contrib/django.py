"""Django integration for cauli (opt-in; cauli core stays framework-agnostic).

Typical project layout, mirroring the conventional Celery setup::

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

What :func:`django_app` gives you on top of a plain ``Cauli()``:

- ``django.setup()`` when needed, so ``cauli-worker --app`` works standalone
  (outside ``manage.py``) with just ``DJANGO_SETTINGS_MODULE`` set.
- Config read from Django settings (``CAULI_REDIS_URL``,
  ``CAULI_DEFAULT_QUEUE``, ``CAULI_RESULT_TTL``, ``CAULI_IDEMP_TTL``).
- Database connection lifecycle parity with Celery's Django fixup
  (:func:`install_db_hooks`): ``close_old_connections`` before and after
  every task on every execution path (sync thread pool, asyncio loops, cpu
  children), so ``CONN_MAX_AGE`` is honored and a connection gone stale
  across a database restart/failover is discarded instead of poisoning the
  worker thread that cached it. Without this, the worker's long-lived
  threads each cache a thread-local Django connection forever.

Enqueue-side, every task additionally has ``delay_on_commit`` /
``apply_async_on_commit`` (defined on ``TaskDef`` core with a lazy Django
import) to defer publishing until the current transaction commits.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from typing import Any, Iterable

from cauli.app import Cauli

_SETTINGS_MAP = (
    ("CAULI_REDIS_URL", "redis_url"),
    ("CAULI_DEFAULT_QUEUE", "default_queue"),
    ("CAULI_RESULT_TTL", "result_ttl"),
    ("CAULI_IDEMP_TTL", "idemp_ttl"),
)


def django_app(settings_module: str | None = None, **overrides: Any) -> Cauli:
    """Build a :class:`cauli.Cauli` app wired for a Django project.

    ``settings_module`` (e.g. ``"myproj.settings"``) is applied via
    ``os.environ.setdefault("DJANGO_SETTINGS_MODULE", ...)``; omit it when the
    environment variable is already set (``manage.py`` sets it). Keyword
    ``overrides`` (``redis_url=...`` etc.) win over Django settings, which win
    over the ``Cauli`` defaults. Registers the DB lifecycle hooks via
    :func:`install_db_hooks`.
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
    kwargs.update(overrides)

    app = Cauli(**kwargs)
    install_db_hooks(app)
    return app


#: Has this process ever opened a Django database connection? Process wide on
#: purpose: the question "is there anything to close" is about the process, not
#: about one app object, and several apps in one process must not each hold a
#: private answer that only their own signal receiver updates.
_connection_opened = False
_CONNECTION_SIGNAL_UID = "cauli.contrib.django.connection_opened"


def _note_connection_opened(**_kwargs: Any) -> None:
    global _connection_opened
    _connection_opened = True


def install_db_hooks(app: Cauli) -> Cauli:
    """Register Django DB connection lifecycle hooks on ``app``.

    Celery-fixup parity (its Django fixup calls ``close_old_connections``
    around every task), mapped onto cauli's lifecycle hooks:

    - before/after every task: ``close_old_connections`` — closes connections
      whose ``CONN_MAX_AGE`` expired or that errored, and (with Django's
      ``CONN_HEALTH_CHECKS``) health-checks the cached connection so one gone
      stale across a database restart is replaced before the task runs.
    - process init: ``connections.close_all`` — a connection opened at app
      import time must not survive into the fork-server's forked cpu children
      (a socket fd shared by every child) nor be reused by the worker after
      the fork-server parent inherited it.

    On the asyncio path the hook runs Django's cleanup through
    ``asgiref.sync_to_async(..., thread_sensitive=True)``, which executes in
    the same thread Django's async ORM interface (``acreate``/``aget``/...)
    runs sync DB code in — closing connections on the event loop thread
    itself would miss the thread that actually holds them.

    That hop is skipped entirely until this process has opened its first
    database connection. It cannot be decided by inspecting
    ``connections.all(initialized_only=True)`` from the event loop thread:
    the connections live in the thread-sensitive executor's context, so the
    loop thread always sees an empty handler and would skip the cleanup even
    when it is needed. Latching on Django's ``connection_created`` signal
    asks the question where the answer actually is. Until something opens a
    connection there is provably nothing to close, and the two thread
    hand-offs per task are pure overhead — measured at 21% of worker CPU on
    an async HTTP workload, worth +61% throughput when skipped. Once a
    connection exists the flag latches on and every task pays the hop again,
    which is the conservative direction: the cost returns, correctness never
    depends on the flag being cleared.

    Called for you by :func:`django_app`; call directly when you build the
    ``Cauli`` app yourself.
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
            close_old_connections()  # sync-pool thread or cpu child
            return None
        if not _connection_opened:
            return None  # nothing has ever been opened; nothing to close
        # Event loop thread (async task): run in asgiref's thread-sensitive
        # executor — the thread Django's async ORM uses for sync DB work.
        # The shim awaits the returned coroutine (PROTOCOL.md section 4.8).
        from asgiref.sync import sync_to_async

        return sync_to_async(close_old_connections, thread_sensitive=True)()

    def cauli_close_all_connections() -> None:
        global _connection_opened
        connections.close_all()
        # A forked cpu child inherits the parent's flag but not its usable
        # connections; close_all just dropped them, so the child starts clean.
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
    """Import ``<pkg>.<related_name>`` for each Django app (or each package in
    ``packages``), so ``@app.task`` decorators in those modules register.

    Mirrors Celery's ``app.autodiscover_tasks()``: a package without a
    ``tasks`` module is skipped silently; a ``tasks`` module that exists but
    fails to import raises (a broken tasks module must be loud, not an
    unregistered-task DLQ mystery at runtime). Returns the imported module
    names. ``app`` is unused beyond intent (imports register against whatever
    cauli app the task modules reference) but keeps call sites explicit about
    which app the discovery is for.
    """
    del app  # see docstring
    if packages is None:
        from django.apps import apps as django_apps

        packages = [cfg.name for cfg in django_apps.get_app_configs()]

    imported: list[str] = []
    for pkg in packages:
        module_name = f"{pkg}.{related_name}"
        try:
            spec = importlib.util.find_spec(module_name)
        except ModuleNotFoundError:
            spec = None  # the parent package itself has no such submodule
        if spec is None:
            continue
        importlib.import_module(module_name)
        imported.append(module_name)
    return imported
