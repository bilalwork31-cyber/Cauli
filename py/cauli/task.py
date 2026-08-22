"""TaskDef: a registered task definition.

The attribute names on :class:`TaskDef` are a wire contract (PROTOCOL.md
section 6): the Rust worker reads them via ``getattr`` through the embedded
interpreter. Do not rename them.
"""

from __future__ import annotations

import inspect
import os
import sys
import warnings
from typing import TYPE_CHECKING, Any, Callable

from cauli import _codec

if TYPE_CHECKING:
    from datetime import datetime

    from cauli.app import Cauli
    from cauli.result import AsyncResult

_VALID_KINDS = ("io", "cpu")


def _default_task_name(fn: Callable[..., Any]) -> str:
    """Build the default ``module.qualname`` name for ``fn``.

    A tasks module run as a script (``python tasks.py``) gives its functions
    ``__module__ == "__main__"``, so the naive default would be
    ``__main__.hello``. The worker imports that same file by module name
    (``importlib.import_module``), so its registry is keyed ``tasks.hello``:
    an envelope stamped ``__main__.hello`` misses the registry and is
    terminally dead lettered on the very first enqueue. So resolve what the
    module will be called once imported normally -- ``__spec__.name`` when
    ``python -m`` set one, otherwise the file stem. When neither exists (a
    REPL, ``python -c``, a notebook) there is no importable name to guess, so
    warn loudly rather than mint a name the worker can never resolve.
    """
    qualname = fn.__qualname__
    module = fn.__module__
    if module != "__main__":
        return f"{module}.{qualname}"

    main = sys.modules.get("__main__")
    spec_name = getattr(getattr(main, "__spec__", None), "name", None)
    if spec_name and spec_name != "__main__":
        return f"{spec_name}.{qualname}"

    path = getattr(main, "__file__", None)
    if path:
        stem = os.path.splitext(os.path.basename(path))[0]
        if stem == "__init__":
            stem = os.path.basename(os.path.dirname(os.path.abspath(path)))
        if stem and stem != "__main__":
            return f"{stem}.{qualname}"

    warnings.warn(
        f"task {qualname!r} is defined in __main__ and no importable module "
        f"name could be resolved, so it is registered as '__main__.{qualname}'. "
        "The worker imports task modules by name, so its registry will not "
        "hold that key and every enqueue of this task is dead lettered as an "
        "unknown task. Pass an explicit @app.task(name=...) or move the task "
        "into an importable module.",
        RuntimeWarning,
        stacklevel=4,
    )
    return f"__main__.{qualname}"


class TaskDef:
    """A registered task. Created by the ``@app.task(...)`` decorator.

    Contract attributes read by the Rust worker (exact names):
    ``name``, ``fn``, ``is_async``, ``kind``, ``queue``, ``max_retries``,
    ``timeout_ms``, ``soft_timeout_ms``, ``backoff_base_ms``,
    ``backoff_factor``, ``backoff_max_ms``, ``jitter``, ``store_result``.
    """

    def __init__(
        self,
        app: "Cauli",
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        kind: str | None = None,
        queue: str | None = None,
        max_retries: int = 3,
        timeout: float = 300.0,
        soft_timeout: float | None = None,
        backoff_base: float = 0.5,
        backoff_factor: float = 2.0,
        backoff_max: float = 60.0,
        jitter: bool = True,
        store_result: bool = True,
    ) -> None:
        if kind is None:
            kind = "io"
        if kind not in _VALID_KINDS:
            raise ValueError(
                f"invalid task kind {kind!r}: must be one of {_VALID_KINDS}"
            )
        timeout_ms = int(round(timeout * 1000))
        soft_timeout_ms = (
            None if soft_timeout is None else int(round(soft_timeout * 1000))
        )
        if soft_timeout_ms is not None and soft_timeout_ms >= timeout_ms:
            raise ValueError(
                f"soft_timeout ({soft_timeout}s) must be strictly less than timeout ({timeout}s)"
            )

        self.app = app
        self.name: str = name or _default_task_name(fn)
        self.fn: Callable[..., Any] = fn
        self.is_async: bool = inspect.iscoroutinefunction(fn)
        self.kind: str = kind
        self.queue: str | None = queue
        self.max_retries: int = int(max_retries)
        self.timeout_ms: int = timeout_ms
        self.soft_timeout_ms: int | None = soft_timeout_ms
        self.backoff_base_ms: int = int(round(backoff_base * 1000))
        self.backoff_factor: float = float(backoff_factor)
        self.backoff_max_ms: int = int(round(backoff_max * 1000))
        self.jitter: bool = bool(jitter)
        self.store_result: bool = bool(store_result)

        # Cosmetic function metadata so the decorated object introspects nicely.
        for attr in ("__module__", "__name__", "__qualname__", "__doc__"):
            try:
                setattr(self, attr, getattr(fn, attr))
            except AttributeError:
                pass
        self.__wrapped__ = fn

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Run the task function inline (no broker involved)."""
        return self.fn(*args, **kwargs)

    def _check_signature(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        """Raise TypeError if ``args``/``kwargs`` do not match the task function.

        Without this, a bad call (a typo'd keyword, a missing required
        argument) enqueues successfully and only fails inside the worker when
        it finally calls ``fn(*args, **kwargs)``. By that point ``.delay()``
        has already returned a normal looking AsyncResult, and the common
        fire and forget pattern never calls ``.get()`` to notice. Checking the
        same way Python itself would (``Signature.bind``) also handles a task
        declared with ``*args``/``**kwargs`` correctly, with no special
        casing.
        """
        try:
            sig = inspect.signature(self.fn)
        except (TypeError, ValueError):
            # Some callables cannot be introspected (e.g. certain builtins or
            # C extension functions). There is nothing safe to check then, so
            # the call proceeds unchecked; the worker's own error remains the
            # only signal for those.
            return
        try:
            sig.bind(*args, **kwargs)
        except TypeError as exc:
            raise TypeError(f"{self.name}: {exc}") from None

    def _check_encodable(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        """Raise TypeError if the call payload cannot reach the wire as JSON.

        A dry run of the same encode ``Cauli._enqueue`` performs on the
        envelope, over just the user-controlled part of it. Only
        :meth:`apply_async_on_commit` needs this: the eager paths encode for
        real a microsecond later and so fail at the call site anyway, while
        the deferred path would not find out until COMMIT. The raw codec
        errors are normalised to TypeError because ``msgspec.EncodeError`` is
        neither a ValueError nor a TypeError subclass, and would slip through
        an ordinary ``except`` around the enqueue.
        """
        try:
            _codec.encode({"args": list(args), "kwargs": kwargs})
        except _codec.ENCODE_ERRORS as exc:
            raise TypeError(
                f"{self.name}: task arguments are not JSON encodable: {exc}"
            ) from exc

    def delay(self, *args: Any, **kwargs: Any) -> "AsyncResult":
        """Enqueue with positional/keyword args. Returns an AsyncResult."""
        self._check_signature(args, kwargs)
        return self.app._enqueue(self, args, kwargs)

    def apply_async(
        self,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
        countdown: float | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
        eta: "datetime | None" = None,
        expires: "float | datetime | None" = None,
    ) -> "AsyncResult":
        """Enqueue with full options. Returns an AsyncResult.

        ``countdown`` (seconds, relative) and ``eta`` (absolute datetime) both
        delay execution via the delayed zset and are mutually exclusive. An
        ``eta`` MUST be timezone aware -- a naive datetime raises ValueError
        rather than being silently reinterpreted as UTC or as local time. An
        ``eta`` in the past is not an error; it means "due now".

        ``expires`` bounds how long the task is worth running: seconds from now
        (number) or an absolute timezone-aware datetime. A task picked up after
        that instant is discarded instead of executed -- DLQ reason
        ``"expired"``, result status ``"expired"`` (PROTOCOL.md section 9.1).

        ``queue`` overrides the app's routing rules, the task queue and the app
        default queue.
        """
        args = tuple(args)
        kwargs = dict(kwargs or {})
        self._check_signature(args, kwargs)
        return self.app._enqueue(
            self,
            args,
            kwargs,
            countdown=countdown,
            queue=queue,
            idempotency_key=idempotency_key,
            eta=eta,
            expires=expires,
        )

    def _async_app(self) -> Any:
        """The app, if it can enqueue on an event loop; otherwise a clear error."""
        enqueue = getattr(self.app, "_aenqueue", None)
        if enqueue is None:
            raise TypeError(
                f"{self.name}: awaiting an enqueue needs an AsyncCauli app, but "
                f"this task is registered on a {type(self.app).__name__}. Build "
                "the app as cauli.AsyncCauli(...) (it is a Cauli, so .delay() "
                "and every other call keep working), or call .delay() here."
            )
        return enqueue

    async def adelay(self, *args: Any, **kwargs: Any) -> "AsyncResult":
        """:meth:`delay` without blocking the event loop. Requires an ``AsyncCauli``.

        The envelope is identical to the one ``delay()`` writes; only the
        Redis client differs (``redis.asyncio`` rather than the blocking one).
        """
        self._check_signature(args, kwargs)
        return await self._async_app()(self, args, kwargs)

    async def aapply_async(
        self,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
        countdown: float | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
        eta: "datetime | None" = None,
        expires: "float | datetime | None" = None,
    ) -> "AsyncResult":
        """:meth:`apply_async` without blocking the event loop. Same options,
        same envelope, same queue precedence. Requires an ``AsyncCauli``."""
        args = tuple(args)
        kwargs = dict(kwargs or {})
        self._check_signature(args, kwargs)
        return await self._async_app()(
            self,
            args,
            kwargs,
            countdown=countdown,
            queue=queue,
            idempotency_key=idempotency_key,
            eta=eta,
            expires=expires,
        )

    def delay_on_commit(self, *args: Any, **kwargs: Any) -> None:
        """Enqueue only after the current Django database transaction commits.

        The footgun this prevents: ``task.delay(obj.id)`` inside
        ``transaction.atomic()`` publishes IMMEDIATELY — the worker can pick
        the task up and query for a row the transaction has not committed yet
        (task fails or reads stale data), and if the transaction rolls back,
        the task runs anyway, referencing a row that never existed. This
        method hands the enqueue to ``django.db.transaction.on_commit``: it
        publishes only if (and when) the surrounding transaction commits, and
        never publishes on rollback. Outside an atomic block Django runs
        ``on_commit`` callbacks immediately, so it degrades to ``delay``.

        Returns ``None`` (not an :class:`~cauli.result.AsyncResult`): no task
        id exists until the commit actually publishes. Requires Django;
        raises RuntimeError otherwise. Uses the default database alias — use
        :meth:`apply_async_on_commit` for a specific one.

        Under ``django.test.TestCase`` nothing is enqueued at all: that class
        rolls its atomic block back, so the callback never runs. See
        :meth:`apply_async_on_commit` for the two ways to observe the enqueue
        in tests.
        """
        self.apply_async_on_commit(args=args, kwargs=kwargs)

    def apply_async_on_commit(
        self,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
        countdown: float | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
        using: str | None = None,
        eta: "datetime | None" = None,
        expires: "float | datetime | None" = None,
    ) -> None:
        """:meth:`apply_async`, deferred to Django's transaction commit.

        Same enqueue options as :meth:`apply_async`, same commit/rollback
        semantics and ``None`` return as :meth:`delay_on_commit`. ``using``
        selects the database alias whose transaction to hook (default
        database when ``None``).

        Note that ``countdown`` and ``expires`` are relative to COMMIT time,
        not to the call: the envelope is built inside the on_commit callback,
        so a long transaction does not eat into either budget. ``eta`` is
        absolute and so is unaffected either way.

        Validation is NOT deferred: the signature check and a JSON encode dry
        run of the arguments run at the call site, so a bad call raises inside
        the atomic block and rolls the transaction back with it.

        In tests, expect NO enqueue by default. ``django.test.TestCase`` wraps
        every test in an atomic block it always rolls back, so the callback is
        discarded and the task is silently never published. Two ways to see
        it: wrap the code under test in
        ``with self.captureOnCommitCallbacks(execute=True):`` (the callbacks
        run at the end of the block, and the context manager also hands you
        the list of them), or subclass ``TransactionTestCase``, which commits
        for real. This applies to ``pytest-django`` too: the ``db`` fixture is
        ``TestCase`` semantics, ``transactional_db`` is
        ``TransactionTestCase``.
        """
        try:
            from django.db import transaction
        except ImportError as exc:  # pragma: no cover - exercised without django
            raise RuntimeError(
                "delay_on_commit/apply_async_on_commit require Django "
                "(the enqueue is deferred via django.db.transaction.on_commit); "
                "install django or use delay/apply_async"
            ) from exc
        frozen_args = tuple(args)
        frozen_kwargs = dict(kwargs or {})
        # Validate BEFORE handing the enqueue to on_commit. Everything the
        # deferred call can still reject -- a mistyped keyword, a payload the
        # codec refuses (a UUID primary key, a model instance, a Decimal) --
        # has to raise HERE, inside the caller's atomic block, so the
        # transaction rolls back together with the lost enqueue. Raising at
        # COMMIT instead is the worst of both: the row is already durable and
        # the task simply never exists.
        self._check_signature(frozen_args, frozen_kwargs)
        self._check_encodable(frozen_args, frozen_kwargs)
        transaction.on_commit(
            lambda: self.apply_async(
                frozen_args,
                frozen_kwargs,
                countdown=countdown,
                queue=queue,
                idempotency_key=idempotency_key,
                eta=eta,
                expires=expires,
            ),
            using=using,
        )

    def __repr__(self) -> str:
        return f"<TaskDef {self.name} kind={self.kind} queue={self.queue!r}>"
