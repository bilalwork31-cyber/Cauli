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
_U32_MAX = 2**32 - 1


def _default_task_name(fn: Callable[..., Any]) -> str:
    """Return ``module.qualname`` as the worker will import it.

    ``python tasks.py`` gives ``__module__ == "__main__"``, but the worker
    imports ``tasks``, so ``__main__.x`` would be dead lettered as unknown.
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

    The worker reads these attributes by name:
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
        max_retries = int(max_retries)
        backoff_base_ms = int(round(backoff_base * 1000))
        backoff_max_ms = int(round(backoff_max * 1000))
        # The worker parses these as unsigned and refuses a zero timeout;
        # anything else dead letters every call, so get() would wait forever.
        if timeout_ms <= 0:
            raise ValueError(f"timeout must be at least 0.001 seconds, got {timeout!r}")
        if not 0 <= max_retries <= _U32_MAX:
            raise ValueError(
                f"max_retries must be between 0 and {_U32_MAX}, got {max_retries!r}"
            )
        for field, value_ms in (
            ("soft_timeout", soft_timeout_ms),
            ("backoff_base", backoff_base_ms),
            ("backoff_max", backoff_max_ms),
        ):
            if value_ms is not None and value_ms < 0:
                raise ValueError(
                    f"{field} must be >= 0 seconds, got {value_ms / 1000:g}"
                )

        self.app = app
        self.name: str = name or _default_task_name(fn)
        self.fn: Callable[..., Any] = fn
        self.is_async: bool = inspect.iscoroutinefunction(fn)
        # Bound once: inspect.signature() per call was most of an enqueue's
        # cost. None means not introspectable (some builtins): calls go unchecked.
        try:
            # follow_wrapped=False: the worker calls `fn` itself, so a
            # decorator that changes the arguments is checked as written.
            self._signature: inspect.Signature | None = inspect.signature(
                fn, follow_wrapped=False
            )
        except (TypeError, ValueError):
            self._signature = None
        self.kind: str = kind
        self.queue: str | None = queue
        self.max_retries: int = max_retries
        self.timeout_ms: int = timeout_ms
        self.soft_timeout_ms: int | None = soft_timeout_ms
        self.backoff_base_ms: int = backoff_base_ms
        self.backoff_factor: float = float(backoff_factor)
        self.backoff_max_ms: int = backoff_max_ms
        self.jitter: bool = bool(jitter)
        self.store_result: bool = bool(store_result)

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
        """Raise TypeError at the call site rather than inside the worker."""
        sig = self._signature
        if sig is None:
            return
        try:
            sig.bind(*args, **kwargs)
        except TypeError as exc:
            raise TypeError(f"{self.name}: {exc}") from None

    def _check_encodable(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        """Raise TypeError if the payload cannot be JSON encoded.

        Only the deferred on_commit path needs this dry run. TypeError because
        ``msgspec.EncodeError`` is neither a TypeError nor a ValueError.
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

        ``countdown`` (seconds from now) and ``eta`` (timezone aware datetime;
        naive raises ValueError, past means due now) are mutually exclusive.
        ``expires`` (seconds from now or aware datetime): a task picked up
        later is discarded with status ``"expired"`` (PROTOCOL.md section 9.1).
        ``queue`` overrides routing rules, the task queue and the app default.
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
        """:meth:`delay` without blocking the event loop. Requires an ``AsyncCauli``."""
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
        """:meth:`apply_async` without blocking the event loop. Requires ``AsyncCauli``."""
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

        Plain ``delay()`` inside ``transaction.atomic()`` publishes at once, so
        the worker can read an uncommitted row or run after a rollback. This
        uses ``transaction.on_commit`` on the default database: it publishes on
        commit, never on rollback, and immediately outside an atomic block.

        Returns ``None``: no task id exists until commit. Requires Django.
        Under ``django.test.TestCase`` nothing is enqueued; see
        :meth:`apply_async_on_commit`.
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

        Options as :meth:`apply_async`, semantics as :meth:`delay_on_commit`.
        ``using`` picks the database alias (default database when ``None``).
        ``countdown`` and ``expires`` count from commit, not from the call.
        Signature and JSON checks run at the call site, so a bad call raises
        inside the atomic block and rolls it back.

        In tests ``django.test.TestCase`` always rolls back, so nothing is
        published. Wrap the code in
        ``with self.captureOnCommitCallbacks(execute=True):`` or subclass
        ``TransactionTestCase``. With pytest-django, ``db`` behaves like
        ``TestCase`` and ``transactional_db`` like ``TransactionTestCase``.
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
        # Raising at COMMIT would leave the row durable and the task lost.
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
