"""TaskDef: a registered task definition.

The attribute names on :class:`TaskDef` are a wire contract (PROTOCOL.md
section 6): the Rust worker reads them via ``getattr`` through the embedded
interpreter. Do not rename them.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from cauli.app import Cauli
    from cauli.result import AsyncResult

_VALID_KINDS = ("io", "cpu")


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
        self.name: str = name or f"{fn.__module__}.{fn.__qualname__}"
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

    def delay(self, *args: Any, **kwargs: Any) -> "AsyncResult":
        """Enqueue with positional/keyword args. Returns an AsyncResult."""
        return self.app._enqueue(self, args, kwargs)

    def apply_async(
        self,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
        countdown: float | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
    ) -> "AsyncResult":
        """Enqueue with full options. Returns an AsyncResult.

        ``countdown`` (seconds) delays execution via the delayed zset.
        ``queue`` overrides both the task queue and the app default queue.
        """
        return self.app._enqueue(
            self,
            tuple(args),
            dict(kwargs or {}),
            countdown=countdown,
            queue=queue,
            idempotency_key=idempotency_key,
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
    ) -> None:
        """:meth:`apply_async`, deferred to Django's transaction commit.

        Same enqueue options as :meth:`apply_async`, same commit/rollback
        semantics and ``None`` return as :meth:`delay_on_commit`. ``using``
        selects the database alias whose transaction to hook (default
        database when ``None``).
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
        transaction.on_commit(
            lambda: self.apply_async(
                frozen_args,
                frozen_kwargs,
                countdown=countdown,
                queue=queue,
                idempotency_key=idempotency_key,
            ),
            using=using,
        )

    def __repr__(self) -> str:
        return f"<TaskDef {self.name} kind={self.kind} queue={self.queue!r}>"
