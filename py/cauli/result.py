"""AsyncResult: a handle to a task's eventual result.

Reads ``cauli:result:{task_id}`` per PROTOCOL.md sections 6 and 8.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from cauli import _codec
from cauli.exceptions import TaskFailedError

if TYPE_CHECKING:
    from cauli.app import Cauli


class _Status(str):
    """A status string that is also callable, returning itself.

    :attr:`AsyncResult.status` is a property, so the Celery idiom
    ``r.status == "success"`` compares two strings instead of silently
    comparing a bound method to a string and being ``False`` forever. Every
    existing call site spells it ``r.status()``, though, and PROTOCOL.md
    documents that spelling, so the value stays callable rather than
    breaking them. Calling it does NOT re-read Redis: the read happened when
    the property was evaluated.
    """

    __slots__ = ()

    def __call__(self) -> "_Status":
        return self


class AsyncResult:
    """Handle to a task result stored in Redis.

    ``duplicate`` becomes ``True`` after :meth:`get` observes a result with
    status ``"duplicate"`` (idempotency guard hit); ``get`` returns ``None``
    in that case, and ``claimant_id`` names the task that holds the
    idempotency key (see :meth:`claimant`). ``expired`` becomes ``True`` when
    the task was discarded unrun past its ``expires`` deadline; :meth:`get`
    raises in that case.
    """

    def __init__(self, task_id: str, app: "Cauli") -> None:
        self.id: str = task_id
        self.app = app
        self.duplicate: bool = False
        self.expired: bool = False
        self.claimant_id: str | None = None

    @property
    def _key(self) -> str:
        return f"cauli:result:{self.id}"

    def _async_redis(self) -> Any:
        """The app's ``redis.asyncio`` client, or a clear error if it has none."""
        getter = getattr(self.app, "_get_async_redis", None)
        if getter is None:
            raise TypeError(
                f"awaiting the result of task {self.id} needs an AsyncCauli "
                f"app, but this handle came from a {type(self.app).__name__}. "
                "Build the app as cauli.AsyncCauli(...), or call .get()."
            )
        return getter()

    def _load(self) -> dict[str, Any] | None:
        """Fetch and decode the result document, or None if the key is absent."""
        return self._decode(self.app._get_redis().get(self._key))

    async def _aload(self) -> dict[str, Any] | None:
        """:meth:`_load` on the asyncio client. Requires an ``AsyncCauli`` app."""
        return self._decode(await self._async_redis().get(self._key))

    def _decode(self, raw: Any) -> dict[str, Any] | None:
        """Decode a raw result value, or None if the key was absent.

        A key that DOES exist but is not a usable result document (bytes that
        are not valid JSON, or JSON that decodes to something other than an
        object) raises :class:`TaskFailedError` with ``type ==
        "InvalidResult"`` naming the task id and the problem, rather than
        leaking msgspec's or Python's own internal exception type up through
        :meth:`status`/:meth:`get`.
        """
        if raw is None:
            return None
        try:
            doc = _codec.decode(raw)
        except _codec.DECODE_ERRORS as exc:
            raise TaskFailedError(
                "InvalidResult",
                f"result document for task {self.id} is not valid JSON: {exc}",
                None,
                "client",
            ) from exc
        if not isinstance(doc, dict):
            raise TaskFailedError(
                "InvalidResult",
                f"result document for task {self.id} must be a JSON object, "
                f"got {type(doc).__name__}",
                None,
                "client",
            )
        return doc

    @property
    def status(self) -> _Status:
        """``"pending" | "success" | "failure" | "duplicate" | "expired"``.

        Readable either way: ``r.status`` (Celery's spelling) and ``r.status()``
        (cauli's own, and the one PROTOCOL.md section 12 documents) both give
        the same string. Each evaluation of the attribute reads Redis once.

        ``"pending"`` means the result key does not exist (yet, or anymore
        after result_ttl expiry). ``"expired"`` means the task passed its
        ``expires`` deadline (or its queue's TTL) before a worker picked it up,
        so it was discarded without running (PROTOCOL.md section 9.1). Raises
        :class:`TaskFailedError` (``type == "InvalidResult"``) for a result
        document that exists but is unusable, including one missing its
        ``"status"`` field. See :meth:`_decode` and :meth:`get`, which treat
        the same document the same way.
        """
        return self._status_of(self._load())

    async def astatus(self) -> _Status:
        """:attr:`status` on the asyncio client. Requires an ``AsyncCauli`` app.

        A coroutine, so this one is only ever ``await r.astatus()``; the
        awaited value is callable too, purely for symmetry with ``status``.
        """
        return self._status_of(await self._aload())

    def _status_of(self, doc: dict[str, Any] | None) -> _Status:
        if doc is None:
            return _Status("pending")
        if "status" not in doc:
            raise TaskFailedError(
                "InvalidResult",
                f'result document for task {self.id} has no "status" field',
                None,
                "client",
            )
        return _Status(doc["status"])

    def get(self, timeout: float | None = None, poll_interval: float = 0.05) -> Any:
        """Block until the result key exists, then resolve it.

        - success: returns the result value.
        - failure: raises :class:`TaskFailedError` (with
          .type/.message/.traceback/.origin).
        - duplicate: sets ``self.duplicate = True`` and ``self.claimant_id``
          (see :meth:`claimant`), and returns ``None``.
        - expired: sets ``self.expired = True`` and raises
          :class:`TaskFailedError` with ``type == "Expired"``. It raises rather
          than returning None because the caller asked for a result that is
          never going to exist -- the task did not run and never will.
        - still pending when ``timeout`` (seconds) expires: raises TimeoutError
          (naming the task id; it does NOT claim the task is still pending,
          since a result that ran and already passed result_ttl looks
          identical to one that never ran).
        - a result document that exists but is unusable (not valid JSON, not
          a JSON object, or a JSON object with no ``"status"`` field): raises
          :class:`TaskFailedError` with ``type == "InvalidResult"`` (see
          :meth:`_decode`).

        This is poll-based (default ``poll_interval`` 0.05s), not push/blocking
        redis-side. Without ``timeout``, ``get()`` can block forever by design:
        a task enqueued with ``store_result=False`` never gets a result key at
        all, and neither does one dead lettered as malformed, unregistered, or
        over its redelivery limit, but only when the task id itself could not
        be recovered from the envelope (rare). In the usual case those three
        get a result key too (a synthesized failure), so ``get()`` raises
        instead of hanging (see PROTOCOL.md §4/§4.4/§8 and ARCHITECTURE.md
        limitation #2). Passing an explicit ``timeout`` is recommended for
        anything but throwaway scripts.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            doc = self._load()
            if doc is not None:
                return self._resolve(doc)
            if deadline is not None and time.monotonic() >= deadline:
                raise self._timeout_error(timeout)
            time.sleep(poll_interval)

    async def aget(
        self, timeout: float | None = None, poll_interval: float = 0.05
    ) -> Any:
        """:meth:`get` without blocking the event loop. Requires an ``AsyncCauli``.

        Identical outcomes and identical exceptions -- the same ``_resolve``
        decides both -- but the poll sleeps on the loop and the Redis read goes
        through ``redis.asyncio``. Awaiting the blocking ``get()`` inside a
        coroutine would park the whole loop thread for the entire wait, which
        for a task that never produces a result key is forever.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            doc = await self._aload()
            if doc is not None:
                return self._resolve(doc)
            if deadline is not None and time.monotonic() >= deadline:
                raise self._timeout_error(timeout)
            await asyncio.sleep(poll_interval)

    def _resolve(self, doc: dict[str, Any]) -> Any:
        """Turn a present result document into a return value or an exception."""
        status = doc.get("status")
        if status == "success":
            return doc.get("result")
        if status == "failure":
            error = doc.get("error") or {}
            raise TaskFailedError(
                error.get("type"),
                error.get("message"),
                error.get("traceback"),
                # Absent against a worker predating the field, which
                # reads as unknown rather than as any known origin.
                error.get("origin"),
            )
        if status == "duplicate":
            self.duplicate = True
            claimant = doc.get("claimant_id")
            self.claimant_id = None if claimant is None else str(claimant)
            return None
        if status == "expired":
            self.expired = True
            error = doc.get("error") or {}
            raise TaskFailedError(
                error.get("type") or "Expired",
                error.get("message") or "task expired before it was executed",
                None,
                error.get("origin"),
            )
        raise TaskFailedError(
            "InvalidResult",
            f"unrecognized result status {status!r}",
            None,
            "client",
        )

    def claimant(self) -> "AsyncResult | None":
        """Handle to the task that suppressed this one, or ``None``.

        Populated once :meth:`get`/:meth:`aget` has resolved a ``"duplicate"``
        result. A suppressed submission never ran, so its own result carries
        nothing but the claimant's id; the claimant's result is where the real
        outcome lives, and a claim is never released even after the claimant
        is dead lettered (PROTOCOL.md section 4.5)::

            r = send_email.delay("a@b.com", idempotency_key="welcome:42")
            if r.get(timeout=5) is None and r.duplicate:
                claimant = r.claimant()
                outcome = claimant.get(timeout=5) if claimant else None

        ``None`` when this task was not deduplicated, when :meth:`get` has not
        run yet, or in the race where the claim key expired before the worker
        could read its holder (section 4.5 writes a null ``claimant_id`` then).
        """
        if self.claimant_id is None:
            return None
        return AsyncResult(self.claimant_id, self.app)

    def _timeout_error(self, timeout: float | None) -> TimeoutError:
        return TimeoutError(
            f"no result key present for task {self.id} after "
            f"{timeout} seconds (the task may not have run yet, or "
            "its result already expired)"
        )

    def __repr__(self) -> str:
        return f"<AsyncResult {self.id}>"
