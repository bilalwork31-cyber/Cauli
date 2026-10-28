"""AsyncResult: a handle to a task's eventual result.

Reads ``cauli:result:{task_id}`` per PROTOCOL.md sections 6 and 8.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Iterator

from cauli import _codec
from cauli.exceptions import TaskFailedError

if TYPE_CHECKING:
    from cauli.app import Cauli

# Below any Redis round trip, so the network sets the real floor.
_POLL_FLOOR = 0.001

# Polls held at the floor before doubling. Pure doubling from 1ms spends 63ms
# reaching 50ms and barely beats a flat 50ms poll; ten flat polls first cover
# the common 1 to 10ms results for thirteen extra reads in total.
_POLL_FLAT = 10


class _Status(str):
    """A status string that is also callable, returning itself.

    ``r.status == "success"`` (Celery) and ``r.status()`` (PROTOCOL.md section
    6) both work. Calling it does not re read Redis.
    """

    __slots__ = ()

    def __call__(self) -> "_Status":
        return self


class AsyncResult:
    """Handle to a task result stored in Redis.

    After :meth:`get` sees a ``"duplicate"`` result, ``duplicate`` is ``True``
    and ``claimant_id`` names the task holding the idempotency key (see
    :meth:`claimant`). ``expired`` is ``True`` once :meth:`get` sees the task
    was discarded unrun past its ``expires`` deadline.
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
        getter = getattr(self.app, "_get_async_redis", None)
        if getter is None:
            raise TypeError(
                f"awaiting the result of task {self.id} needs an AsyncCauli "
                f"app, but this handle came from a {type(self.app).__name__}. "
                "Build the app as cauli.AsyncCauli(...), or call .get()."
            )
        return getter()

    def _load(self) -> dict[str, Any] | None:
        return self._decode(self.app._get_redis().get(self._key))

    async def _aload(self) -> dict[str, Any] | None:
        return self._decode(await self._async_redis().get(self._key))

    def _decode(self, raw: Any) -> dict[str, Any] | None:
        """Decode a raw result value, or None if the key was absent.

        A present but unusable document raises :class:`TaskFailedError` with
        ``type == "InvalidResult"``, never a msgspec or builtin exception.
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

        ``r.status`` and ``r.status()`` give the same string; each evaluation
        reads Redis once. ``"pending"`` means no result key (not yet, or past
        ``result_ttl``). ``"expired"`` means the task passed its ``expires``
        deadline or queue TTL unrun (PROTOCOL.md section 9.1). An unusable
        document, including one with no ``"status"``, raises
        :class:`TaskFailedError` with ``type == "InvalidResult"``.
        """
        return self._status_of(self._load())

    async def astatus(self) -> _Status:
        """:attr:`status` on the asyncio client. Requires an ``AsyncCauli`` app."""
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

    @staticmethod
    def _poll_delays(ceiling: float) -> Iterator[float]:
        """Ten polls at 1ms, then doubling up to ``ceiling`` (used as is below 1ms)."""
        delay = min(_POLL_FLOOR, ceiling)
        for _ in range(_POLL_FLAT):
            yield delay
        while True:
            delay = min(delay * 2, ceiling)
            yield delay

    def get(self, timeout: float | None = None, poll_interval: float = 0.05) -> Any:
        """Block until the result key exists, then resolve it.

        - success: returns the result value.
        - failure: raises :class:`TaskFailedError`.
        - duplicate: sets ``duplicate`` and ``claimant_id``, returns ``None``.
        - expired: sets ``expired``, raises :class:`TaskFailedError` with
          ``type == "Expired"``.
        - no key after ``timeout`` seconds: raises ``TimeoutError``. A result
          already past ``result_ttl`` looks the same as one never run.
        - unusable document: raises :class:`TaskFailedError` with
          ``type == "InvalidResult"``.

        Polls from 1ms, doubling up to ``poll_interval``. Without ``timeout``
        it can block forever: ``store_result=False`` tasks never get a result
        key, nor does a dead lettered envelope whose task id could not be
        recovered (PROTOCOL.md sections 4, 4.4 and 8, docs/architecture.md).
        Pass a ``timeout``.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        delays = self._poll_delays(poll_interval)
        while True:
            doc = self._load()
            if doc is not None:
                return self._resolve(doc)
            if deadline is not None and time.monotonic() >= deadline:
                raise self._timeout_error(timeout)
            time.sleep(next(delays))

    async def aget(
        self, timeout: float | None = None, poll_interval: float = 0.05
    ) -> Any:
        """:meth:`get` without blocking the event loop. Requires an ``AsyncCauli``.

        Same outcomes, exceptions and backoff as :meth:`get`.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        delays = self._poll_delays(poll_interval)
        while True:
            doc = await self._aload()
            if doc is not None:
                return self._resolve(doc)
            if deadline is not None and time.monotonic() >= deadline:
                raise self._timeout_error(timeout)
            await asyncio.sleep(next(delays))

    def _resolve(self, doc: dict[str, Any]) -> Any:
        status = doc.get("status")
        if status == "success":
            return doc.get("result")
        if status == "failure":
            error = doc.get("error") or {}
            raise TaskFailedError(
                error.get("type"),
                error.get("message"),
                error.get("traceback"),
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

        The suppressed submission never ran; the claimant's result holds the
        real outcome. A claim is never released, even after the claimant is
        dead lettered (PROTOCOL.md section 4.5)::

            r = send_email.delay("a@b.com", idempotency_key="welcome:42")
            if r.get(timeout=5) is None and r.duplicate:
                claimant = r.claimant()
                outcome = claimant.get(timeout=5) if claimant else None

        ``None`` when the task was not deduplicated, before :meth:`get` ran,
        or when the claim key expired before the worker read its holder.
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
