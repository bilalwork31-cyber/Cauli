"""AsyncResult: a handle to a task's eventual result.

Reads ``cauli:result:{task_id}`` per PROTOCOL.md sections 6 and 8.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from cauli import _codec
from cauli.exceptions import TaskFailedError

if TYPE_CHECKING:
    from cauli.app import Cauli


class AsyncResult:
    """Handle to a task result stored in Redis.

    ``duplicate`` becomes ``True`` after :meth:`get` observes a result with
    status ``"duplicate"`` (idempotency guard hit); ``get`` returns ``None``
    in that case. ``expired`` becomes ``True`` when the task was discarded
    unrun past its ``expires`` deadline; :meth:`get` raises in that case.
    """

    def __init__(self, task_id: str, app: "Cauli") -> None:
        self.id: str = task_id
        self.app = app
        self.duplicate: bool = False
        self.expired: bool = False

    def _load(self) -> dict[str, Any] | None:
        """Fetch and decode the result document, or None if the key is absent.

        A key that DOES exist but is not a usable result document (bytes that
        are not valid JSON, or JSON that decodes to something other than an
        object) raises :class:`TaskFailedError` with ``type ==
        "InvalidResult"`` naming the task id and the problem, rather than
        leaking msgspec's or Python's own internal exception type up through
        :meth:`status`/:meth:`get`.
        """
        raw = self.app._get_redis().get(f"cauli:result:{self.id}")
        if raw is None:
            return None
        try:
            doc = _codec.decode(raw)
        except _codec.DECODE_ERRORS as exc:
            raise TaskFailedError(
                "InvalidResult",
                f"result document for task {self.id} is not valid JSON: {exc}",
                None,
            ) from exc
        if not isinstance(doc, dict):
            raise TaskFailedError(
                "InvalidResult",
                f"result document for task {self.id} must be a JSON object, "
                f"got {type(doc).__name__}",
                None,
            )
        return doc

    def status(self) -> str:
        """Return ``"pending" | "success" | "failure" | "duplicate" | "expired"``.

        ``"pending"`` means the result key does not exist (yet, or anymore
        after result_ttl expiry). ``"expired"`` means the task passed its
        ``expires`` deadline (or its queue's TTL) before a worker picked it up,
        so it was discarded without running (PROTOCOL.md section 9.1). Raises
        :class:`TaskFailedError` (``type == "InvalidResult"``) for a result
        document that exists but is unusable, including one missing its
        ``"status"`` field. See :meth:`_load` and :meth:`get`, which treat
        the same document the same way.
        """
        doc = self._load()
        if doc is None:
            return "pending"
        if "status" not in doc:
            raise TaskFailedError(
                "InvalidResult",
                f'result document for task {self.id} has no "status" field',
                None,
            )
        return str(doc["status"])

    def get(self, timeout: float | None = None, poll_interval: float = 0.05) -> Any:
        """Block until the result key exists, then resolve it.

        - success: returns the result value.
        - failure: raises :class:`TaskFailedError` (with .type/.message/.traceback).
        - duplicate: sets ``self.duplicate = True`` and returns ``None``.
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
          :meth:`_load`).

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
                status = doc.get("status")
                if status == "success":
                    return doc.get("result")
                if status == "failure":
                    error = doc.get("error") or {}
                    raise TaskFailedError(
                        error.get("type"), error.get("message"), error.get("traceback")
                    )
                if status == "duplicate":
                    self.duplicate = True
                    return None
                if status == "expired":
                    self.expired = True
                    error = doc.get("error") or {}
                    raise TaskFailedError(
                        error.get("type") or "Expired",
                        error.get("message") or "task expired before it was executed",
                        None,
                    )
                raise TaskFailedError(
                    "InvalidResult", f"unrecognized result status {status!r}", None
                )
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"no result key present for task {self.id} after "
                    f"{timeout} seconds (the task may not have run yet, or "
                    "its result already expired)"
                )
            time.sleep(poll_interval)

    def __repr__(self) -> str:
        return f"<AsyncResult {self.id}>"
