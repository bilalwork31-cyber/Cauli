"""AsyncResult: a handle to a task's eventual result.

Reads ``rupy:result:{task_id}`` per PROTOCOL.md sections 6 and 8.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from rupy.exceptions import TaskFailedError

if TYPE_CHECKING:
    from rupy.app import Rupy


class AsyncResult:
    """Handle to a task result stored in Redis.

    ``duplicate`` becomes ``True`` after :meth:`get` observes a result with
    status ``"duplicate"`` (idempotency guard hit); ``get`` returns ``None``
    in that case.
    """

    def __init__(self, task_id: str, app: "Rupy") -> None:
        self.id: str = task_id
        self.app = app
        self.duplicate: bool = False

    def _load(self) -> dict[str, Any] | None:
        raw = self.app._get_redis().get(f"rupy:result:{self.id}")
        if raw is None:
            return None
        return json.loads(raw)

    def status(self) -> str:
        """Return ``"pending" | "success" | "failure" | "duplicate"``.

        ``"pending"`` means the result key does not exist (yet, or anymore
        after result_ttl expiry).
        """
        doc = self._load()
        if doc is None:
            return "pending"
        return str(doc.get("status", "pending"))

    def get(self, timeout: float | None = None, poll_interval: float = 0.05) -> Any:
        """Block until the result key exists, then resolve it.

        - success: returns the result value.
        - failure: raises :class:`TaskFailedError` (with .type/.message/.traceback).
        - duplicate: sets ``self.duplicate = True`` and returns ``None``.
        - still pending when ``timeout`` (seconds) expires: raises TimeoutError.

        This is poll-based (default ``poll_interval`` 0.05s), not push/blocking
        redis-side. Without ``timeout``, ``get()`` can block forever by design:
        a malformed/unregistered/redelivery-limit-exhausted task, or one
        enqueued with ``store_result=False``, never gets a result key at all
        (see PROTOCOL.md §4/§4.4 and ARCHITECTURE.md limitation #3). Passing an
        explicit ``timeout`` is recommended for anything but throwaway scripts.
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
                raise TaskFailedError(
                    "InvalidResult", f"unrecognized result status {status!r}", None
                )
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"task {self.id} still pending after {timeout} seconds"
                )
            time.sleep(poll_interval)

    def __repr__(self) -> str:
        return f"<AsyncResult {self.id}>"
