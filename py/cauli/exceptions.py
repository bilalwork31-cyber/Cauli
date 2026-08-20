"""cauli exceptions.

See PROTOCOL.md sections 4.2, 6 and 8. ``Retry`` and ``SoftTimeLimitExceeded``
are part of the wire contract: the Rust worker matches them by the classes
exposed as ``cauli.Retry`` / ``cauli.SoftTimeLimitExceeded``.
"""

from __future__ import annotations


class Retry(Exception):
    """Raise inside a task to force a retry with an explicit delay.

    The worker recognizes this exception class and reads its ``.countdown``
    attribute (float seconds, or ``None`` to use the computed backoff).
    Still bounded by the task's ``max_retries``.
    """

    countdown: float | None

    def __init__(self, countdown: float | None = None) -> None:
        super().__init__(countdown)
        self.countdown = countdown

    def __str__(self) -> str:
        return f"retry requested (countdown={self.countdown})"


class SoftTimeLimitExceeded(Exception):
    """Injected into a running task when its soft time limit expires.

    A task may catch this to do cleanup and return normally; if it propagates,
    the task is treated as failed (retryable).
    """


class TaskFailedError(Exception):
    """Raised by :meth:`cauli.AsyncResult.get` when a task finished with status failure.

    Attributes mirror the error JSON from PROTOCOL.md section 8:
    ``type`` (exception class name), ``message``, ``traceback`` (may be truncated),
    and ``origin``.

    ``origin`` says who minted the error: ``"worker"`` for cauli machinery
    (a time limit, a dead letter, a shim failure), ``"task"`` for an exception
    that propagated out of your own code, and ``"client"`` for one this
    package synthesized locally, such as ``InvalidResult``. It is ``None``
    when unknown, which is what a worker predating the field reports.
    """

    def __init__(
        self,
        type_: str | None = None,
        message: str | None = None,
        traceback_: str | None = None,
        origin: str | None = None,
    ) -> None:
        # origin is deliberately kept out of the base args: ``.args`` and the
        # repr built from it stay the same 3-tuple they were, so adding the
        # field cannot change what an existing caller sees.
        super().__init__(type_, message, traceback_)
        self.type = type_
        self.message = message
        self.traceback = traceback_
        self.origin = origin

    def __str__(self) -> str:
        text = f"{self.type}: {self.message}"
        if self.traceback:
            text += "\n" + self.traceback
        return text
