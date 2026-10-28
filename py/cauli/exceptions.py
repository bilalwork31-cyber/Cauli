"""cauli exceptions (PROTOCOL.md sections 4.2, 6 and 8).

The worker matches ``cauli.Retry`` and ``cauli.SoftTimeLimitExceeded`` by class.
"""

from __future__ import annotations


class Retry(Exception):
    """Raise inside a task to retry it after ``countdown`` seconds.

    ``None`` uses the computed backoff. Still bounded by ``max_retries``.
    """

    countdown: float | None

    def __init__(self, countdown: float | None = None) -> None:
        super().__init__(countdown)
        self.countdown = countdown

    def __str__(self) -> str:
        return f"retry requested (countdown={self.countdown})"


class SoftTimeLimitExceeded(Exception):
    """Signals that a task's soft time limit expired, so it can stop early.

    Catch it to clean up and return normally; if it propagates, the task
    fails (retryable).

    In a ``def`` task this class is raised into the running thread, so
    ``except SoftTimeLimitExceeded`` works. An ``async def`` task is
    cancelled instead: the body observes ``asyncio.CancelledError`` and its
    ``finally`` blocks run, but ``except SoftTimeLimitExceeded`` never fires.

    The failure cauli reports is ``SoftTimeLimitExceeded`` on both lanes. See
    the ``soft_timeout`` row in docs/configuration.md.
    """


class TaskFailedError(Exception):
    """Raised by :meth:`cauli.AsyncResult.get` for a failed task.

    Attributes mirror the error JSON (PROTOCOL.md section 8): ``type``,
    ``message``, ``traceback`` (may be truncated) and ``origin``:
    ``"worker"`` (time limit, dead letter, shim failure), ``"task"`` (raised
    by your code), ``"client"`` (synthesized locally, such as
    ``InvalidResult``) or ``None`` (unknown).
    """

    def __init__(
        self,
        type_: str | None = None,
        message: str | None = None,
        traceback_: str | None = None,
        origin: str | None = None,
    ) -> None:
        # origin stays out of .args so .args and repr keep their 3 tuple shape.
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
