"""What a running task knows about itself.

.. code-block:: python

    from cauli import current_task

    @app.task()
    def charge(order_id):
        ctx = current_task()
        log.info("charging %s (task %s, attempt %s)", order_id, ctx.id, ctx.retries)

Backed by a :class:`contextvars.ContextVar`: a thread local would leak
between coroutines on one loop thread, a global between pool threads.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

__all__ = ["TaskContext", "current_task", "set_current_task", "reset_current_task"]


class TaskContext:
    """Read only facts about the task executing on this thread or coroutine.

    ``id``
        The task id, the same as the caller's ``AsyncResult.id``.
    ``retries``
        Retries so far, ``0`` on the first attempt, ``None`` if unreported.
    ``max_retries``
        The task's retry ceiling, ``None`` if unreported.
    ``task``
        The registered task name.
    ``queue``
        The queue the envelope was published to, ``None`` if unreported.
    """

    __slots__ = ("id", "task", "retries", "max_retries", "queue")

    def __init__(
        self,
        id: str | None,
        task: str | None = None,
        retries: int | None = None,
        max_retries: int | None = None,
        queue: str | None = None,
    ) -> None:
        self.id = id
        self.task = task
        self.retries = retries
        self.max_retries = max_retries
        self.queue = queue

    def is_last_attempt(self) -> bool:
        """True when this attempt is the last before the dead letter queue.

        ``False`` when either count is unreported.
        """
        if self.retries is None or self.max_retries is None:
            return False
        return self.retries >= self.max_retries

    def __repr__(self) -> str:
        return (
            f"<TaskContext id={self.id!r} task={self.task!r} "
            f"retries={self.retries!r} queue={self.queue!r}>"
        )


_current: ContextVar[TaskContext | None] = ContextVar(
    "cauli_current_task", default=None
)


def current_task() -> TaskContext | None:
    """The :class:`TaskContext` for the task running here, or ``None`` outside one."""
    return _current.get()


def set_current_task(ctx: "TaskContext | None") -> "Token[TaskContext | None]":
    """Install ``ctx`` as the current task context and return a reset token.

    For execution lanes (worker/src/shim.py, ``cauli._exec``), which pass the
    token to :func:`reset_current_task` in a ``finally``.
    """
    return _current.set(ctx)


def reset_current_task(token: "Token[TaskContext | None]") -> None:
    """Undo a :func:`set_current_task`.

    A token from another context clears the value instead of raising: the
    sync pool reuses threads, and a stale id must not reach the next task.
    """
    try:
        _current.reset(token)
    except ValueError:
        _current.set(None)


def make_context(request: dict[str, Any], task_id: str | None = None) -> TaskContext:
    """Build a :class:`TaskContext` from a cpu lane request dict.

    ``task_id`` overrides the request ``id``, which carries a per attempt
    suffix (``{envelope id}.{seq}``, worker/src/exec.rs).
    """
    raw_id = task_id if task_id is not None else request.get("id")
    retries = request.get("retries")
    max_retries = request.get("max_retries")
    return TaskContext(
        id=None if raw_id is None else str(raw_id),
        task=request.get("task"),
        retries=None if retries is None else int(retries),
        max_retries=None if max_retries is None else int(max_retries),
        queue=request.get("queue"),
    )
