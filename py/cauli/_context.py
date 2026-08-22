"""Per-execution task context: what a running task knows about itself.

Celery users reach for ``self.request.id`` and ``self.request.retries``
constantly. cauli exposes the same two facts (plus the task name and queue)
through a module-level lookup instead of a bound first argument, so nothing
changes about how a task is declared or called and a task that never asks
pays nothing:

.. code-block:: python

    from cauli import current_task

    @app.task()
    def charge(order_id):
        ctx = current_task()
        log.info("charging %s (task %s, attempt %s)", order_id, ctx.id, ctx.retries)

The backing store is a :class:`contextvars.ContextVar`, which is the only
mechanism that is simultaneously thread-correct (the sync io thread pool runs
many tasks on one process) and asyncio-correct (a coroutine started on a loop
inherits the context it was created in, and concurrent coroutines on the same
loop thread do NOT share it). A plain thread local would be wrong on the
async lane; a global would be wrong on both.

Outside a running task -- in a web request, in a REPL, in the caller that
enqueued the task -- :func:`current_task` returns ``None``.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

__all__ = ["TaskContext", "current_task", "set_current_task", "reset_current_task"]


class TaskContext:
    """Read-only facts about the task executing on this thread/coroutine.

    ``id``
        The task id, identical to the ``AsyncResult.id`` the enqueuing side
        holds. Use it to correlate logs, or as the natural idempotency row key.
    ``retries``
        How many times this task has already been retried; ``0`` on the first
        attempt. ``None`` when the executing worker does not report it (see
        the note in :func:`current_task`).
    ``max_retries``
        The retry ceiling from the task definition, so a task can tell it is
        on its last attempt: ``ctx.retries == ctx.max_retries``. ``None`` when
        not reported.
    ``task``
        The registered task name.
    ``queue``
        The queue the envelope was published to, or ``None`` when not reported.
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
        """True when this attempt is the final one before the dead letter queue.

        Returns ``False`` when either count is unreported rather than
        guessing: escalating on a wrong answer is worse than not escalating.
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
    """The :class:`TaskContext` for the task running here, or ``None``.

    ``None`` means "not inside a cauli task": the enqueuing process, a web
    request, a test that calls the task function directly.

    Coverage note: the cpu lane (``@app.task(kind="cpu")``, executed by
    ``cauli._exec``) populates this from the request it receives. The two io
    lanes run inside the Rust worker's embedded interpreter, which must pass
    the envelope's ``id`` and ``retries`` into
    :func:`set_current_task`; see the module docstring of
    ``worker/src/shim.py``. Against a worker that does not, this returns
    ``None`` on those lanes rather than inventing an id.
    """
    return _current.get()


def set_current_task(ctx: "TaskContext | None") -> "Token[TaskContext | None]":
    """Install ``ctx`` as the current task context. Returns a reset token.

    Public on purpose: the worker's embedded shim (``worker/src/shim.py``)
    and any future execution lane call this at the task boundary and pass the
    returned token to :func:`reset_current_task` in a ``finally``. Callers
    outside an execution lane have no reason to touch it.
    """
    return _current.set(ctx)


def reset_current_task(token: "Token[TaskContext | None]") -> None:
    """Undo a :func:`set_current_task`, restoring the previous context.

    Tolerates a token minted in a different context (which
    ``ContextVar.reset`` rejects with ValueError): the sync io pool reuses
    threads, and a lane that loses the exact context must still leave no
    stale id visible to the next task on that thread.
    """
    try:
        _current.reset(token)
    except ValueError:
        _current.set(None)


def make_context(request: dict[str, Any], task_id: str | None = None) -> TaskContext:
    """Build a :class:`TaskContext` from a cpu-lane request dict.

    ``task_id`` overrides the request's own ``id`` field, which on the cpu
    lane carries a per-attempt wire suffix (``{envelope id}.{seq}``,
    worker/src/exec.rs) rather than the bare task id the caller holds.
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
