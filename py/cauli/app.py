"""Cauli application object: task registry + enqueue.

Implements the client enqueue rules from PROTOCOL.md sections 2 and 3.
The attribute names ``_tasks``, ``redis_url``, ``default_queue``,
``result_ttl``, ``idemp_ttl`` are read by the Rust worker (section 6).
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from typing import Any, Callable

import redis

from cauli.result import AsyncResult
from cauli.task import TaskDef

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_QUEUE_NAME_RE = re.compile(r"[a-zA-Z0-9_.-]+\Z")


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _dumps(obj: Any) -> str:
    # allow_nan=False: NaN/Infinity are not valid JSON and would poison the
    # Rust-side parser; fail loudly at enqueue time instead.
    return json.dumps(obj, separators=(",", ":"), allow_nan=False)


def _redact_redis_url(url: str) -> str:
    """Mask ``user:password@`` userinfo before a redis URL reaches logs/repr.

    ``redis://user:password@host/0`` is a common shape; without this the
    password would appear in plaintext wherever ``repr(app)`` or a log line
    includes ``redis_url`` (audit M4).
    """
    scheme_sep = url.find("://")
    if scheme_sep == -1:
        return url
    after = url[scheme_sep + 3 :]
    at = after.find("@")
    if at == -1:
        return url
    return f"{url[:scheme_sep]}://***@{after[at + 1 :]}"


class Cauli:
    """The application: holds config, the task registry, and a lazy Redis client."""

    def __init__(
        self,
        redis_url: str | None = None,
        default_queue: str = "default",
        result_ttl: int = 3600,
        idemp_ttl: int = 86400,
    ) -> None:
        # Resolution order: explicit arg > env CAULI_REDIS_URL > default.
        self.redis_url: str = (
            redis_url or os.environ.get("CAULI_REDIS_URL") or _DEFAULT_REDIS_URL
        )
        self.default_queue: str = default_queue
        self.result_ttl: int = result_ttl
        self.idemp_ttl: int = idemp_ttl
        self._tasks: dict[str, TaskDef] = {}
        self._redis: redis.Redis | None = None
        self._redis_lock = threading.Lock()

    def _get_redis(self) -> redis.Redis:
        """Lazily create the redis-py client (no connection is made until first command).

        Double-checked locking (audit L6): without the lock, two threads
        racing the first call could each build their own client, and one
        pool would leak.
        """
        if self._redis is None:
            with self._redis_lock:
                if self._redis is None:
                    self._redis = redis.Redis.from_url(self.redis_url)
        return self._redis

    def task(
        self,
        _fn: Callable[..., Any] | None = None,
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
    ) -> TaskDef | Callable[[Callable[..., Any]], TaskDef]:
        """Register a function as a task. Usable as ``@app.task`` or ``@app.task(...)``.

        Seconds-based options (timeout, soft_timeout, backoff_*) are converted
        to milliseconds on the TaskDef. ``kind=None`` means ``"io"``; ``"cpu"``
        must be explicit.
        """

        def decorate(fn: Callable[..., Any]) -> TaskDef:
            task_def = TaskDef(
                self,
                fn,
                name=name,
                kind=kind,
                queue=queue,
                max_retries=max_retries,
                timeout=timeout,
                soft_timeout=soft_timeout,
                backoff_base=backoff_base,
                backoff_factor=backoff_factor,
                backoff_max=backoff_max,
                jitter=jitter,
                store_result=store_result,
            )
            self._tasks[task_def.name] = task_def
            return task_def

        if _fn is not None:
            return decorate(_fn)
        return decorate

    def _enqueue(
        self,
        task: TaskDef,
        args: tuple[Any, ...],
        kwargs: dict[str, Any] | None,
        countdown: float | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
    ) -> AsyncResult:
        """Build the envelope and enqueue it (PROTOCOL.md section 3).

        Queue precedence: call-site ``queue`` > task queue > app default_queue.
        With ``countdown``: ZADD to ``cauli:delayed:{queue}`` (score = fire time,
        mirrored into ``not_before``); no XADD. Otherwise XADD to
        ``cauli:q:{queue}`` with the single field ``e``.
        """
        queue_name = queue or task.queue or self.default_queue
        if not _QUEUE_NAME_RE.match(queue_name):
            raise ValueError(
                f"invalid queue name {queue_name!r}: must match [a-zA-Z0-9_.-]+"
            )
        now = _now_ms()
        task_id = uuid.uuid4().hex
        envelope: dict[str, Any] = {
            "v": 1,
            "id": task_id,
            "task": task.name,
            "args": list(args),
            "kwargs": dict(kwargs or {}),
            "queue": queue_name,
            "kind": task.kind,
            "retries": 0,
            "max_retries": task.max_retries,
            "backoff_base_ms": task.backoff_base_ms,
            "backoff_factor": task.backoff_factor,
            "backoff_max_ms": task.backoff_max_ms,
            "jitter": task.jitter,
            "timeout_ms": task.timeout_ms,
            "soft_timeout_ms": task.soft_timeout_ms,
            "idempotency_key": idempotency_key,
            "store_result": task.store_result,
            "enqueued_at": now,
            "not_before": None,
        }
        client = self._get_redis()
        if countdown is not None:
            fire_at = now + int(round(countdown * 1000))
            envelope["not_before"] = fire_at
            client.zadd(f"cauli:delayed:{queue_name}", {_dumps(envelope): fire_at})
        else:
            client.xadd(f"cauli:q:{queue_name}", {"e": _dumps(envelope)})
        return AsyncResult(task_id, self)

    def __repr__(self) -> str:
        return (
            f"<Cauli default_queue={self.default_queue!r} "
            f"tasks={len(self._tasks)} redis_url={_redact_redis_url(self.redis_url)!r}>"
        )
