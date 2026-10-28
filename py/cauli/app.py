"""Cauli application object: task registry and enqueue (PROTOCOL.md 2 and 3).

The worker reads ``_tasks``, ``redis_url``, ``default_queue``, ``result_ttl``
and ``idemp_ttl`` by name (section 6).
"""

from __future__ import annotations

import fnmatch
import logging
import os
import re
import threading
import time
import weakref
from datetime import datetime
from typing import Any, Callable, Iterable, overload

import redis

from cauli import _codec
from cauli.result import AsyncResult
from cauli.schedules import ScheduleEntry
from cauli.task import TaskDef

log = logging.getLogger("cauli.app")

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
# The worker's --redis-timeout default. Explicit because redis-py's default
# differs across the versions "redis>=5.0.1" allows.
_DEFAULT_SOCKET_TIMEOUT = 5
# redis-py 8's default pool raises on the 101st concurrent caller; a blocking
# pool makes that caller wait, up to the socket timeout, instead.
_POOL_MAX_CONNECTIONS = 100
# The worker's --max-envelope-bytes default; keep the two equal.
_DEFAULT_MAX_ENVELOPE_BYTES = 1_048_576
_QUEUE_NAME_RE = re.compile(r"[a-zA-Z0-9_.-]+\Z")
_U64_MAX = 2**64 - 1

#: :attr:`Cauli.queue_ttl` key for every queue without its own entry (PROTOCOL.md 9.2).
QUEUE_TTL_WILDCARD = "*"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _aware_epoch_ms(value: datetime, field: str) -> int:
    """Epoch ms of an aware datetime; a naive one has no correct guess, so it raises."""
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(
            f"{field} must be a timezone-aware datetime; {value!r} is naive. "
            "Attach a timezone explicitly, e.g. "
            "datetime.now(timezone.utc) or dt.replace(tzinfo=ZoneInfo('Europe/Berlin'))."
        )
    return int(value.timestamp() * 1000)


def _normalize_routes(routes: Any) -> list[tuple[str | None, Any]]:
    """Normalize ``task_routes`` into an ordered ``[(glob | None, dest)]`` list.

    Accepts a mapping, ``(glob, dest)`` pairs, or bare callables, which match
    every task (PROTOCOL.md section 9.3).
    """
    if not routes:
        return []
    if isinstance(routes, dict):
        items = list(routes.items())
    else:
        items = []
        for element in routes:
            if callable(element):
                items.append((None, element))
                continue
            try:
                pattern, dest = element
            except (TypeError, ValueError):
                raise ValueError(
                    f"task_routes element {element!r} must be a (pattern, destination) "
                    "pair or a callable router"
                ) from None
            items.append((pattern, dest))
    out: list[tuple[str | None, Any]] = []
    for pattern, dest in items:
        if pattern is not None and not isinstance(pattern, str):
            raise ValueError(f"task_routes pattern {pattern!r} must be a str glob")
        out.append((pattern, dest))
    return out


def _normalize_queue_ttl(queue_ttl: Any) -> dict[str, float]:
    """Normalize ``queue_ttl`` into ``{queue: seconds}``; a bare number means ``"*"``."""
    if queue_ttl is None:
        return {}
    if isinstance(queue_ttl, bool):
        raise ValueError("queue_ttl cannot be a bool")
    if isinstance(queue_ttl, (int, float)):
        ttl = float(queue_ttl)
        if ttl <= 0:
            raise ValueError(f"queue_ttl must be > 0 seconds, got {queue_ttl!r}")
        return {QUEUE_TTL_WILDCARD: ttl}
    if not isinstance(queue_ttl, dict):
        raise ValueError(
            f"queue_ttl must be a number or a mapping, got {type(queue_ttl).__name__}"
        )
    out: dict[str, float] = {}
    for name, ttl in queue_ttl.items():
        value = float(ttl)
        if value <= 0:
            raise ValueError(f"queue_ttl[{name!r}] must be > 0 seconds, got {ttl!r}")
        out[str(name)] = value
    return out


#: Options of :meth:`cauli.task.TaskDef.apply_async` that enqueue_many accepts.
_CALL_OPTIONS = ("countdown", "queue", "idempotency_key", "eta", "expires")


def _normalize_call(
    call: Any,
) -> tuple[TaskDef, tuple[Any, ...], dict[str, Any], dict[str, Any]]:
    """Normalize one :meth:`Cauli.enqueue_many` element to ``(task, args, kwargs, options)``."""
    if isinstance(call, TaskDef):
        return call, (), {}, {}
    if not isinstance(call, (tuple, list)) or not call:
        raise TypeError(
            "enqueue_many element must be a TaskDef or a non-empty (task, args, "
            f"kwargs, options) tuple, got {type(call).__name__}"
        )
    if len(call) > 4:
        raise ValueError(
            f"enqueue_many element has {len(call)} parts; the tuple form is "
            "(task, args, kwargs, options), at most 4"
        )
    task, args, kwargs, options = list(call) + [None] * (4 - len(call))
    if not isinstance(task, TaskDef):
        raise TypeError(
            "enqueue_many element must start with a registered task "
            f"(@app.task), got {type(task).__name__}"
        )
    opts = dict(options or {})
    unknown = sorted(set(opts) - set(_CALL_OPTIONS))
    if unknown:
        raise TypeError(
            f"unknown enqueue_many option(s) {unknown} for task {task.name!r}; "
            f"allowed: {list(_CALL_OPTIONS)}"
        )
    return task, tuple(args or ()), dict(kwargs or {}), opts


def _dumps(obj: Any) -> bytes:
    # len() of the result is the wire size the worker measures.
    return _codec.encode(obj)


def _redact_redis_url(url: str) -> str:
    """Mask userinfo and credential query values before a redis URL reaches logs or repr."""
    scheme_sep = url.find("://")
    if scheme_sep == -1:
        return url
    rest = url[scheme_sep + 3 :]
    # Up to the query, not the first "/" or "#": a base64 password may hold
    # "/". Last "@", for a password that itself contains "@".
    query_start = rest.find("?")
    at = rest.rfind("@", 0, len(rest) if query_start == -1 else query_start)
    mask = "***@" if at != -1 else ""
    rest = rest[at + 1 :]
    authority_end = len(rest)
    for ch in "/?#":
        i = rest.find(ch)
        if i != -1:
            authority_end = min(authority_end, i)
    authority, tail = rest[:authority_end], rest[authority_end:]
    return f"{url[:scheme_sep]}://{mask}{authority}{_redact_query_credentials(tail)}"


def _is_credential_key(name: str) -> bool:
    # redis-py percent-decodes query keys (parse_qs), so a key holding a "%"
    # may spell "password". pass/user are redis-rs's unix socket keys.
    return name in ("password", "username", "pass", "user") or "%" in name


def _redact_query_credentials(tail: str) -> str:
    """Mask credential values in a URL's query string; keys stay visible."""
    q = tail.find("?")
    if q == -1:
        return tail
    path, rest = tail[:q], tail[q + 1 :]
    fragment = ""
    h = rest.find("#")
    if h != -1:
        rest, fragment = rest[:h], rest[h:]
    pairs = []
    for pair in rest.split("&"):
        name, sep, _value = pair.partition("=")
        pairs.append(f"{name}=***" if sep and _is_credential_key(name) else pair)
    return f"{path}?{'&'.join(pairs)}{fragment}"


class Cauli:
    """The application: holds config, the task registry, and a lazy Redis client."""

    def __init__(
        self,
        redis_url: str | None = None,
        default_queue: str = "default",
        result_ttl: int = 3600,
        idemp_ttl: int = 86400,
        task_routes: Any = None,
        queue_ttl: Any = None,
        redis_client: Any = None,
        max_envelope_bytes: int = _DEFAULT_MAX_ENVELOPE_BYTES,
    ) -> None:
        env_redis_url = os.environ.get("CAULI_REDIS_URL")
        self.redis_url: str = redis_url or env_redis_url or _DEFAULT_REDIS_URL
        if not redis_url and not env_redis_url:
            # An unrelated redis on 6379 would swallow tasks silently. warning
            # is visible with no logging configured.
            log.warning(
                "no redis_url given and CAULI_REDIS_URL is not set, defaulting to %s",
                _DEFAULT_REDIS_URL,
            )
        self.default_queue: str = default_queue
        # Redis rejects `SET ... EX 0`, so get() would hang forever.
        if result_ttl <= 0:
            raise ValueError(f"result_ttl must be > 0 seconds, got {result_ttl!r}")
        if idemp_ttl <= 0:
            raise ValueError(f"idemp_ttl must be > 0 seconds, got {idemp_ttl!r}")
        self.result_ttl: int = result_ttl
        self.idemp_ttl: int = idemp_ttl
        if max_envelope_bytes <= 0:
            raise ValueError(
                f"max_envelope_bytes must be > 0 bytes, got {max_envelope_bytes!r}"
            )
        self.max_envelope_bytes: int = int(max_envelope_bytes)
        # Overrides a task's decorator queue, never a per call queue= (PROTOCOL.md 9.3).
        self.task_routes: list[tuple[str | None, Any]] = _normalize_routes(task_routes)
        # Also enforced by the worker at dispatch (PROTOCOL.md 9.2).
        self.queue_ttl: dict[str, float] = _normalize_queue_ttl(queue_ttl)
        self._tasks: dict[str, TaskDef] = {}
        # cauli-beat syncs these into Redis, the source of truth from then on.
        self._periodic: dict[str, ScheduleEntry] = {}
        # Read by name by the worker and the cpu child (PROTOCOL.md 4.8).
        self._before_task_hooks: list[Callable[[], Any]] = []
        self._after_task_hooks: list[Callable[[], Any]] = []
        self._process_init_hooks: list[Callable[[], Any]] = []
        # A client or zero arg factory, for what has no URL form (Sentinel,
        # TLS context, shared pool). The factory runs lazily on first use.
        self.redis_client: Any = redis_client
        self._redis: redis.Redis | None = None
        self._redis_lock = threading.Lock()

    def _new_redis(self) -> redis.Redis:
        """Build the client: the injected one (or its factory), else ``redis_url``."""
        if self.redis_client is None:
            return redis.Redis.from_pool(
                redis.BlockingConnectionPool.from_url(
                    self.redis_url,
                    max_connections=_POOL_MAX_CONNECTIONS,
                    timeout=_DEFAULT_SOCKET_TIMEOUT,
                    socket_timeout=_DEFAULT_SOCKET_TIMEOUT,
                )
            )
        return self.redis_client() if callable(self.redis_client) else self.redis_client

    def _get_redis(self) -> redis.Redis:
        """Lazily create the redis-py client; the lock stops two racing threads leaking a pool."""
        if self._redis is None:
            with self._redis_lock:
                if self._redis is None:
                    self._redis = self._new_redis()
        return self._redis

    def before_task(self, fn: Callable[[], Any]) -> Callable[[], Any]:
        """Register ``fn`` to run before every task, as a decorator or a plain call.

        ``fn`` takes no arguments and runs in the thread that runs the task, on
        every lane. For an ``async def`` task a returned awaitable is awaited.
        A hook that raises is logged to stderr and skipped; it never fails the
        task. Hooks run in registration order.
        """
        self._before_task_hooks.append(fn)
        return fn

    def after_task(self, fn: Callable[[], Any]) -> Callable[[], Any]:
        """Register ``fn`` to run after every task, whatever the outcome. As :meth:`before_task`."""
        self._after_task_hooks.append(fn)
        return fn

    def process_init(self, fn: Callable[[], Any]) -> Callable[[], Any]:
        """Register ``fn`` to run once per cauli process, after app import, before any task.

        Also runs in the fork server parent before the first fork, so a
        resource opened at import is closed before children inherit it.
        Errors are handled as in :meth:`before_task`.
        """
        self._process_init_hooks.append(fn)
        return fn

    @overload
    def task(self, _fn: Callable[..., Any]) -> TaskDef: ...

    @overload
    def task(
        self,
        _fn: None = ...,
        *,
        name: str | None = ...,
        kind: str | None = ...,
        queue: str | None = ...,
        max_retries: int = ...,
        timeout: float = ...,
        soft_timeout: float | None = ...,
        backoff_base: float = ...,
        backoff_factor: float = ...,
        backoff_max: float = ...,
        jitter: bool = ...,
        store_result: bool = ...,
    ) -> Callable[[Callable[..., Any]], TaskDef]: ...

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

        Times are in seconds. ``kind`` defaults to ``"io"``; ``"cpu"`` must be
        explicit. See docs/configuration.md for every option.
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
            if task_def.name in self._tasks:
                raise ValueError(f"duplicate task name {task_def.name!r}")
            self._tasks[task_def.name] = task_def
            return task_def

        if _fn is not None:
            return decorate(_fn)
        return decorate

    def add_periodic_task(
        self,
        name: str,
        task: "TaskDef | str",
        schedule: Any,
        args: Any = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        expires: float | None = None,
        idempotent: bool = False,
        enabled: bool = True,
        on_missed: str = "fire_once",
        max_lateness: float | None = None,
    ) -> ScheduleEntry:
        """Declare a periodic schedule entry in code (PROTOCOL.md section 10).

        ``schedule`` is ``interval(...)``, ``crontab(...)``, a ``timedelta`` or
        seconds. ``name`` is the Redis key, so renaming starts a fresh entry.
        Only registers the definition; ``cauli-beat`` syncs and fires it.
        """
        if name in self._periodic:
            raise ValueError(f"duplicate periodic task name {name!r}")
        task_name = task.name if isinstance(task, TaskDef) else str(task)
        entry = ScheduleEntry(
            name=name,
            task=task_name,
            schedule=schedule,
            args=args,
            kwargs=kwargs,
            queue=queue,
            expires=expires,
            idempotent=idempotent,
            enabled=enabled,
            on_missed=on_missed,
            max_lateness=max_lateness,
            source="code",
        )
        self._periodic[name] = entry
        return entry

    def check_periodic_tasks(self) -> None:
        """Raise if a declared periodic entry names a task this app has not registered.

        Call after the app module is fully imported: an entry may name a task
        decorated later in the same module.
        """
        for entry in self._periodic.values():
            if entry.task not in self._tasks:
                raise ValueError(
                    f"periodic task {entry.name!r} names task {entry.task!r}, "
                    "which is not registered on this app (typo, or its "
                    "@app.task has not been imported yet)"
                )

    def _route(self, task_name: str, args: Any, kwargs: dict[str, Any]) -> str | None:
        """First matching route's queue, or None (PROTOCOL.md 9.3); a router returning None falls through."""
        for pattern, dest in self.task_routes:
            if pattern is not None and not fnmatch.fnmatchcase(task_name, pattern):
                continue
            if callable(dest):
                dest = dest(task_name, args, kwargs)
                if dest is None:
                    continue
            if isinstance(dest, dict):
                dest = dest.get("queue")
                if dest is None:
                    continue
            if dest is not None:
                return str(dest)
        return None

    def _resolve_queue(
        self,
        task_name: str,
        args: Any,
        kwargs: dict[str, Any],
        explicit: str | None = None,
        task: TaskDef | None = None,
        entry_queue: str | None = None,
    ) -> str:
        """Per call (or beat entry) queue, then task_routes, task queue, default (PROTOCOL.md 9.3)."""
        chosen = explicit if explicit is not None else entry_queue
        if chosen is None:
            chosen = self._route(task_name, args, kwargs)
        if chosen is None and task is not None:
            chosen = task.queue
        if chosen is None:
            chosen = self.default_queue
        chosen = str(chosen)
        if not _QUEUE_NAME_RE.match(chosen):
            raise ValueError(
                f"invalid queue name {chosen!r}: must match [a-zA-Z0-9_.-]+"
            )
        return chosen

    def _queue_ttl_ms(self, queue_name: str) -> int | None:
        ttl = self.queue_ttl.get(queue_name, self.queue_ttl.get(QUEUE_TTL_WILDCARD))
        return None if ttl is None else int(ttl * 1000)

    def make_envelope(
        self,
        task_name: str,
        args: Any = (),
        kwargs: dict[str, Any] | None = None,
        *,
        task: TaskDef | None = None,
        queue: str | None = None,
        entry_queue: str | None = None,
        idempotency_key: str | None = None,
        expires: "float | datetime | None" = None,
        eta: "datetime | None" = None,
        countdown: float | None = None,
        now: int | None = None,
        task_id: str | None = None,
    ) -> tuple[dict[str, Any], str, int | None]:
        """Build a PROTOCOL.md section 2 envelope. Does NOT touch Redis.

        Returns ``(envelope, queue_name, fire_at_ms)``; ``fire_at_ms`` is None
        to publish now. Shared with ``cauli-beat`` so both write the same
        envelope. Without ``task`` protocol defaults are used. Raises
        ValueError when the task would fire after it expires.
        """
        if countdown is not None and eta is not None:
            raise ValueError(
                "pass either countdown (relative seconds) or eta (absolute "
                "datetime), not both"
            )
        if idempotency_key is not None and not isinstance(idempotency_key, str):
            raise TypeError(
                f"idempotency_key must be a str, got {type(idempotency_key).__name__}; "
                f"pass str({idempotency_key!r}) explicitly"
            )
        kwargs = dict(kwargs or {})
        args = list(args or ())
        now = _now_ms() if now is None else int(now)
        queue_name = self._resolve_queue(
            task_name, args, kwargs, explicit=queue, task=task, entry_queue=entry_queue
        )

        fire_at: int | None = None
        if countdown is not None:
            fire_at = now + int(round(float(countdown) * 1000))
        elif eta is not None:
            fire_at = _aware_epoch_ms(eta, "eta")
        not_before = fire_at
        if fire_at is not None and fire_at <= now:
            fire_at = None

        queue_ttl_ms = self._queue_ttl_ms(queue_name)
        expires_at: int | None = None
        if expires is not None:
            if isinstance(expires, datetime):
                expires_at = _aware_epoch_ms(expires, "expires")
            elif isinstance(expires, bool):
                raise ValueError("expires cannot be a bool")
            elif isinstance(expires, (int, float)):
                expires_at = now + int(round(float(expires) * 1000))
            else:
                raise TypeError(
                    "expires must be seconds (number) or a timezone-aware datetime, "
                    f"got {type(expires).__name__}"
                )
            # Out of u64 range fails the worker's parse, so get() waits forever.
            if not 0 <= expires_at <= _U64_MAX:
                raise ValueError(
                    f"expires={expires!r} gives epoch ms {expires_at}, outside "
                    f"0..{_U64_MAX}"
                )
        elif queue_ttl_ms is not None:
            # A default only; the worker applies the earlier of the two (PROTOCOL.md 9.2).
            expires_at = now + queue_ttl_ms

        # Expiry counts from enqueue, so queue_ttl=300 with countdown=600 would
        # be discarded unrun with nothing raised. Refuse it here.
        deadline = expires_at
        if queue_ttl_ms is not None:
            ttl_deadline = now + queue_ttl_ms
            deadline = ttl_deadline if deadline is None else min(deadline, ttl_deadline)
        if not_before is not None and deadline is not None and not_before > deadline:
            sources = []
            if expires is not None:
                sources.append(f"expires={expires!r}")
            if queue_ttl_ms is not None:
                sources.append(f"queue_ttl {queue_ttl_ms / 1000:g}s on {queue_name!r}")
            raise ValueError(
                f"task {task_name!r} would fire {(not_before - deadline) / 1000:g}s "
                f"after it expires, so a worker would discard it unrun. Its "
                f"expiry is measured from enqueue, not from the due time "
                f"(PROTOCOL.md section 9.2): {' and '.join(sources)} versus a "
                f"delay of {(not_before - now) / 1000:g}s. Raise the expiry "
                "above the delay, or enqueue the task nearer to when it is due."
            )

        envelope: dict[str, Any] = {
            "v": 1,
            # 32 hex chars as uuid4().hex, at under half the cost; never parsed as a UUID.
            "id": task_id or os.urandom(16).hex(),
            "task": task_name,
            "args": args,
            "kwargs": kwargs,
            "queue": queue_name,
            "kind": task.kind if task is not None else "io",
            "retries": 0,
            "max_retries": task.max_retries if task is not None else 3,
            "backoff_base_ms": task.backoff_base_ms if task is not None else 500,
            "backoff_factor": task.backoff_factor if task is not None else 2.0,
            "backoff_max_ms": task.backoff_max_ms if task is not None else 60000,
            "jitter": task.jitter if task is not None else True,
            "timeout_ms": task.timeout_ms if task is not None else 300000,
            "soft_timeout_ms": task.soft_timeout_ms if task is not None else None,
            "idempotency_key": idempotency_key,
            "store_result": task.store_result if task is not None else True,
            "enqueued_at": now,
            "not_before": not_before,
            "expires_at": expires_at,
        }
        return envelope, queue_name, fire_at

    def _enqueue(
        self,
        task: TaskDef,
        args: tuple[Any, ...],
        kwargs: dict[str, Any] | None,
        countdown: float | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
        eta: "datetime | None" = None,
        expires: "float | datetime | None" = None,
    ) -> AsyncResult:
        """Build the envelope and enqueue it (PROTOCOL.md section 3)."""
        envelope, queue_name, fire_at = self.make_envelope(
            task.name,
            args,
            kwargs,
            task=task,
            queue=queue,
            idempotency_key=idempotency_key,
            countdown=countdown,
            eta=eta,
            expires=expires,
        )
        raw = self._encode_envelope(envelope)
        self._publish(self._get_redis(), raw, queue_name, fire_at)
        return AsyncResult(envelope["id"], self)

    def enqueue_many(
        self, calls: Iterable["TaskDef | tuple[Any, ...]"]
    ) -> list[AsyncResult]:
        """Enqueue a batch of calls in ONE pipelined round trip.

        ``calls`` is an iterable of either a bare task (called with no
        arguments) or a tuple ``(task, args, kwargs, options)`` whose trailing
        parts may be omitted. ``options`` is a mapping of the same keywords
        :meth:`~cauli.task.TaskDef.apply_async` takes (``countdown``,
        ``queue``, ``idempotency_key``, ``eta``, ``expires``)::

            app.enqueue_many([
                (send_email, ("a@example.com",)),
                (send_email, ("b@example.com",), {"template": "welcome"}),
                (rebuild, (), {}, {"countdown": 30, "queue": "slow"}),
            ])

        Routing, queue TTL and expiry behave as for ``.delay()``. Every call is
        validated before anything is written, so a bad one publishes nothing.
        Not a transaction: a Redis failure part way can leave earlier entries
        published. Returns one :class:`~cauli.result.AsyncResult` per call, in order.
        """
        prepared = self._prepare_many(calls)
        if not prepared:
            return []
        with self._get_redis().pipeline(transaction=False) as pipe:
            for _task_id, raw, queue_name, fire_at in prepared:
                self._publish(pipe, raw, queue_name, fire_at)
            pipe.execute()
        return [AsyncResult(task_id, self) for task_id, _raw, _q, _f in prepared]

    def _prepare_many(
        self, calls: Iterable["TaskDef | tuple[Any, ...]"]
    ) -> list[tuple[str, bytes, str, int | None]]:
        """Validate and encode a whole batch into ``(id, raw, queue, fire_at)`` before any write."""
        prepared: list[tuple[str, bytes, str, int | None]] = []
        for call in calls:
            task, args, kwargs, options = _normalize_call(call)
            task._check_signature(args, kwargs)
            envelope, queue_name, fire_at = self.make_envelope(
                task.name,
                args,
                kwargs,
                task=task,
                queue=options.get("queue"),
                idempotency_key=options.get("idempotency_key"),
                countdown=options.get("countdown"),
                eta=options.get("eta"),
                expires=options.get("expires"),
            )
            prepared.append(
                (envelope["id"], self._encode_envelope(envelope), queue_name, fire_at)
            )
        return prepared

    def _encode_envelope(self, envelope: dict[str, Any]) -> bytes:
        """Encode one envelope, refusing one the worker would drop unparsed (get() would hang)."""
        raw = _dumps(envelope)
        if len(raw) > self.max_envelope_bytes:
            raise ValueError(
                f"task {envelope['task']!r} envelope is {len(raw)} bytes, over "
                f"the {self.max_envelope_bytes} byte limit set by "
                "Cauli(max_envelope_bytes=...), which must match the worker's "
                "--max-envelope-bytes. A worker discards an oversize entry "
                "before parsing it, so it would never write a result and "
                ".get() would wait forever. Pass a reference (an object key, a "
                "row id) instead of the payload itself, or raise both limits."
            )
        return raw

    @staticmethod
    def _publish(client: Any, raw: bytes, queue: str, fire_at: int | None) -> Any:
        """The one Redis write of an enqueue; the async paths await its return value."""
        if fire_at is not None:
            return client.zadd(f"cauli:delayed:{queue}", {raw: fire_at})
        return client.xadd(f"cauli:q:{queue}", {"e": raw})

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__} default_queue={self.default_queue!r} "
            f"tasks={len(self._tasks)} redis_url={_redact_redis_url(self.redis_url)!r}>"
        )


class AsyncCauli(Cauli):
    """A :class:`Cauli` that can also enqueue from a running event loop.

    Adds a ``redis.asyncio`` client and the ``a`` prefixed calls; everything
    else, ``.delay()`` included, works as on ``Cauli``::

        app = AsyncCauli(redis_url="redis://localhost:6379/0")

        @app.task
        async def send_email(to: str) -> None: ...

        @router.post("/signup")
        async def signup(...):
            result = await send_email.adelay("a@example.com")
            return {"task_id": result.id}

    The envelope is the same as ``.delay()`` writes. The client is cached per
    event loop; call ``await app.aclose()`` on that loop at shutdown.
    """

    def __init__(
        self, *args: Any, async_redis_client: Any = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        # The asyncio twin of ``redis_client``.
        self.async_redis_client: Any = async_redis_client
        self._aredis: weakref.WeakKeyDictionary[Any, Any] = weakref.WeakKeyDictionary()

    def _new_async_redis(self) -> Any:
        # Lazy: the worker imports this module and never enqueues.
        from redis import asyncio as aioredis

        if self.async_redis_client is None:
            return aioredis.Redis.from_pool(
                aioredis.BlockingConnectionPool.from_url(
                    self.redis_url,
                    max_connections=_POOL_MAX_CONNECTIONS,
                    timeout=_DEFAULT_SOCKET_TIMEOUT,
                    socket_timeout=_DEFAULT_SOCKET_TIMEOUT,
                )
            )
        supplied = self.async_redis_client
        return supplied() if callable(supplied) else supplied

    def _get_async_redis(self) -> Any:
        """The running loop's cached ``redis.asyncio`` client.

        Per loop: redis-py binds a pool to the loop that first used it, and a
        second loop gets "Event loop is closed". A threading lock, because an
        ``asyncio.Lock`` binds to one loop too.
        """
        import asyncio

        loop = asyncio.get_running_loop()
        client = self._aredis.get(loop)
        if client is None:
            with self._redis_lock:
                client = self._aredis.get(loop)
                if client is None:
                    # The client's connections keep its loop alive, so a weak
                    # key alone never frees a finished asyncio.run().
                    for closed in [lp for lp in self._aredis if lp.is_closed()]:
                        del self._aredis[closed]
                    client = self._aredis[loop] = self._new_async_redis()
        return client

    async def _aenqueue(
        self,
        task: TaskDef,
        args: tuple[Any, ...],
        kwargs: dict[str, Any] | None,
        countdown: float | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
        eta: "datetime | None" = None,
        expires: "float | datetime | None" = None,
    ) -> AsyncResult:
        envelope, queue_name, fire_at = self.make_envelope(
            task.name,
            args,
            kwargs,
            task=task,
            queue=queue,
            idempotency_key=idempotency_key,
            countdown=countdown,
            eta=eta,
            expires=expires,
        )
        raw = self._encode_envelope(envelope)
        await self._publish(self._get_async_redis(), raw, queue_name, fire_at)
        return AsyncResult(envelope["id"], self)

    async def aenqueue_many(
        self, calls: Iterable["TaskDef | tuple[Any, ...]"]
    ) -> list[AsyncResult]:
        """The asyncio mirror of :meth:`Cauli.enqueue_many`."""
        prepared = self._prepare_many(calls)
        if not prepared:
            return []
        async with self._get_async_redis().pipeline(transaction=False) as pipe:
            for _task_id, raw, queue_name, fire_at in prepared:
                # Not awaited: a buffered pipeline command returns the pipeline.
                self._publish(pipe, raw, queue_name, fire_at)
            await pipe.execute()
        return [AsyncResult(task_id, self) for task_id, _raw, _q, _f in prepared]

    async def aclose(self) -> None:
        """Close the running loop's asyncio client and pool; idempotent. The sync client stays open."""
        import asyncio

        client = self._aredis.pop(asyncio.get_running_loop(), None)
        if client is None:
            return
        # `aclose()` from redis-py 5.0.1; `close()` before.
        closer = getattr(client, "aclose", None) or client.close
        await closer()
