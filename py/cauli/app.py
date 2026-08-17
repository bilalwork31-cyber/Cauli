"""Cauli application object: task registry + enqueue.

Implements the client enqueue rules from PROTOCOL.md sections 2 and 3.
The attribute names ``_tasks``, ``redis_url``, ``default_queue``,
``result_ttl``, ``idemp_ttl`` are read by the Rust worker (section 6).
"""

from __future__ import annotations

import fnmatch
import os
import re
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable

import redis

from cauli import _codec
from cauli.result import AsyncResult
from cauli.schedules import ScheduleEntry
from cauli.task import TaskDef

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_QUEUE_NAME_RE = re.compile(r"[a-zA-Z0-9_.-]+\Z")

#: Wildcard key in :attr:`Cauli.queue_ttl` meaning "every queue without an
#: explicit entry". Also read by the Rust worker (PROTOCOL.md section 9.2).
QUEUE_TTL_WILDCARD = "*"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _aware_epoch_ms(value: datetime, field: str) -> int:
    """Convert a timezone-AWARE datetime to epoch ms, or refuse.

    A naive datetime is rejected rather than guessed at. Celery's
    ``enable_utc`` reinterprets naive datetimes as UTC, which silently shifts
    every ``eta`` by the local offset for anyone who passes
    ``datetime.now()``; guessing "local" instead is just as wrong on a server
    whose TZ differs from the developer's laptop. There is no correct default,
    so there is no default.
    """
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

    Accepted inputs (PROTOCOL.md section 9.3): a mapping ``{glob: dest}``
    (insertion ordered), a sequence of ``(glob, dest)`` pairs, or a sequence
    mixing pairs with bare callables (a bare callable is consulted for every
    task, i.e. its pattern is ``None``).
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
    """Normalize ``queue_ttl`` into ``{queue: seconds}`` with a ``"*"`` fallback.

    A bare number means "every queue"; a mapping is per queue and may itself
    carry a ``"*"`` entry as the fallback.
    """
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


def _dumps(obj: Any) -> "bytes | str":
    # _codec rejects NaN/Infinity on both backends: they are not valid JSON
    # and would poison the Rust-side parser; fail loudly at enqueue time
    # instead. Returns bytes (msgspec) or str (stdlib); redis accepts both.
    return _codec.encode(obj)


def _redact_redis_url(url: str) -> str:
    """Mask ``user:password@`` userinfo, and ``password=``/``username=``
    query parameters, before a redis URL reaches logs/repr.

    Both are shapes redis-py accepts as real credentials (audit M4); without
    this either would appear in plaintext wherever ``repr(app)`` or a log
    line includes ``redis_url``.
    """
    scheme_sep = url.find("://")
    if scheme_sep == -1:
        return url
    rest = url[scheme_sep + 3 :]
    # Authority ends at the first "/", "?" or "#"; "@" means the
    # userinfo/host boundary only within it, so an "@" inside a later
    # password= value (masked separately below) can never be mistaken for a
    # second one and corrupt the visible host.
    authority_end = len(rest)
    for ch in "/?#":
        i = rest.find(ch)
        if i != -1:
            authority_end = min(authority_end, i)
    authority, tail = rest[:authority_end], rest[authority_end:]
    # Last "@", not first: urllib.parse (what redis-py itself uses) resolves
    # a password containing a literal "@" the same way, so splitting at the
    # first one would leave that password's own tail in plaintext right
    # after the mask.
    at = authority.rfind("@")
    new_authority = f"***@{authority[at + 1 :]}" if at != -1 else authority
    return f"{url[:scheme_sep]}://{new_authority}{_redact_query_credentials(tail)}"


def _redact_query_credentials(tail: str) -> str:
    """Mask ``password=``/``username=`` VALUES in a URL's query string (the
    form redis-py accepts straight as connection kwargs, no userinfo
    involved). Keys and every other query parameter stay visible.
    """
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
        pairs.append(f"{name}=***" if sep and name in ("password", "username") else pair)
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
    ) -> None:
        # Resolution order: explicit arg > env CAULI_REDIS_URL > default.
        self.redis_url: str = (
            redis_url or os.environ.get("CAULI_REDIS_URL") or _DEFAULT_REDIS_URL
        )
        self.default_queue: str = default_queue
        # result_ttl=0 reads like "disabled" but Redis rejects `SET key val EX
        # 0`, so the result key would never be written and AsyncResult.get()
        # would hang forever; validate the same way _normalize_queue_ttl does.
        if result_ttl <= 0:
            raise ValueError(f"result_ttl must be > 0 seconds, got {result_ttl!r}")
        if idemp_ttl <= 0:
            raise ValueError(f"idemp_ttl must be > 0 seconds, got {idemp_ttl!r}")
        self.result_ttl: int = result_ttl
        self.idemp_ttl: int = idemp_ttl
        # App-level routing rules (PROTOCOL.md section 9.3): pattern -> queue,
        # applied at enqueue time. They OVERRIDE a task's own decorator queue,
        # which is the point -- an operator re-routes without editing task code
        # -- but never a per-call `queue=`, which is explicit runtime intent.
        self.task_routes: list[tuple[str | None, Any]] = _normalize_routes(task_routes)
        # Per-queue maximum age (PROTOCOL.md section 9.2). `{"*": seconds}` is
        # the fallback for queues with no explicit entry. Read by BOTH the
        # client (stamps `expires_at` at enqueue) and the Rust worker (enforced
        # at dispatch even for envelopes that predate the setting).
        self.queue_ttl: dict[str, float] = _normalize_queue_ttl(queue_ttl)
        self._tasks: dict[str, TaskDef] = {}
        # Code-declared periodic schedule entries (PROTOCOL.md section 10).
        # `cauli-beat` upserts these into Redis at startup; Redis is the
        # source of truth from then on.
        self._periodic: dict[str, ScheduleEntry] = {}
        # Lifecycle hooks (PROTOCOL.md section 4.8). The worker reads these
        # exact attribute names by getattr through the embedded interpreter
        # and in the cpu child executor; keep them as plain lists of zero-arg
        # callables. Registration order is call order.
        self._before_task_hooks: list[Callable[[], Any]] = []
        self._after_task_hooks: list[Callable[[], Any]] = []
        self._process_init_hooks: list[Callable[[], Any]] = []
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

    def before_task(self, fn: Callable[[], Any]) -> Callable[[], Any]:
        """Register ``fn`` to run immediately before EVERY task executes.

        Usable as a decorator (``@app.before_task``) or a plain call
        (``app.before_task(close_old_connections)``). ``fn`` is called with
        no arguments, in the same thread/process that is about to run the
        task, on every execution path: the worker's sync io thread pool, its
        embedded asyncio loop threads, and the forked/stdio cpu children.

        On the asyncio path a hook may return an awaitable; the worker awaits
        it on the loop thread (sync-pool and cpu paths call hooks purely
        synchronously and ignore a returned awaitable).

        A hook that raises is logged (stderr) and skipped; it never fails the
        task and never stops the remaining hooks from running. Registration
        order is call order. Typical use: per-task resource lifecycle such as
        Django's ``close_old_connections`` (see ``cauli.contrib.django``).
        """
        self._before_task_hooks.append(fn)
        return fn

    def after_task(self, fn: Callable[[], Any]) -> Callable[[], Any]:
        """Register ``fn`` to run after EVERY task finishes (success, failure
        or timeout inside the task). Same calling convention, execution
        contexts and error handling as :meth:`before_task`."""
        self._after_task_hooks.append(fn)
        return fn

    def process_init(self, fn: Callable[[], Any]) -> Callable[[], Any]:
        """Register ``fn`` to run once in every cauli-managed Python process
        after the app is imported and before any task executes.

        Runs in: the worker's embedded interpreter (at app load), each forked
        cpu child (right after fork, before serving), each stdio-mode cpu
        child (after import), and the fork-server parent (before the first
        fork — so resources opened at import time can be closed before any
        child can inherit them). Same error handling as :meth:`before_task`.
        """
        self._process_init_hooks.append(fn)
        return fn

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

        ``schedule`` is a :class:`~cauli.schedules.Schedule` (``interval(...)``
        or ``crontab(...)``), a ``timedelta``, or a number of seconds. ``name``
        is the entry's stable identity: it is the Redis field key, so renaming
        an entry creates a new one (with a fresh schedule slot) and orphans the
        old one.

        This only registers the definition. ``cauli-beat`` syncs it into Redis
        at startup and owns the runtime state from then on; nothing here talks
        to Redis and nothing here fires anything.
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

    def _route(self, task_name: str, args: Any, kwargs: dict[str, Any]) -> str | None:
        """First matching app-level route's queue, or None (PROTOCOL.md 9.3).

        A route destination is a queue name, a ``{"queue": ...}`` mapping, or a
        callable ``(task_name, args, kwargs) -> str | dict | None``; a callable
        returning None means "no opinion, keep looking", so a router can fall
        through to the next rule.
        """
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
        """Queue precedence, highest first (PROTOCOL.md section 9.3):

        1. per-call ``queue=`` (or a beat entry's ``queue``) -- explicit intent
        2. app-level ``task_routes`` -- the operator's re-routing lever
        3. the task's own decorator ``queue=``
        4. ``app.default_queue``
        """
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

        Returns ``(envelope, queue_name, fire_at_ms)`` where ``fire_at_ms`` is
        None for "publish to the stream now" and an epoch-ms score for "hold in
        the delayed zset until then".

        Shared by :meth:`_enqueue` and by ``cauli-beat``, deliberately: a task
        published by the scheduler must be byte-for-byte the same shape as one
        published by ``.delay()``, or the two would drift on exactly the fields
        (routing, queue TTL, expiry) this is here to centralize. ``task`` is
        the registered :class:`TaskDef` when known; when it is not (a schedule
        entry naming a task this process has not imported), protocol defaults
        are used and the worker's registry still wins at execution time.
        """
        if countdown is not None and eta is not None:
            raise ValueError(
                "pass either countdown (relative seconds) or eta (absolute "
                "datetime), not both"
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
        # An eta/countdown already in the past is not an error and is not
        # "late": it just means the task is due now, so it goes straight to the
        # stream instead of taking a pointless trip through the delayed zset.
        not_before = fire_at
        if fire_at is not None and fire_at <= now:
            fire_at = None

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
        else:
            ttl_ms = self._queue_ttl_ms(queue_name)
            if ttl_ms is not None:
                # Only a client-side DEFAULT, which is why it is in the `else`:
                # an explicit `expires` is stamped as given and is never clamped
                # here. The queue TTL is still a ceiling -- the worker takes the
                # earlier of the two at dispatch (PROTOCOL.md section 9.2), so
                # an over-long `expires` cannot outlive the queue's configured
                # max age. Enforcing that is the worker's job, not the client's.
                expires_at = now + ttl_ms

        envelope: dict[str, Any] = {
            "v": 1,
            "id": task_id or uuid.uuid4().hex,
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
        """Build the envelope and enqueue it (PROTOCOL.md section 3).

        Queue precedence: call-site ``queue`` > app ``task_routes`` > task
        queue > app ``default_queue``. With ``countdown`` or ``eta``: ZADD to
        ``cauli:delayed:{queue}`` (score = fire time, mirrored into
        ``not_before``); no XADD. Otherwise XADD to ``cauli:q:{queue}`` with
        the single field ``e``.
        """
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
        client = self._get_redis()
        if fire_at is not None:
            client.zadd(f"cauli:delayed:{queue_name}", {_dumps(envelope): fire_at})
        else:
            client.xadd(f"cauli:q:{queue_name}", {"e": _dumps(envelope)})
        return AsyncResult(envelope["id"], self)

    def __repr__(self) -> str:
        return (
            f"<Cauli default_queue={self.default_queue!r} "
            f"tasks={len(self._tasks)} redis_url={_redact_redis_url(self.redis_url)!r}>"
        )
