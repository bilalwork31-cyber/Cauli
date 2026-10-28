"""cauli-beat: the periodic scheduler (PROTOCOL.md section 10).

Schedule state lives in Redis, so several replicas are the supported setup.
A lease picks one ticking leader, but a pause or partition can defeat a lease,
so exactly once firing comes from a compare and set instead: one Lua script
advances an entry's slot from S to S' and publishes, only if the slot is
still S. Slots are a pure function of the previous slot and "now" is Redis
``TIME``, so skewed replicas agree on S'.

Failure semantics:

- Leader dies mid tick: each slot is fired and advanced, or neither; the next
  leader fires the rest late, once.
- Leader dies between ticks: up to ``--lock-ttl`` seconds of added lateness.
- Beat was down: a due entry fires once and missed slots are coalesced (see
  :class:`cauli.schedules.ScheduleEntry` for ``max_lateness``).
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import threading
import time
import uuid
from typing import Any

import redis

from cauli import _codec
from cauli.schedules import ScheduleEntry

log = logging.getLogger("cauli.beat")

# PROTOCOL.md section 10.1.
SCHEDULE_KEY = "cauli:beat:schedule"  # HASH  name -> definition JSON
DUE_KEY = "cauli:beat:due"  # ZSET  name -> next fire epoch ms
REV_KEY = "cauli:beat:rev"  # HASH  name -> schedule fingerprint
STATE_KEY = "cauli:beat:state"  # HASH  name -> last-firing JSON
RUNS_KEY = "cauli:beat:runs"  # HASH  name -> total run count
LOCK_KEY = "cauli:beat:lock"  # STRING lease holder id

DEFAULT_LOCK_TTL = 30.0
DEFAULT_MAX_INTERVAL = 5.0
#: Floor on the tick sleep, so a clock jump cannot busy spin against Redis.
MIN_SLEEP = 0.05
#: Most due entries claimed per tick.
DUE_BATCH = 500
#: Retry delay for an entry that could not be fired (its slot stays due).
FAILED_FIRE_RETRY = 1.0


# Fires only if the stored slot still equals ARGV[2]. Mode 'none' advances
# without publishing (on_missed="skip").
# Publish stays above the slot ZADD: Lua does not roll back on error, so a
# failed publish after the advance would silently lose the firing.
_CLAIM_LUA = """
local cur = redis.call('ZSCORE', KEYS[1], ARGV[1])
if cur == false then return 0 end
if tonumber(cur) ~= tonumber(ARGV[2]) then return 0 end
if ARGV[4] == 'stream' then
  redis.call('XADD', KEYS[4], '*', 'e', ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
elseif ARGV[4] == 'delayed' then
  redis.call('ZADD', KEYS[4], tonumber(ARGV[6]), ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[7])
return 1
"""

# Without the rev check a slow replica could reseed a slot a fast one had
# already fired, firing it twice.
_SEED_LUA = """
local rev = redis.call('HGET', KEYS[2], ARGV[1])
if rev == ARGV[3] and redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
return 1
"""


class RedisClusterUnsupported(RuntimeError):
    """A beat Lua script hit Redis Cluster's CROSSSLOT.

    Not a RedisError, so run() logs it as permanent rather than as a blip:
    the beat keys and queue keys never share a hash slot.
    """


def _crossslot(exc: "redis.exceptions.ResponseError") -> bool:
    # Newer redis-py raises ClusterCrossSlotError with the word rewritten
    # away; older redis-py keeps the raw "CROSSSLOT" message.
    cross_slot_type = getattr(redis.exceptions, "ClusterCrossSlotError", ())
    if isinstance(exc, cross_slot_type):
        return True
    return "CROSSSLOT" in str(exc).upper()


_REFRESH_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


class ScheduleStore:
    """Everything beat needs from a broker (PROTOCOL.md section 10.6).

    Another backend needs two primitives: a lease only its holder can
    refresh or release, and an atomic compare and set bundled with the
    publish (:meth:`claim_and_publish`). Only the second is needed for
    correctness.
    """

    def now_ms(self) -> int:
        raise NotImplementedError

    def load(self) -> dict[str, ScheduleEntry]:
        raise NotImplementedError

    def upsert(self, entry: ScheduleEntry) -> None:
        raise NotImplementedError

    def delete(self, name: str) -> None:
        raise NotImplementedError

    def drop_slot(self, name: str) -> None:
        raise NotImplementedError

    def seed(self, name: str, slot_ms: int, rev: str) -> bool:
        raise NotImplementedError

    def slots_and_revs(self) -> tuple[dict[str, int], dict[str, str]]:
        raise NotImplementedError

    def due(self, now_ms: int, limit: int = DUE_BATCH) -> list[tuple[str, int]]:
        raise NotImplementedError

    def claim_and_publish(
        self,
        name: str,
        expected_slot: int,
        next_slot: int,
        envelope: dict[str, Any] | None,
        queue: str,
        fire_at: int | None,
        state: dict[str, Any],
    ) -> bool:
        raise NotImplementedError

    def state(self, name: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def acquire_lock(self, holder: str, ttl_ms: int) -> bool:
        raise NotImplementedError

    def refresh_lock(self, holder: str, ttl_ms: int) -> bool:
        raise NotImplementedError

    def release_lock(self, holder: str) -> None:
        raise NotImplementedError

    def lock_holder(self) -> str | None:
        raise NotImplementedError


class RedisScheduleStore(ScheduleStore):
    """Redis implementation of :class:`ScheduleStore`."""

    def __init__(self, client: "redis.Redis") -> None:
        self.client = client
        self._claim = client.register_script(_CLAIM_LUA)
        self._seed = client.register_script(_SEED_LUA)
        self._refresh = client.register_script(_REFRESH_LUA)
        self._release = client.register_script(_RELEASE_LUA)

    def now_ms(self) -> int:
        """Redis ``TIME``, so replica clock skew cannot fire early or late."""
        seconds, micros = self.client.time()
        return int(seconds) * 1000 + int(micros) // 1000

    def load(self) -> dict[str, ScheduleEntry]:
        raw = self.client.hgetall(SCHEDULE_KEY)
        out: dict[str, ScheduleEntry] = {}
        for key, value in raw.items():
            name = _text(key) or ""
            try:
                doc = _codec.decode(value)
                doc.setdefault("name", name)
                out[name] = ScheduleEntry.from_dict(doc)
            except Exception as exc:
                # One bad entry must not stop every other entry firing.
                log.error("beat: ignoring unusable schedule entry %r: %s", name, exc)
        return out

    def upsert(self, entry: ScheduleEntry) -> None:
        self.client.hset(SCHEDULE_KEY, entry.name, _codec.encode(entry.to_dict()))

    def delete(self, name: str) -> None:
        pipe = self.client.pipeline(transaction=False)
        pipe.hdel(SCHEDULE_KEY, name)
        pipe.zrem(DUE_KEY, name)
        pipe.hdel(REV_KEY, name)
        pipe.hdel(STATE_KEY, name)
        pipe.hdel(RUNS_KEY, name)
        pipe.execute()

    def drop_slot(self, name: str) -> None:
        """Forget an entry's next-fire slot without deleting its definition."""
        pipe = self.client.pipeline(transaction=False)
        pipe.zrem(DUE_KEY, name)
        pipe.hdel(REV_KEY, name)
        pipe.execute()

    def seed(self, name: str, slot_ms: int, rev: str) -> bool:
        try:
            return bool(
                self._seed(keys=[DUE_KEY, REV_KEY], args=[name, int(slot_ms), rev])
            )
        except redis.exceptions.ResponseError as exc:
            if not _crossslot(exc):
                raise
            raise RedisClusterUnsupported(
                f"beat cannot seed schedule entries on redis cluster: {DUE_KEY} "
                f"and {REV_KEY} do not share a hash slot, so this script always "
                "fails with CROSSSLOT; no periodic task can ever be seeded on "
                "this deployment"
            ) from exc

    def slots_and_revs(self) -> tuple[dict[str, int], dict[str, str]]:
        """Next fire slots and fingerprints, in one round trip."""
        pipe = self.client.pipeline(transaction=False)
        pipe.zrange(DUE_KEY, 0, -1, withscores=True)
        pipe.hgetall(REV_KEY)
        rows, revs = pipe.execute()
        return (
            {(_text(n) or ""): int(s) for n, s in rows},
            {(_text(k) or ""): (_text(v) or "") for k, v in revs.items()},
        )

    def due(self, now_ms: int, limit: int = DUE_BATCH) -> list[tuple[str, int]]:
        rows = self.client.zrangebyscore(
            DUE_KEY, "-inf", now_ms, start=0, num=limit, withscores=True
        )
        return [(_text(name) or "", int(score)) for name, score in rows]

    def claim_and_publish(
        self,
        name: str,
        expected_slot: int,
        next_slot: int,
        envelope: dict[str, Any] | None,
        queue: str,
        fire_at: int | None,
        state: dict[str, Any],
    ) -> bool:
        if envelope is None:
            mode, payload, target = "none", "", DUE_KEY
        elif fire_at is not None:
            mode, payload, target = (
                "delayed",
                _codec.encode(envelope),
                f"cauli:delayed:{queue}",
            )
        else:
            mode, payload, target = (
                "stream",
                _codec.encode(envelope),
                f"cauli:q:{queue}",
            )

        state_json = _codec.encode(state)
        try:
            return bool(
                self._claim(
                    keys=[DUE_KEY, STATE_KEY, RUNS_KEY, target],
                    args=[
                        name,
                        int(expected_slot),
                        int(next_slot),
                        mode,
                        payload,
                        int(fire_at or 0),
                        state_json,
                    ],
                )
            )
        except redis.exceptions.ResponseError as exc:
            if not _crossslot(exc):
                raise
            raise RedisClusterUnsupported(
                f"beat cannot claim and publish on redis cluster: cauli:beat:* "
                f"and {target} do not share a hash slot, so this script always "
                "fails with CROSSSLOT; no periodic task can ever fire on this "
                "deployment"
            ) from exc

    def state(self, name: str) -> dict[str, Any] | None:
        raw = self.client.hget(STATE_KEY, name)
        if raw is None:
            return None
        try:
            doc = _codec.decode(raw)
        except Exception:
            return None
        doc["total_run_count"] = int(self.client.hget(RUNS_KEY, name) or 0)
        return doc

    def acquire_lock(self, holder: str, ttl_ms: int) -> bool:
        return bool(self.client.set(LOCK_KEY, holder, nx=True, px=ttl_ms))

    def refresh_lock(self, holder: str, ttl_ms: int) -> bool:
        return bool(self._refresh(keys=[LOCK_KEY], args=[holder, int(ttl_ms)]))

    def release_lock(self, holder: str) -> None:
        self._release(keys=[LOCK_KEY], args=[holder])

    def lock_holder(self) -> str | None:
        return _text(self.client.get(LOCK_KEY))


class Beat:
    """The scheduler loop. One instance per ``cauli-beat`` process."""

    def __init__(
        self,
        app: Any,
        store: ScheduleStore | None = None,
        lock_ttl: float = DEFAULT_LOCK_TTL,
        max_interval: float = DEFAULT_MAX_INTERVAL,
        instance_id: str | None = None,
        use_lock: bool = True,
    ) -> None:
        self.app = app
        if store is None:
            store = RedisScheduleStore(app._get_redis())
        self.store = store
        self.lock_ttl = float(lock_ttl)
        if self.lock_ttl <= 0:
            raise ValueError("lock_ttl must be > 0 seconds")
        self.max_interval = max(float(max_interval), MIN_SLEEP)
        self.instance_id = (
            instance_id
            or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        )
        self.use_lock = use_lock
        # Two failed refreshes in a row still leave a third of the lease.
        self.refresh_interval = self.lock_ttl / 3.0
        self._is_leader = not use_lock
        self._next_refresh = 0.0
        self._reconciled = False
        #: Slots published by this process.
        self.fired = 0
        self.skipped = 0
        self.lost_races = 0
        # Here, not in add_periodic_task: the task may be decorated later in
        # the module. Logged, not raised, so one typo cannot stop every entry.
        try:
            self.app.check_periodic_tasks()
        except ValueError as exc:
            log.error("beat: %s", exc)

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def _hold_leadership(self, monotonic: float) -> bool:
        """Acquire or renew the lease. Returns True if we may tick now."""
        if not self.use_lock:
            return True
        if self._is_leader:
            if monotonic < self._next_refresh:
                return True
            if self.store.refresh_lock(self.instance_id, int(self.lock_ttl * 1000)):
                self._next_refresh = monotonic + self.refresh_interval
                return True
            log.warning("beat: lost the scheduler lease, stepping down to standby")
            self._is_leader = False
            self._reconciled = False
            return False
        if self.store.acquire_lock(self.instance_id, int(self.lock_ttl * 1000)):
            log.info(
                "beat: acquired the scheduler lease (instance %s)", self.instance_id
            )
            self._is_leader = True
            self._next_refresh = monotonic + self.refresh_interval
            return True
        return False

    def sync_code_entries(self) -> None:
        """Push the app's code-declared entries into Redis, and reap orphans.

        A ``source == "code"`` entry no longer declared in code is deleted;
        entries with any other source are never touched (PROTOCOL.md 10.3).
        """
        declared: dict[str, ScheduleEntry] = dict(
            getattr(self.app, "_periodic", {}) or {}
        )
        existing = self.store.load()
        for entry in declared.values():
            current = existing.get(entry.name)
            if current is None or current.to_dict() != entry.to_dict():
                self.store.upsert(entry)
        for name, entry in existing.items():
            if entry.source == "code" and name not in declared:
                log.info(
                    "beat: removing schedule entry %r (no longer declared in code)",
                    name,
                )
                self.store.delete(name)

    def tick(self) -> float:
        """Seed, claim and publish everything due. Returns seconds to sleep."""
        started = time.monotonic()
        now = self.store.now_ms()
        entries = self.store.load()
        slots, revs = self.store.slots_and_revs()

        for name, entry in entries.items():
            if not entry.enabled:
                if name in slots:
                    log.warning(
                        "beat: entry %r is disabled; dropping its next fire slot. "
                        "It will not run again until it is enabled",
                        name,
                    )
                    self.store.drop_slot(name)
                continue
            rev = entry.rev()
            if name in slots and revs.get(name) == rev:
                continue
            try:
                first = entry.schedule.next_after(now)
            except ValueError as exc:
                log.error("beat: entry %r has an impossible schedule: %s", name, exc)
                continue
            if self.store.seed(name, first, rev):
                log.info(
                    "beat: seeded entry %r (%r) first slot %s",
                    name,
                    entry.schedule,
                    first,
                )

        # A slot with no surviving definition would otherwise sit due forever.
        for name in slots:
            if name not in entries:
                log.warning(
                    "beat: dropping the next fire slot for %r: it has no schedule "
                    "definition any more (deleted, or unreadable and logged above)",
                    name,
                )
                self.store.drop_slot(name)

        due = self.store.due(now)
        if len(due) >= DUE_BATCH:
            log.warning(
                "beat: %d entries came due at once, which is the per tick cap; "
                "the rest are deferred to the next tick and will fire late",
                len(due),
            )
        failed: set[str] = set()
        for name, slot in due:
            entry = entries.get(name)
            if entry is None or not entry.enabled:
                log.warning(
                    "beat: dropping due slot %s for %r: its definition was deleted "
                    "or disabled during this tick, so that slot will not fire",
                    slot,
                    name,
                )
                self.store.drop_slot(name)
                continue
            if not self._fire(entry, slot, now):
                failed.add(name)

        sleep_s = self._sleep_for(now, started, failed)
        return min(sleep_s, FAILED_FIRE_RETRY) if failed else sleep_s

    def _fire(self, entry: ScheduleEntry, slot: int, now: int) -> bool:
        """False when the slot could not be claimed and is still due."""
        try:
            next_slot, missed = entry.schedule.advance_past_with_missed(slot, now)
        except ValueError as exc:
            log.error("beat: cannot advance entry %r: %s", entry.name, exc)
            return False

        lateness = now - slot
        # Judged on the newest coalesced slot: after a day of downtime
        # yesterday's 03:00 is too late, but today's may not be.
        suppress = (
            entry.on_missed == "skip"
            and entry.max_lateness_ms is not None
            and lateness > entry.max_lateness_ms
            and entry.schedule.advance_past(slot, now - entry.max_lateness_ms - 1) > now
        )

        envelope: dict[str, Any] | None = None
        queue = ""
        fire_at: int | None = None
        if not suppress:
            task_def = getattr(self.app, "_tasks", {}).get(entry.task)
            idempotency_key = f"beat:{entry.name}:{slot}" if entry.idempotent else None
            try:
                envelope, queue, fire_at = self.app.make_envelope(
                    entry.task,
                    entry.args,
                    entry.kwargs,
                    task=task_def,
                    entry_queue=entry.queue,
                    expires=entry.expires,
                    idempotency_key=idempotency_key,
                    now=now,
                )
            except Exception as exc:
                log.error("beat: cannot build envelope for %r: %s", entry.name, exc)
                return False
            # Informational; the worker preserves but ignores it (PROTOCOL.md 2).
            envelope["beat_name"] = entry.name
            envelope["beat_slot"] = slot

        state = {
            "last_slot": slot,
            "fired_at": now,
            "lateness_ms": lateness,
            "status": "skipped" if suppress else "fired",
            "task_id": None if envelope is None else envelope["id"],
            "next_slot": next_slot,
            "instance": self.instance_id,
        }
        try:
            won = self.store.claim_and_publish(
                entry.name, slot, next_slot, envelope, queue, fire_at, state
            )
        except Exception as exc:
            log.error("beat: claim failed for %r: %s", entry.name, exc)
            return False

        if not won:
            self.lost_races += 1
            log.debug("beat: lost the claim race for %r slot %s", entry.name, slot)
            return True
        if suppress:
            self.skipped += 1
            log.warning(
                "beat: entry %r slot %s was %.1fs late (> max_lateness %.1fs) "
                "and on_missed='skip': not firing, advancing to %s",
                entry.name,
                slot,
                lateness / 1000.0,
                (entry.max_lateness or 0.0),
                next_slot,
            )
            return True
        self.fired += 1
        if missed is None:
            coalesced = ", coalescing more missed slots than the fast forward bound"
        elif missed:
            coalesced = f", coalescing {missed} missed slots that will not fire"
        else:
            coalesced = ""
        log.log(
            logging.WARNING if coalesced else logging.INFO,
            "beat: fired %r (task %s -> queue %s, id %s, slot %s, %dms late, next %s)%s",
            entry.name,
            entry.task,
            queue,
            envelope["id"] if envelope else "-",
            slot,
            lateness,
            next_slot,
            coalesced,
        )
        return True

    def _sleep_for(self, now: int, started: float, failed: set[str]) -> float:
        """Sleep until the next slot, bounded by max_interval and the lease.

        The tick's own duration is subtracted, or a short interval drifts
        into coalescing slots that were never missed. ``failed`` slots are
        still due and left out, so they cannot spin the loop.
        """
        upcoming = [
            slot
            for name, slot in self.store.due(
                now + int(self.max_interval * 1000), limit=len(failed) + 1
            )
            if name not in failed
        ]
        if upcoming:
            wait = (upcoming[0] - now) / 1000.0
        else:
            wait = self.max_interval
        wait = min(wait, self.max_interval)
        if self.use_lock:
            wait = min(wait, self.refresh_interval)
        return max(wait - (time.monotonic() - started), MIN_SLEEP)

    def run(self, stop: threading.Event | None = None) -> None:
        """Loop until ``stop`` is set (or forever). Releases the lease on exit."""
        stop = stop or threading.Event()
        log.info(
            "cauli-beat started: instance=%s entries=%d lock_ttl=%.1fs max_interval=%.1fs",
            self.instance_id,
            len(getattr(self.app, "_periodic", {}) or {}),
            self.lock_ttl,
            self.max_interval,
        )
        try:
            while not stop.is_set():
                try:
                    if not self._hold_leadership(time.monotonic()):
                        stop.wait(min(1.0, self.refresh_interval))
                        continue
                    if not self._reconciled:
                        # Leader only, so a rolling deploy's two versions do
                        # not delete each other's entries as orphans.
                        self.sync_code_entries()
                        self._reconciled = True
                    sleep_s = self.tick()
                except RedisClusterUnsupported as exc:
                    # Permanent, so not logged as a retryable blip.
                    log.error("beat: %s", exc)
                    self._next_refresh = 0.0
                    sleep_s = 1.0
                except redis.exceptions.RedisError as exc:
                    # Do not step down: acquire_lock is SET NX and cannot take
                    # back our own lease, so one blip would cost a full
                    # lock_ttl. A forced refresh renews it or steps down.
                    log.error("beat: redis error (%s); retrying in 1s", exc)
                    self._next_refresh = 0.0
                    sleep_s = 1.0
                stop.wait(sleep_s)
        finally:
            if self.use_lock and self._is_leader:
                try:
                    self.store.release_lock(self.instance_id)
                    log.info("beat: released the scheduler lease")
                except Exception:  # pragma: no cover - best effort on shutdown
                    pass


def load_app(spec: str) -> Any:
    """Import ``module:attr`` with CWD on sys.path, like the worker's --app."""
    import importlib

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    module_name, sep, attr = spec.partition(":")
    if not sep or not attr:
        attr = "app"
    return getattr(importlib.import_module(module_name), attr)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cauli-beat",
        description="cauli periodic scheduler (PROTOCOL.md section 10). "
        "Safe to run as several replicas: schedule state lives in Redis and "
        "each slot fires exactly once per Redis dataset (section 10.5 covers "
        "what a failover can still replay).",
    )
    p.add_argument("--app", required=True, help="app location as module:attr")
    p.add_argument(
        "--redis-url", default=None, help="overrides app.redis_url / CAULI_REDIS_URL"
    )
    p.add_argument(
        "--lock-ttl",
        type=float,
        default=DEFAULT_LOCK_TTL,
        help="scheduler lease seconds; also the worst-case failover lateness "
        f"after a leader dies (default {DEFAULT_LOCK_TTL:g})",
    )
    p.add_argument(
        "--max-interval",
        type=float,
        default=DEFAULT_MAX_INTERVAL,
        help="longest sleep between ticks; also how quickly a schedule change "
        f"in Redis is noticed (default {DEFAULT_MAX_INTERVAL:g})",
    )
    p.add_argument(
        "--instance-id", default=None, help="lease holder id (default host:pid:rand)"
    )
    p.add_argument(
        "--no-lock",
        action="store_true",
        help="tick without taking the lease. Still exactly-once (the CAS does "
        "that), but every replica does the polling work; for debugging.",
    )
    p.add_argument("--once", action="store_true", help="run a single tick and exit")
    p.add_argument("--log-level", default="info")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        app = load_app(args.app)
    except Exception as exc:
        log.error("failed to load app %r: %s", args.app, exc)
        return 1
    # An injected redis_client= ignores redis_url, so an explicit --redis-url
    # is a contradiction; the ambient env var simply loses.
    if getattr(app, "redis_client", None) is not None:
        if args.redis_url:
            log.error(
                "bad configuration: --redis-url cannot override the app's "
                "injected redis_client=, which is what beat connects through. "
                "Drop one of the two."
            )
            return 1
        log.info("beat: connecting through the app's injected redis client")
    elif args.redis_url:
        app.redis_url = args.redis_url
        app._redis = None
    elif os.environ.get("CAULI_REDIS_URL"):
        app.redis_url = os.environ["CAULI_REDIS_URL"]
        app._redis = None

    try:
        beat = Beat(
            app,
            lock_ttl=args.lock_ttl,
            max_interval=args.max_interval,
            instance_id=args.instance_id,
            use_lock=not args.no_lock,
        )
    except ValueError as exc:
        log.error("bad configuration: %s", exc)
        return 1

    if args.once:
        if beat._hold_leadership(0.0):
            beat.sync_code_entries()
            beat.tick()
            if beat.use_lock:
                beat.store.release_lock(beat.instance_id)
        else:
            log.info(
                "beat: another instance holds the lease and is already "
                "reconciling and scheduling; --once did nothing"
            )
        return 0

    stop = threading.Event()

    def _stop(signum: int, _frame: Any) -> None:
        log.info("beat: signal %s, shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    beat.run(stop)
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
