"""cauli-beat: the periodic scheduler (PROTOCOL.md section 10).

Design in one paragraph. Schedule state lives in Redis, never in a local file,
so every beat replica sees the same schedule and the same "what has already
fired" state. Replicas take a **lease** so that normally exactly one of them is
doing the ticking, and failover is automatic when the leader dies. But the
lease is NOT what makes firing exactly-once -- a lease can always be defeated
by a stop-the-world pause or a partition, so relying on it for safety would be
a bug waiting for a bad day. Safety comes from a **compare-and-set on the
entry's next-fire slot**: advancing a slot from S to S' and publishing the task
happen inside ONE Lua script, and the script refuses unless the stored slot is
still exactly S. Two replicas that both believe they are the leader therefore
still produce exactly one firing per slot; the loser's CAS simply returns 0.

Why that works even with skewed clocks: the next slot is computed by
:mod:`cauli.schedules` as a pure function of the PREVIOUS slot, not of "now"
(see that module's docstring). Both replicas compute the same S' from the same
S, so the CAS is a real mutual exclusion rather than a race between two
different proposed values. "Now" itself is read from Redis (``TIME``), not from
the replica, so a replica whose clock is minutes off does not fire early or
late -- it just agrees with everyone else.

Contrast with Celery's beat, which persists last-run times to a local
``shelve`` file with no locking at all; its own docs tell you to make sure only
one is running, and two of them means every cron fires twice. Running two
``cauli-beat`` replicas for availability is the supported configuration.

Failure semantics, stated rather than implied:

- **Leader dies mid-tick.** Advance-and-publish is one atomic script, so a slot
  is either fired-and-advanced or neither. A leader that dies between two
  entries of the same tick leaves the remaining entries un-advanced; the next
  leader finds them due and fires them (late, but exactly once).
- **Leader dies between ticks.** The lease expires after ``--lock-ttl``
  seconds, another replica acquires it and continues. Worst-case added lateness
  is one lease TTL.
- **Beat was down for a while.** A due entry fires ONCE on recovery and its
  slot is then fast-forwarded past the present: missed slots are coalesced, not
  replayed. Set ``max_lateness`` with ``on_missed="skip"`` on entries where a
  very late firing is worse than none (see
  :class:`cauli.schedules.ScheduleEntry`).
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

# PROTOCOL.md section 10.1 key layout.
SCHEDULE_KEY = "cauli:beat:schedule"  # HASH  name -> definition JSON
DUE_KEY = "cauli:beat:due"  # ZSET  name -> next fire epoch ms
REV_KEY = "cauli:beat:rev"  # HASH  name -> schedule fingerprint
STATE_KEY = "cauli:beat:state"  # HASH  name -> last-firing JSON
RUNS_KEY = "cauli:beat:runs"  # HASH  name -> total run count
LOCK_KEY = "cauli:beat:lock"  # STRING lease holder id

DEFAULT_LOCK_TTL = 30.0
DEFAULT_MAX_INTERVAL = 5.0
#: Never sleep less than this between ticks, so a pathological schedule (or a
#: clock jump) cannot turn the loop into a busy spin against Redis.
MIN_SLEEP = 0.05
#: Most due entries claimed per tick. Bounds one tick's work when a large
#: schedule comes due at once (e.g. every entry at midnight).
DUE_BATCH = 500


# --------------------------------------------------------------------------
# Lua
# --------------------------------------------------------------------------

# The whole safety argument lives here. `expected` is the slot this caller
# believes is current; if the stored score still equals it, this caller wins:
# the slot advances and the task is published, atomically. Any other caller
# racing the same slot then sees a different score and returns 0.
#
# mode 'none' advances the slot without publishing (an entry whose firing was
# suppressed by on_missed="skip", and the CROSSSLOT fallback path below).
_CLAIM_LUA = """
local cur = redis.call('ZSCORE', KEYS[1], ARGV[1])
if cur == false then return 0 end
if tonumber(cur) ~= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
if ARGV[4] == 'stream' then
  redis.call('XADD', KEYS[4], '*', 'e', ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
elseif ARGV[4] == 'delayed' then
  redis.call('ZADD', KEYS[4], tonumber(ARGV[6]), ARGV[5])
  redis.call('HINCRBY', KEYS[3], ARGV[1], 1)
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[7])
return 1
"""

# Seed (or reseed) an entry's slot. Idempotent across replicas: whoever gets
# here first writes both the score and the rev, and every later caller sees a
# matching rev plus an existing score and does nothing. Without the rev check a
# slow replica could reseed a slot that a fast one had already fired, firing it
# twice.
_SEED_LUA = """
local rev = redis.call('HGET', KEYS[3], ARGV[1])
if rev == ARGV[3] and redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
return 1
"""

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


# --------------------------------------------------------------------------
# Store (the broker seam)
# --------------------------------------------------------------------------


class ScheduleStore:
    """Everything beat needs from a broker (PROTOCOL.md section 10.6).

    Deliberately small, and deliberately not a general broker abstraction. A
    non-Redis backend has to provide exactly two non-trivial primitives:

    1. a **lease** with a TTL that only its holder can refresh or release
       (:meth:`acquire_lock` / :meth:`refresh_lock` / :meth:`release_lock`);
    2. an atomic **compare-and-set on a per-entry value, bundled with the
       publish** (:meth:`claim_and_publish`).

    Only (2) is required for correctness. SQS has neither and would need an
    external store (a DynamoDB conditional write is the usual answer); RabbitMQ
    has neither natively either. This class is where that lands, so the rest of
    beat never touches a Redis command.
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
        # Flipped to False the first time the atomic claim+publish trips over
        # a Redis Cluster CROSSSLOT error (the beat keys and cauli:q:{queue}
        # do not share a hash slot). See `claim_and_publish`.
        self._atomic_publish = True

    # -- clock ------------------------------------------------------------
    def now_ms(self) -> int:
        """Authoritative "now", read from REDIS rather than from this process.

        Replica clock skew then cannot make one instance fire early and another
        late; every replica is comparing against the same clock the slots are
        stored against.
        """
        seconds, micros = self.client.time()
        return int(seconds) * 1000 + int(micros) // 1000

    # -- definitions ------------------------------------------------------
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
                # One corrupt/unsupported entry must not stop the whole
                # scheduler: skip it loudly and keep every other entry firing.
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

    # -- slots ------------------------------------------------------------
    def seed(self, name: str, slot_ms: int, rev: str) -> bool:
        return bool(
            self._seed(
                keys=[DUE_KEY, STATE_KEY, REV_KEY], args=[name, int(slot_ms), rev]
            )
        )

    def slots_and_revs(self) -> tuple[dict[str, int], dict[str, str]]:
        """Current next-fire slots and schedule fingerprints, in one round trip.

        The tick uses this to skip the (cron) next-slot computation entirely
        for entries that are already correctly seeded -- otherwise every tick
        would re-derive every entry's next occurrence just to discover the seed
        script has nothing to do.
        """
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
        if mode != "none" and self._atomic_publish:
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
                if "CROSSSLOT" not in str(exc).upper():
                    raise
                # Redis Cluster: the beat keys and the queue key live in
                # different slots, so no script can touch both. Degrade
                # explicitly rather than silently: the CAS still guarantees no
                # DUPLICATE firing, but a crash in the gap between the advance
                # and the publish now loses that one firing.
                log.warning(
                    "beat: redis cluster CROSSSLOT on the atomic claim+publish; "
                    "falling back to claim-then-publish (a crash in the gap now "
                    "drops a single firing instead of being atomic)"
                )
                self._atomic_publish = False

        won = bool(
            self._claim(
                keys=[DUE_KEY, STATE_KEY, RUNS_KEY, DUE_KEY],
                args=[
                    name,
                    int(expected_slot),
                    int(next_slot),
                    "none",
                    "",
                    0,
                    state_json,
                ],
            )
        )
        if won and envelope is not None:
            if fire_at is not None:
                self.client.zadd(target, {payload: fire_at})
            else:
                self.client.xadd(target, {"e": payload})
            self.client.hincrby(RUNS_KEY, name, 1)
        return won

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

    # -- lease ------------------------------------------------------------
    def acquire_lock(self, holder: str, ttl_ms: int) -> bool:
        return bool(self.client.set(LOCK_KEY, holder, nx=True, px=ttl_ms))

    def refresh_lock(self, holder: str, ttl_ms: int) -> bool:
        return bool(self._refresh(keys=[LOCK_KEY], args=[holder, int(ttl_ms)]))

    def release_lock(self, holder: str) -> None:
        self._release(keys=[LOCK_KEY], args=[holder])

    def lock_holder(self) -> str | None:
        return _text(self.client.get(LOCK_KEY))


# --------------------------------------------------------------------------
# The scheduler
# --------------------------------------------------------------------------


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
        # Refresh at a third of the lease so two consecutive refresh failures
        # (a redis blip) still leave a full third of the lease in hand.
        self.refresh_interval = self.lock_ttl / 3.0
        self._is_leader = not use_lock
        self._next_refresh = 0.0
        self._reconciled = False
        #: Slots actually published by THIS process. The HA test reads it, and
        #: it is the number an operator wants in a metric.
        self.fired = 0
        self.skipped = 0
        self.lost_races = 0
        # Runs here, once per process, rather than from add_periodic_task: a
        # task name may resolve to an @app.task decorated later in the same
        # module, so a name still missing is only a real typo once the whole
        # app module has finished importing, which construction time
        # guarantees. Logged instead of left to raise: one typo must not stop
        # every other entry in the schedule from being reconciled and fired.
        try:
            self.app.check_periodic_tasks()
        except ValueError as exc:
            log.error("beat: %s", exc)

    # -- leadership -------------------------------------------------------
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
            # Someone else holds it now (our lease lapsed during a pause, a
            # GC stall, a redis failover). Step down immediately -- and note
            # that even if we had NOT noticed, the CAS in claim_and_publish
            # would still have kept every slot single-fired.
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

    # -- schedule sync ----------------------------------------------------
    def sync_code_entries(self) -> None:
        """Push the app's code-declared entries into Redis, and reap orphans.

        Reconciliation rule (PROTOCOL.md section 10.3): entries declared in
        code are upserted and carry ``source == "code"``; an entry in Redis
        with ``source == "code"`` that no longer exists in code is DELETED, so
        removing an ``add_periodic_task`` call actually unschedules it. Entries
        with any other source (created through the API, or a future admin view)
        are never touched by this.
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

    # -- one tick ---------------------------------------------------------
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
                continue  # already seeded, definition unchanged
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
            self._fire(entry, slot, now)

        return self._sleep_for(now, started)

    def _fire(self, entry: ScheduleEntry, slot: int, now: int) -> None:
        try:
            next_slot, missed = entry.schedule.advance_past_with_missed(slot, now)
        except ValueError as exc:
            log.error("beat: cannot advance entry %r: %s", entry.name, exc)
            return

        lateness = now - slot
        suppress = (
            entry.on_missed == "skip"
            and entry.max_lateness_ms is not None
            and lateness > entry.max_lateness_ms
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
                return
            # Provenance: which schedule entry and which slot produced this
            # task. Informational only -- the worker preserves but ignores it
            # (PROTOCOL.md section 2).
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
            return

        if not won:
            # Another replica advanced this slot first. Expected under
            # contention; the whole point is that it is harmless.
            self.lost_races += 1
            log.debug("beat: lost the claim race for %r slot %s", entry.name, slot)
            return
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
            return
        self.fired += 1
        # A coalesced firing silently consumes every slot it slept through
        # (PROTOCOL.md section 10.4), so say how many rather than leaving an
        # operator to divide lateness by the cadence.
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

    def _sleep_for(self, now: int, started: float) -> float:
        """Sleep until the next slot, bounded by max_interval and the lease.

        ``now`` is the instant the tick STARTED, so the tick's own duration is
        subtracted from the wait. Without that correction every tick wakes a
        tick-duration late, and a schedule whose interval is close to the tick
        cost drifts far enough to start coalescing slots that were never
        actually missed.
        """
        upcoming = self.store.due(now + int(self.max_interval * 1000), limit=1)
        if upcoming:
            wait = (upcoming[0][1] - now) / 1000.0
        else:
            wait = self.max_interval
        wait = min(wait, self.max_interval)
        if self.use_lock:
            # Never sleep past our own lease refresh, or we would wake up
            # holding nothing.
            wait = min(wait, self.refresh_interval)
        return max(wait - (time.monotonic() - started), MIN_SLEEP)

    # -- run --------------------------------------------------------------
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
                        # Standby: poll for the lease. Nothing is scheduled
                        # here, so this costs one GET-ish command per poll.
                        stop.wait(min(1.0, self.refresh_interval))
                        continue
                    if not self._reconciled:
                        # Reconcile code-vs-Redis only as leader, so a rolling
                        # deploy's two versions do not fight over orphan
                        # deletion from both sides at once.
                        self.sync_code_entries()
                        self._reconciled = True
                    sleep_s = self.tick()
                except redis.exceptions.RedisError as exc:
                    # Do NOT step down here. A transient error says nothing
                    # about whether the lease is still ours, and `acquire_lock`
                    # is SET NX -- it cannot reacquire a key whose value is
                    # already our own id. Stepping down would therefore strand
                    # this instance as a standby, unable to take back a lease it
                    # never lost, until that lease expired: one blip costing a
                    # full `lock_ttl` of no scheduling. Force a refresh instead.
                    # It renews when we still hold it, and returns 0 -> the
                    # normal step-down path when we genuinely do not.
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


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


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
    if args.redis_url:
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
