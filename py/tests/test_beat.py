"""cauli-beat: seeding, firing, missed-slot policy, the CAS, and the lease.

PROTOCOL.md section 10. The concurrency claim ("each schedule slot fires
exactly once, however many replicas are running") is tested here at the CAS
level under real thread contention, and end to end with two real OS processes
in test_beat_ha.py.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import timedelta

import pytest
import redis as redis_lib

from cauli import Cauli, crontab, interval
from cauli import beat as beat_module
from cauli.beat import (
    DUE_BATCH,
    DUE_KEY,
    LOCK_KEY,
    REV_KEY,
    RUNS_KEY,
    SCHEDULE_KEY,
    STATE_KEY,
    Beat,
    RedisScheduleStore,
)
from cauli.schedules import ScheduleEntry


class FrozenStore(RedisScheduleStore):
    """RedisScheduleStore with a settable clock.

    Only `now_ms` is stubbed; every write still goes through the real Lua and
    the real Redis, so the CAS being tested is the shipping one.
    """

    def __init__(self, client, now: int) -> None:
        super().__init__(client)
        self.clock = now

    def now_ms(self) -> int:
        return self.clock


class BlipStore(FrozenStore):
    """FrozenStore whose Nth `load` raises, standing in for a momentary blip."""

    def __init__(self, client, now: int, fail_on: int) -> None:
        super().__init__(client, now)
        self.fail_on = fail_on
        self.loads = 0

    def load(self):
        self.loads += 1
        if self.loads == self.fail_on:
            raise redis_lib.exceptions.ConnectionError("transient blip")
        return super().load()


@pytest.fixture()
def store(redis_client):
    return FrozenStore(redis_client, now=1_700_000_000_000)


@pytest.fixture()
def beat_app(redis_url):
    return Cauli(redis_url=redis_url)


def make_beat(app, store, **kw):
    kw.setdefault("use_lock", False)
    return Beat(app, store=store, **kw)


def stream(redis_client, queue="default"):
    return [json.loads(f[b"e"]) for _sid, f in redis_client.xrange(f"cauli:q:{queue}")]


# ------------------------------------------------------------- seeding


def test_first_tick_seeds_but_does_not_fire(beat_app, store, redis_client):
    beat_app.add_periodic_task("tick", "app.ping", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()

    beat.tick()
    assert beat.fired == 0, "a freshly registered entry must not fire immediately"
    slot = redis_client.zscore(DUE_KEY, "tick")
    assert slot == store.clock + 60_000
    assert (
        redis_client.hget(REV_KEY, "tick").decode() == beat_app._periodic["tick"].rev()
    )


def test_entry_fires_when_its_slot_arrives_and_advances(beat_app, store, redis_client):
    beat_app.add_periodic_task("tick", "app.ping", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()

    store.clock += 60_000
    beat.tick()
    assert beat.fired == 1
    envs = stream(redis_client)
    assert len(envs) == 1
    assert envs[0]["task"] == "app.ping"
    assert envs[0]["beat_name"] == "tick"
    assert envs[0]["beat_slot"] == 1_700_000_000_000 + 60_000
    assert redis_client.zscore(DUE_KEY, "tick") == store.clock + 60_000
    assert int(redis_client.hget(RUNS_KEY, "tick")) == 1

    # A tick with nothing due publishes nothing.
    beat.tick()
    assert beat.fired == 1
    assert len(stream(redis_client)) == 1


def test_reseeds_when_the_schedule_definition_changes(beat_app, store, redis_client):
    beat_app.add_periodic_task("tick", "app.ping", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    assert redis_client.zscore(DUE_KEY, "tick") == store.clock + 60_000

    # Operator edits the schedule (in code here; a Redis/admin edit is the
    # same path -- both change the entry's rev).
    beat_app._periodic.clear()
    beat_app.add_periodic_task("tick", "app.ping", interval(5))
    beat.sync_code_entries()
    beat.tick()
    assert redis_client.zscore(DUE_KEY, "tick") == store.clock + 5_000


def test_disabled_entry_is_not_scheduled(beat_app, store, redis_client):
    beat_app.add_periodic_task("off", "app.ping", interval(1), enabled=False)
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    store.clock += 10_000
    beat.tick()
    assert beat.fired == 0
    assert redis_client.zscore(DUE_KEY, "off") is None
    assert redis_client.exists("cauli:q:default") == 0


def test_disabling_an_active_entry_drops_its_slot(beat_app, store, redis_client):
    beat_app.add_periodic_task("e", "app.ping", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    assert redis_client.zscore(DUE_KEY, "e") is not None

    entry = beat_app._periodic["e"]
    entry.enabled = False
    store.upsert(entry)
    beat.tick()
    assert redis_client.zscore(DUE_KEY, "e") is None


def test_unusable_entry_is_skipped_not_fatal(beat_app, store, redis_client, caplog):
    beat_app.add_periodic_task("good", "app.ping", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    redis_client.hset(
        SCHEDULE_KEY, "broken", b'{"name":"broken","schedule":{"type":"?"}}'
    )

    beat.tick()  # must not raise
    store.clock += 60_000
    beat.tick()
    assert beat.fired == 1, "one bad entry must not stop the rest of the schedule"


def test_impossible_crontab_is_reported_not_raised(beat_app, store):
    beat_app.add_periodic_task(
        "never", "app.ping", crontab(minute=0, hour=0, day_of_month=30, month=2)
    )
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()  # must not raise
    assert beat.fired == 0


def test_unregistered_periodic_task_is_named_at_error_on_beat_startup(
    beat_app, store, caplog
):
    """check_periodic_tasks() is wired into Beat.__init__, not into
    add_periodic_task itself, since a name may resolve to an @app.task
    decorated later in the same module. Beat startup is after the whole app
    module has imported, so a name still missing there is a real typo: name
    it loudly and keep going, the same as every other unusable entry."""

    @beat_app.task(name="app.ping")
    def ping():
        return None

    beat_app.add_periodic_task("good", "app.ping", interval(60))
    beat_app.add_periodic_task("typo_job", "app.pign", interval(60))  # never registered

    with caplog.at_level(logging.ERROR, logger="cauli.beat"):
        beat = make_beat(beat_app, store)  # must not raise

    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1, errors
    assert "typo_job" in errors[0] and "app.pign" in errors[0]

    beat.sync_code_entries()
    beat.tick()
    store.clock += 60_000
    beat.tick()
    assert store.state("good")["status"] == "fired", "the good entry must still fire"


# -------------------------------------------------------- missed slots


def test_downtime_coalesces_missed_slots_into_one_firing(beat_app, store, redis_client):
    beat_app.add_periodic_task("tick", "app.ping", interval(1))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()

    # Beat was down for an hour: 3600 slots elapsed.
    store.clock += 3_600_000
    beat.tick()
    assert beat.fired == 1, "missed slots are coalesced, never replayed"
    assert len(stream(redis_client)) == 1
    # And the next slot is in the future, so the backlog is truly gone.
    assert redis_client.zscore(DUE_KEY, "tick") > store.clock


def test_on_missed_skip_suppresses_a_too_late_firing_but_still_advances(
    beat_app, store, redis_client
):
    beat_app.add_periodic_task(
        "report", "app.report", interval(60), on_missed="skip", max_lateness=30
    )
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()

    store.clock += 60_000 + 120_000  # due, and 120s late (> max_lateness 30s)
    beat.tick()
    assert beat.fired == 0
    assert beat.skipped == 1
    assert redis_client.exists("cauli:q:default") == 0
    # The schedule is not stuck: the slot moved past now.
    assert redis_client.zscore(DUE_KEY, "report") > store.clock
    state = store.state("report")
    assert state["status"] == "skipped"
    assert state["lateness_ms"] == 120_000


def test_on_missed_skip_still_fires_when_within_max_lateness(
    beat_app, store, redis_client
):
    beat_app.add_periodic_task(
        "report", "app.report", interval(60), on_missed="skip", max_lateness=30
    )
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    store.clock += 60_000 + 5_000  # 5s late, inside the budget
    beat.tick()
    assert (beat.fired, beat.skipped) == (1, 0)


def test_default_policy_fires_however_late(beat_app, store, redis_client):
    beat_app.add_periodic_task("tick", "app.ping", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    store.clock += 86_400_000  # a day late
    beat.tick()
    assert beat.fired == 1


# ------------------------------------------------------------ payload


def test_published_envelope_carries_args_queue_expiry_and_provenance(
    beat_app, store, redis_client
):
    beat_app.add_periodic_task(
        "nightly",
        "app.report",
        interval(10),
        args=[1, "x"],
        kwargs={"deep": True},
        queue="reports",
        expires=300,
    )
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    store.clock += 10_000
    beat.tick()

    env = stream(redis_client, "reports")[0]
    assert env["task"] == "app.report"
    assert env["args"] == [1, "x"]
    assert env["kwargs"] == {"deep": True}
    assert env["queue"] == "reports"
    assert env["expires_at"] == store.clock + 300_000
    assert env["beat_name"] == "nightly"
    assert env["beat_slot"] == 1_700_000_010_000
    assert env["idempotency_key"] is None


def test_idempotent_entry_keys_each_slot(beat_app, store, redis_client):
    beat_app.add_periodic_task("tick", "app.ping", interval(10), idempotent=True)
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    store.clock += 10_000
    beat.tick()
    store.clock += 10_000
    beat.tick()

    keys = [e["idempotency_key"] for e in stream(redis_client)]
    assert keys == ["beat:tick:1700000010000", "beat:tick:1700000020000"]


def test_beat_publishes_through_the_app_routing_rules(redis_url, redis_client):
    app = Cauli(redis_url=redis_url, task_routes={"cron.*": "scheduled"})
    app.add_periodic_task("rotate", "cron.rotate", interval(10))
    store = FrozenStore(redis_client, now=1_700_000_000_000)
    beat = make_beat(app, store)
    beat.sync_code_entries()
    beat.tick()
    store.clock += 10_000
    beat.tick()
    assert stream(redis_client, "scheduled")[0]["task"] == "cron.rotate"


def test_registered_task_defaults_are_used_when_available(
    beat_app, store, redis_client
):
    @beat_app.task(name="app.heavy", kind="cpu", max_retries=7, timeout=42.0)
    def heavy():
        return None

    beat_app.add_periodic_task("h", heavy, timedelta(seconds=10))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    store.clock += 10_000
    beat.tick()
    env = stream(redis_client)[0]
    assert (env["kind"], env["max_retries"], env["timeout_ms"]) == ("cpu", 7, 42000)


# ------------------------------------------------------- reconciliation


def test_code_entries_are_upserted_and_orphans_reaped(beat_app, store, redis_client):
    beat_app.add_periodic_task("a", "app.a", interval(60))
    beat_app.add_periodic_task("b", "app.b", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    assert set(store.load()) == {"a", "b"}

    # "b" is removed from the code: the next sync must unschedule it, or
    # deleting an add_periodic_task call would silently keep firing forever.
    del beat_app._periodic["b"]
    beat.sync_code_entries()
    assert set(store.load()) == {"a"}
    assert redis_client.zscore(DUE_KEY, "b") is None
    assert redis_client.hget(REV_KEY, "b") is None


def test_non_code_entries_survive_reconciliation(beat_app, store, redis_client):
    # What a future Django-admin view writes: source != "code". Beat must
    # schedule it and must never delete it just because no code declares it.
    store.upsert(
        ScheduleEntry(
            name="admin-made", task="app.adhoc", schedule=interval(10), source="redis"
        )
    )
    beat_app.add_periodic_task("code-made", "app.ping", interval(10))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    assert set(store.load()) == {"admin-made", "code-made"}

    beat.tick()
    store.clock += 10_000
    beat.tick()
    assert {e["task"] for e in stream(redis_client)} == {"app.adhoc", "app.ping"}
    assert beat.fired == 2


def test_a_slot_with_no_definition_is_dropped(beat_app, store, redis_client):
    beat_app.add_periodic_task("a", "app.a", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    redis_client.hdel(SCHEDULE_KEY, "a")  # definition yanked out from under it
    beat.tick()
    assert redis_client.zscore(DUE_KEY, "a") is None


# ---------------------------------------------------------------- CAS


def test_claim_is_exactly_once_under_real_thread_contention(
    beat_app, store, redis_client
):
    """The safety property, tested directly: N racers, one winner, one task.

    This is deliberately NOT a test that a lock helper returns True. Every
    thread here believes it is the leader and every thread runs the shipping
    claim_and_publish against the same slot; only the compare-and-set decides.
    """
    racers = 24
    slot = 1_700_000_000_000
    redis_client.zadd(DUE_KEY, {"tick": slot})

    envelope, queue, _fire = beat_app.make_envelope("app.ping")
    barrier = threading.Barrier(racers)
    results: list[bool] = []
    lock = threading.Lock()

    def race(n: int) -> None:
        # A separate store per thread, exactly like separate processes.
        s = RedisScheduleStore(redis_client)
        env = dict(envelope, id=f"{n:032x}")
        barrier.wait()
        won = s.claim_and_publish(
            "tick", slot, slot + 60_000, env, queue, None, {"by": n}
        )
        with lock:
            results.append(won)

    threads = [threading.Thread(target=race, args=(n,)) for n in range(racers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)

    assert len(results) == racers
    assert sum(results) == 1, f"exactly one claim may win, got {sum(results)}"
    assert len(stream(redis_client)) == 1, "exactly one task may be published"
    assert redis_client.zscore(DUE_KEY, "tick") == slot + 60_000
    assert int(redis_client.hget(RUNS_KEY, "tick")) == 1


def test_claim_refuses_a_stale_or_missing_slot(beat_app, store, redis_client):
    s = RedisScheduleStore(redis_client)
    env, queue, _ = beat_app.make_envelope("app.ping")
    # No slot at all.
    assert s.claim_and_publish("gone", 100, 200, env, queue, None, {}) is False
    # Slot exists but has already been advanced by someone else.
    redis_client.zadd(DUE_KEY, {"tick": 500})
    assert s.claim_and_publish("tick", 100, 200, env, queue, None, {}) is False
    assert redis_client.zscore(DUE_KEY, "tick") == 500
    assert redis_client.exists("cauli:q:default") == 0
    # Correct expectation wins.
    assert s.claim_and_publish("tick", 500, 600, env, queue, None, {}) is True


def test_claim_lua_creates_before_destroying_on_xadd_error(
    beat_app, store, redis_client
):
    """F1 reproduction, live: force the XADD (now ordered first) to error the
    way the real reproduction did, WRONGTYPE on the target stream key, and
    assert the actual property under test. Before the fix, ZADD (the slot
    advance) ran first, so this failure would have moved the slot to
    slot + 60_000 with nothing ever published: the firing silently lost. The
    slot must instead still read the ORIGINAL expected value, unconsumed,
    and no run must have been counted.
    """
    slot = 1_700_000_000_000
    redis_client.zadd(DUE_KEY, {"tick": slot})
    env, queue, _ = beat_app.make_envelope("app.ping")
    redis_client.set(f"cauli:q:{queue}", "not-a-stream")

    with pytest.raises(redis_lib.exceptions.ResponseError, match="WRONGTYPE"):
        store.claim_and_publish("tick", slot, slot + 60_000, env, queue, None, {})

    assert redis_client.zscore(DUE_KEY, "tick") == slot
    assert int(redis_client.hget(RUNS_KEY, "tick") or 0) == 0


def test_two_beats_ticking_simultaneously_fire_each_slot_once(
    beat_app, store, redis_client
):
    """Two schedulers, no lease at all, ticking at the same instant.

    ``use_lock=False`` is the worst case the lease exists to prevent: both
    instances run a full tick for every slot. The point is that correctness
    does not depend on the lease -- only on the CAS. A barrier makes both
    instances enter each tick together so the claims genuinely race.
    """
    steps = 8
    beat_app.add_periodic_task("tick", "app.ping", interval(1))
    store_b = FrozenStore(redis_client, now=store.clock)
    a = make_beat(beat_app, store)
    b = make_beat(beat_app, store_b)
    a.sync_code_entries()

    gate = threading.Barrier(2)
    errors: list[BaseException] = []

    def tick_together(scheduler: Beat) -> None:
        try:
            gate.wait(timeout=30)
            scheduler.tick()
        except BaseException as exc:  # noqa: BLE001 - re-raised by the assert below
            errors.append(exc)

    for step in range(1, steps + 1):
        store.clock = 1_700_000_000_000 + step * 1_000
        store_b.clock = store.clock
        threads = [
            threading.Thread(target=tick_together, args=(scheduler,))
            for scheduler in (a, b)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)

    assert not errors, errors
    envs = stream(redis_client)
    slots = [e["beat_slot"] for e in envs]
    assert len(slots) == len(set(slots)), f"a slot fired more than once: {slots}"
    assert slots == sorted(slots)
    # The very first step only seeds (a brand new entry does not fire
    # immediately), so `steps` clock advances produce `steps - 1` firings.
    assert a.fired + b.fired == len(envs) == steps - 1
    assert slots == [1_700_000_000_000 + n * 1_000 for n in range(2, steps + 1)]


# --------------------------------------------------------------- lease


def test_lease_is_exclusive_and_refreshable(beat_app, redis_client):
    store = RedisScheduleStore(redis_client)
    assert store.acquire_lock("A", 5_000) is True
    assert store.acquire_lock("B", 5_000) is False
    assert store.lock_holder() == "A"

    assert store.refresh_lock("A", 5_000) is True
    assert store.refresh_lock("B", 5_000) is False, "a non-holder cannot extend a lease"
    assert store.lock_holder() == "A"

    store.release_lock("B")
    assert store.lock_holder() == "A", (
        "a non-holder cannot release someone else's lease"
    )
    store.release_lock("A")
    assert store.lock_holder() is None
    assert store.acquire_lock("B", 5_000) is True


def test_lease_expiry_hands_leadership_over(beat_app, redis_client):
    store = RedisScheduleStore(redis_client)
    assert store.acquire_lock("dead-leader", 100) is True
    assert redis_client.pttl(LOCK_KEY) <= 100
    # Simulate the lease lapsing (what happens when the leader is SIGKILLed).
    redis_client.delete(LOCK_KEY)
    assert store.acquire_lock("standby", 5_000) is True
    assert store.refresh_lock("dead-leader", 5_000) is False


def test_leader_steps_down_when_it_loses_the_lease(beat_app, redis_client):
    store = RedisScheduleStore(redis_client)
    beat = Beat(beat_app, store=store, lock_ttl=30, use_lock=True, instance_id="A")
    assert beat._hold_leadership(0.0) is True
    assert beat.is_leader

    # Its lease lapses and another instance takes it while A was paused.
    redis_client.delete(LOCK_KEY)
    RedisScheduleStore(redis_client).acquire_lock("B", 30_000)
    assert beat._hold_leadership(1000.0) is False
    assert beat.is_leader is False


def test_a_transient_redis_error_does_not_strand_the_leader(beat_app, redis_client):
    """A blip must not cost a whole lease of scheduling.

    `acquire_lock` is SET NX, so an instance that drops to standby while its
    own id is still sitting in the lease key can never take that lease back --
    SET NX just keeps failing against its own value until the key expires. So
    the loop must not treat "a redis call failed" as "I lost the lease": it
    re-verifies with a refresh, which renews when the lease is still ours and
    steps down properly when it is not.
    """
    beat_app.add_periodic_task("tick", "app.ping", interval(10))
    store = BlipStore(redis_client, now=1_700_000_000_000, fail_on=2)
    beat = Beat(
        beat_app,
        store=store,
        lock_ttl=30,
        max_interval=0.05,
        use_lock=True,
        instance_id="A",
    )

    def until(predicate, timeout=20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not predicate():
            time.sleep(0.02)
        return predicate()

    stop = threading.Event()
    thread = threading.Thread(target=beat.run, args=(stop,), daemon=True)
    thread.start()
    try:
        # Tick 1 seeds; tick 2 is the blip; keep going until it is behind us.
        recovered = until(lambda: store.loads >= 4)
        # Now bring the seeded slot due. Firing it is the real proof that the
        # loop resumed SCHEDULING, not merely that it kept a lease.
        store.clock += 10_000
        fired = until(lambda: beat.fired >= 1)
        # Sampled while the loop is still running; `run` releases on exit.
        still_leader = beat.is_leader
        holder = redis_client.get(LOCK_KEY)
    finally:
        stop.set()
        thread.join(timeout=20)

    assert recovered, f"the loop stalled after the blip (loads={store.loads})"
    assert still_leader, "a transient redis error stranded the leader as a standby"
    assert holder == b"A", "the lease was dropped over a blip that never lost it"
    assert fired, "the leader held the lease but never scheduled anything again"
    assert beat.fired == 1, f"the slot fired {beat.fired} times, not once"


def test_state_and_run_count_are_readable(beat_app, store, redis_client):
    beat_app.add_periodic_task("tick", "app.ping", interval(10))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    for _ in range(3):
        store.clock += 10_000
        beat.tick()

    state = store.state("tick")
    assert state["total_run_count"] == 3
    assert state["status"] == "fired"
    assert state["last_slot"] == 1_700_000_030_000
    assert state["next_slot"] == 1_700_000_040_000
    assert state["task_id"] is not None
    assert store.state("nope") is None


def test_keys_are_namespaced_under_cauli_beat(redis_client, beat_app, store):
    beat_app.add_periodic_task("tick", "app.ping", interval(10))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    store.clock += 10_000
    beat.tick()
    RedisScheduleStore(redis_client).acquire_lock("x", 5_000)

    keys = {k.decode() for k in redis_client.keys("cauli:beat:*")}
    assert keys == {SCHEDULE_KEY, DUE_KEY, REV_KEY, STATE_KEY, RUNS_KEY, LOCK_KEY}


# --------------------------------------------------- reconciliation and the lease


class StopAfter(threading.Event):
    """Lets ``run()`` make exactly ``passes`` trips round the loop, then stops it."""

    def __init__(self, passes: int) -> None:
        super().__init__()
        self.left = passes

    def wait(self, timeout: float | None = None) -> bool:
        self.left -= 1
        if self.left <= 0:
            self.set()
        return True


def test_a_standby_does_not_reconcile_before_it_holds_the_lease(
    beat_app, store, redis_client, redis_url
):
    """Starting a replica must never unschedule the leader's entries.

    PROTOCOL.md section 10.3: reconciliation runs only while holding the lease.
    `run()` used to reconcile once before the loop, so during a rolling deploy a
    replica running the OLD code deleted every entry the new code had added, and
    the leader never restored them because it had already reconciled.
    """
    beat_app.add_periodic_task("kept", "app.a", interval(60))
    beat_app.add_periodic_task("added-by-the-new-version", "app.b", interval(60))
    leader = Beat(beat_app, store=store, use_lock=True, instance_id="leader")
    leader.sync_code_entries()
    assert leader._hold_leadership(0.0)
    leader.tick()
    assert set(store.load()) == {"kept", "added-by-the-new-version"}

    old_app = Cauli(redis_url=redis_url)
    old_app.add_periodic_task("kept", "app.a", interval(60))
    standby = Beat(
        old_app,
        store=FrozenStore(redis_client, store.clock),
        use_lock=True,
        instance_id="standby",
    )
    standby.run(StopAfter(2))

    assert not standby.is_leader
    assert redis_client.get(LOCK_KEY) == b"leader"
    assert set(store.load()) == {"kept", "added-by-the-new-version"}
    assert redis_client.zscore(DUE_KEY, "added-by-the-new-version") is not None


def test_once_does_not_reconcile_while_another_instance_holds_the_lease(
    beat_app, store, redis_client, redis_url, monkeypatch
):
    beat_app.add_periodic_task("kept", "app.a", interval(60))
    beat_app.add_periodic_task("added-by-the-new-version", "app.b", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    RedisScheduleStore(redis_client).acquire_lock("someone-else", 60_000)

    old_app = Cauli(redis_url=redis_url)
    old_app.add_periodic_task("kept", "app.a", interval(60))
    monkeypatch.setattr(beat_module, "load_app", lambda spec: old_app)

    assert beat_module.main(["--app", "x:app", "--redis-url", redis_url, "--once"]) == 0

    assert set(store.load()) == {"kept", "added-by-the-new-version"}
    assert redis_client.get(LOCK_KEY) == b"someone-else"


def test_once_reconciles_and_ticks_when_it_can_take_the_lease(
    beat_app, redis_client, redis_url, monkeypatch
):
    """The system cron deployment: --once is the only scheduler, so it must
    reconcile, fire, and hand the lease straight back."""
    stale = ScheduleEntry(name="stale", task="app.gone", schedule=interval(60))
    RedisScheduleStore(redis_client).upsert(stale)

    app = Cauli(redis_url=redis_url)
    app.add_periodic_task("live", "app.a", interval(60))
    monkeypatch.setattr(beat_module, "load_app", lambda spec: app)

    assert beat_module.main(["--app", "x:app", "--redis-url", redis_url, "--once"]) == 0

    store = RedisScheduleStore(redis_client)
    assert set(store.load()) == {"live"}
    assert redis_client.zscore(DUE_KEY, "live") is not None
    assert redis_client.get(LOCK_KEY) is None


# ------------------------------------------------- announcing dropped work


def warnings_from(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


def test_a_coalesced_firing_reports_how_many_slots_it_swallowed(
    beat_app, store, redis_client, caplog
):
    """PROTOCOL.md section 10.4 coalesces a backlog into one firing. The count
    of slots that will never run is the number an operator needs, and lateness
    alone does not give it."""
    beat_app.add_periodic_task("nightly", "app.report", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()

    store.clock += 6 * 3600 * 1000  # six hours down, 60s cadence
    with caplog.at_level(logging.INFO, logger="cauli.beat"):
        beat.tick()

    assert beat.fired == 1
    fired = [m for m in warnings_from(caplog) if "fired 'nightly'" in m]
    assert len(fired) == 1, warnings_from(caplog)
    assert "359 missed slots" in fired[0]


def test_a_firing_that_is_on_time_stays_at_info_without_a_count(
    beat_app, store, redis_client, caplog
):
    beat_app.add_periodic_task("p", "app.a", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()

    store.clock += 60_000
    with caplog.at_level(logging.INFO, logger="cauli.beat"):
        beat.tick()

    assert beat.fired == 1
    assert not [m for m in warnings_from(caplog) if "fired" in m]
    assert not [m for m in warnings_from(caplog) if "missed slots" in m]


def test_dropping_the_slot_of_a_vanished_definition_is_announced(
    beat_app, store, redis_client, caplog
):
    beat_app.add_periodic_task("a", "app.a", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()
    redis_client.hdel(SCHEDULE_KEY, "a")

    with caplog.at_level(logging.INFO, logger="cauli.beat"):
        beat.tick()

    assert redis_client.zscore(DUE_KEY, "a") is None
    assert [m for m in warnings_from(caplog) if "'a'" in m and "no schedule" in m]


def test_dropping_the_slot_of_a_disabled_entry_is_announced(
    beat_app, store, redis_client, caplog
):
    beat_app.add_periodic_task("a", "app.a", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()

    entry = store.load()["a"]
    entry.enabled = False
    store.upsert(entry)
    with caplog.at_level(logging.INFO, logger="cauli.beat"):
        beat.tick()
        beat.tick()  # steady state: the warning must not repeat every tick

    assert redis_client.zscore(DUE_KEY, "a") is None
    disabled = [m for m in warnings_from(caplog) if "'a'" in m and "disabled" in m]
    assert len(disabled) == 1, disabled


def test_deferring_a_mass_due_backlog_is_announced(
    beat_app, store, redis_client, caplog
):
    for i in range(DUE_BATCH + 20):
        beat_app.add_periodic_task(f"e{i:04d}", "app.a", interval(60))
    beat = make_beat(beat_app, store)
    beat.sync_code_entries()
    beat.tick()

    store.clock += 60_000
    with caplog.at_level(logging.INFO, logger="cauli.beat"):
        beat.tick()

    deferred = [m for m in warnings_from(caplog) if "came due at once" in m]
    assert len(deferred) == 1, deferred
    assert str(DUE_BATCH) in deferred[0]
