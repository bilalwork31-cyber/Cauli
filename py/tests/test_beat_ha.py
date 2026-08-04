"""Two real cauli-beat PROCESSES against one Redis (PROTOCOL.md section 10.5).

This is the test the whole design exists for, so it uses real OS processes and
a real wall clock rather than a stubbed one:

1. two replicas run the same schedule at the same time; every slot must be
   published exactly once -- no duplicates, in order, on cadence;
2. the leader is SIGKILLed mid-run (no chance to release its lease); the
   standby must take over and keep firing, with the outage bounded by the
   lease and with no slot fired twice across the handover.

Celery's beat cannot pass (1) at all -- its state is a local shelve file with
no locking, so two of them double-fire everything.

Note on what is asserted strictly and what is not. "No duplicates" is the
correctness claim and is asserted exactly. Perfect slot CONTIGUITY is not
asserted, because dropping slots under load is documented, intended behavior
(PROTOCOL.md section 10.4: a tick that runs late coalesces the slots it slept
through instead of replaying them), and this suite shares a contended box.
Coverage is asserted as a floor instead, which still fails loudly if the
scheduler stops keeping up.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
import redis as redis_lib

INTERVAL = 0.5  # seconds between slots
STEP_MS = int(INTERVAL * 1000)


@pytest.fixture()
def beat_module(tmp_path: Path) -> Path:
    """A tiny app module for the beat subprocesses to import."""
    module = tmp_path / "ha_beat_app.py"
    module.write_text(
        "import os\n"
        "from cauli import Cauli, interval\n"
        "app = Cauli(redis_url=os.environ['CAULI_REDIS_URL'])\n"
        f"app.add_periodic_task('ha', 'app.ping', interval({INTERVAL}))\n"
    )
    return module


def spawn_beat(
    module: Path, redis_url: str, instance: str, lock_ttl: float
) -> subprocess.Popen:
    env = dict(os.environ)
    env["CAULI_REDIS_URL"] = redis_url
    env["PYTHONPATH"] = str(module.parent) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "cauli.beat",
            "--app",
            f"{module.stem}:app",
            "--redis-url",
            redis_url,
            "--lock-ttl",
            str(lock_ttl),
            "--max-interval",
            "0.5",
            "--instance-id",
            instance,
            "--log-level",
            "info",
        ],
        cwd=str(module.parent),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def published(client: "redis_lib.Redis") -> list[dict]:
    return [json.loads(f[b"e"]) for _sid, f in client.xrange("cauli:q:default")]


def wait_for(predicate, timeout: float, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def stop(proc: subprocess.Popen, sig=signal.SIGTERM) -> str:
    if proc.poll() is None:
        proc.send_signal(sig)
    try:
        out, _ = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate(timeout=10)
    return out or ""


def check_slots(envs: list[dict], expected_min: int) -> list[int]:
    """The exactly-once contract, asserted on what actually reached the queue."""
    assert len(envs) >= expected_min, (
        f"only {len(envs)} firings, wanted >= {expected_min}"
    )
    slots = [e["beat_slot"] for e in envs]

    dupes = sorted({s for s in slots if slots.count(s) > 1})
    assert not dupes, f"DUPLICATE slot fired -- exactly-once is broken: {dupes}"
    assert slots == sorted(slots), f"slots published out of order: {slots}"
    for a, b in zip(slots, slots[1:]):
        assert (b - a) % STEP_MS == 0, (
            f"slot {b} is off the {STEP_MS}ms cadence after {a}"
        )
    assert all(e["beat_name"] == "ha" for e in envs)
    assert all(e["task"] == "app.ping" for e in envs)
    assert len({e["id"] for e in envs}) == len(envs), "task ids must be unique"
    return slots


def coverage(slots: list[int]) -> float:
    """Fraction of the slots between the first and last firing that fired."""
    span = (slots[-1] - slots[0]) // STEP_MS + 1
    return len(slots) / span


def test_two_beat_replicas_fire_each_slot_exactly_once(
    beat_module, redis_url, redis_client
):
    # A generous lease here: this test is about duplicate suppression while
    # BOTH replicas are healthy, so leadership should not churn. Failover
    # timing is the next test's job.
    lock_ttl = 10.0
    a = spawn_beat(beat_module, redis_url, "A", lock_ttl)
    b = spawn_beat(beat_module, redis_url, "B", lock_ttl)
    try:
        holder = wait_for(lambda: redis_client.get("cauli:beat:lock"), timeout=20)
        assert holder is not None, "no replica ever acquired the lease"
        assert holder.decode() in {"A", "B"}
        assert a.poll() is None and b.poll() is None

        assert wait_for(lambda: len(published(redis_client)) >= 12, timeout=30), (
            "the schedule never got going"
        )
    finally:
        out_a = stop(a)
        out_b = stop(b)

    envs = published(redis_client)
    slots = check_slots(envs, expected_min=10)
    assert coverage(slots) >= 0.8, (
        f"only {coverage(slots):.0%} of slots in the window fired: {slots}"
    )

    # Every firing is accounted for by exactly one replica's log, and only one
    # replica ever did any of it: the lease kept the standby idle.
    fired_a = out_a.count("beat: fired")
    fired_b = out_b.count("beat: fired")
    assert fired_a + fired_b == len(envs), (
        f"log/stream mismatch: A={fired_a} B={fired_b} stream={len(envs)}"
    )
    assert min(fired_a, fired_b) == 0, (
        "both replicas fired: the lease is not restricting ticking to one leader "
        f"(A={fired_a}, B={fired_b})"
    )
    assert int(redis_client.hget("cauli:beat:runs", "ha")) == len(envs)
    assert "lost the scheduler lease" not in out_a + out_b


def test_failover_when_the_leader_is_sigkilled(beat_module, redis_url, redis_client):
    # Short lease: failover latency is bounded by it, and the test has to
    # observe the handover inside a reasonable runtime.
    lock_ttl = 3.0
    procs = {
        name: spawn_beat(beat_module, redis_url, name, lock_ttl) for name in ("A", "B")
    }
    try:
        holder = wait_for(lambda: redis_client.get("cauli:beat:lock"), timeout=20)
        assert holder is not None
        leader_id = holder.decode()
        standby_id = "B" if leader_id == "A" else "A"

        before = wait_for(
            lambda: len(published(redis_client)) >= 4 and published(redis_client),
            timeout=30,
        )
        assert before, "the leader never started firing"
        count_before = len(before)
        last_before = before[-1]["beat_slot"]

        # SIGKILL: no shutdown handler runs, so the lease is NOT released --
        # the standby has to wait it out, exactly like a crashed pod.
        procs[leader_id].send_signal(signal.SIGKILL)
        procs[leader_id].wait(timeout=10)
        killed_at = time.monotonic()

        assert wait_for(
            lambda: (redis_client.get("cauli:beat:lock") or b"").decode() == standby_id,
            timeout=lock_ttl + 15,
        ), f"{standby_id} never took over the lease after the leader was killed"
        takeover_s = time.monotonic() - killed_at
        assert takeover_s <= lock_ttl + 5, f"failover took {takeover_s:.1f}s"

        # Holding the lease is not the point; scheduling is.
        assert wait_for(
            lambda: len(published(redis_client)) >= count_before + 4, timeout=30
        ), "the surviving replica took the lease but never fired"
    finally:
        outs = {name: stop(p) for name, p in procs.items()}

    envs = published(redis_client)
    slots = check_slots(envs, expected_min=count_before + 4)

    # Nothing was re-fired across the handover: the survivor resumed the
    # cadence rather than replaying, and the pre-kill slots stayed unique.
    after = [s for s in slots if s > last_before]
    assert after, "no slot fired after the handover"
    assert len(set(slots)) == len(slots)

    # The outage is one coalesced gap, bounded by the lease -- not a backlog
    # replay and not a permanently stalled schedule.
    gaps = [(b - a) // STEP_MS for a, b in zip(slots, slots[1:])]
    outage = max(gaps) * INTERVAL
    assert outage <= lock_ttl + 8, f"outage of {outage:.1f}s exceeds the lease budget"

    # ...and once it has taken over, the survivor keeps cadence rather than
    # limping. (`after[0]` is the firing that closed the outage gap, so the
    # steady-state window starts after it.)
    resumed = after[1:]
    assert len(resumed) >= 3, f"survivor only fired {len(after)} times"
    assert coverage(resumed) >= 0.8, f"survivor is dropping slots: {resumed}"

    fired = {name: out.count("beat: fired") for name, out in outs.items()}
    assert sum(fired.values()) == len(envs), f"{fired} vs {len(envs)} published"
    assert fired[standby_id] > 0, "the standby never fired anything"
    assert "acquired the scheduler lease" in outs[standby_id]
