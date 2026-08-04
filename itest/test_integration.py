"""Cross-component e2e: real cauli client + real cauli-worker binary + real cauli._exec children."""

import json
import os
import signal
import subprocess
import time
import uuid

import pytest
import redis as redis_lib

PORT = 6394
HOME = os.path.expanduser("~")
BIN = os.environ.get("CAULI_WORKER_BIN", f"{HOME}/rupy-target/release/cauli-worker")
VENV = os.environ.get("CAULI_VENV", f"{HOME}/rupy-venv")
HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def stack():
    subprocess.run(
        [
            "redis-server",
            "--port",
            str(PORT),
            "--save",
            "",
            "--appendonly",
            "no",
            "--daemonize",
            "yes",
        ],
        check=True,
    )
    r = redis_lib.Redis(port=PORT)
    for _ in range(50):
        try:
            r.ping()
            break
        except redis_lib.exceptions.ConnectionError:
            time.sleep(0.1)
    r.flushall()

    env = dict(os.environ)
    env["VIRTUAL_ENV"] = VENV
    env["PATH"] = f"{VENV}/bin:" + env.get("PATH", "")
    env["CAULI_REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
    worker = subprocess.Popen(
        [
            BIN,
            "--app",
            "itest_app:app",
            "--queues",
            "default,routed,shortlived",
            "--redis-url",
            f"redis://127.0.0.1:{PORT}/0",
            "--cpu-workers",
            "2",
            "--io-concurrency",
            "64",
            "--visibility-timeout",
            "30",
            "--python",
            f"{VENV}/bin/python",
            "--log-level",
            "info",
        ],
        cwd=HERE,
        env=env,
        stdout=open(f"{HERE}/worker.log", "wb"),
        stderr=subprocess.STDOUT,
    )
    os.environ["CAULI_REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
    yield r, worker
    worker.send_signal(signal.SIGTERM)
    try:
        worker.wait(timeout=15)
    except subprocess.TimeoutExpired:
        worker.kill()
    subprocess.run(["redis-cli", "-p", str(PORT), "shutdown", "nosave"], check=False)


def _app():
    import itest_app

    return itest_app


def test_sync_io_roundtrip(stack):
    m = _app()
    res = m.echo.delay("hello")
    assert res.get(timeout=30) == {"echo": "hello"}  # doubles as worker readiness gate


def test_async_io(stack):
    m = _app()
    assert m.aecho.delay(42).get(timeout=15) == {"aecho": 42}


def test_cpu_child_real_exec(stack):
    m = _app()
    assert m.cpu_math.delay(21).get(timeout=20) == 42


def test_retry_then_succeed(stack):
    m = _app()
    path = f"/tmp/cauli-itest-flaky-{uuid.uuid4().hex}"
    out = m.flaky.delay(path, 2).get(timeout=30)
    assert out == {"succeeded_on_attempt": 2}
    assert os.path.getsize(path) == 3  # 2 failures + 1 success


def test_final_failure_dlq_and_error(stack):
    from cauli import TaskFailedError

    r, _ = stack
    m = _app()
    res = m.always_fail.delay()
    with pytest.raises(TaskFailedError) as ei:
        res.get(timeout=30)
    assert ei.value.type == "RuntimeError"
    assert "nope" in ei.value.message
    entries = r.xrange("cauli:dlq:default")
    ids = [json.loads(fields[b"e"])["id"] for _, fields in entries]
    assert res.id in ids


def test_idempotency_dedup(stack):
    m = _app()
    path = f"/tmp/cauli-itest-idemp-{uuid.uuid4().hex}"
    key = f"itest-{uuid.uuid4().hex}"
    r1 = m.counted.apply_async(args=(path,), idempotency_key=key)
    r2 = m.counted.apply_async(args=(path,), idempotency_key=key)
    s1, s2 = set(), set()
    deadline = time.time() + 20
    while time.time() < deadline:
        s1, s2 = {r1.status()}, {r2.status()}
        if "pending" not in s1 | s2:
            break
        time.sleep(0.1)
    assert {next(iter(s1)), next(iter(s2))} == {"success", "duplicate"}
    assert os.path.getsize(path) == 1


def test_idempotency_key_allows_retry(stack):
    # C1 regression: idempotency_key + a task that fails once then succeeds
    # must actually retry and finish "success" -- previously it silently
    # resolved as "duplicate" against its own earlier claim, forever.
    m = _app()
    path = f"/tmp/cauli-itest-idemp-retry-{uuid.uuid4().hex}"
    key = f"itest-retry-{uuid.uuid4().hex}"
    res = m.flaky_idemp.apply_async(args=(path, 1), idempotency_key=key)
    out = res.get(timeout=30)
    assert out == {"succeeded_on_attempt": 1}
    assert res.status() == "success"


def test_countdown_delays_execution(stack):
    r, _ = stack
    m = _app()
    t0 = time.time()
    res = m.echo.apply_async(args=("later",), countdown=1.2)
    assert res.get(timeout=15) == {"echo": "later"}
    elapsed = time.time() - t0
    assert elapsed >= 1.15, f"ran too early: {elapsed:.2f}s"
    raw = json.loads(r.get(f"cauli:result:{res.id}"))
    assert raw["status"] == "success"


def test_cpu_soft_timeout(stack):
    from cauli import TaskFailedError

    m = _app()
    res = m.slow_cpu.delay()
    t0 = time.time()
    with pytest.raises(TaskFailedError) as ei:
        res.get(timeout=30)
    assert ei.value.type == "SoftTimeLimitExceeded"
    assert (
        time.time() - t0 < 8
    )  # soft-killed at ~0.3s, not the 5s sleep or 10s hard limit


def test_eta_delays_execution_to_an_absolute_instant(stack):
    from datetime import datetime, timedelta, timezone

    r, _ = stack
    m = _app()
    when = datetime.now(timezone.utc) + timedelta(seconds=1.2)
    t0 = time.time()
    res = m.echo.apply_async(args=("eta",), eta=when)
    assert res.get(timeout=15) == {"echo": "eta"}
    assert time.time() - t0 >= 1.1, "ran before its eta"
    env_score = when.timestamp() * 1000
    raw = json.loads(r.get(f"cauli:result:{res.id}"))
    assert raw["status"] == "success"
    assert env_score > 0


def test_expired_task_is_discarded_without_running(stack):
    """PROTOCOL section 9.1: expired work is dropped at dispatch, not executed.

    The marker file is the proof that the task body never ran -- a result key
    saying "expired" would be satisfied by a task that ran and then got
    relabelled.
    """
    from cauli import TaskFailedError

    r, _ = stack
    m = _app()
    path = f"/tmp/cauli-itest-expired-{uuid.uuid4().hex}"
    # Delayed past its own expiry: due at +1.5s, dead at +0.4s.
    res = m.marker.apply_async(args=(path,), countdown=1.5, expires=0.4)

    with pytest.raises(TaskFailedError) as ei:
        res.get(timeout=20)
    assert ei.value.type == "Expired"
    assert res.expired is True
    assert res.status() == "expired"
    assert not os.path.exists(path), "an expired task must not execute"

    entries = r.xrange("cauli:dlq:default")
    expired = [
        json.loads(f[b"e"])["id"] for _sid, f in entries if f[b"reason"] == b"expired"
    ]
    assert res.id in expired


def test_unexpired_task_still_runs(stack):
    m = _app()
    path = f"/tmp/cauli-itest-live-{uuid.uuid4().hex}"
    res = m.marker.apply_async(args=(path,), expires=60)
    assert res.get(timeout=20) == "ran"
    assert os.path.exists(path)


def test_queue_ttl_expires_a_backlogged_entry(stack):
    """PROTOCOL section 9.2: the worker enforces the queue TTL at dispatch.

    The envelope here is written straight to the stream with an `enqueued_at`
    two hours in the past and NO expires_at, so the only thing that can drop it
    is the worker's own `queue_ttl` config for `shortlived` (1s).
    """
    r, _ = stack
    m = _app()
    path = f"/tmp/cauli-itest-qttl-{uuid.uuid4().hex}"
    env, queue, _fire = m.app.make_envelope(
        m.marker.name, args=[path], task=m.marker, queue="shortlived"
    )
    env["enqueued_at"] = int(time.time() * 1000) - 7_200_000
    env["expires_at"] = None
    r.xadd(f"cauli:q:{queue}", {"e": json.dumps(env)})

    deadline = time.time() + 20
    while time.time() < deadline:
        raw = r.get(f"cauli:result:{env['id']}")
        if raw:
            break
        time.sleep(0.1)
    assert raw, "the worker never resolved the stale entry"
    assert json.loads(raw)["status"] == "expired"
    assert not os.path.exists(path), "a task past its queue TTL must not execute"


def test_app_level_routing_moves_a_task_to_another_queue(stack):
    r, _ = stack
    m = _app()
    res = m.routed_task.delay(5)
    assert res.get(timeout=20) == {"routed": 5}
    # It really went through `routed`, not `default`: the route pattern in
    # itest_app has no counterpart anywhere in the task's own definition.
    assert r.exists("cauli:q:routed")
    routed_ids = {
        json.loads(f[b"e"])["id"] for _sid, f in r.xrange("cauli:dlq:routed") or []
    }
    assert res.id not in routed_ids


def test_worker_survived_everything(stack):
    _, worker = stack
    assert worker.poll() is None
