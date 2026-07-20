"""Cross-component e2e: real rupy client + real rupy-worker binary + real rupy._exec children."""
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
BIN = os.environ.get("RUPY_WORKER_BIN", f"{HOME}/rupy-target/release/rupy-worker")
VENV = os.environ.get("RUPY_VENV", f"{HOME}/rupy-venv")
HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def stack():
    subprocess.run(["redis-server", "--port", str(PORT), "--save", "", "--appendonly", "no",
                    "--daemonize", "yes"], check=True)
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
    env["RUPY_REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
    worker = subprocess.Popen(
        [BIN, "--app", "itest_app:app",
         "--redis-url", f"redis://127.0.0.1:{PORT}/0",
         "--cpu-workers", "2", "--io-concurrency", "64",
         "--visibility-timeout", "30", "--python", f"{VENV}/bin/python",
         "--log-level", "info"],
        cwd=HERE, env=env,
        stdout=open(f"{HERE}/worker.log", "wb"), stderr=subprocess.STDOUT,
    )
    os.environ["RUPY_REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
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
    assert res.get(timeout=30) == {"echo": "hello"}   # doubles as worker readiness gate


def test_async_io(stack):
    m = _app()
    assert m.aecho.delay(42).get(timeout=15) == {"aecho": 42}


def test_cpu_child_real_exec(stack):
    m = _app()
    assert m.cpu_math.delay(21).get(timeout=20) == 42


def test_retry_then_succeed(stack):
    m = _app()
    path = f"/tmp/rupy-itest-flaky-{uuid.uuid4().hex}"
    out = m.flaky.delay(path, 2).get(timeout=30)
    assert out == {"succeeded_on_attempt": 2}
    assert os.path.getsize(path) == 3  # 2 failures + 1 success


def test_final_failure_dlq_and_error(stack):
    from rupy import TaskFailedError
    r, _ = stack
    m = _app()
    res = m.always_fail.delay()
    with pytest.raises(TaskFailedError) as ei:
        res.get(timeout=30)
    assert ei.value.type == "RuntimeError"
    assert "nope" in ei.value.message
    entries = r.xrange("rupy:dlq:default")
    ids = [json.loads(fields[b"e"])["id"] for _, fields in entries]
    assert res.id in ids


def test_idempotency_dedup(stack):
    m = _app()
    path = f"/tmp/rupy-itest-idemp-{uuid.uuid4().hex}"
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


def test_countdown_delays_execution(stack):
    r, _ = stack
    m = _app()
    t0 = time.time()
    res = m.echo.apply_async(args=("later",), countdown=1.2)
    assert res.get(timeout=15) == {"echo": "later"}
    elapsed = time.time() - t0
    assert elapsed >= 1.15, f"ran too early: {elapsed:.2f}s"
    raw = json.loads(r.get(f"rupy:result:{res.id}"))
    assert raw["status"] == "success"


def test_cpu_soft_timeout(stack):
    from rupy import TaskFailedError
    m = _app()
    res = m.slow_cpu.delay()
    t0 = time.time()
    with pytest.raises(TaskFailedError) as ei:
        res.get(timeout=30)
    assert ei.value.type == "SoftTimeLimitExceeded"
    assert time.time() - t0 < 8  # soft-killed at ~0.3s, not the 5s sleep or 10s hard limit


def test_worker_survived_everything(stack):
    _, worker = stack
    assert worker.poll() is None
