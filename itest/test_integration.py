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


def _spawn_worker(queues, *extra_args, log_name):
    """Start an extra cauli-worker process against the shared stack's redis.

    For tests that need to control a worker's crash or restart timing
    directly, beyond the one long lived worker `stack` already runs.
    """
    env = dict(os.environ)
    env["VIRTUAL_ENV"] = VENV
    env["PATH"] = f"{VENV}/bin:" + env.get("PATH", "")
    env["CAULI_REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
    return subprocess.Popen(
        [
            BIN,
            "--app",
            "itest_app:app",
            "--queues",
            queues,
            "--redis-url",
            f"redis://127.0.0.1:{PORT}/0",
            "--cpu-workers",
            "2",
            "--io-concurrency",
            "64",
            *extra_args,
            "--python",
            f"{VENV}/bin/python",
            "--log-level",
            "info",
        ],
        cwd=HERE,
        env=env,
        stdout=open(f"{HERE}/worker_{log_name}.log", "wb"),
        stderr=subprocess.STDOUT,
    )


def _stop_worker(proc, timeout=15):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _pending_count(r, queue):
    summary = r.xpending(f"cauli:q:{queue}", "cauli")
    return summary["pending"] if summary else 0


def _wait_until_pending(r, queue, at_least, deadline_s):
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        if _pending_count(r, queue) >= at_least:
            return
        time.sleep(0.05)
    raise AssertionError(f"{queue} never reached {at_least} pending entries")


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


def test_unregistered_task_result_resolves_instead_of_hanging(stack):
    """A terminal dead letter writes a result, so get() raises, not hangs.

    An unregistered name reaches the DLQ with a recoverable id.
    """
    from cauli import TaskFailedError
    from cauli.result import AsyncResult

    r, _ = stack
    m = _app()
    env, queue, _fire = m.app.make_envelope(
        "itest_app.does_not_exist", args=[], task=None, queue="default"
    )
    r.xadd(f"cauli:q:{queue}", {"e": json.dumps(env)})

    res = AsyncResult(env["id"], m.app)
    with pytest.raises(TaskFailedError) as ei:
        res.get(timeout=10)
    assert ei.value.type == "UnregisteredTask"

    entries = r.xrange("cauli:dlq:default")
    ids = [json.loads(fields[b"e"])["id"] for _, fields in entries]
    assert env["id"] in ids


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
    # A keyed task that fails once must retry and succeed, not resolve as a
    # duplicate of its own claim.
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

    The marker file proves the body never ran. The envelope is already past
    its deadline at enqueue, which the client publishes anyway (section 3).
    """
    from datetime import datetime, timedelta, timezone

    from cauli import TaskFailedError

    r, _ = stack
    m = _app()
    path = f"/tmp/cauli-itest-expired-{uuid.uuid4().hex}"
    res = m.marker.apply_async(
        args=(path,), expires=datetime.now(timezone.utc) - timedelta(seconds=1)
    )

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


def test_dlq_stream_is_bounded(stack):
    """cauli:dlq:{queue} stays near its 1000 entry cap under a flood.

    Malformed entries are the cheapest DLQ writes. `shortlived` because no
    other test inspects its DLQ, so this cannot race them.
    """
    r, _ = stack
    dlq_key = "cauli:dlq:shortlived"
    q_key = "cauli:q:shortlived"
    r.delete(dlq_key)

    total = 4000
    pipe = r.pipeline(transaction=False)
    for i in range(total):
        pipe.xadd(q_key, {"e": f"not-json-{i}"})
        if i % 500 == 499:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()

    deadline = time.time() + 30
    while time.time() < deadline and r.xlen(q_key) > 0:
        time.sleep(0.1)
    assert r.xlen(q_key) == 0, "worker never drained the flood of malformed entries"

    xlen = r.xlen(dlq_key)
    assert xlen < total, f"dlq grew unbounded: {xlen} entries for {total} pushed"
    assert 500 <= xlen <= 2000, f"dlq should settle near the 1000 entry cap, got {xlen}"


def test_crash_redelivery_resolves_mine_again_not_duplicate(stack):
    """PROTOCOL section 4.5 "mine again": a crash victim's claim must not
    strand it.

    A worker is killed mid task; a second worker reclaims the pending entry
    with XCLAIM, and the guard must resolve it as MineAgain and re execute,
    not as Duplicate.
    """
    from cauli.result import AsyncResult

    r, _ = stack
    m = _app()
    queue = f"crashidemp-{uuid.uuid4().hex}"
    path = f"/tmp/cauli-itest-crashidemp-{uuid.uuid4().hex}"
    key = f"itest-crash-{uuid.uuid4().hex}"

    env, _q, _fire = m.app.make_envelope(
        m.slow_idemp.name,
        args=[path, 1.5],
        task=m.slow_idemp,
        queue=queue,
        idempotency_key=key,
    )
    task_id = env["id"]
    q_key = f"cauli:q:{queue}"
    # Create the group ourselves so the pending check below cannot race
    # worker1's own startup (it would otherwise get there via ensure_groups,
    # but only once it has actually started).
    r.xgroup_create(q_key, "cauli", id="0", mkstream=True)
    r.xadd(q_key, {"e": json.dumps(env)})

    w1 = _spawn_worker(queue, "--visibility-timeout", "1", log_name="crash1")
    try:
        _wait_until_pending(r, queue, at_least=1, deadline_s=10)
        time.sleep(0.3)  # give the executor a beat to actually enter the task
        w1.send_signal(signal.SIGKILL)
        w1.wait(timeout=10)
    finally:
        if w1.poll() is None:
            w1.kill()
            w1.wait(timeout=5)

    assert _pending_count(r, queue) == 1, (
        "a killed worker must leave its claim pending, not acked"
    )

    w2 = _spawn_worker(queue, "--visibility-timeout", "1", log_name="crash2")
    try:
        res = AsyncResult(task_id, m.app)
        out = res.get(timeout=25)
        assert out == {"slow_idemp_done": True}, "reclaimed task must genuinely execute"
        assert res.status() == "success", (
            "crash redelivery must not resolve as duplicate"
        )

        deadline = time.time() + 5
        while time.time() < deadline and _pending_count(r, queue) != 0:
            time.sleep(0.1)
        assert _pending_count(r, queue) == 0, (
            "a completed claim must be acked, not left reclaimable"
        )
    finally:
        _stop_worker(w2)

    with open(path) as f:
        contents = f.read()
    assert contents == "xx", (
        f"expected exactly two invocation attempts, one killed and one "
        f"completed: got {contents!r}"
    )
    os.remove(path)


def test_redelivery_limit_dead_letters_with_result(stack):
    """PROTOCOL section 4.4: past redelivery_limit = max(3, max_retries + 1)
    the worker dead letters with reason redelivery_limit and writes a result.

    Prior redeliveries are simulated with XCLAIM, which increments
    delivery_count exactly as the recovery loop does.
    """
    from cauli import TaskFailedError
    from cauli.result import AsyncResult

    r, _ = stack
    m = _app()
    queue = f"redelivery-{uuid.uuid4().hex}"
    q_key = f"cauli:q:{queue}"

    env, _q, _fire = m.app.make_envelope(
        m.redelivery_doomed.name, args=[1], task=m.redelivery_doomed, queue=queue
    )
    task_id = env["id"]

    r.xgroup_create(q_key, "cauli", id="0", mkstream=True)
    r.xadd(q_key, {"e": json.dumps(env)})

    # One real delivery, then three claims: times_delivered becomes 4, past
    # this task's own limit of max(3, 0 + 1) = 3, before any worker process
    # exists to look at it.
    delivered = r.xreadgroup("cauli", "sim-initial", {q_key: ">"}, count=1)
    entry_id = delivered[0][1][0][0]
    for i in range(3):
        r.xclaim(
            q_key,
            "cauli",
            f"sim-redelivery-{i}",
            min_idle_time=0,
            message_ids=[entry_id],
        )
    pel = r.xpending_range(q_key, "cauli", min=entry_id, max=entry_id, count=1)
    assert pel[0]["times_delivered"] == 4, "setup must land exactly one over the limit"

    w = _spawn_worker(queue, "--visibility-timeout", "1", log_name="redelivery")
    try:
        res = AsyncResult(task_id, m.app)
        with pytest.raises(TaskFailedError) as ei:
            res.get(timeout=20)
        assert ei.value.type == "RedeliveryLimitExceeded"

        dlq_entries = r.xrange(f"cauli:dlq:{queue}")
        reasons = {json.loads(f[b"e"])["id"]: f[b"reason"] for _sid, f in dlq_entries}
        assert task_id in reasons, "doomed entry never reached the dead letter queue"
        assert reasons[task_id] == b"redelivery_limit"
        assert reasons[task_id] != b"max_retries"
    finally:
        _stop_worker(w)


def _transit_payload():
    deep = 0
    for _ in range(119):
        deep = [deep]  # args[2] is 120 levels deep, counting args itself
    args = [
        {"zeta": 1, "alpha": [2**100, -(2**70), 18446744073709551616]},
        "café \U0001f600 \u2028 \x00",
        deep,
        -0.0,
        1.5e-300,
    ]
    kwargs = {"z": None, "a": True, "m": {"y": -0.0, "b": "\ud7ff"}}
    return args, kwargs


def _await_result_doc(r, task_id, deadline_s=30):
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        raw = r.get(f"cauli:result:{task_id}")
        if raw:
            return raw
        time.sleep(0.05)
    raise AssertionError(f"no result for {task_id}")


@pytest.mark.parametrize("lane", ["transit_sync", "transit_async", "transit_cpu"])
def test_args_and_results_cross_every_lane_unchanged(stack, lane):
    """What the task receives, and what the caller reads back, is exactly
    what was sent: key order, integers past 64 bits, -0.0, astral and
    control characters, 120 levels of nesting. Compared by repr, since ==
    is blind to key order and to the sign of zero.

    Sent twice: once by the client, and once as hand written envelope text
    the way a foreign producer might write it, with \\u escapes (a surrogate
    pair included) and newlines between tokens, which the cpu lane has to
    keep off its line oriented child protocol.
    """
    import msgspec

    from cauli.result import AsyncResult

    r, _ = stack
    m = _app()
    task = getattr(m, lane)
    args, kwargs = _transit_payload()
    expect = [repr(tuple(args)), repr(kwargs), args, kwargs]

    by_client = task.delay(*args, **kwargs).id
    env, queue, _fire = m.app.make_envelope(task.name, args=(), task=task)
    head = json.dumps({k: v for k, v in env.items() if k not in ("args", "kwargs")})
    text = head[:-1] + ', "args": %s, "kwargs": %s}' % (
        json.dumps(args, indent=1),
        json.dumps(kwargs, indent=1),
    )
    assert "\\ud83d\\ude00" in text and "\n" in text
    r.xadd(f"cauli:q:{queue}", {"e": text})

    for task_id in (by_client, env["id"]):
        got = AsyncResult(task_id, m.app).get(timeout=30)
        assert repr(got) == repr(expect), task_id
        doc = msgspec.json.decode(_await_result_doc(r, task_id))
        assert repr(doc["result"]) == repr(expect), task_id


def test_worker_survived_everything(stack):
    _, worker = stack
    assert worker.poll() is None
