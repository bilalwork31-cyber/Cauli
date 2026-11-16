"""RabbitMQ and SQS as the ready queue (PROTOCOL.md section 14), end to end:
the real client, the real cauli-worker binary and a real broker.

Each broker's tests run when its server is named in the environment and skip
otherwise:

    CAULI_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:5672/
    CAULI_TEST_SQS_ENDPOINT=http://127.0.0.1:9324   (ElasticMQ or LocalStack)

The reconnect test closes RabbitMQ's connections with `rabbitmqctl`, which
needs the broker's own account. CAULI_TEST_RABBITMQCTL names the command
(for example "docker exec rabbit rabbitmqctl"); the test skips when it fails.
"""

import json
import os
import signal
import subprocess
import time
import uuid

import pytest
import redis as redis_lib

from cauli import TaskFailedError, _broker, interval
from cauli.beat import Beat
from cauli.result import AsyncResult

PORT = 6397
REDIS_URL = f"redis://127.0.0.1:{PORT}/0"
HOME = os.path.expanduser("~")
BIN = os.environ.get("CAULI_WORKER_BIN", f"{HOME}/rupy-target/release/cauli-worker")
VENV = os.environ.get("CAULI_VENV", f"{HOME}/rupy-venv")
HERE = os.path.dirname(os.path.abspath(__file__))
AMQP_URL = os.environ.get("CAULI_TEST_AMQP_URL")
SQS_ENDPOINT = os.environ.get("CAULI_TEST_SQS_ENDPOINT")
# Short, so a killed worker's SQS messages come back within a test's patience.
VISIBILITY_TIMEOUT_S = 4

SQS_ENV = {
    "AWS_ENDPOINT_URL_SQS": SQS_ENDPOINT or "",
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_EC2_METADATA_DISABLED": "true",
}

BROKERS = [
    pytest.param(
        "amqp",
        marks=pytest.mark.skipif(
            AMQP_URL is None, reason="CAULI_TEST_AMQP_URL is not set"
        ),
    ),
    pytest.param(
        "sqs",
        marks=pytest.mark.skipif(
            SQS_ENDPOINT is None, reason="CAULI_TEST_SQS_ENDPOINT is not set"
        ),
    ),
]


class Stack:
    """One broker, one throwaway Redis, one queue name nothing else uses."""

    def __init__(self, kind):
        self.kind = kind
        self.url = AMQP_URL if kind == "amqp" else "sqs://us-east-1"
        self.queue = f"it{uuid.uuid4().hex[:10]}"
        self.redis = redis_lib.Redis(port=PORT)
        self.worker = None
        self.starts = 0

    def env(self):
        env = dict(os.environ)
        env["VIRTUAL_ENV"] = VENV
        env["PATH"] = f"{VENV}/bin:" + env.get("PATH", "")
        env["CAULI_REDIS_URL"] = REDIS_URL
        if self.kind == "sqs":
            env.update(SQS_ENV)
        return env

    def start_worker(self):
        self.starts += 1
        self.log = f"{HERE}/worker_{self.kind}_{self.starts}.log"
        self.worker = subprocess.Popen(
            [
                BIN,
                "--app",
                "itest_app:app",
                "--queues",
                self.queue,
                "--redis-url",
                REDIS_URL,
                "--broker-url",
                self.url,
                "--cpu-workers",
                "2",
                "--io-concurrency",
                "32",
                "--visibility-timeout",
                str(VISIBILITY_TIMEOUT_S),
                "--python",
                f"{VENV}/bin/python",
            ],
            cwd=HERE,
            env=self.env(),
            stdout=open(self.log, "wb"),
            stderr=subprocess.STDOUT,
        )

    def stop_worker(self):
        if self.worker is None or self.worker.poll() is not None:
            return
        self.worker.send_signal(signal.SIGTERM)
        try:
            self.worker.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.worker.kill()
            self.worker.wait(timeout=5)

    def kill_worker(self):
        self.worker.kill()
        self.worker.wait(timeout=5)

    def worker_log(self):
        with open(self.log, errors="replace") as f:
            return f.read()

    def delete_queue(self):
        publisher = _broker.open_broker(self.url)
        if self.kind == "amqp":
            publisher.publish_many([])
            publisher._channel.queue_delete(_broker.amqp_queue_name(self.queue))
        else:
            client = publisher._sqs()
            client.delete_queue(QueueUrl=publisher._queue_url(client, self.queue))


@pytest.fixture(scope="module", params=BROKERS)
def stack(request):
    subprocess.run(
        ["redis-server", "--port", str(PORT), "--save", "", "--appendonly", "no"]
        + ["--daemonize", "yes"],
        check=True,
    )
    stack = Stack(request.param)
    for _ in range(50):
        try:
            stack.redis.ping()
            break
        except redis_lib.exceptions.ConnectionError:
            time.sleep(0.1)
    stack.redis.flushall()

    saved_env = {name: os.environ.get(name) for name in SQS_ENV}
    if stack.kind == "sqs":
        os.environ.update(SQS_ENV)
    import itest_app

    app = itest_app.app
    saved_app = (app.redis_url, app._redis, app.broker_url, app._broker)
    app.redis_url, app._redis, app.broker_url, app._broker = (
        REDIS_URL,
        None,
        stack.url,
        None,
    )

    stack.start_worker()
    stack.app = itest_app
    yield stack

    stack.stop_worker()
    stack.delete_queue()
    app.redis_url, app._redis, app.broker_url, app._broker = saved_app
    for name, value in saved_env.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    subprocess.run(["redis-cli", "-p", str(PORT), "shutdown", "nosave"], check=False)


def _wait_for(predicate, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


def _size(path):
    return os.path.getsize(path) if os.path.exists(path) else 0


def test_every_lane_runs_a_task_and_stores_its_result(stack):
    m = stack.app
    assert m.echo.apply_async(args=("hi",), queue=stack.queue).get(timeout=30) == {
        "echo": "hi"
    }
    assert m.aecho.apply_async(args=(7,), queue=stack.queue).get(timeout=15) == {
        "aecho": 7
    }
    assert m.cpu_math.apply_async(args=(21,), queue=stack.queue).get(timeout=20) == 42


def test_the_ready_queue_is_not_in_redis(stack):
    m = stack.app
    m.echo.apply_async(args=("x",), queue=stack.queue).get(timeout=15)
    assert stack.redis.exists(f"cauli:q:{stack.queue}") == 0


def test_a_batch_runs_every_task_exactly_once(stack):
    m = stack.app
    path = f"/tmp/cauli-itest-batch-{uuid.uuid4().hex}"
    results = m.app.enqueue_many(
        [(m.counted, (path,), {}, {"queue": stack.queue}) for _ in range(300)]
    )
    for result in results:
        assert result.get(timeout=60) == "counted"
    assert _size(path) == 300


def test_a_retry_goes_through_the_delayed_set_and_back_to_the_broker(stack):
    m = stack.app
    path = f"/tmp/cauli-itest-flaky-{uuid.uuid4().hex}"
    out = m.flaky.apply_async(args=(path, 2), queue=stack.queue).get(timeout=30)
    assert out == {"succeeded_on_attempt": 2}
    assert _size(path) == 3
    assert stack.redis.zcard(f"cauli:delayed:{stack.queue}") == 0


def test_a_countdown_waits_in_redis_then_runs(stack):
    m = stack.app
    started = time.monotonic()
    result = m.echo.apply_async(args=("later",), queue=stack.queue, countdown=1.5)
    assert stack.redis.zcard(f"cauli:delayed:{stack.queue}") == 1
    assert result.get(timeout=20) == {"echo": "later"}
    assert time.monotonic() - started >= 1.4


def test_a_final_failure_is_dead_lettered_in_redis(stack):
    m = stack.app
    result = m.always_fail.apply_async(queue=stack.queue)
    with pytest.raises(TaskFailedError) as raised:
        result.get(timeout=30)
    assert raised.value.type == "RuntimeError"
    dead = [
        json.loads(fields[b"e"])["id"]
        for _sid, fields in stack.redis.xrange(f"cauli:dlq:{stack.queue}")
    ]
    assert result.id in dead


def test_an_unregistered_task_resolves_instead_of_hanging(stack):
    m = stack.app
    envelope, _queue, _fire = m.app.make_envelope(
        "itest_app.does_not_exist", args=[], queue=stack.queue
    )
    m.app._get_broker().publish_many([(stack.queue, json.dumps(envelope).encode())])
    with pytest.raises(TaskFailedError) as raised:
        AsyncResult(envelope["id"], m.app).get(timeout=15)
    assert raised.value.type == "UnregisteredTask"


def test_a_body_that_is_not_an_envelope_is_dead_lettered(stack):
    m = stack.app
    marker = f"not-json-{uuid.uuid4().hex}"
    m.app._get_broker().publish_many([(stack.queue, marker.encode())])

    def dead_lettered():
        return any(
            fields[b"e"].decode() == marker and fields[b"reason"] == b"malformed"
            for _sid, fields in stack.redis.xrange(f"cauli:dlq:{stack.queue}")
        )

    _wait_for(dead_lettered, 15, "the malformed body in the DLQ")


def test_an_idempotency_key_suppresses_the_second_task(stack):
    m = stack.app
    path = f"/tmp/cauli-itest-idemp-{uuid.uuid4().hex}"
    key = f"itest-{uuid.uuid4().hex}"
    first = m.counted.apply_async(args=(path,), idempotency_key=key, queue=stack.queue)
    second = m.counted.apply_async(args=(path,), idempotency_key=key, queue=stack.queue)
    _wait_for(
        lambda: "pending" not in {first.status(), second.status()}, 20, "both results"
    )
    assert {str(first.status()), str(second.status())} == {"success", "duplicate"}
    assert _size(path) == 1


def test_beat_fires_through_the_delayed_set(stack):
    m = stack.app
    path = f"/tmp/cauli-itest-beat-{uuid.uuid4().hex}"
    name = f"it-beat-{uuid.uuid4().hex[:8]}"
    m.app.add_periodic_task(
        name, m.counted, interval(1.0), args=(path,), queue=stack.queue
    )
    try:
        beat = Beat(m.app, use_lock=False)
        beat.sync_code_entries()
        deadline = time.time() + 15
        while beat.fired == 0 and time.time() < deadline:
            beat.tick()
            time.sleep(0.2)
        assert beat.fired >= 1
        _wait_for(lambda: _size(path) >= 1, 15, "the scheduled task to run")
    finally:
        del m.app._periodic[name]
        Beat(m.app, use_lock=False).sync_code_entries()


def test_a_redis_outage_holds_the_outcome_and_never_reruns_the_task(stack):
    """Redis refuses the result write; the delivery stays unacked and the
    outcome is written once Redis takes it, without a second execution."""
    m = stack.app
    path = f"/tmp/cauli-itest-outage-{uuid.uuid4().hex}"
    stack.redis.config_set("maxmemory", 1)
    try:
        result = m.counted.apply_async(args=(path,), queue=stack.queue)
        _wait_for(lambda: _size(path) == 1, 20, "the task to run")
        time.sleep(2 * VISIBILITY_TIMEOUT_S)
        assert str(result.status()) == "pending"
        assert _size(path) == 1, (
            "the task ran again while its result could not be written"
        )
    finally:
        stack.redis.config_set("maxmemory", 0)
    assert result.get(timeout=20) == "counted"
    time.sleep(VISIBILITY_TIMEOUT_S + 2)
    assert _size(path) == 1, "the delivery was not acked after the write landed"
    assert "holding the delivery unacked" in stack.worker_log()


def test_a_redis_outage_holds_a_keyed_task_instead_of_running_it_unguarded(stack):
    """With Redis as the queue an outage stops deliveries, so the idempotency
    claim may fail open. Here deliveries continue, and the task must wait."""
    m = stack.app
    path = f"/tmp/cauli-itest-claim-{uuid.uuid4().hex}"
    stack.redis.config_set("maxmemory", 1)
    try:
        result = m.counted.apply_async(
            args=(path,), queue=stack.queue, idempotency_key=f"itest-{uuid.uuid4().hex}"
        )
        time.sleep(VISIBILITY_TIMEOUT_S + 2)
        assert _size(path) == 0, "the task ran without its idempotency claim"
    finally:
        stack.redis.config_set("maxmemory", 0)
    assert result.get(timeout=30) == "counted"
    assert _size(path) == 1
    assert "waiting for redis" in stack.worker_log()


def test_a_task_that_keeps_killing_its_worker_is_dead_lettered(stack):
    """max_retries 0 gives a redelivery limit of 3: four executions, then the
    fifth delivery is dead lettered without running (PROTOCOL.md 4.4)."""
    m = stack.app
    path = f"/tmp/cauli-itest-poison-{uuid.uuid4().hex}"
    result = m.kill_worker.apply_async(args=(path,), queue=stack.queue)
    for attempt in range(1, 5):
        _wait_for(lambda: _size(path) == attempt, 40, f"execution {attempt}")
        stack.worker.wait(timeout=10)
        stack.start_worker()
    with pytest.raises(TaskFailedError) as raised:
        result.get(timeout=40)
    assert raised.value.type == "RedeliveryLimitExceeded"
    assert _size(path) == 4
    assert stack.worker.poll() is None, "the fifth delivery must not run"


def test_a_killed_workers_task_runs_again_on_the_next_worker(stack):
    m = stack.app
    path = f"/tmp/cauli-itest-crash-{uuid.uuid4().hex}"
    result = m.slow_idemp.apply_async(args=(path, 2), queue=stack.queue)
    _wait_for(lambda: _size(path) == 1, 20, "the task to start")
    stack.kill_worker()
    stack.start_worker()
    assert result.get(timeout=40) == {"slow_idemp_done": True}
    assert _size(path) == 2


def test_a_graceful_stop_finishes_the_task_in_flight(stack):
    m = stack.app
    path = f"/tmp/cauli-itest-drain-{uuid.uuid4().hex}"
    result = m.slow_idemp.apply_async(args=(path, 2), queue=stack.queue)
    _wait_for(lambda: _size(path) == 1, 20, "the task to start")
    stack.stop_worker()
    assert stack.worker.returncode == 0
    assert result.get(timeout=5) == {"slow_idemp_done": True}
    stack.start_worker()
    assert m.echo.apply_async(args=("back",), queue=stack.queue).get(timeout=30) == {
        "echo": "back"
    }
    assert _size(path) == 1


def test_the_worker_survives_its_broker_connection_being_closed(stack):
    if stack.kind != "amqp":
        pytest.skip("SQS is request and response: there is no connection to close")
    m = stack.app
    assert m.echo.apply_async(args=("before",), queue=stack.queue).get(timeout=30) == {
        "echo": "before"
    }
    rabbitmqctl = os.environ.get("CAULI_TEST_RABBITMQCTL", "rabbitmqctl").split()
    try:
        closed = subprocess.run(
            [*rabbitmqctl, "close_all_connections", "cauli itest"],
            capture_output=True,
        ).returncode
    except FileNotFoundError:
        closed = None
    if closed != 0:
        pytest.skip("rabbitmqctl is not usable here; set CAULI_TEST_RABBITMQCTL")
    assert m.echo.apply_async(args=("after",), queue=stack.queue).get(timeout=30) == {
        "echo": "after"
    }
    assert "rabbitmq connection lost" in stack.worker_log()


def test_an_envelope_sqs_can_never_take_is_dead_lettered_not_retried(stack):
    if stack.kind != "sqs":
        pytest.skip("RabbitMQ takes any body up to its own much larger limit")
    m = stack.app
    envelope, _queue, _fire = m.app.make_envelope(
        "itest_app.echo", args=["x" * 1_100_000], queue=stack.queue
    )
    raw = json.dumps(envelope).encode()
    stack.redis.zadd(f"cauli:delayed:{stack.queue}", {raw: 0})
    with pytest.raises(TaskFailedError) as raised:
        AsyncResult(envelope["id"], m.app).get(timeout=15)
    assert raised.value.type == "Malformed"
    assert "SQS message limit" in raised.value.message
    _wait_for(
        lambda: stack.redis.zcard(f"cauli:delayed:{stack.queue}") == 0,
        10,
        "the refused envelope to leave the delayed set",
    )


def test_a_body_the_endpoint_refuses_does_not_hold_back_its_batch(stack):
    """ElasticMQ keeps the 256 KiB message limit SQS had before 2025, so it
    refuses a body that passes the worker's own 1 MiB check. The refusal
    fails the whole SendMessageBatch call, and must land on that one task."""
    if stack.kind != "sqs":
        pytest.skip("RabbitMQ takes any body up to its own much larger limit")
    m = stack.app
    try:
        m.echo.apply_async(args=("x" * 300_000,), queue=stack.queue)
    except Exception:
        pass
    else:
        pytest.skip("this endpoint accepts a 300 KB body")

    envelopes = [
        m.app.make_envelope("itest_app.echo", args=[arg], queue=stack.queue)[0]
        for arg in ("first", "x" * 300_000, "last")
    ]
    stack.redis.zadd(
        f"cauli:delayed:{stack.queue}",
        {json.dumps(envelope).encode(): 0 for envelope in envelopes},
    )

    small_first, refused, small_last = (AsyncResult(e["id"], m.app) for e in envelopes)
    assert small_first.get(timeout=20) == {"echo": "first"}
    assert small_last.get(timeout=20) == {"echo": "last"}
    with pytest.raises(TaskFailedError) as raised:
        refused.get(timeout=20)
    assert raised.value.type == "Malformed"
    # The dead letter is written before the envelope leaves the delayed set.
    _wait_for(
        lambda: stack.redis.zcard(f"cauli:delayed:{stack.queue}") == 0,
        10,
        "the refused envelope to leave the delayed set",
    )
