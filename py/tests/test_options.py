"""Test 3: task options land in TaskDef attrs AND the envelope; queue precedence; idempotency_key."""

from __future__ import annotations

import json
import logging
import threading

import pytest

from cauli import Cauli


def _single_envelope(redis_client, queue):
    entries = redis_client.xrange(f"cauli:q:{queue}")
    assert len(entries) == 1, f"expected exactly one entry on cauli:q:{queue}"
    _sid, fields = entries[0]
    assert set(fields.keys()) == {b"e"}
    return json.loads(fields[b"e"])


def test_custom_options_land_in_taskdef_and_envelope(app, redis_client):
    @app.task(
        name="reports.crunch",
        kind="cpu",
        queue="crunchq",
        max_retries=7,
        timeout=10,
        soft_timeout=2,
        backoff_base=1.0,
        backoff_factor=3.0,
        backoff_max=30.0,
        jitter=False,
        store_result=False,
    )
    def crunch(n):
        return n * n

    # Registry + TaskDef attributes (exact names read by the Rust worker).
    td = app._tasks["reports.crunch"]
    assert td is crunch
    assert td.name == "reports.crunch"
    assert td.fn(3) == 9
    assert td.is_async is False
    assert td.kind == "cpu"
    assert td.queue == "crunchq"
    assert td.max_retries == 7
    assert td.timeout_ms == 10000
    assert td.soft_timeout_ms == 2000
    assert td.backoff_base_ms == 1000
    assert td.backoff_factor == 3.0
    assert td.backoff_max_ms == 30000
    assert td.jitter is False
    assert td.store_result is False

    crunch.delay(4)
    env = _single_envelope(redis_client, "crunchq")
    assert env["task"] == "reports.crunch"
    assert env["queue"] == "crunchq"
    assert env["kind"] == "cpu"
    assert env["max_retries"] == 7
    assert env["timeout_ms"] == 10000
    assert env["soft_timeout_ms"] == 2000
    assert env["backoff_base_ms"] == 1000
    assert env["backoff_factor"] == 3.0
    assert env["backoff_max_ms"] == 30000
    assert env["jitter"] is False
    assert env["store_result"] is False
    assert env["args"] == [4]


def test_queue_precedence(app, redis_client):
    @app.task(queue="taskq")
    def with_queue():
        return None

    @app.task()
    def without_queue():
        return None

    assert with_queue.queue == "taskq"
    assert without_queue.queue is None  # worker falls back to app.default_queue

    # task queue beats app default
    with_queue.delay()
    assert _single_envelope(redis_client, "taskq")["queue"] == "taskq"

    # apply_async queue= beats task queue
    with_queue.apply_async(queue="override")
    env = _single_envelope(redis_client, "override")
    assert env["queue"] == "override"

    # no task queue: app default_queue wins
    without_queue.delay()
    assert _single_envelope(redis_client, "default")["queue"] == "default"


def test_app_default_queue_used(redis_url, redis_client):
    app2 = Cauli(redis_url=redis_url, default_queue="dq")

    @app2.task()
    def t():
        return None

    t.delay()
    env = _single_envelope(redis_client, "dq")
    assert env["queue"] == "dq"


def test_idempotency_key_lands_in_envelope(app, redis_client):
    @app.task()
    def t():
        return None

    t.apply_async(idempotency_key="order-42")
    env = _single_envelope(redis_client, "default")
    assert env["idempotency_key"] == "order-42"

    # and stays null when not given
    redis_client.delete("cauli:q:default")
    t.delay()
    assert _single_envelope(redis_client, "default")["idempotency_key"] is None


def test_redis_url_resolution(monkeypatch):
    monkeypatch.delenv("CAULI_REDIS_URL", raising=False)
    assert Cauli().redis_url == "redis://localhost:6379/0"

    monkeypatch.setenv("CAULI_REDIS_URL", "redis://envhost:6399/2")
    assert Cauli().redis_url == "redis://envhost:6399/2"
    assert (
        Cauli(redis_url="redis://explicit:1234/0").redis_url
        == "redis://explicit:1234/0"
    )


def test_default_redis_url_logs_a_warning(monkeypatch, caplog):
    # An unconfigured Cauli() silently resolving to localhost is exactly how
    # tasks vanish into an unrelated redis already on 6379 (a common box
    # state) with no error anywhere. The default itself is unchanged; only
    # its visibility is fixed. warning (not info) so it is seen even with no
    # logging configured at all, via logging's own last resort handler.
    monkeypatch.delenv("CAULI_REDIS_URL", raising=False)
    with caplog.at_level(logging.WARNING, logger="cauli.app"):
        app = Cauli()
    assert app.redis_url == "redis://localhost:6379/0"
    assert "redis://localhost:6379/0" in caplog.text
    assert "defaulting" in caplog.text


def test_explicit_redis_url_does_not_log_the_default_notice(caplog):
    with caplog.at_level(logging.WARNING, logger="cauli.app"):
        Cauli(redis_url="redis://explicit:1234/0")
    assert "defaulting" not in caplog.text


def test_env_redis_url_does_not_log_the_default_notice(monkeypatch, caplog):
    monkeypatch.setenv("CAULI_REDIS_URL", "redis://envhost:6399/2")
    with caplog.at_level(logging.WARNING, logger="cauli.app"):
        Cauli()
    assert "defaulting" not in caplog.text


def test_repr_redacts_credentials():
    # M4 regression: a redis URL with embedded credentials must never appear
    # in plaintext in repr() (logs/tracebacks commonly include repr output).
    app = Cauli(redis_url="redis://user:hunter2@dbhost:6379/0")
    r = repr(app)
    assert "hunter2" not in r
    assert "user:hunter2" not in r
    assert "redis://***@dbhost:6379/0" in r

    # URLs without userinfo are left alone.
    app2 = Cauli(redis_url="redis://dbhost:6379/0")
    assert "redis://dbhost:6379/0" in repr(app2)

    # M4 follow up: userinfo is split at the LAST at sign, matching
    # urllib.parse (what redis-py itself uses) -- splitting at the first one
    # used to leave the rest of a password containing "@" in plaintext.
    app3 = Cauli(redis_url="redis://user:p@ss@dbhost:6379/0")
    r3 = repr(app3)
    assert "p@ss" not in r3
    assert "ss@dbhost" not in r3
    assert "redis://***@dbhost:6379/0" in r3

    # The query parameter credential form: no "@" anywhere, so the old
    # masker returned it completely unchanged. redis-py accepts this form
    # straight as connection kwargs, and it reaches beat and every log line
    # too, so it is a live exposure, not a theoretical one.
    app4 = Cauli(redis_url="redis://dbhost:6379/0?password=s3cr3t")
    r4 = repr(app4)
    assert "s3cr3t" not in r4
    assert "redis://dbhost:6379/0?password=***" in r4

    app5 = Cauli(redis_url="redis://dbhost:6379/0?username=svc&password=s3cr3t")
    r5 = repr(app5)
    assert "svc" not in r5
    assert "s3cr3t" not in r5

    # Both forms together, so the two fixes cannot interfere with each other.
    app6 = Cauli(redis_url="redis://user:p@ss@dbhost:6379/0?password=alsosecret")
    r6 = repr(app6)
    assert "p@ss" not in r6
    assert "alsosecret" not in r6
    assert "redis://***@dbhost:6379/0?password=***" in r6


def test_get_redis_is_thread_safe(redis_url):
    # L6 regression: concurrent first-use must not race to build two
    # separate redis-py clients (one pool would leak).
    app = Cauli(redis_url=redis_url)
    seen: list[object] = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        seen.append(app._get_redis())

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(seen) == 8
    assert all(client is seen[0] for client in seen), (
        "all threads must observe the same client"
    )


def test_get_redis_sets_explicit_socket_timeout(redis_url):
    # Regression: _get_redis used to call redis.Redis.from_url with no
    # socket_timeout, so whether a stuck redis (paused, swapping, a
    # partition dropping packets) ever raised instead of hanging forever
    # depended entirely on whichever redis-py version happened to be
    # installed. Assert the client now carries an explicit value, matching
    # the Rust worker's --redis-timeout default, regardless of that
    # version's own default.
    from cauli.app import _DEFAULT_SOCKET_TIMEOUT

    app = Cauli(redis_url=redis_url)
    client = app._get_redis()
    assert (
        client.connection_pool.connection_kwargs.get("socket_timeout")
        == _DEFAULT_SOCKET_TIMEOUT
    )


def test_check_periodic_tasks_accepts_a_registered_task(redis_url):
    app = Cauli(redis_url=redis_url)

    @app.task(name="jobs.ping")
    def ping():
        return None

    app.add_periodic_task("ping_every_minute", "jobs.ping", schedule=60)
    app.check_periodic_tasks()  # must not raise


def test_check_periodic_tasks_rejects_an_unregistered_task_name(redis_url):
    app = Cauli(redis_url=redis_url)
    app.add_periodic_task("typo_job", "jobs.pign", schedule=60)  # never registered

    with pytest.raises(ValueError, match="typo_job"):
        app.check_periodic_tasks()


def test_check_periodic_tasks_allows_the_task_registered_after_the_entry(redis_url):
    # The ordering problem: add_periodic_task may legitimately be declared
    # before the task it names is decorated later in the same module. The
    # check only runs when explicitly asked (schedule start), by which point
    # every legitimate ordering inside one module has already settled.
    app = Cauli(redis_url=redis_url)
    app.add_periodic_task("late_job", "jobs.late", schedule=60)

    @app.task(name="jobs.late")
    def late():
        return None

    app.check_periodic_tasks()  # must not raise


def test_add_periodic_task_itself_does_not_validate_the_task_name(redis_url):
    # Declaration time cannot reject a typo without also rejecting the
    # legitimate "task registered later" ordering above, so it must accept
    # any string here; check_periodic_tasks is where validation belongs.
    app = Cauli(redis_url=redis_url)
    app.add_periodic_task("never_registered", "jobs.does_not_exist", schedule=60)
    assert "never_registered" in app._periodic
