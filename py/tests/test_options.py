"""Test 3: task options land in TaskDef attrs AND the envelope; queue precedence; idempotency_key."""

from __future__ import annotations

import json
import threading

from rupy import Rupy


def _single_envelope(redis_client, queue):
    entries = redis_client.xrange(f"rupy:q:{queue}")
    assert len(entries) == 1, f"expected exactly one entry on rupy:q:{queue}"
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
    app2 = Rupy(redis_url=redis_url, default_queue="dq")

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
    redis_client.delete("rupy:q:default")
    t.delay()
    assert _single_envelope(redis_client, "default")["idempotency_key"] is None


def test_redis_url_resolution(monkeypatch):
    monkeypatch.delenv("RUPY_REDIS_URL", raising=False)
    assert Rupy().redis_url == "redis://localhost:6379/0"

    monkeypatch.setenv("RUPY_REDIS_URL", "redis://envhost:6399/2")
    assert Rupy().redis_url == "redis://envhost:6399/2"
    assert (
        Rupy(redis_url="redis://explicit:1234/0").redis_url == "redis://explicit:1234/0"
    )


def test_repr_redacts_credentials():
    # M4 regression: a redis URL with embedded credentials must never appear
    # in plaintext in repr() (logs/tracebacks commonly include repr output).
    app = Rupy(redis_url="redis://user:hunter2@dbhost:6379/0")
    r = repr(app)
    assert "hunter2" not in r
    assert "user:hunter2" not in r
    assert "redis://***@dbhost:6379/0" in r

    # URLs without userinfo are left alone.
    app2 = Rupy(redis_url="redis://dbhost:6379/0")
    assert "redis://dbhost:6379/0" in repr(app2)


def test_get_redis_is_thread_safe(redis_url):
    # L6 regression: concurrent first-use must not race to build two
    # separate redis-py clients (one pool would leak).
    app = Rupy(redis_url=redis_url)
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
