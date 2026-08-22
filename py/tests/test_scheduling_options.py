"""eta, expires, queue TTL and app-level routing at enqueue time.

PROTOCOL.md sections 9.1 (expiry), 9.2 (queue TTL) and 9.3 (routing).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from cauli import Cauli
from helpers import ENVELOPE_KEYS, now_ms

UTC = timezone.utc


def _stream(client, queue="default"):
    return [json.loads(f[b"e"]) for _sid, f in client.xrange(f"cauli:q:{queue}")]


def _delayed(client, queue="default"):
    rows = client.zrange(f"cauli:delayed:{queue}", 0, -1, withscores=True)
    return [(json.loads(raw), int(score)) for raw, score in rows]


# ------------------------------------------------------------------ eta


def test_eta_zadds_delayed_with_absolute_score(app, redis_client):
    @app.task()
    def ping():
        return None

    when = datetime.now(UTC) + timedelta(seconds=30)
    res = ping.apply_async(eta=when)

    assert redis_client.exists("cauli:q:default") == 0
    rows = _delayed(redis_client)
    assert len(rows) == 1
    env, score = rows[0]
    assert score == int(when.timestamp() * 1000)
    assert env["not_before"] == score
    assert env["id"] == res.id
    assert set(env.keys()) == ENVELOPE_KEYS


def test_eta_honours_the_datetime_own_timezone_not_the_local_one():
    # The same instant expressed in two zones must produce the same score.
    # This is the bug that "naive datetimes are UTC" configuration flags cause:
    # the offset silently disappears.
    tokyo = datetime(2030, 1, 1, 9, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
    utc = datetime(2030, 1, 1, 0, 0, tzinfo=UTC)
    app = Cauli(redis_url="redis://127.0.0.1:1/0")

    @app.task(name="t")
    def t():
        return None

    a, _q, fire_a = app.make_envelope("t", task=t, eta=tokyo)
    b, _q, fire_b = app.make_envelope("t", task=t, eta=utc)
    assert fire_a == fire_b == int(utc.timestamp() * 1000)
    assert a["not_before"] == b["not_before"]


def test_naive_eta_is_refused_rather_than_guessed(app):
    @app.task()
    def ping():
        return None

    with pytest.raises(ValueError, match="timezone-aware"):
        ping.apply_async(eta=datetime(2030, 1, 1, 12, 0))


def test_eta_in_the_past_goes_straight_to_the_stream(app, redis_client):
    @app.task()
    def ping():
        return None

    past = datetime.now(UTC) - timedelta(hours=1)
    ping.apply_async(eta=past)

    assert redis_client.exists("cauli:delayed:default") == 0
    envs = _stream(redis_client)
    assert len(envs) == 1
    # not_before still records the requested instant (it is the audit trail),
    # even though the entry was published immediately.
    assert envs[0]["not_before"] == int(past.timestamp() * 1000)


def test_eta_and_countdown_are_mutually_exclusive(app):
    @app.task()
    def ping():
        return None

    with pytest.raises(ValueError, match="not both"):
        ping.apply_async(countdown=5, eta=datetime.now(UTC))


# --------------------------------------------------------------- expires


def test_expires_seconds_sets_absolute_expires_at(app, redis_client):
    @app.task()
    def ping():
        return None

    t0 = now_ms()
    ping.apply_async(expires=90)
    t1 = now_ms()

    env = _stream(redis_client)[0]
    assert t0 + 90_000 <= env["expires_at"] <= t1 + 90_000


def test_expires_accepts_an_aware_datetime(app, redis_client):
    @app.task()
    def ping():
        return None

    when = datetime.now(UTC) + timedelta(minutes=5)
    ping.apply_async(expires=when)
    assert _stream(redis_client)[0]["expires_at"] == int(when.timestamp() * 1000)


def test_naive_expires_datetime_is_refused(app):
    @app.task()
    def ping():
        return None

    with pytest.raises(ValueError, match="timezone-aware"):
        ping.apply_async(expires=datetime(2030, 1, 1))


def test_expires_survives_the_delayed_path(app, redis_client):
    @app.task()
    def ping():
        return None

    ping.apply_async(countdown=60, expires=120)
    env, _score = _delayed(redis_client)[0]
    assert env["expires_at"] is not None
    assert env["expires_at"] > env["not_before"]


# ------------------------------------------------------------- queue TTL


def test_queue_ttl_stamps_expires_at_by_default(redis_url, redis_client):
    app = Cauli(redis_url=redis_url, queue_ttl=45)

    @app.task(name="t")
    def t():
        return None

    t0 = now_ms()
    t.delay()
    env = _stream(redis_client)[0]
    assert t0 + 45_000 <= env["expires_at"] <= now_ms() + 45_000


def test_per_queue_ttl_overrides_the_wildcard(redis_url, redis_client):
    app = Cauli(redis_url=redis_url, queue_ttl={"*": 600, "bulk": 10})

    @app.task(name="t")
    def t():
        return None

    t.apply_async(queue="bulk")
    t.apply_async(queue="other")
    now = now_ms()
    bulk = _stream(redis_client, "bulk")[0]
    other = _stream(redis_client, "other")[0]
    assert bulk["expires_at"] - now < 11_000
    assert other["expires_at"] - now > 500_000


def test_explicit_expires_beats_the_queue_ttl_client_side(redis_url, redis_client):
    # The client stamp is a DEFAULT. The worker separately enforces the queue
    # TTL as a ceiling at dispatch (PROTOCOL section 9.2), so an over-long
    # `expires` still cannot outlive the queue's configured max age.
    app = Cauli(redis_url=redis_url, queue_ttl=10)

    @app.task(name="t")
    def t():
        return None

    t.apply_async(expires=3600)
    env = _stream(redis_client)[0]
    assert env["expires_at"] - now_ms() > 3_000_000


def test_countdown_past_the_queue_ttl_is_refused_at_enqueue(redis_url, redis_client):
    # queue_ttl is measured from ENQUEUE, not from the due time (PROTOCOL
    # section 9.2), so this task would sit in the delayed zset for 10 minutes
    # and then be discarded unrun. Refused at the call site instead.
    app = Cauli(redis_url=redis_url, queue_ttl=300)

    @app.task(name="t")
    def t():
        return None

    with pytest.raises(ValueError, match="after it expires"):
        t.apply_async(countdown=600)
    assert redis_client.zcard("cauli:delayed:default") == 0
    assert redis_client.xlen("cauli:q:default") == 0


def test_eta_past_the_queue_ttl_is_refused_even_with_a_long_expires(
    redis_url, redis_client
):
    # The effective deadline is the EARLIER of `expires` and the queue TTL, so
    # a generous per-call `expires` does not rescue the task.
    app = Cauli(redis_url=redis_url, queue_ttl=60)

    @app.task(name="t")
    def t():
        return None

    later = datetime.now(timezone.utc) + timedelta(hours=2)
    with pytest.raises(ValueError, match="queue_ttl 60s"):
        t.apply_async(eta=later, expires=7200)
    assert redis_client.zcard("cauli:delayed:default") == 0


def test_countdown_past_an_explicit_expires_is_refused(redis_url, redis_client):
    app = Cauli(redis_url=redis_url)

    @app.task(name="t")
    def t():
        return None

    with pytest.raises(ValueError, match="expires=30"):
        t.apply_async(countdown=120, expires=30)
    assert redis_client.zcard("cauli:delayed:default") == 0


def test_countdown_within_the_queue_ttl_still_enqueues(redis_url, redis_client):
    app = Cauli(redis_url=redis_url, queue_ttl=600)

    @app.task(name="t")
    def t():
        return None

    t.apply_async(countdown=60)
    assert redis_client.zcard("cauli:delayed:default") == 1


def test_queue_ttl_validation():
    with pytest.raises(ValueError):
        Cauli(redis_url="redis://127.0.0.1:1/0", queue_ttl=0)
    with pytest.raises(ValueError):
        Cauli(redis_url="redis://127.0.0.1:1/0", queue_ttl={"a": -1})
    with pytest.raises(ValueError):
        Cauli(redis_url="redis://127.0.0.1:1/0", queue_ttl=True)


def test_result_ttl_and_idemp_ttl_validation():
    # result_ttl=0 (or negative) makes Redis reject `SET key val EX 0`; the
    # result key is then never written and AsyncResult.get() hangs forever.
    with pytest.raises(ValueError):
        Cauli(redis_url="redis://127.0.0.1:1/0", result_ttl=0)
    with pytest.raises(ValueError):
        Cauli(redis_url="redis://127.0.0.1:1/0", result_ttl=-1)
    with pytest.raises(ValueError):
        Cauli(redis_url="redis://127.0.0.1:1/0", idemp_ttl=0)
    with pytest.raises(ValueError):
        Cauli(redis_url="redis://127.0.0.1:1/0", idemp_ttl=-1)


# --------------------------------------------------------------- routing


def _routed_app(routes, **kw):
    return Cauli(redis_url="redis://127.0.0.1:1/0", task_routes=routes, **kw)


def test_glob_route_sends_a_task_to_another_queue():
    app = _routed_app({"myapp.email.*": "emails"})

    @app.task(name="myapp.email.send")
    def send():
        return None

    @app.task(name="myapp.other.thing")
    def thing():
        return None

    assert app.make_envelope("myapp.email.send", task=send)[1] == "emails"
    assert app.make_envelope("myapp.other.thing", task=thing)[1] == "default"


def test_routes_override_the_decorator_queue_but_not_an_explicit_call_queue():
    # The whole point of app-level routing: an operator re-routes a task
    # WITHOUT editing the code that declared `queue=`.
    app = _routed_app({"*.report": "reports"})

    @app.task(name="pkg.report", queue="hardcoded")
    def report():
        return None

    assert app.make_envelope("pkg.report", task=report)[1] == "reports"
    # ... but a per-call queue= is explicit runtime intent and still wins.
    assert app.make_envelope("pkg.report", task=report, queue="adhoc")[1] == "adhoc"


def test_first_matching_route_wins_in_declaration_order():
    app = _routed_app([("a.*", "first"), ("a.b", "second")])
    assert app.make_envelope("a.b")[1] == "first"


def test_dict_destination_and_callable_router():
    def by_arg(name, args, kwargs):
        return "big" if args and args[0] > 100 else None

    app = _routed_app([("*", by_arg), ("*", {"queue": "small"})])
    assert app.make_envelope("t", args=(500,))[1] == "big"
    # The callable returned None -> "no opinion", so the next rule applies.
    assert app.make_envelope("t", args=(1,))[1] == "small"


def test_bare_callable_route_entry():
    app = _routed_app([lambda name, a, k: "cron" if name.startswith("cron.") else None])
    assert app.make_envelope("cron.rotate")[1] == "cron"
    assert app.make_envelope("web.ping")[1] == "default"


def test_routes_apply_to_a_real_enqueue(redis_url, redis_client):
    app = Cauli(redis_url=redis_url, task_routes={"*.slow": "bulk"})

    @app.task(name="pkg.slow")
    def slow():
        return None

    slow.delay()
    assert redis_client.exists("cauli:q:default") == 0
    env = _stream(redis_client, "bulk")[0]
    assert env["queue"] == "bulk"


def test_route_to_an_invalid_queue_name_is_rejected():
    app = _routed_app({"*": "bad queue"})
    with pytest.raises(ValueError, match="invalid queue name"):
        app.make_envelope("t")


def test_malformed_routes_are_rejected_at_construction():
    with pytest.raises(ValueError):
        _routed_app([("only-one-element",)])
    with pytest.raises(ValueError):
        _routed_app([(123, "q")])


# ---------------------------------------------------- envelope parity


def test_make_envelope_without_a_taskdef_uses_protocol_defaults():
    # Beat publishes entries whose task may not be imported in this process
    # (e.g. one created through a future admin view). The envelope must still
    # be a valid section 2 envelope; the worker's registry is authoritative for
    # kind at execution time anyway.
    app = Cauli(redis_url="redis://127.0.0.1:1/0")
    env, queue, fire_at = app.make_envelope("never.imported", args=[1])
    assert set(env.keys()) == ENVELOPE_KEYS
    assert queue == "default" and fire_at is None
    assert env["kind"] == "io"
    assert env["max_retries"] == 3
    assert env["timeout_ms"] == 300000
    assert env["store_result"] is True
