"""Async SQLAlchemy integration e2e: real cauli-worker + real Postgres.

Proves:
  1. concurrent tasks each get their own AsyncSession and connection;
  2. Postgres backends stay within the pool ceiling (pool_size 5 +
     max_overflow 10 = 15) under twice that many tasks;
  3. connections return to the pool: the count settles back to 5 and every
     task in the burst completes.

Infrastructure: throwaway redis on port 6419 and an already running
Postgres, role and database "bench", on the default port (CAULI_ITEST_PG_*
override). That Postgres is shared, so measurements filter pg_stat_activity
by a per run application_name (CAULI_ITEST_APPNAME), plus one baseline of
the role's total. Skips if sqlalchemy/psycopg, the worker binary or the
bench Postgres is unavailable.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
import uuid

import pytest
import redis as redis_lib

pytest.importorskip("sqlalchemy")
psycopg = pytest.importorskip("psycopg")

REDIS_PORT = 6419
REDIS_URL = f"redis://127.0.0.1:{REDIS_PORT}/0"
HOME = os.path.expanduser("~")
BIN = os.environ.get("CAULI_WORKER_BIN", f"{HOME}/rupy-target/release/cauli-worker")
VENV = os.environ.get("CAULI_VENV", f"{HOME}/rupy-venv")
HERE = os.path.dirname(os.path.abspath(__file__))

PG_HOST = os.environ.get("CAULI_ITEST_PG_HOST", "127.0.0.1")
PG_PORT = os.environ.get("CAULI_ITEST_PG_PORT", "5432")
PG_DB = os.environ.get("CAULI_ITEST_PG_DB", "bench")
PG_USER = os.environ.get("CAULI_ITEST_PG_USER", "bench")
PG_PASSWORD = os.environ.get("CAULI_ITEST_PG_PASSWORD", "bench")
PG_DSN = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Fresh every run, so pg_stat_activity queries see only this worker.
APPNAME = f"cauli-itest-sqlalchemy-{uuid.uuid4().hex[:8]}"

pytestmark = pytest.mark.skipif(
    not os.path.exists(BIN), reason=f"cauli-worker binary not found at {BIN}"
)


def _pg_query(sql, params=None):
    with psycopg.connect(PG_DSN, autocommit=True, connect_timeout=5) as conn:
        return conn.execute(sql, params).fetchall()


def _bench_total_count() -> int:
    return _pg_query(
        "select count(*) from pg_stat_activity where usename = %s", (PG_USER,)
    )[0][0]


def _run_backend_count() -> int:
    return _pg_query(
        "select count(*) from pg_stat_activity where application_name = %s",
        (APPNAME,),
    )[0][0]


def _worker_env():
    env = dict(os.environ)
    env["VIRTUAL_ENV"] = VENV
    env["PATH"] = f"{VENV}/bin:" + env.get("PATH", "")
    env["CAULI_REDIS_URL"] = REDIS_URL
    env["CAULI_ITEST_PG_HOST"] = PG_HOST
    env["CAULI_ITEST_PG_PORT"] = PG_PORT
    env["CAULI_ITEST_PG_DB"] = PG_DB
    env["CAULI_ITEST_PG_USER"] = PG_USER
    env["CAULI_ITEST_PG_PASSWORD"] = PG_PASSWORD
    env["CAULI_ITEST_APPNAME"] = APPNAME
    return env


def _spawn_worker():
    # One process and one loop so the pool ceiling is what is under test;
    # --io-concurrency 32 is above that ceiling so the pool, not cauli's
    # admission gate, is the bottleneck.
    return subprocess.Popen(
        [
            BIN,
            "--app",
            "sqlalchemy_site:app",
            "--queues",
            "sqlalchemy",
            "--redis-url",
            REDIS_URL,
            "--procs",
            "1",
            "--io-threads",
            "4",
            "--io-loops",
            "1",
            "--io-concurrency",
            "32",
            "--cpu-workers",
            "1",
            "--visibility-timeout",
            "30",
            "--python",
            f"{VENV}/bin/python",
            "--log-level",
            "info",
        ],
        cwd=HERE,
        env=_worker_env(),
        stdout=open(f"{HERE}/worker_sqlalchemy.log", "wb"),
        stderr=subprocess.STDOUT,
    )


def _wait_group(r, queue, secs=30):
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            r.execute_command("XINFO", "GROUPS", f"cauli:q:{queue}")
            return
        except redis_lib.exceptions.ResponseError:
            time.sleep(0.1)
        except redis_lib.exceptions.ConnectionError:
            time.sleep(0.1)
    raise AssertionError(f"worker never created the consumer group on {queue}")


@pytest.fixture(scope="session")
def stack():
    """Baseline bench connection count, redis, and the real worker."""
    try:
        baseline_total = _bench_total_count()
    except Exception as exc:
        pytest.skip(f"bench Postgres not reachable at {PG_DSN}: {exc}")

    subprocess.run(
        [
            "redis-server",
            "--port",
            str(REDIS_PORT),
            "--save",
            "",
            "--appendonly",
            "no",
            "--daemonize",
            "yes",
        ],
        check=True,
    )
    r = redis_lib.Redis(port=REDIS_PORT)
    for _ in range(50):
        try:
            r.ping()
            break
        except redis_lib.exceptions.ConnectionError:
            time.sleep(0.1)
    r.flushall()

    # Before importing sqlalchemy_site, which reads CAULI_REDIS_URL.
    os.environ["CAULI_REDIS_URL"] = REDIS_URL
    import sqlalchemy_site  # noqa: F401  (registers session_probe against app)

    worker = _spawn_worker()
    _wait_group(r, "sqlalchemy")
    yield r, worker, baseline_total
    worker.send_signal(signal.SIGTERM)
    try:
        worker.wait(timeout=20)
    except subprocess.TimeoutExpired:
        worker.kill()
    subprocess.run(
        ["redis-cli", "-p", str(REDIS_PORT), "shutdown", "nosave"], check=False
    )


def _probe():
    import sqlalchemy_site

    return sqlalchemy_site.session_probe


def test_concurrent_tasks_get_distinct_sessions(stack):
    """Five concurrent tasks holding a connection show five distinct backends."""
    probe = _probe()
    n = 5
    started = time.monotonic()
    results = [probe.delay(0.5) for _ in range(n)]
    pids = [res.get(timeout=30)["pid"] for res in results]
    elapsed = time.monotonic() - started

    assert len(set(pids)) == n, f"expected {n} distinct sessions, saw {pids}"
    # Serialized would take about n * 0.5s.
    assert elapsed < n * 0.5, (
        f"{n} probes took {elapsed:.2f}s, looks serialized rather than concurrent"
    )


def test_connections_stay_bounded_and_are_returned_under_concurrency(stack):
    """30 tasks against a 15 connection pool: a leaked session would exhaust
    the pool and fail a probe (max_retries=0), and the backend count would
    not settle back down.
    """
    _, _, baseline_total = stack
    # Up to pool_size, not 0: earlier tests leave the base slots pooled.
    starting = _run_backend_count()
    assert starting <= 5, (
        f"{starting} backend(s) already open for this run before the burst "
        "started, more than the pool's base size accounts for"
    )

    n = 30
    probe = _probe()
    results = [probe.delay(0.2) for _ in range(n)]

    peak = 0
    deadline = time.monotonic() + 55
    while not all(res.status() != "pending" for res in results):
        peak = max(peak, _run_backend_count())
        if time.monotonic() > deadline:
            raise AssertionError("burst of 30 tasks never finished within 55s")
        time.sleep(0.05)
    peak = max(peak, _run_backend_count())

    pids = [res.get(timeout=60)["pid"] for res in results]
    assert len(pids) == n, "not every task in the burst completed"

    assert peak >= 5, (
        f"peak concurrent backend count was only {peak}; the burst never "
        "actually pressured the pool, so the boundedness check below "
        "proves nothing"
    )
    assert peak <= 15, (
        f"peak concurrent backend count was {peak}, above the 15-connection "
        "pool_size+max_overflow ceiling -- connections grew with task "
        "count instead of staying bounded"
    )

    settle_deadline = time.monotonic() + 10
    settled = _run_backend_count()
    while settled > 5 and time.monotonic() < settle_deadline:
        time.sleep(0.2)
        settled = _run_backend_count()
    assert settled <= 5, (
        f"{settled} backend(s) for this run still open after the burst "
        "finished -- connections accumulated instead of being returned "
        "to the pool between tasks"
    )

    final_total = _bench_total_count()
    assert final_total - baseline_total <= 5, (
        f"bench role connection count went from {baseline_total} to "
        f"{final_total} across the run, more than this worker's own pool "
        "ceiling could account for"
    )
