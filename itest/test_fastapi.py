"""FastAPI / SQLAlchemy integration e2e: real cauli-worker + real Postgres.

Proves, end to end, the three claims cauli.contrib.fastapi's module
docstring makes about session and connection lifecycle, the ones its own
test docstring (py/tests/test_contrib_fastapi.py) admits were only ever
checked by a manual run against pg_stat_activity, not a committed test:
  1. concurrent tasks each get their own AsyncSession/connection, never one
     shared between two tasks in flight at once;
  2. the number of simultaneous Postgres backends stays bounded by
     SQLAlchemy's pool ceiling (pool_size 5 + max_overflow 10 = 15, the
     stock defaults, left untouched here) under a task count well past that
     ceiling, rather than growing with the task count;
  3. connections are returned to the pool between tasks instead of
     accumulating: the backend count comes back down to pool_size (5) once
     a burst finishes, and every task in the burst still completes even
     though the burst is twice the pool's ceiling.

Infrastructure: throwaway redis on port 6411 (639x/64xx families are in use
by concurrent audit work) and the audit's own already-running Postgres,
role and database both "bench", on the default port (CAULI_ITEST_PG_* env
vars override host/port/db/user/password). That Postgres is shared with
other work on this box, so every measurement below filters
pg_stat_activity by a unique application_name generated fresh for this run
and threaded into the worker via CAULI_ITEST_APPNAME, rather than trusting
an absolute count; a baseline of the bench role's total connection count is
also taken before the worker starts, for the one assertion that looks at
the role's total instead of just this run's tag.
Skips if sqlalchemy/psycopg or the cauli-worker binary or a reachable
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

REDIS_PORT = 6411
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

# Fresh every run: the one thing that lets every pg_stat_activity query
# below tell this worker's own connections apart from other bench-role
# connections already on this box (or opened by other work while this test
# runs), without needing an absolute count anywhere except the one baseline
# assertion, which subtracts what was already there before the worker
# started.
APPNAME = f"cauli-itest-fastapi-{uuid.uuid4().hex[:8]}"

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
    # --procs 1: one process, one engine, one pool -- the connection-count
    # arithmetic in the fastapi.py module docstring stays procs * ceiling
    # with procs pinned at 1, so the ceiling itself is what's under test.
    # --io-loops 1: the module's own hazard, pins the pool to one loop.
    # --io-concurrency 32 is well above the 15 connection pool ceiling on
    # purpose, so cauli's own admission gate is never the bottleneck being
    # measured -- the SQLAlchemy pool is.
    return subprocess.Popen(
        [
            BIN,
            "--app",
            "fastapi_site:app",
            "--queues",
            "fastapi",
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
        stdout=open(f"{HERE}/worker_fastapi.log", "wb"),
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

    # Client-side env BEFORE importing fastapi_site: the test process is the
    # "web app" here, it only enqueues (delay() talks to redis, never to
    # Postgres), but it still needs CAULI_REDIS_URL set for that.
    os.environ["CAULI_REDIS_URL"] = REDIS_URL
    import fastapi_site  # noqa: F401  (registers session_probe against app)

    worker = _spawn_worker()
    _wait_group(r, "fastapi")
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
    import fastapi_site

    return fastapi_site.session_probe


def test_concurrent_tasks_get_distinct_sessions(stack):
    """Five tasks in flight at once, each holding its connection for half a
    second, must show five distinct Postgres backends: proof, at the real
    worker and real Postgres level, of the ContextVar isolation
    py/tests/test_contrib_fastapi.py already proves at the hook level."""
    probe = _probe()
    n = 5
    started = time.monotonic()
    results = [probe.delay(0.5) for _ in range(n)]
    pids = [res.get(timeout=30)["pid"] for res in results]
    elapsed = time.monotonic() - started

    assert len(set(pids)) == n, f"expected {n} distinct sessions, saw {pids}"
    # Serialized (one connection reused/waited-on rather than five running
    # at once) would take roughly n * 0.5s; concurrent takes roughly one
    # hold period plus scheduling slack.
    assert elapsed < n * 0.5, (
        f"{n} probes took {elapsed:.2f}s, looks serialized rather than concurrent"
    )


def test_connections_stay_bounded_and_are_returned_under_concurrency(stack):
    """The property this module exists for. pool_size=5 and max_overflow=10
    are SQLAlchemy's stock defaults, left untouched here on purpose: 30
    tasks is twice the 15-connection ceiling they imply, so if a session
    ever leaked instead of returning to the pool, the connections behind it
    would never free up, later probes' session.execute() would block on an
    exhausted pool and then raise (max_retries=0 in fastapi_site.py turns
    that into a failed .get() instead of a retry quietly hiding it), and
    the backend count would keep climbing instead of settling back down.
    That is what makes every assertion below able to fail for real, not
    just on "no exception raised".
    """
    _, _, baseline_total = stack
    # At most pool_size (5) here, not 0: an earlier test in this session may
    # have already filled the pool's base slots, and those stay pooled
    # (open, idle) rather than closing between tests. That is expected
    # steady state, not a leak; anything above 5 would not be.
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
