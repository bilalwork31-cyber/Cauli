"""Django integration e2e: real Django project + real Postgres + real worker.

Proves the four Django-facing behaviors end to end:
  1. lifecycle hooks fire in the task's own thread/process on all three
     execution paths (sync pool, asyncio loop, forked cpu child);
  2. delay_on_commit publishes on commit and never on rollback;
  3. CONN_MAX_AGE is honored: one persistent connection reused across tasks
     at CONN_MAX_AGE=600, zero connections left behind at CONN_MAX_AGE=0;
  4. a crash-restart of Postgres does not poison the worker: the FIRST
     attempt of a DB task on every execution path succeeds afterwards
     (max_retries=0, so a stale cached connection cannot hide behind a
     retry).

Infrastructure: throwaway redis on port 6395 (suite family 639x; ad-hoc
debugging should use 6460-6480 instead) and a throwaway user-owned Postgres
started via initdb/pg_ctl on port 54329 (crash-restartable without root).
Skips if django/psycopg or the postgres server binaries are unavailable.
"""

import glob
import os
import shutil
import signal
import subprocess
import tempfile
import time
import uuid

import pytest
import redis as redis_lib

pytest.importorskip("django")
pytest.importorskip("psycopg")

REDIS_PORT = 6395
REDIS_URL = f"redis://127.0.0.1:{REDIS_PORT}/0"
PG_PORT = "54329"
PG_DB = "cauli_itest"
PG_USER = "cauli"
HOME = os.path.expanduser("~")
BIN = os.environ.get("CAULI_WORKER_BIN", f"{HOME}/rupy-target/release/cauli-worker")
VENV = os.environ.get("CAULI_VENV", f"{HOME}/rupy-venv")
HERE = os.path.dirname(os.path.abspath(__file__))


def _pg_bin_dir():
    candidates = sorted(glob.glob("/usr/lib/postgresql/*/bin"), reverse=True)
    for c in candidates:
        if os.path.exists(os.path.join(c, "initdb")):
            return c
    initdb = shutil.which("initdb")
    return os.path.dirname(initdb) if initdb else None


PG_BIN = _pg_bin_dir()

pytestmark = pytest.mark.skipif(
    PG_BIN is None, reason="postgres server binaries (initdb/pg_ctl) not found"
)


class Pg:
    """Throwaway single-user Postgres: initdb + pg_ctl, crash-restartable."""

    def __init__(self, basedir):
        self.basedir = basedir
        self.datadir = os.path.join(basedir, "data")  # must not exist pre-initdb
        self.sockdir = os.path.join(basedir, "sock")

    def ctl(self, *args, check=True):
        # Always pass -l: pg_ctl's start/restart daemon otherwise inherits our
        # captured stdout/stderr pipes and capture_output blocks on EOF that
        # never comes (pg_ctl itself exits, the postmaster keeps the pipe).
        return subprocess.run(
            [
                os.path.join(PG_BIN, "pg_ctl"),
                "-D",
                self.datadir,
                "-l",
                os.path.join(self.basedir, "pg.log"),
                *args,
            ],
            check=check,
            capture_output=True,
            text=True,
            timeout=90,
        )

    def init_and_start(self):
        os.makedirs(self.sockdir, exist_ok=True)
        subprocess.run(
            [
                os.path.join(PG_BIN, "initdb"),
                "-D",
                self.datadir,
                "-U",
                PG_USER,
                "-A",
                "trust",
                "--no-sync",
            ],
            check=True,
            capture_output=True,
        )
        self.ctl(
            "-w",
            "-o",
            f"-p {PG_PORT} -c listen_addresses=127.0.0.1 "
            f"-c unix_socket_directories={self.sockdir}",
            "start",
        )
        subprocess.run(
            [
                os.path.join(PG_BIN, "createdb"),
                "-h",
                "127.0.0.1",
                "-p",
                PG_PORT,
                "-U",
                PG_USER,
                PG_DB,
            ],
            check=True,
            capture_output=True,
        )

    def restart_immediate(self):
        """Crash-style restart: kills every backend (client sockets go stale)."""
        self.ctl("-w", "-m", "immediate", "restart")

    def stop(self):
        self.ctl("-m", "immediate", "stop", check=False)

    def sql(self, query):
        out = subprocess.run(
            [
                os.path.join(PG_BIN, "psql"),
                "-h",
                "127.0.0.1",
                "-p",
                PG_PORT,
                "-U",
                PG_USER,
                "-d",
                PG_DB,
                "-tAc",
                query,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return out.stdout.strip()


def _worker_env(appname, conn_max_age, hooklog):
    env = dict(os.environ)
    env["VIRTUAL_ENV"] = VENV
    env["PATH"] = f"{VENV}/bin:" + env.get("PATH", "")
    env["CAULI_REDIS_URL"] = REDIS_URL
    env["CAULI_ITEST_PG_PORT"] = PG_PORT
    env["CAULI_ITEST_APPNAME"] = appname
    env["CAULI_ITEST_CONN_MAX_AGE"] = str(conn_max_age)
    env["CAULI_ITEST_HOOKLOG"] = hooklog
    return env


def _spawn_worker(queues, appname, conn_max_age, hooklog, logfile):
    # --io-threads 1 / --cpu-workers 1: every sync task reuses ONE pool thread
    # and every cpu task ONE child, so a cached stale connection CANNOT be
    # dodged by landing on a fresh thread/child — the lifecycle hooks are the
    # only thing standing between a DB restart and a task failure.
    return subprocess.Popen(
        [
            BIN,
            "--app",
            "django_site.cauli:app",
            "--queues",
            queues,
            "--redis-url",
            REDIS_URL,
            "--io-threads",
            "1",
            "--io-loops",
            "1",
            "--cpu-workers",
            "1",
            "--io-concurrency",
            "16",
            "--visibility-timeout",
            "30",
            "--python",
            f"{VENV}/bin/python",
            "--log-level",
            "info",
        ],
        cwd=HERE,
        env=_worker_env(appname, conn_max_age, hooklog),
        stdout=open(logfile, "wb"),
        stderr=subprocess.STDOUT,
    )


def _stop_worker(worker):
    worker.send_signal(signal.SIGTERM)
    try:
        worker.wait(timeout=20)
    except subprocess.TimeoutExpired:
        worker.kill()


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
def pg():
    datadir = tempfile.mkdtemp(prefix="cauli-itest-pg-")
    server = Pg(datadir)
    server.init_and_start()
    yield server
    server.stop()
    shutil.rmtree(datadir, ignore_errors=True)


@pytest.fixture(scope="session")
def stack(pg):
    """redis + migrated schema + the main worker (CONN_MAX_AGE=600)."""
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

    # Client-side env BEFORE importing the django project: the test process
    # is the "web app" here (it enqueues, and runs delay_on_commit inside
    # real transactions).
    os.environ["CAULI_REDIS_URL"] = REDIS_URL
    os.environ["CAULI_ITEST_PG_PORT"] = PG_PORT
    os.environ["CAULI_ITEST_APPNAME"] = "cauli-itest-client"
    import django_site.cauli  # noqa: F401  (django.setup + task registration)

    from django.core.management import call_command

    call_command("migrate", run_syncdb=True, verbosity=0)

    hooklog = os.path.join(tempfile.gettempdir(), f"cauli-hooklog-{uuid.uuid4().hex}")
    worker = _spawn_worker(
        "django", "cauli-itest-worker", 600, hooklog, f"{HERE}/worker_django.log"
    )
    _wait_group(r, "django")
    yield r, worker, hooklog
    _stop_worker(worker)
    subprocess.run(
        ["redis-cli", "-p", str(REDIS_PORT), "shutdown", "nosave"], check=False
    )
    if os.path.exists(hooklog):
        os.unlink(hooklog)


def _tasks():
    from django_site.dapp import tasks

    return tasks


def _read_marks(hooklog):
    marks = set()
    if os.path.exists(hooklog):
        with open(hooklog) as f:
            for line in f:
                phase, pid, tid = line.split()
                marks.add((phase, int(pid), int(tid)))
    return marks


def test_hooks_fire_on_all_three_paths(stack):
    _, worker, hooklog = stack
    t = _tasks()
    results = {
        "sync": t.probe.delay("s").get(timeout=30),
        "async": t.aprobe.delay("a").get(timeout=30),
        "cpu": t.cpu_probe.delay("c").get(timeout=30),
    }
    time.sleep(0.5)  # let the trailing after-hook writes land
    marks = _read_marks(hooklog)
    contexts = set()
    for path, res in results.items():
        ctx = (res["pid"], res["tid"])
        contexts.add(ctx)
        assert ("before", *ctx) in marks, f"before hook missed the {path} path"
        assert ("after", *ctx) in marks, f"after hook missed the {path} path"
    # Three genuinely distinct execution contexts, not one context observed
    # three times: sync pool thread + loop thread (worker pid), cpu child.
    assert len(contexts) == 3
    assert results["sync"]["pid"] == worker.pid
    assert results["cpu"]["pid"] != worker.pid, "cpu probe must run in a child"


def test_delay_on_commit_publishes_only_on_commit(stack, pg):
    from django.db import transaction

    r, _, _ = stack
    t = _tasks()

    marker = f"on-commit-{uuid.uuid4().hex}"
    qlen0 = r.xlen("cauli:q:django")
    with transaction.atomic():
        assert t.db_add.delay_on_commit(marker) is None
        assert r.xlen("cauli:q:django") == qlen0, "published before commit"
    deadline = time.time() + 30
    while time.time() < deadline:
        if pg.sql(f"SELECT count(*) FROM dapp_item WHERE name='{marker}'") == "1":
            break
        time.sleep(0.2)
    else:
        raise AssertionError("committed delay_on_commit task never executed")

    rolled_back = f"rollback-{uuid.uuid4().hex}"

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        with transaction.atomic():
            t.db_add.delay_on_commit(rolled_back)
            raise Boom()
    time.sleep(2)  # would have been consumed by now if it had been published
    assert pg.sql(f"SELECT count(*) FROM dapp_item WHERE name='{rolled_back}'") == "0"
    assert r.xlen("cauli:q:django") == qlen0


def test_conn_max_age_600_reuses_one_connection(stack, pg):
    t = _tasks()
    for i in range(3):
        res = t.db_add.delay(f"persist-{i}").get(timeout=30)
        assert res["count"] == 1
    n = int(
        pg.sql(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE application_name='cauli-itest-worker'"
        )
    )
    # One sync pool thread, CONN_MAX_AGE=600: exactly one persistent
    # connection serves all three tasks. 0 would mean connections are being
    # torn down despite CONN_MAX_AGE; >1 would mean they leak per task.
    assert n == 1, f"expected exactly 1 persistent worker connection, saw {n}"


def test_conn_max_age_0_closes_after_every_task(stack, pg):
    r, _, _ = stack
    hooklog = os.path.join(tempfile.gettempdir(), f"cauli-hooklog-{uuid.uuid4().hex}")
    w = _spawn_worker(
        "cma0", "cauli-itest-cma0", 0, hooklog, f"{HERE}/worker_django_cma0.log"
    )
    try:
        _wait_group(r, "cma0")
        t = _tasks()
        for i in range(2):
            res = t.db_add.apply_async(args=(f"cma0-{i}",), queue="cma0")
            assert res.get(timeout=30)["count"] == 1
        deadline = time.time() + 10
        while time.time() < deadline:
            n = int(
                pg.sql(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE application_name='cauli-itest-cma0'"
                )
            )
            if n == 0:
                break
            time.sleep(0.2)
        assert n == 0, (
            f"CONN_MAX_AGE=0 worker still holds {n} connection(s) after its "
            "tasks finished; the after-task hook is not closing expired "
            "connections"
        )
    finally:
        _stop_worker(w)
        if os.path.exists(hooklog):
            os.unlink(hooklog)


def test_db_restart_survival_all_three_paths(stack, pg):
    """THE defect this integration exists to fix: cached thread-local
    connections going stale across a database restart. Every path first
    caches a live connection, Postgres crash-restarts, and then the FIRST
    attempt of each follow-up task must succeed (max_retries=0 — a retry
    cannot mask a stale-connection failure)."""
    t = _tasks()
    suffix = uuid.uuid4().hex[:8]
    assert t.db_add.delay(f"pre-s-{suffix}").get(timeout=30)["count"] == 1
    assert t.adb_add.delay(f"pre-a-{suffix}").get(timeout=30)["count"] == 1
    assert t.cpu_db_add.delay(f"pre-c-{suffix}").get(timeout=30)["count"] == 1

    pg.restart_immediate()

    # Without close_old_connections (+ CONN_HEALTH_CHECKS) before each task,
    # each of these raises OperationalError/InterfaceError on the dead socket
    # cached by its (sole) pool thread / executor thread / cpu child.
    assert t.db_add.delay(f"post-s-{suffix}").get(timeout=30)["count"] == 1
    assert t.adb_add.delay(f"post-a-{suffix}").get(timeout=30)["count"] == 1
    assert t.cpu_db_add.delay(f"post-c-{suffix}").get(timeout=30)["count"] == 1
