"""Framework ceiling sweep: how fast can each framework go when the stores
are not the thing stopping it, and which resource actually stops it.

The question this answers is "what is the framework's own limit", so a number
here is worthless without knowing where the CPU went. Every run therefore
samples per process CPU for three groups over the SAME window the throughput
slope is taken from, and each row is labelled:

  framework-bound : worker saturated its cores, stores had headroom
  store-bound     : Postgres or Redis saturated first, the framework never
                    got to show its limit
  box-bound       : the three groups together saturated the whole box, so the
                    split is not attributable on this hardware

That last verdict is the honest and common one on a 6 vCPU machine where the
worker, Redis, Postgres and the driver all share the same cores. It is
reported rather than hidden, because a "ceiling" measured on a saturated box
is a property of the box.

Postgres is made deliberately cheap first (see prepare_pg): UNLOGGED table and
synchronous_commit=off remove WAL fsync from the commit path, which is the
dominant cost of a small INSERT and has nothing to do with the framework above
it. Both frameworks in a comparison get the identical treatment.

Usage:
    python3 ceiling.py --workload django
    python3 ceiling.py --workload fastapi
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import redis

from common import DONE_KEY, REDIS_URL

BENCH_DIR = Path(__file__).resolve().parent
CAULI = os.environ.get("CAULI_WORKER_BIN", "cauli-worker")
CELERY = os.environ.get("CELERY_BIN", "celery")
TASKIQ = os.environ.get("TASKIQ_BIN", "taskiq")
PY = os.environ.get("BENCH_PYTHON", sys.executable)
CLK = os.sysconf("SC_CLK_TCK")
CORES = os.cpu_count() or 1


# --------------------------------------------------------------------------
# CPU accounting
# --------------------------------------------------------------------------

def _proc_groups():
    """pid -> group, for every process we care about, sampled fresh each time
    because worker processes fork, respawn and recycle mid run."""
    groups = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\0", b" ").decode("utf8", "replace")
        except (OSError, ProcessLookupError):
            continue
        if not cmd:
            continue
        low = cmd.lower()
        if "redis-server" in low:
            groups[entry] = "redis"
        elif low.startswith("postgres") or "/postgres " in low:
            groups[entry] = "postgres"
        elif "cauli-worker" in low or "cauli._exec" in low:
            groups[entry] = "worker"
        elif "celery" in low and "worker" in low:
            groups[entry] = "worker"
        elif "taskiq" in low and "worker" in low:
            groups[entry] = "worker"
        elif "cauli_worker_shim" in low:
            groups[entry] = "worker"
    return groups


def _jiffies():
    """group -> cumulative jiffies. Dead pids simply drop out; a process that
    exits mid window loses its tail, which is noted in the report rather than
    corrected for."""
    out = {"redis": 0, "postgres": 0, "worker": 0}
    for pid, group in _proc_groups().items():
        try:
            with open(f"/proc/{pid}/stat") as f:
                parts = f.read().rsplit(") ", 1)[1].split()
            out[group] += int(parts[11]) + int(parts[12])
        except (OSError, IndexError, ValueError):
            continue
    return out


# --------------------------------------------------------------------------
# one measured run
# --------------------------------------------------------------------------

def prepare_pg():
    """Take WAL fsync out of the INSERT path, identically for every framework.

    Without this the lane measures Postgres's durability settings, not the
    framework: a small INSERT with synchronous_commit=on is dominated by the
    commit fsync, and every framework queues behind the same disk.
    """
    dsn = os.environ.get("BENCH_PG_DSN", "postgresql://bench:bench@127.0.0.1:5432/bench")
    sql = (
        "ALTER TABLE bench_io SET UNLOGGED; "
        "ALTER DATABASE bench SET synchronous_commit = off; "
        "TRUNCATE bench_io;"
    )
    r = subprocess.run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  ! postgres prep failed: {r.stderr.strip()[:200]}", file=sys.stderr)
        return False
    print("  postgres: bench_io UNLOGGED, synchronous_commit=off, truncated",
          file=sys.stderr)
    return True


_PURELIB = None


def bench_env(env_extra):
    """The environment run.sh builds, replicated exactly.

    cauli-worker embeds the libpython it was linked against, NOT whichever venv
    is active, so the venv's site-packages have to be on PYTHONPATH explicitly
    or the app import dies with ModuleNotFoundError before a single task runs.
    The repo's own py/ directory goes on directly rather than relying on an
    editable install's .pth, which a bare PYTHONPATH entry does not process.
    Skipping this is why the first version of this file measured cauli at zero.
    """
    global _PURELIB
    if _PURELIB is None:
        _PURELIB = subprocess.run(
            [PY, "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
            capture_output=True, text=True,
        ).stdout.strip()
    env = {**os.environ, **env_extra}
    parts = [str(BENCH_DIR.parent / "py"), _PURELIB]
    if env.get("PYTHONPATH"):
        parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(p for p in parts if p)
    return env


_WORKER_PATTERNS = ("cauli-worker", "cauli._exec", "celery", "taskiq", "dramatiq")


def _survivors():
    """Worker processes still alive that should not be.

    Celery answers SIGTERM with a "warm shutdown" that waits on prefetched
    tasks, and it can simply never finish: a first version of this file left
    celery prefork pools running for 2.4 HOURS, competing for the same cores as
    every measurement that followed. Killing the process group is not enough,
    so every run ends by checking that nothing survived and reporting it when
    something does. A silent leak here contaminates every later row.
    """
    out = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\0", b" ").decode("utf8", "replace")
        except OSError:
            continue
        low = cmd.lower()
        if any(pat in low for pat in _WORKER_PATTERNS) and "ceiling.py" not in low:
            out.append(int(entry))
    return out


def _terminate(proc):
    """SIGTERM the group, then SIGKILL it, without trusting either to work."""
    for sig in (15, 9):
        try:
            os.killpg(os.getpgid(proc.pid), sig)
        except (ProcessLookupError, PermissionError):
            break
        try:
            proc.wait(timeout=8 if sig == 15 else 5)
            return
        except subprocess.TimeoutExpired:
            continue


def measure(lane, cmd, n, timeout, env_extra):
    """Enqueue with no worker, then start it and take the drain slope and the
    CPU split over the same middle-80% window."""
    env = bench_env(env_extra)
    stale = _survivors()
    if stale:
        print(f"    ! {len(stale)} stale worker process(es) before this run; "
              f"killing before measuring", file=sys.stderr)
        for pid in stale:
            try:
                os.kill(pid, 9)
            except (ProcessLookupError, PermissionError):
                pass
        time.sleep(1.0)
    r = redis.Redis.from_url(REDIS_URL)
    r.flushall()
    r.set(DONE_KEY, 0)

    enq = subprocess.run([PY, "enqueue.py", lane, str(n)],
                         cwd=BENCH_DIR, env=env, capture_output=True, text=True)
    if enq.returncode == 0:
        # A worker left alive by a previous sweep entry would drain this queue
        # before the one under test starts, and the counter would already be at
        # n on the first poll.
        r.set(DONE_KEY, 0)
    if enq.returncode != 0:
        print(f"  ! enqueue failed: {enq.stderr.strip()[:300]}", file=sys.stderr)
        return None

    log = open(f"/tmp/ceiling-{lane}.log", "w")
    worker = subprocess.Popen(cmd, cwd=BENCH_DIR, env=env, stdout=log,
                              stderr=subprocess.STDOUT, start_new_session=True)
    samples = []
    t0 = time.perf_counter()
    try:
        while True:
            count = int(r.get(DONE_KEY) or 0)
            samples.append((time.perf_counter(), count, _jiffies()))
            if count >= n or time.perf_counter() - t0 > timeout:
                break
            time.sleep(0.05)
    finally:
        _terminate(worker)
        log.close()
        leftover = _survivors()
        if leftover:
            print(f"    ! {len(leftover)} worker process(es) survived the kill, "
                  f"force killing: {leftover[:4]}", file=sys.stderr)
            for pid in leftover:
                try:
                    os.kill(pid, 9)
                except (ProcessLookupError, PermissionError):
                    pass
            time.sleep(0.5)

    done = samples[-1][1]
    if done < 0.9 * n:
        return {"rate": None, "done": done, "n": n, "why": "did not drain"}
    if len(samples) < 3:
        # Completed, but too fast (or polled too coarsely) to take a slope from.
        # Reporting this as a failure is what hid a working celery run behind a
        # DID NOT DRAIN label.
        return {"rate": None, "done": done, "n": n, "why": "too few samples"}

    def at(target):
        for i in range(1, len(samples)):
            (tp, cp, jp), (tc, cc, jc) = samples[i - 1], samples[i]
            if cp <= target <= cc:
                return (tc, jc) if cc == cp else (
                    tp + (target - cp) / (cc - cp) * (tc - tp), jc)
        return None

    lo, hi = at(0.1 * n), at(0.9 * n)
    if lo is None or hi is None:
        return {"rate": None, "done": done, "n": n, "why": "no slope bracket"}
    (t_lo, j_lo), (t_hi, j_hi) = lo, hi
    window = t_hi - t_lo
    if window <= 0:
        return {"rate": None, "done": done, "n": n, "why": "zero window"}

    cores = {g: (j_hi[g] - j_lo[g]) / CLK / window for g in j_lo}
    return {
        "rate": 0.8 * n / window,
        "done": done,
        "n": n,
        "window_s": window,
        "cores": cores,
        "total_cores": sum(cores.values()),
    }


def verdict(res):
    """Where did the box go? Reported per row, never assumed."""
    if not res or res.get("rate") is None:
        return "DID NOT DRAIN"
    c = res["cores"]
    used = res["total_cores"]
    if used > 0.85 * CORES:
        return f"box-bound ({used:.1f}/{CORES} cores busy)"
    if c["redis"] > 0.9:
        return "redis-bound"
    if c["postgres"] > 0.85 * (CORES - c["worker"]):
        return "postgres-bound"
    return "framework-bound"


# --------------------------------------------------------------------------
# sweeps
# --------------------------------------------------------------------------

def django_sweep():
    """Django ORM insert, sync task bodies. cauli sync vs Celery prefork."""
    out = []
    for procs, threads in ((4, 16), (6, 24), (8, 32)):
        out.append((
            f"cauli sync --procs {procs} --io-threads {threads}",
            "cauli_sync_django",
            [CAULI, "-A", "tasks_cauli_sync_django:app", "--procs", str(procs),
             "--io-threads", str(threads), "--io-concurrency", str(threads),
             "--redis-url", REDIS_URL],
            {},
        ))
    for conc in (8, 16, 32):
        out.append((
            f"celery prefork -c {conc}",
            "celery_django",
            [CELERY, "-A", "tasks_celery_django", "worker", "-c", str(conc),
             "-P", "prefork", "--prefetch-multiplier=1", "--without-heartbeat",
             "--without-gossip", "--without-mingle", "-l", "warning"],
            {},
        ))
    return out


def fastapi_sweep():
    """SQLAlchemy 2.0 async ORM insert, async task bodies: the FastAPI pairing.
    cauli async vs taskiq. Pool is per process, so procs x pool is compared
    against max_connections and kept under it for both sides equally."""
    out = []
    for procs, conc in ((4, 32), (6, 40), (8, 40)):
        out.append((
            f"cauli async --procs {procs} --io-concurrency {conc}",
            "cauli_async_sqlalchemy",
            [CAULI, "-A", "tasks_cauli_async_sqlalchemy:app", "--procs", str(procs),
             "--io-concurrency", str(conc), "--redis-url", REDIS_URL],
            {"BENCH_PG_POOL_MAX": str(conc)},
        ))
    for workers, conc in ((4, 32), (6, 40), (8, 40)):
        out.append((
            f"taskiq --workers {workers} --max-async-tasks {conc}",
            "taskiq_sqlalchemy",
            [TASKIQ, "worker", "tasks_taskiq_sqlalchemy:broker",
             "--workers", str(workers), "--max-async-tasks", str(conc),
             "--max-prefetch", str(conc), "--log-level", "WARNING"],
            {"BENCH_PG_POOL_MAX": str(conc)},
        ))
    return out


def dispatch_sweep():
    """Pure framework ceiling: no database, no ORM, no web framework. The task
    body is `pass`, so everything measured is the queue runtime itself --
    fetch, decode, dispatch, execute, ack.

    Redis is still in the picture because it is the broker and cannot be
    removed, so its CPU is sampled at every point. A row where redis sits well
    under one core is the FRAMEWORK's ceiling; a row where redis approaches a
    full core is redis's, and says nothing about the framework above it.

    Each framework is swept across its own concurrency knob and reported at its
    own best point, not at a single config chosen to suit one of them. That is
    the fairness rule this repo already applies to Celery's prefetch
    multiplier, generalised.
    """
    out = []
    # cauli async: --io-concurrency is the gate; procs multiply GILs.
    for procs, conc in ((4, 96), (6, 96), (8, 96), (12, 96)):
        out.append((
            f"cauli async --procs {procs} --io-concurrency {conc}",
            "cauli_async",
            [CAULI, "-A", "tasks_cauli_async:app", "--procs", str(procs),
             "--io-concurrency", str(conc), "--redis-url", REDIS_URL],
            {},
        ))
    # cauli sync: threads under one GIL per proc.
    for procs, threads in ((6, 80), (12, 80), (16, 80)):
        out.append((
            f"cauli sync --procs {procs} --io-threads {threads}",
            "cauli_sync",
            [CAULI, "-A", "tasks_cauli_sync:app", "--procs", str(procs),
             "--io-threads", str(threads), "--io-concurrency", str(threads),
             "--redis-url", REDIS_URL],
            {},
        ))
    # taskiq: the async competitor.
    for workers, tasks in ((8, 100), (12, 100)):
        out.append((
            f"taskiq --workers {workers} --max-async-tasks {tasks}",
            "taskiq",
            [TASKIQ, "worker", "tasks_taskiq:broker", "--workers", str(workers),
             "--max-async-tasks", str(tasks), "--max-prefetch", str(tasks),
             "--log-level", "WARNING"],
            {},
        ))
    # Celery prefork, its own optimum: prefetch swept, not pinned to 1.
    for conc, prefetch in ((4, 1), (8, 4), (12, 4)):
        out.append((
            f"celery prefork -c {conc} --prefetch {prefetch}",
            "celery",
            [CELERY, "-A", "tasks_celery", "worker", "-c", str(conc),
             "-P", "prefork", f"--prefetch-multiplier={prefetch}",
             "--without-heartbeat", "--without-gossip", "--without-mingle",
             "-l", "warning"],
            {},
        ))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", choices=("django", "fastapi", "dispatch"),
                    required=True)
    ap.add_argument("--n", type=int, default=20_000)
    ap.add_argument("--timeout", type=int, default=180)
    args = ap.parse_args()

    print(f"cores={CORES}  workload={args.workload}  n={args.n}", file=sys.stderr)
    if args.workload in ("django", "fastapi"):
        if not prepare_pg():
            return 1
    else:
        print("  no database in this workload: task body is a no-op",
              file=sys.stderr)

    sweep = {
        "django": django_sweep,
        "fastapi": fastapi_sweep,
        "dispatch": dispatch_sweep,
    }[args.workload]()
    rows = []
    for label, lane, cmd, env_extra in sweep:
        print(f"\n=== {label} ===", file=sys.stderr)
        res = measure(lane, cmd, args.n, args.timeout, env_extra)
        rows.append((label, res))
        if res and res.get("rate"):
            c = res["cores"]
            print(f"  {res['rate']:.0f}/s   worker {c['worker']:.2f}  "
                  f"pg {c['postgres']:.2f}  redis {c['redis']:.2f} cores  "
                  f"-> {verdict(res)}", file=sys.stderr)
        else:
            why = (res or {}).get("why", "worker never started")
            print(f"  NO RESULT: {why} ({(res or {}).get('done', 0)}/{args.n})",
                  file=sys.stderr)
            tail = Path(f"/tmp/ceiling-{lane}.log")
            if tail.exists():
                for line in tail.read_text(errors="replace").splitlines():
                    if "ERROR" in line or "Error" in line or "error:" in line:
                        print(f"    {line[:160]}", file=sys.stderr)
                        break

    print()
    print("=" * 96)
    print(f"CEILING SWEEP  -  {args.workload}   (box: {CORES} vCPU shared by "
          f"worker, Postgres, Redis and the driver)")
    print("=" * 96)
    print(f"{'config':<46}{'tasks/s':>10}{'worker':>8}{'pg':>7}{'redis':>7}"
          f"{'  where the limit is'}")
    for label, res in rows:
        if res and res.get("rate"):
            c = res["cores"]
            print(f"{label:<46}{res['rate']:>10.0f}{c['worker']:>8.2f}"
                  f"{c['postgres']:>7.2f}{c['redis']:>7.2f}  {verdict(res)}")
        else:
            why = (res or {}).get("why", "worker never started")
            print(f"{label:<46}{'-':>10}{'-':>8}{'-':>7}{'-':>7}  {why}")
    print("\ncores columns are CPU-cores consumed during the measured window, "
          "not percentages.")
    print("Postgres was made cheap on purpose (UNLOGGED + synchronous_commit=off) "
          "so the row\nmeasures the framework rather than WAL fsync.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
