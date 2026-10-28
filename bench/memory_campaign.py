"""Memory per unit of concurrency: N sleeping tasks in flight, PSS summed over
the worker's whole process group (Celery prefork forks, cauli spawns, so PSS,
not RSS, is the fair sum; see memory_report.py).

Each run: flush Redis, start the worker in its own session, enqueue N hold
tasks, wait until all N have counted themselves started, settle, then read
PSS from /proc/<pid>/smaps_rollup for every process in the session.

Run from bench/:  python3 memory_campaign.py --sizes 100,1000,10000
"""

import argparse
import os
import signal
import subprocess
import sys
import sysconfig
import time
from pathlib import Path

import redis

from common import REDIS_URL, STARTED_KEY

BENCH_DIR = Path(__file__).resolve().parent
CAULI_WORKER = os.environ.get("CAULI_WORKER_BIN", "cauli-worker")
CELERY_BIN = os.environ.get("CELERY_BIN", "celery")
TASKIQ_BIN = os.environ.get("TASKIQ_BIN", "taskiq")
START_TIMEOUT_S = 180
SETTLE_S = 3


def configs(n: int) -> list[tuple[str, list[str], str]]:
    """(label, worker command, enqueue lane) per framework at concurrency n."""
    rows = [
        ("cauli async, 1 process", [CAULI_WORKER, "-A", "tasks_cauli_async_hold:app", "--procs", "1",
                                     "--io-concurrency", str(n), "--redis-url", REDIS_URL], "cauli"),
        ("cauli async, -c (auto processes)", [CAULI_WORKER, "-A", "tasks_cauli_async_hold:app", "-c", str(n),
                                              "--redis-url", REDIS_URL], "cauli"),
        ("celery gevent", [CELERY_BIN, "-A", "tasks_celery_hold", "worker", "-P", "gevent", "-c", str(n),
                           "--without-heartbeat", "--without-gossip", "--without-mingle", "-l", "warning"], "celery"),
        ("taskiq, 1 worker", [TASKIQ_BIN, "worker", "tasks_taskiq_hold:broker", "--workers", "1",
                              "--max-async-tasks", str(n), "--log-level", "WARNING"], "taskiq"),
    ]  # fmt: skip
    if n <= 100:
        rows.append(("celery prefork", [CELERY_BIN, "-A", "tasks_celery_hold", "worker", "-P", "prefork",
                                         "-c", str(n), "--without-heartbeat", "--without-gossip",
                                         "--without-mingle", "-l", "warning"], "celery"))  # fmt: skip
    return rows


def env() -> dict[str, str]:
    site = sysconfig.get_path("purelib")
    return dict(os.environ, PYTHONPATH=os.pathsep.join([str(BENCH_DIR), str(BENCH_DIR.parent / "py"), site]))


def enqueue(lane: str, n: int) -> None:
    code = {
        "cauli": "import tasks_cauli_async_hold as t; t.app.enqueue_many([t.hold] * {n})",
        "celery": "import tasks_celery_hold as t\nfor _ in range({n}): t.hold.delay()",
        "taskiq": (
            "import asyncio, tasks_taskiq_hold as t\n"
            "async def main():\n    await t.broker.startup()\n"
            "    await asyncio.gather(*(t.hold.kiq() for _ in range({n})))\n"
            "    await t.broker.shutdown()\nasyncio.run(main())"
        ),
    }[lane].format(n=n)
    subprocess.run([sys.executable, "-c", code], cwd=BENCH_DIR, env=env(), check=True)


def session_pss_mib(sid: int) -> tuple[float, int]:
    total_kb, procs = 0, 0
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            if os.getsid(int(pid)) != sid:
                continue
            with open(f"/proc/{pid}/smaps_rollup") as f:
                total_kb += next(int(line.split()[1]) for line in f if line.startswith("Pss:"))
            procs += 1
        except (OSError, StopIteration):
            continue
    return total_kb / 1024, procs


def measure(label: str, cmd: list[str], lane: str, n: int) -> str:
    r = redis.Redis.from_url(REDIS_URL)
    r.flushall()
    proc = subprocess.Popen(
        cmd, cwd=BENCH_DIR, env=env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )  # fmt: skip
    try:
        enqueue(lane, n)
        deadline = time.monotonic() + START_TIMEOUT_S
        while int(r.get(STARTED_KEY) or 0) < n:
            if time.monotonic() > deadline or proc.poll() is not None:
                return f"| {label} | {n:,} | did not reach {n} in flight ({int(r.get(STARTED_KEY) or 0)} started) | | |"
            time.sleep(0.25)
        time.sleep(SETTLE_S)
        mib, procs = session_pss_mib(proc.pid)
        return f"| {label} | {n:,} | {mib:,.1f} | {procs} | {mib * 1024 / n:,.1f} |"
    finally:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", default="100,1000,10000")
    args = parser.parse_args()
    print("| worker | in flight | PSS MiB | processes | KiB per task |")
    print("|---|---:|---:|---:|---:|")
    for n in (int(s) for s in args.sizes.split(",")):
        for label, cmd, lane in configs(n):
            print(measure(label, cmd, lane, n), flush=True)


if __name__ == "__main__":
    main()
