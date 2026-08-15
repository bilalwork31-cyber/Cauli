"""Chaos test: enqueue N uniquely-tagged tasks, let the worker get partway
through, kill -9 it (no graceful drain -- a real crash, not a clean stop),
start a fresh worker, and see what comes out the other side. Reports lost
tags (never executed -- data loss), duplicate tags (executed more than
once -- expected/acceptable under at-least-once, but must be counted, not
hidden), and total recovery time (kill to fully drained).

CLI: chaos_driver.py <lane> <N> <kill_at_fraction> <recovery_timeout_s>
  lane: cauli_sync_chaos | celery_chaos_acks_late | celery_chaos_default |
        arq_chaos | dramatiq_chaos
"""

import asyncio
import subprocess
import sys
import time
from collections import Counter

import redis

from common import REDIS_URL

EXEC_KEY = "bench:chaos:executions"

# lane -> (module, attr, enqueue mechanism: delay | send | arq)
LANES = {
    "cauli_sync_chaos": ("tasks_cauli_sync_chaos", "chaos", "delay"),
    "celery_chaos_acks_late": ("tasks_celery_chaos_acks_late", "chaos", "delay"),
    "celery_chaos_default": ("tasks_celery_chaos_default", "chaos", "delay"),
    "arq_chaos": ("tasks_arq_chaos", "chaos", "arq"),
    "dramatiq_chaos": ("tasks_dramatiq_chaos", "chaos", "send"),
}


def enqueue_all(module_name, attr_name, mechanism, n):
    import importlib

    mod = importlib.import_module(module_name)

    if mechanism == "delay":
        fn = getattr(mod, attr_name)
        for i in range(n):
            fn.delay(f"tag-{i}")
    elif mechanism == "send":
        fn = getattr(mod, attr_name)
        for i in range(n):
            fn.send(f"tag-{i}")
    elif mechanism == "arq":
        from arq.connections import create_pool

        async def run():
            pool = await create_pool(mod.redis_settings)
            for i in range(n):
                await pool.enqueue_job(attr_name, f"tag-{i}")
            await pool.aclose()

        asyncio.run(run())
    else:
        raise SystemExit(f"unknown mechanism {mechanism!r}")


def start_worker(lane, worker_cmd, log_path):
    proc = subprocess.Popen(
        worker_cmd,
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        preexec_fn=__import__("os").setsid,
    )
    return proc


def kill_worker(proc, sig):
    import os
    import signal

    try:
        os.killpg(os.getpgid(proc.pid), sig)
    except ProcessLookupError:
        pass


def main():
    lane = sys.argv[1]
    n = int(sys.argv[2])
    kill_at_fraction = float(sys.argv[3]) if len(sys.argv) > 3 else 0.3
    recovery_timeout_s = float(sys.argv[4]) if len(sys.argv) > 4 else 60.0
    worker_cmd = sys.argv[5:]

    if lane not in LANES:
        raise SystemExit(f"unknown lane {lane!r}, expected one of {list(LANES)}")
    module_name, attr_name, mechanism = LANES[lane]

    r = redis.Redis.from_url(REDIS_URL)
    r.flushall()

    print(f"[enqueue] {n} tagged tasks for {lane}", file=sys.stderr)
    enqueue_all(module_name, attr_name, mechanism, n)

    print(f"[worker] starting: {' '.join(worker_cmd)}", file=sys.stderr)
    proc = start_worker(lane, worker_cmd, "/tmp/chaos_worker.log")

    target = int(n * kill_at_fraction)
    t_start = time.monotonic()
    while r.llen(EXEC_KEY) < target:
        if time.monotonic() - t_start > 30:
            print("worker never reached kill threshold in 30s", file=sys.stderr)
            break
        time.sleep(0.05)

    executed_before_kill = r.llen(EXEC_KEY)
    print(f"[kill] SIGKILL at {executed_before_kill}/{n} executed", file=sys.stderr)
    import signal

    t_kill = time.monotonic()
    kill_worker(proc, signal.SIGKILL)
    proc.wait(timeout=5)

    print(f"[worker] restarting fresh: {' '.join(worker_cmd)}", file=sys.stderr)
    proc2 = start_worker(lane, worker_cmd, "/tmp/chaos_worker2.log")

    while True:
        count = r.llen(EXEC_KEY)
        # A tag can appear more than once (duplicates); "recovered" means
        # every distinct tag has appeared at least once, not just N pushes.
        seen = {v.decode() for v in r.lrange(EXEC_KEY, 0, -1)}
        if len(seen) >= n:
            break
        if time.monotonic() - t_kill > recovery_timeout_s:
            print(
                f"[timeout] recovery_timeout_s={recovery_timeout_s} exceeded, "
                f"{len(seen)}/{n} distinct tags seen",
                file=sys.stderr,
            )
            break
        time.sleep(0.05)

    t_recovered = time.monotonic()
    recovery_s = t_recovered - t_kill

    kill_worker(proc2, signal.SIGTERM)
    try:
        proc2.wait(timeout=10)
    except subprocess.TimeoutExpired:
        kill_worker(proc2, signal.SIGKILL)

    raw = r.lrange(EXEC_KEY, 0, -1)
    tags = [v.decode() for v in raw]
    counts = Counter(tags)
    all_tags = {f"tag-{i}" for i in range(n)}
    seen_tags = set(counts)
    lost = all_tags - seen_tags
    duplicated = {t: c for t, c in counts.items() if c > 1}

    print()
    print(f"lane: {lane}")
    print(f"N: {n}, executed before kill: {executed_before_kill}")
    print(f"recovery time (kill to fully drained or timeout): {recovery_s:.2f}s")
    print(f"lost tags (never executed): {len(lost)}")
    print(f"duplicate tags (executed >1x): {len(duplicated)}")
    print(f"total executions recorded: {len(tags)} (vs N={n})")
    if lost:
        print(f"  sample lost: {sorted(lost)[:5]}")
    if duplicated:
        sample = list(duplicated.items())[:5]
        print(f"  sample duplicates: {sample}")


if __name__ == "__main__":
    main()
