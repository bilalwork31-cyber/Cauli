"""Segfault blast-radius test: enqueue N long-sleeping `hold` tasks (innocent
in-flight work) plus one `segfault` task in the same batch, start the
worker, and see what survives.

Ground truth for the top-level process is subprocess.Popen.poll(), not
pgrep: a dead child becomes a zombie that still matches its old name/PID
until reaped, which silently produced a false "still alive" reading in an
earlier version of this script. poll() reflects the real exit status
(negative return = killed by that signal number).

For multi-process pools (Celery prefork), the top-level Popen is the
arbiter/master; individual ForkPoolWorker children are tracked separately
via pgrep since the top-level process is expected to survive regardless.

CLI: segfault_driver.py <lane> <n_holds> <observe_s> <child_pgrep_pattern> <worker_cmd...>
  lane: cauli_async_segfault | cauli_async_segfault_fixed | celery_segfault |
        arq_segfault | dramatiq_segfault
"""

import asyncio
import os
import signal
import subprocess
import sys
import time

import redis

from common import REDIS_URL

# lane -> (module, enqueue mechanism: delay | send | arq)
LANES = {
    "cauli_async_segfault": ("tasks_cauli_async_segfault", "delay"),
    "cauli_async_segfault_fixed": ("tasks_cauli_async_segfault_fixed", "delay"),
    "celery_segfault": ("tasks_celery_segfault", "delay"),
    "arq_segfault": ("tasks_arq_segfault", "arq"),
    "dramatiq_segfault": ("tasks_dramatiq_segfault", "send"),
}


def enqueue(module_name, mechanism, n_holds):
    import importlib

    mod = importlib.import_module(module_name)

    if mechanism == "delay":
        for _ in range(n_holds):
            mod.hold.delay()
        mod.segfault.delay()
    elif mechanism == "send":
        for _ in range(n_holds):
            mod.hold.send()
        mod.segfault.send()
    elif mechanism == "arq":
        from arq.connections import create_pool

        async def run():
            pool = await create_pool(mod.redis_settings)
            for _ in range(n_holds):
                await pool.enqueue_job("hold")
            await pool.enqueue_job("segfault")
            await pool.aclose()

        asyncio.run(run())
    else:
        raise SystemExit(f"unknown mechanism {mechanism!r}")


def child_pids(pattern):
    out = subprocess.run(["pgrep", "-x", pattern], capture_output=True, text=True)
    return set(int(p) for p in out.stdout.split())


def main():
    lane = sys.argv[1]
    n_holds = int(sys.argv[2])
    observe_s = float(sys.argv[3]) if len(sys.argv) > 3 else 8.0
    child_pattern = sys.argv[4]
    worker_cmd = sys.argv[5:]

    if lane not in LANES:
        raise SystemExit(f"unknown lane {lane!r}")
    module_name, mechanism = LANES[lane]

    r = redis.Redis.from_url(REDIS_URL)
    r.flushall()

    print(f"[enqueue] {n_holds} hold tasks + 1 segfault task", file=sys.stderr)
    enqueue(module_name, mechanism, n_holds)

    print(f"[worker] starting: {' '.join(worker_cmd)}", file=sys.stderr)
    log = open("/tmp/segfault_worker.log", "w")
    proc = subprocess.Popen(worker_cmd, stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid)
    time.sleep(3)

    children_before = child_pids(child_pattern)
    print(f"[baseline] top-level pid {proc.pid}, child pids: {sorted(children_before)}", file=sys.stderr)

    top_level_died_at = None
    children_ever_missing = set()
    children_ever_new = set()
    t0 = time.monotonic()
    t_end = t0 + observe_s
    while time.monotonic() < t_end:
        rc = proc.poll()
        if rc is not None and top_level_died_at is None:
            top_level_died_at = time.monotonic() - t0
            print(f"[event] top-level process exited at t={top_level_died_at:.2f}s, returncode={rc}", file=sys.stderr)
        cur = child_pids(child_pattern)
        children_ever_missing |= children_before - cur
        children_ever_new |= cur - children_before
        time.sleep(0.2)

    children_after = child_pids(child_pattern)

    print()
    print(f"lane: {lane}")
    print(f"top-level process (the one this script spawned) died: {top_level_died_at is not None}")
    if top_level_died_at is not None:
        print(f"  died at t={top_level_died_at:.2f}s, no auto-respawn observed by this script")
        print("  every in-flight `hold` task in this process was lost with it")
    print(f"child pool before: {sorted(children_before)}")
    print(f"child pool after:  {sorted(children_after)}")
    print(f"children that disappeared at some point: {sorted(children_ever_missing)}")
    print(f"children that appeared (respawned): {sorted(children_ever_new)}")

    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            time.sleep(1)
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass


if __name__ == "__main__":
    main()
