"""Drain-rate driver: measures WORKER capacity, isolated from client speed.

Why this exists (and why runner.sh's driver.py cannot answer this question):

driver.py enqueues while the worker is already consuming, then reports
``exec_tps = N / (t_done - enqueue_end)``. That is a valid number only while
the worker is the bottleneck. Once the worker drains faster than the client
can enqueue -- which is exactly what happens for small cpu tasks -- most tasks
are already finished by the time ``enqueue_end`` is reached, the execution
window collapses toward zero, and exec_tps reports a rate ABOVE the machine's
physical roofline. Measured on this box: 12,892 tps for 0.5 ms tasks on 6
cores, where 6 cores / 0.5 ms caps out at 12,000. The metric had stopped
measuring the worker and started measuring the driver.

This driver separates the two phases completely:

  phase 1 (enqueue): fill the queue with N tasks, NO worker running.
  phase 2 (drain):   start the worker, then sample completion count over time.

Reported rate is the slope over the middle 80% of the drain curve, so worker
startup, the ramp as children warm, and the tail as the queue empties are all
excluded -- what is left is steady-state capacity. Total wall drain time is
reported alongside it, since the trimmed slope is the optimistic reading and
both belong in the record.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import redis as redis_lib


def redis_client(port: int, db: int) -> redis_lib.Redis:
    return redis_lib.Redis(host="127.0.0.1", port=port, db=db)


def get_task(stack: str, task: str):
    if stack == "celery":
        import tasks_celery as mod

        # Celery has no async lane; its io arm is the sync task.
        return {"cpu": mod.cpu_task, "io": mod.io_task, "io_async": mod.io_task}[task]
    import tasks_cauli as mod

    return {
        "cpu": mod.cpu_task,
        "io": mod.io_task,
        "io_async": mod.io_task_async,
    }[task]


def phase_enqueue(args) -> int:
    task = get_task(args.stack, args.task)
    t0 = time.time()
    for _ in range(args.n):
        task.delay()
    dt = time.time() - t0
    print(
        f"[drain] enqueued {args.n} in {dt:.2f}s ({args.n / max(dt, 1e-9):.0f}/s)",
        file=sys.stderr,
    )
    return 0


def phase_drain(args) -> int:
    # Completion counter: both stacks write exactly one result key per finished
    # task, so the delta in key count is the completion count. Celery uses a
    # dedicated backend db (clean baseline); cauli shares db 0 with its stream,
    # whose key count is constant while entries drain, so the delta is still
    # the number of results.
    backend_db = 1 if args.stack == "celery" else 0
    r = redis_client(args.redis_port, backend_db)
    baseline = r.dbsize()

    samples: list[tuple[float, int]] = []
    t0 = time.time()
    deadline = t0 + args.timeout
    done = 0
    while time.time() < deadline:
        done = max(0, r.dbsize() - baseline)
        samples.append((time.time() - t0, done))
        if done >= args.n:
            break
        time.sleep(args.interval)

    wall = time.time() - t0
    rate_trimmed, window = steady_state_rate(samples, args.n)
    out = {
        "scenario": args.scenario,
        "stack": args.stack,
        "task": args.task,
        "n": args.n,
        "done": done,
        "cpu_iter": int(os.environ.get("BENCH_CPU_ITER", "94000")),
        "drain_wall_s": round(wall, 3),
        "drain_tps_wall": round(done / max(wall, 1e-9), 2),
        "drain_tps_steady": round(rate_trimmed, 2),
        "steady_window_s": round(window, 3),
        "samples": len(samples),
        "complete": done >= args.n,
    }
    if args.out:
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2)
    print(
        f"[drain] {args.scenario} stack={args.stack} n={args.n} done={done} "
        f"complete={out['complete']} drain_tps_steady={out['drain_tps_steady']} "
        f"drain_tps_wall={out['drain_tps_wall']} wall={out['drain_wall_s']}s"
    )
    return 0 if done >= args.n else 1


def steady_state_rate(samples: list[tuple[float, int]], n: int) -> tuple[float, float]:
    """Slope over the middle 80% of the completion curve.

    Trims the first and last 10% of COMPLETIONS (not of time): the head is
    worker startup plus the ramp while children warm up, and the tail is the
    queue running dry with idle workers. Both would drag a naive
    total/elapsed figure below steady-state capacity.
    """
    if len(samples) < 4 or n <= 0:
        return 0.0, 0.0
    lo_target, hi_target = 0.1 * n, 0.9 * n
    lo = next(((t, c) for t, c in samples if c >= lo_target), None)
    hi = next(((t, c) for t, c in reversed(samples) if c <= hi_target), None)
    if lo is None or hi is None:
        return 0.0, 0.0
    dt, dc = hi[0] - lo[0], hi[1] - lo[1]
    if dt <= 0 or dc <= 0:
        return 0.0, 0.0
    return dc / dt, dt


def main() -> int:
    ap = argparse.ArgumentParser(description="drain-rate driver (worker capacity)")
    ap.add_argument("--stack", required=True, choices=["celery", "cauli"])
    ap.add_argument("--phase", required=True, choices=["enqueue", "drain"])
    ap.add_argument("--task", default="cpu", choices=["cpu", "io", "io_async"])
    ap.add_argument("--n", type=int, default=8000)
    ap.add_argument("--scenario", default="drain")
    ap.add_argument(
        "--redis-port", type=int, default=int(os.environ.get("BENCH_REDIS_PORT", "6390"))
    )
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=0.1)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    return phase_enqueue(args) if args.phase == "enqueue" else phase_drain(args)


if __name__ == "__main__":
    raise SystemExit(main())
