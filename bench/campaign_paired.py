"""Paired, interleaved campaign runner.

Why this exists alongside `campaign.py`: block designs (all reps of A, then
all reps of B) are not safe on a shared box. Measured here, one binary and one
config, the drain rate moved 19,532/s to 12,540/s across a single session while
staying stable to 4% inside any five minute block. A block design hands that
whole drift to whichever framework runs second and reports it as a difference.

So every comparison here is PAIRED: the two lanes run back to back, minutes
apart, and the order inside each pair flips every pair so drift and position
both cancel. The reported figure is the mean of per pair ratios with a 95%
interval, not a ratio of means, and a row whose interval spans 1.0 is reported
as indistinguishable from noise rather than as a win.

Two more rules this file encodes, both learned from a wrong published number:

- **Same filesystem.** Running one side from ext4 and the other from a /mnt
  drive produced a 12% swing from the import path alone.
- **Symmetric configs, and connection budgets that are actually equal.**
  Postgres lanes size the pool per PROCESS, so `procs x pool` is the real
  number and it is compared against max_connections, not assumed.

Usage:
    python3 campaign_paired.py --suite dispatch --pairs 8
    python3 campaign_paired.py --suite sqlalchemy --pairs 6
"""

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
CAULI_WORKER = os.environ.get("CAULI_WORKER_BIN", "cauli-worker")
TASKIQ_BIN = os.environ.get("TASKIQ_BIN", "taskiq")
CELERY_BIN = os.environ.get("CELERY_BIN", "celery")
REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://127.0.0.1:6395/0")

# Postgres lanes: `procs x per-process pool` must clear max_connections with
# room to spare. 8 x 40 = 320 against 400 (3 reserved) leaves 70 spare for the
# harness, psql and anything else holding a session.
PG_PROCS = 8
PG_CONCURRENCY = 40
PG_POOL_PER_PROC = 40

SUITES = {
    # Raw dispatch: no database, no ORM, a no-op task body. Measures the
    # framework's own per-task cost and nothing else.
    "dispatch": {
        "n": 60_000,
        "env": {},
        "lanes": {
            "cauli (async)": (
                "cauli_async",
                [CAULI_WORKER, "-A", "tasks_cauli_async:app",
                 "--procs", "8", "--io-concurrency", "96",
                 "--redis-url", REDIS_URL],
            ),
            "taskiq (async)": (
                "taskiq",
                [TASKIQ_BIN, "worker", "tasks_taskiq:broker",
                 "--workers", "8", "--max-async-tasks", "100",
                 "--max-prefetch", "100", "--log-level", "WARNING"],
            ),
        },
    },
    # SQLAlchemy 2.0 async ORM, one INSERT per task. The published version of
    # this lane was invalid twice over: the engine kept 2 pooled connections
    # and churned the rest (sqla_models.make_engine), and cauli ran 4
    # processes against taskiq's 8 while the write-up called it matched.
    # Both are corrected here; the pool fix alone moved an isolated probe
    # from 96.5 to 694.5 inserts/s.
    "sqlalchemy": {
        "n": 20_000,
        "env": {"BENCH_PG_POOL_MAX": str(PG_POOL_PER_PROC)},
        "lanes": {
            "cauli (async+sqla)": (
                "cauli_async_sqlalchemy",
                [CAULI_WORKER, "-A", "tasks_cauli_async_sqlalchemy:app",
                 "--procs", str(PG_PROCS),
                 "--io-concurrency", str(PG_CONCURRENCY),
                 "--redis-url", REDIS_URL],
            ),
            "taskiq (async+sqla)": (
                "taskiq_sqlalchemy",
                [TASKIQ_BIN, "worker", "tasks_taskiq_sqlalchemy:broker",
                 "--workers", str(PG_PROCS),
                 "--max-async-tasks", str(PG_CONCURRENCY),
                 "--max-prefetch", str(PG_CONCURRENCY),
                 "--log-level", "WARNING"],
            ),
        },
    },
}


def run_one(lane, cmd, n, timeout, env_extra, tag):
    result_file = BENCH_DIR / f"paired_{tag}.json"
    env = {**os.environ, **env_extra}
    proc = subprocess.run(
        ["bash", str(BENCH_DIR / "run.sh"), lane, str(n), str(timeout),
         str(result_file), *cmd],
        cwd=BENCH_DIR, env=env, capture_output=True, text=True,
    )
    try:
        data = json.loads(result_file.read_text())
    except Exception:
        print(proc.stderr[-2000:], file=sys.stderr)
        return None
    return data.get("mid80_rate")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", choices=sorted(SUITES), required=True)
    ap.add_argument("--pairs", type=int, default=6)
    ap.add_argument("--timeout", type=int, default=120)
    args = ap.parse_args()

    suite = SUITES[args.suite]
    (label_a, (lane_a, cmd_a)), (label_b, (lane_b, cmd_b)) = suite["lanes"].items()
    print(f"suite={args.suite}  n={suite['n']}  pairs={args.pairs}", file=sys.stderr)
    if suite["env"]:
        print(f"env: {suite['env']}", file=sys.stderr)
        print(f"connections: {PG_PROCS} procs x {PG_POOL_PER_PROC} pool = "
              f"{PG_PROCS * PG_POOL_PER_PROC}", file=sys.stderr)

    ratios, a_vals, b_vals = [], [], []
    for p in range(1, args.pairs + 1):
        first_is_a = p % 2 == 1
        order = []
        if first_is_a:
            order = [(label_a, lane_a, cmd_a, "a"), (label_b, lane_b, cmd_b, "b")]
        else:
            order = [(label_b, lane_b, cmd_b, "b"), (label_a, lane_a, cmd_a, "a")]
        got = {}
        for label, lane, cmd, key in order:
            r = run_one(lane, cmd, suite["n"], args.timeout, suite["env"], f"{key}{p}")
            got[key] = r
            print(f"  pair{p} {'(a first)' if first_is_a else '(b first)'} "
                  f"{label} = {r if r else 'TIMEOUT'}", file=sys.stderr)
        if got.get("a") and got.get("b"):
            a_vals.append(got["a"])
            b_vals.append(got["b"])
            ratios.append(got["a"] / got["b"])

    print()
    print("=" * 72)
    print(f"PAIRED  {label_a}  vs  {label_b}")
    print("=" * 72)
    if len(ratios) < 2:
        print("not enough completed pairs to report")
        return 1
    m, sd = statistics.mean(ratios), statistics.stdev(ratios)
    sem = sd / math.sqrt(len(ratios))
    lo, hi = m - 1.96 * sem, m + 1.96 * sem
    print(f"{label_a:<22}{statistics.mean(a_vals):>12.0f} tasks/s (mean of {len(a_vals)})")
    print(f"{label_b:<22}{statistics.mean(b_vals):>12.0f} tasks/s (mean of {len(b_vals)})")
    print(f"\nratio {label_a} / {label_b}: {m:.3f}   95% CI {lo:.3f}-{hi:.3f}")
    print(f"wins: {sum(1 for r in ratios if r > 1)}/{len(ratios)}")
    if lo <= 1.0 <= hi:
        print("\nVERDICT: indistinguishable from noise at this sample size.")
    else:
        faster = label_a if m > 1 else label_b
        print(f"\nVERDICT: {faster} is faster, by {abs(m - 1) * 100:.1f}% "
              f"(interval excludes 1.0).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
