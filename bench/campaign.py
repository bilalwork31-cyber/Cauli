#!/usr/bin/env python3
"""Reproduce RESULTS.md: run each framework's tuned config N times through
run.sh and print the mean mid-80% drain rate. Pinned configs are the winners
of a wider concurrency/thread sweep (see RESULTS.md "Tuning notes") -- this
script does not re-search, it re-measures.

Usage: python3 campaign.py [--reps N]
Requires cauli-worker, celery, taskiq, redis-server on PATH.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
CAULI_WORKER = os.environ.get("CAULI_WORKER_BIN", "cauli-worker")
CELERY_BIN = os.environ.get("CELERY_BIN", "celery")
TASKIQ_BIN = os.environ.get("TASKIQ_BIN", "taskiq")
REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://127.0.0.1:6395/0")

CONFIGS = [
    {
        "label": "celery (sync)",
        "framework": "celery",
        "n": 30_000,
        "cmd": [
            CELERY_BIN, "-A", "tasks_celery", "worker",
            "-c", "4", "-P", "prefork", "--prefetch-multiplier=1",
            "--without-heartbeat", "--without-gossip", "--without-mingle",
            "-l", "warning",
        ],
    },
    {
        "label": "cauli (sync)",
        "framework": "cauli_sync",
        "n": 100_000,
        "cmd": [
            CAULI_WORKER, "-A", "tasks_cauli_sync:app",
            "--procs", "12", "--io-threads", "80", "--io-concurrency", "80",
            "--redis-url", REDIS_URL,
        ],
    },
    {
        "label": "taskiq (async)",
        "framework": "taskiq",
        "n": 60_000,
        "cmd": [
            TASKIQ_BIN, "worker", "tasks_taskiq:broker",
            "--workers", "8", "--max-async-tasks", "100", "--max-prefetch", "100",
            "--log-level", "WARNING",
        ],
    },
    {
        "label": "cauli (async)",
        "framework": "cauli_async",
        "n": 150_000,
        "cmd": [
            CAULI_WORKER, "-A", "tasks_cauli_async:app",
            "--procs", "8", "--io-concurrency", "96",
            "--redis-url", REDIS_URL,
        ],
    },
]


def run_one(cfg, timeout=60):
    result_file = BENCH_DIR / "campaign_result.json"
    args = [
        "bash", str(BENCH_DIR / "run.sh"),
        cfg["framework"], str(cfg["n"]), str(timeout), str(result_file),
        *cfg["cmd"],
    ]
    subprocess.run(args, check=True, cwd=BENCH_DIR)
    return json.loads(result_file.read_text())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    args = ap.parse_args()

    rows = []
    for cfg in CONFIGS:
        rates = []
        for i in range(args.reps):
            print(f"=== {cfg['label']} rep {i + 1}/{args.reps} ===", file=sys.stderr)
            result = run_one(cfg)
            if result["timed_out"] or result["mid80_rate"] is None:
                print(f"  WARNING: rep {i + 1} timed out or had no mid80 sample", file=sys.stderr)
                continue
            rates.append(result["mid80_rate"])
        rows.append((cfg["label"], rates))

    print()
    print(f"{'framework':<16} {'mean tasks/s':>14} {'stdev':>10} {'reps':>6}")
    for label, rates in rows:
        if not rates:
            print(f"{label:<16} {'no data':>14}")
            continue
        mean = statistics.mean(rates)
        stdev = statistics.stdev(rates) if len(rates) > 1 else 0.0
        print(f"{label:<16} {mean:>14.1f} {stdev:>10.1f} {len(rates):>6}")


if __name__ == "__main__":
    main()
