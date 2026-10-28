"""Drain rate benchmarks: dispatch, cpu and free-threaded. Each run preloads
its tasks with no worker running, then times the worker draining them."""

import argparse

from bench.runner import (
    Lane,
    arq_cmd,
    cauli_cmd,
    celery_cmd,
    dramatiq_cmd,
    log_path,
    paired,
    paired_table,
    require_env,
    taskiq_cmd,
)

CAULI = "bench.apps.cauli"
CELERY = "bench.apps.celery"
# One Celery node is one consumer and tops out near 870/s whatever -c is, so
# Celery scales by nodes. 8 solo nodes was its best measured configuration.
CELERY_NODES = 8
CPU_PROCS = 6
CPU_TASK_MS = (0.5, 2, 10, 50)
CPU_WORK_S = 10
FT_SLOTS = "10"


def scaled(base: float, scale: float) -> int:
    return max(1, round(base * scale))


def dispatch_lanes(scale: float) -> list[Lane]:
    """Each framework at its best measured configuration, with a task count
    that keeps every run near five seconds."""
    arq_python = require_env("ARQ_PYTHON")
    celery_nodes = [
        celery_cmd("-P", "solo", "--prefetch-multiplier=32", "-n", f"w{i}@%h")
        for i in range(CELERY_NODES)
    ]
    async_flags = ["--procs", "8", "--io-concurrency", "96"]
    sync_flags = ["--procs", "12", "--io-threads", "80", "--io-concurrency", "80"]
    taskiq_flags = [
        "--workers",
        "8",
        "--max-async-tasks",
        "100",
        "--max-prefetch",
        "100",
    ]
    return [
        Lane(
            "cauli async",
            CAULI,
            "noop_async",
            scaled(150_000, scale),
            [cauli_cmd(*async_flags)],
        ),
        Lane(
            "cauli sync",
            CAULI,
            "noop_sync",
            scaled(100_000, scale),
            [cauli_cmd(*sync_flags)],
        ),
        Lane(
            "taskiq",
            "bench.apps.taskiq",
            "noop",
            scaled(50_000, scale),
            [taskiq_cmd(*taskiq_flags)],
        ),
        Lane(
            "dramatiq",
            "bench.apps.dramatiq",
            "noop",
            scaled(60_000, scale),
            [dramatiq_cmd(12, 8)],
        ),
        Lane(
            f"celery {CELERY_NODES} solo nodes",
            CELERY,
            "noop",
            scaled(40_000, scale),
            celery_nodes,
        ),
        Lane(
            "arq",
            "bench.apps.arq",
            "noop",
            scaled(5_000, scale),
            [arq_cmd(arq_python)],
            arq_python,
        ),
    ]


def dispatch(args: argparse.Namespace) -> None:
    lanes = dispatch_lanes(args.scale)
    rows = paired(lanes, (), args.rounds, args.timeout, log_path("dispatch"))
    print(paired_table(rows, lanes[0].label))


def cpu(args: argparse.Namespace) -> None:
    cauli_flags = ["--procs", "1", "--cpu-workers", str(CPU_PROCS)]
    celery_flags = ["-P", "prefork", "-c", str(CPU_PROCS), "--prefetch-multiplier=1"]
    for ms in CPU_TASK_MS:
        n = scaled(CPU_WORK_S * CPU_PROCS * 1000 / ms, args.scale)
        lanes = [
            Lane("cauli cpu", CAULI, "burn", n, [cauli_cmd(*cauli_flags)]),
            Lane("celery prefork", CELERY, "burn", n, [celery_cmd(*celery_flags)]),
        ]
        rows = paired(lanes, (ms,), args.rounds, args.timeout, log_path("cpu"))
        print(f"{ms} ms tasks, {n:,} per run\n")
        print(paired_table(rows, lanes[0].label), end="\n\n", flush=True)


def free_threaded(args: argparse.Namespace) -> None:
    gil_python, gil_worker = require_env("GIL_PYTHON"), require_env("GIL_WORKER")
    ft_python, ft_worker = require_env("FT_PYTHON"), require_env("FT_WORKER")
    threads = ["--io-threads", FT_SLOTS, "--io-concurrency", FT_SLOTS]
    cases = [
        ("noop, sync, 10 threads", "noop_sync", 60_000, threads),
        ("pure Python burn, sync, 10 threads", "burn_thread", 6_000, threads),
        (
            "pure Python burn, cpu lane, 10 processes",
            "burn_process",
            6_000,
            ["--cpu-workers", FT_SLOTS],
        ),
    ]
    # PYTHON_GIL=0 makes the free threaded interpreter fail at start rather
    # than quietly re-enable the GIL for an incompatible extension.
    ft_env = {"PYTHON_GIL": "0"}
    for label, task, base_n, flags in cases:
        n = scaled(base_n, args.scale)
        gil_cmd = cauli_cmd("--procs", "1", *flags, worker=gil_worker)
        ft_cmd = cauli_cmd("--procs", "1", *flags, worker=ft_worker)
        lanes = [
            Lane("GIL", CAULI, task, n, [gil_cmd], gil_python),
            Lane("free threaded", CAULI, task, n, [ft_cmd], ft_python, ft_env),
        ]
        rows = paired(lanes, (), args.rounds, args.timeout, log_path("free-threaded"))
        print(f"{label}, {n:,} tasks per run\n")
        print(paired_table(rows, lanes[0].label), end="\n\n", flush=True)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rounds", type=int, default=6, help="each round runs every lane once"
    )
    parser.add_argument(
        "--scale", type=float, default=1.0, help="multiplies every task count"
    )
    parser.add_argument(
        "--timeout", type=float, default=180, help="seconds one drain may take"
    )
