"""Memory per in flight task: hold N sleeping tasks at once and sum PSS over
every process of the worker."""

import argparse
import time

from bench import STARTED_KEY
from bench.runner import (
    Lane,
    cauli_cmd,
    celery_cmd,
    connect,
    enqueue,
    log_path,
    pss_mib,
    reset,
    taskiq_cmd,
)

START_TIMEOUT_S = 180
SETTLE_S = 3
# One OS process per slot: past this Celery prefork is a fork bomb, not a data point.
PREFORK_MAX = 100


def lanes(n: int) -> list[Lane]:
    cauli, celery, size = "bench.apps.cauli", "bench.apps.celery", str(n)
    one_proc = cauli_cmd("--procs", "1", "--io-concurrency", size)
    taskiq = taskiq_cmd("--workers", "1", "--max-async-tasks", size)
    out = [
        Lane("cauli async, 1 process", cauli, "hold", n, [one_proc]),
        Lane(
            "cauli async, -c (processes derived)",
            cauli,
            "hold",
            n,
            [cauli_cmd("-c", size)],
        ),
        Lane(
            "celery gevent", celery, "hold", n, [celery_cmd("-P", "gevent", "-c", size)]
        ),
        Lane("taskiq, 1 worker", "bench.apps.taskiq", "hold", n, [taskiq]),
    ]
    if n <= PREFORK_MAX:
        out.append(
            Lane(
                "celery prefork",
                celery,
                "hold",
                n,
                [celery_cmd("-P", "prefork", "-c", size)],
            )
        )
    return out


def measure(lane: Lane) -> str:
    r = connect()
    reset(r)
    with lane.worker(log_path("memory")) as worker:
        enqueue(lane, [(lane.task, ())] * lane.n)
        deadline = time.monotonic() + START_TIMEOUT_S
        while (started := int(r.get(STARTED_KEY) or 0)) < lane.n:
            if time.monotonic() > deadline:
                return f"| {lane.label} | {lane.n:,} | only {started:,} in flight after {START_TIMEOUT_S}s | | |"
            time.sleep(0.25)
        time.sleep(SETTLE_S)
        pids = worker.pids()
        mib = pss_mib(pids)
    return f"| {lane.label} | {lane.n:,} | {mib:,.1f} | {len(pids)} | {mib * 1024 / lane.n:,.1f} |"


def run(args: argparse.Namespace) -> None:
    print("| worker | in flight | PSS MiB | processes | KiB per task |")
    print("|---|---:|---:|---:|---:|")
    for n in args.sizes:
        for lane in lanes(n):
            print(measure(lane), flush=True)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--sizes",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[100, 1000, 10000],
        help="comma separated in flight counts",
    )
