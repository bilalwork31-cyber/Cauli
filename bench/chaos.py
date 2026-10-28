"""Crash behaviour. kill -9: SIGKILL a worker mid run, start a fresh one and
count tags lost and duplicated. Segfault: one task segfaults among sleeping
ones; record what dies and what keeps running."""

import argparse
import time
from collections import Counter

from bench import EXEC_KEY
from bench.runner import (
    Lane,
    arq_cmd,
    cauli_cmd,
    celery_cmd,
    connect,
    dramatiq_cmd,
    enqueue,
    log_path,
    require_env,
    reset,
)

HOLDS = 10
# Every hold and the segfault must run at once, or the segfault waits behind
# holds that sleep for an hour.
SLOTS = HOLDS + 2
POLL_S = 0.05
CAULI, CELERY, DRAMATIQ, ARQ = (
    "bench.apps.cauli",
    "bench.apps.celery",
    "bench.apps.dramatiq",
    "bench.apps.arq",
)
CELERY_PREFORK = ["-P", "prefork", "--prefetch-multiplier=1"]


def kill_lanes(n: int, arq_python: str) -> list[Lane]:
    cauli = cauli_cmd("--procs", "2", "--io-threads", "8", "--visibility-timeout", "5")
    acks_late = celery_cmd(*CELERY_PREFORK, "-c", "4", app=f"{CELERY}:acks_late_app")
    return [
        Lane("cauli", CAULI, "chaos", n, [cauli]),
        Lane(
            "celery acks_late, visibility_timeout 5",
            CELERY,
            "chaos_acks_late",
            n,
            [acks_late],
        ),
        Lane(
            "celery default",
            CELERY,
            "chaos",
            n,
            [celery_cmd(*CELERY_PREFORK, "-c", "4")],
        ),
        Lane("dramatiq", DRAMATIQ, "chaos", n, [dramatiq_cmd(2, 8)]),
        Lane("arq", ARQ, "chaos", n, [arq_cmd(arq_python)], arq_python),
    ]


def segfault_lanes(arq_python: str) -> list[Lane]:
    return [
        Lane(
            "cauli async, 1 process", CAULI, "segfault", 1, [cauli_cmd("--procs", "1")]
        ),
        Lane(
            "cauli async, 3 processes",
            CAULI,
            "segfault",
            1,
            [cauli_cmd("--procs", "3")],
        ),
        Lane(
            'cauli, segfault in a kind="cpu" task',
            CAULI,
            "segfault_cpu",
            1,
            [cauli_cmd("--procs", "1")],
        ),
        Lane(
            "celery prefork",
            CELERY,
            "segfault",
            1,
            [celery_cmd(*CELERY_PREFORK, "-c", str(SLOTS))],
        ),
        Lane("dramatiq", DRAMATIQ, "segfault", 1, [dramatiq_cmd(2, SLOTS)]),
        Lane("arq", ARQ, "segfault", 1, [arq_cmd(arq_python)], arq_python),
    ]


def kill_run(lane: Lane, kill_at: int, recovery_s: float) -> str:
    r = connect()
    reset(r)
    tags = [f"tag-{i}" for i in range(lane.n)]
    enqueue(lane, [(lane.task, (tag,)) for tag in tags])
    log = log_path("chaos")
    with lane.worker(log):
        deadline = time.monotonic() + recovery_s
        while r.llen(EXEC_KEY) < kill_at:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"{lane.label}: under {kill_at} executed after {recovery_s:.0f}s, see {log}"
                )
            time.sleep(POLL_S)
        before = r.llen(EXEC_KEY)
    killed = time.monotonic()
    with lane.worker(log):
        while (
            len(set(r.lrange(EXEC_KEY, 0, -1))) < lane.n
            and time.monotonic() - killed < recovery_s
        ):
            time.sleep(POLL_S)
        recovered = time.monotonic() - killed
    counts = Counter(v.decode() for v in r.lrange(EXEC_KEY, 0, -1))
    lost = sum(1 for tag in tags if tag not in counts)
    duplicated = sum(1 for c in counts.values() if c > 1)
    recovery = f"{recovered:.1f}s" if not lost else f"not within {recovery_s:.0f}s"
    return f"| {lane.label} | {before} | {lost} | {duplicated} | {recovery} |"


def segfault_run(lane: Lane, observe_s: float) -> str:
    r = connect()
    reset(r)
    enqueue(lane, [("hold", ())] * HOLDS + [(lane.task, ())])
    seen: set[int] = set()
    with lane.worker(log_path("chaos")) as worker:
        end = time.monotonic() + observe_s
        while time.monotonic() < end:
            seen |= set(worker.pids())
            time.sleep(POLL_S)
        alive = set(worker.pids())
        code = worker.procs[0].poll()
    top = "alive" if code is None else f"exited, code {code}"
    return f"| {lane.label} | {top} | {len(seen - alive)} | {len(alive)} |"


def run(args: argparse.Namespace) -> None:
    arq_python = require_env("ARQ_PYTHON")
    kill_at = round(args.tasks * args.kill_at)
    print(f"kill -9 at {kill_at} of {args.tasks} tasks executed\n")
    print("| lane | executed before kill | lost | duplicated | recovery |")
    print("|---|---:|---:|---:|---|")
    for lane in kill_lanes(args.tasks, arq_python):
        print(kill_run(lane, kill_at, args.recovery_timeout), flush=True)
    print(
        f"\nsegfault among {HOLDS} sleeping tasks, observed for {args.observe:.0f}s\n"
    )
    print("| lane | top level process | processes died | processes alive at end |")
    print("|---|---|---:|---:|")
    for lane in segfault_lanes(arq_python):
        print(segfault_run(lane, args.observe), flush=True)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tasks", type=int, default=500, help="tagged tasks per kill -9 run"
    )
    parser.add_argument(
        "--kill-at",
        type=float,
        default=0.32,
        help="fraction executed when the kill lands",
    )
    parser.add_argument(
        "--recovery-timeout",
        type=float,
        default=120,
        help="seconds to wait for every tag",
    )
    # Long enough for a crash handler that captures a core dump, which took
    # 7.5 s for one cauli-worker process under WSL.
    parser.add_argument(
        "--observe", type=float, default=20, help="seconds to watch a segfault run"
    )
