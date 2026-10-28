"""GIL build against free threaded build, same worker source, interleaved.

Each side is an interpreter plus a cauli-worker built against it:

    GIL_PYTHON=~/venv312/bin/python   GIL_WORKER=~/target312/release/cauli-worker \\
    FT_PYTHON=~/venv314t/bin/python   FT_WORKER=~/target314t/release/cauli-worker \\
    python3 free_threaded.py --pairs 3

Both venvs need cauli (editable from ../py) and redis. One worker process
per run (--procs 1), drain rate over the middle 80% of N tasks, order flipped
every pair. Each lane's CPU cost is reported as cores in use: rate times the
body's measured duration on that interpreter.
"""

import argparse
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

import redis

from common import DONE_KEY, REDIS_URL

BENCH_DIR = Path(__file__).resolve().parent
STALL_S = 120
LANES = {
    "noop, sync, 10 threads": (
        "noop",
        60_000,
        ["--io-threads", "10", "--io-concurrency", "10"],
    ),
    "burn, sync, 10 threads": (
        "burn_thread",
        6_000,
        ["--io-threads", "10", "--io-concurrency", "10"],
    ),
    "burn, cpu lane, 10 processes": ("burn_cpu", 6_000, ["--cpu-workers", "10"]),
}


def side_env(python: str) -> dict[str, str]:
    probe = "import sysconfig; print(sysconfig.get_path('purelib')); print(sysconfig.get_config_var('LIBDIR'))"
    purelib, libdir = subprocess.check_output([python, "-c", probe], text=True).split()
    # The worker embeds libpython and reads no venv of its own, so its
    # site-packages and loader path are passed explicitly (see run.sh).
    return dict(
        os.environ,
        PYTHONPATH=os.pathsep.join(
            [str(BENCH_DIR), str(BENCH_DIR.parent / "py"), purelib]
        ),
        LD_LIBRARY_PATH=libdir,
        BENCH_REDIS_URL=REDIS_URL,
    )


def burn_ms(python: str, env: dict[str, str]) -> float:
    code = "import timeit, tasks_cauli_threads as t; print(timeit.timeit(t.burn, number=200) / 200 * 1000)"
    return float(subprocess.check_output([python, "-c", code], env=env, text=True))


def drain_rate(python: str, worker: str, env: dict[str, str], lane: str) -> float:
    task, n, flags = LANES[lane]
    r = redis.Redis.from_url(REDIS_URL)
    r.flushall()
    enqueue = f"import tasks_cauli_threads as t; t.app.enqueue_many([t.{task}] * {n})"
    subprocess.run([python, "-c", enqueue], env=env, check=True)
    proc = subprocess.Popen(
        [worker, "-A", "tasks_cauli_threads:app", "--procs", "1", *flags],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )  # fmt: skip
    try:
        start = None
        deadline = time.monotonic() + STALL_S
        while True:
            done, now = int(r.get(DONE_KEY) or 0), time.monotonic()
            if start is None and done >= n // 10:
                start = (now, done)
            if done >= n * 9 // 10:
                return (done - start[1]) / (now - start[0])
            if now > deadline:
                raise RuntimeError(f"{lane}: stalled at {done} of {n} tasks")
            time.sleep(0.02)
    finally:
        os.killpg(proc.pid, 9)
        proc.wait()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pairs", type=int, default=3)
    args = parser.parse_args()
    missing = [
        k
        for k in ("GIL_PYTHON", "GIL_WORKER", "FT_PYTHON", "FT_WORKER")
        if not os.environ.get(k)
    ]
    if missing:
        sys.exit(f"set {', '.join(missing)}: see the module docstring")
    sides = {
        "gil": (os.environ["GIL_PYTHON"], os.environ["GIL_WORKER"]),
        "ft": (os.environ["FT_PYTHON"], os.environ["FT_WORKER"]),
    }
    envs = {name: side_env(py) for name, (py, _w) in sides.items()}
    envs["ft"]["PYTHON_GIL"] = (
        "0"  # fail loudly rather than measure a GIL that came back
    )
    body_ms = {name: burn_ms(sides[name][0], envs[name]) for name in sides}
    print(
        f"burn body: GIL {body_ms['gil']:.2f} ms, free threaded {body_ms['ft']:.2f} ms"
    )
    for lane in LANES:
        rates = {"gil": [], "ft": []}
        for i in range(args.pairs):
            for name in ("gil", "ft") if i % 2 == 0 else ("ft", "gil"):
                python, worker = sides[name]
                rates[name].append(drain_rate(python, worker, envs[name], lane))
        gil, ft = statistics.median(rates["gil"]), statistics.median(rates["ft"])
        cores = ""
        if "burn" in lane:
            cores = f"  cores: GIL {gil * body_ms['gil'] / 1000:.2f}, free threaded {ft * body_ms['ft'] / 1000:.2f}"
        print(
            f"{lane:30s} GIL {gil:8.0f}/s  free threaded {ft:8.0f}/s  x{ft / gil:.2f}{cores}"
        )


if __name__ == "__main__":
    main()
