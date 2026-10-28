"""Shared machinery: build a worker's environment, enqueue with no worker
running, start and kill a worker's whole process tree, measure a drain, and
compare lanes in interleaved rounds."""

import json
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import redis

from bench import DONE_KEY, REDIS_URL

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
CAULI_WORKER = os.environ.get("CAULI_WORKER_BIN", "cauli-worker")
POLL_S = 0.05
KILL_WAIT_S = 5
CLK_TCK = os.sysconf("SC_CLK_TCK")
# Two sided 95% Student t quantiles by degrees of freedom; 1.96 past 30.
T95 = [12.71, 4.30, 3.18, 2.78, 2.57, 2.45, 2.36, 2.31, 2.26, 2.23, 2.20, 2.18, 2.16, 2.14, 2.13,
       2.12, 2.11, 2.10, 2.09, 2.09, 2.08, 2.07, 2.07, 2.06, 2.06, 2.06, 2.05, 2.05, 2.05, 2.04]  # fmt: skip
ENQUEUE_CODE = "import importlib, json, sys; importlib.import_module(sys.argv[1]).enqueue(json.load(sys.stdin))"


@dataclass
class Lane:
    """One framework configuration: the interpreter that enqueues for it, the
    app module that knows how, and the commands that make up its worker."""

    label: str
    app: str
    task: str
    n: int
    cmds: list[list[str]]
    python: str = PYTHON
    env: dict[str, str] = field(default_factory=dict)

    def worker(self, log: Path) -> "Worker":
        return Worker(self.label, self.cmds, log, self.python, self.env)


def cauli_cmd(*flags: str, worker: str = CAULI_WORKER) -> list[str]:
    return [worker, "-A", "bench.apps.cauli:app", "--redis-url", REDIS_URL, *flags]


def celery_cmd(*flags: str, app: str = "bench.apps.celery") -> list[str]:
    quiet = ["--without-heartbeat", "--without-gossip", "--without-mingle"]
    return [
        PYTHON,
        "-m",
        "celery",
        "-A",
        app,
        "worker",
        *quiet,
        "-l",
        "warning",
        *flags,
    ]


def taskiq_cmd(*flags: str) -> list[str]:
    app = "bench.apps.taskiq:broker"
    return [PYTHON, "-m", "taskiq", "worker", app, "--log-level", "WARNING", *flags]


def dramatiq_cmd(processes: int, threads: int) -> list[str]:
    sizes = ["--processes", str(processes), "--threads", str(threads)]
    return [PYTHON, "-m", "dramatiq", "bench.apps.dramatiq", *sizes]


def arq_cmd(python: str) -> list[str]:
    return [python, "-m", "arq", "bench.apps.arq.WorkerSettings"]


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"set {name}: see bench/README.md")
    return value


def child_env(python: str, extra: dict[str, str]) -> dict[str, str]:
    # cauli-worker embeds libpython and reads no venv, so the interpreter's
    # site-packages and library dir are passed explicitly. The clone's py/ goes
    # first so an editable install elsewhere cannot shadow the code under test.
    probe = "import sysconfig; print(sysconfig.get_path('purelib')); print(sysconfig.get_config_var('LIBDIR'))"
    purelib, libdir = subprocess.check_output([python, "-c", probe], text=True).split()
    path = os.pathsep.join([str(REPO_ROOT), str(REPO_ROOT / "py"), purelib])
    return dict(
        os.environ,
        PYTHONPATH=path,
        LD_LIBRARY_PATH=libdir,
        BENCH_REDIS_URL=REDIS_URL,
        **extra,
    )


def connect() -> redis.Redis:
    r = redis.Redis.from_url(REDIS_URL)
    r.ping()
    return r


def reset(r: redis.Redis) -> None:
    r.flushall()
    r.set(DONE_KEY, 0)


def enqueue(lane: Lane, jobs: list[tuple[str, tuple]]) -> None:
    subprocess.run(
        [lane.python, "-c", ENQUEUE_CODE, lane.app],
        input=json.dumps(jobs),
        text=True,
        check=True,
        cwd=REPO_ROOT,
        env=child_env(lane.python, lane.env),
    )


def _stat(pid: str) -> list[str]:
    with open(f"/proc/{pid}/stat") as f:
        return f.read().rsplit(") ", 1)[1].split()


def session_pids(sids: set[int]) -> list[int]:
    """Every process in the worker's sessions. Sessions, not process groups:
    cauli-worker gives each supervised process its own group."""
    out = []
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            state, _ppid, _pgrp, sid = _stat(pid)[:4]
            if state != "Z" and int(sid) in sids:
                out.append(int(pid))
        except (OSError, IndexError):
            continue
    return out


def cpu_seconds(pids: list[int]) -> float:
    total = 0
    for pid in pids:
        try:
            parts = _stat(str(pid))
        except OSError:
            continue
        total += int(parts[11]) + int(parts[12])
    return total / CLK_TCK


class Worker:
    """Commands started each in its own session and killed as a whole tree on
    exit. Killing waits until no process of the tree is left: a leaked worker
    competes with every later measurement."""

    def __init__(
        self,
        label: str,
        cmds: list[list[str]],
        log: Path,
        python: str = PYTHON,
        env: dict[str, str] | None = None,
    ):
        self.label, self.cmds, self.log = label, cmds, log
        self.env = child_env(python, env or {})
        self.procs: list[subprocess.Popen] = []

    def __enter__(self) -> "Worker":
        with self.log.open("ab") as out:
            for cmd in self.cmds:
                self.procs.append(
                    subprocess.Popen(
                        cmd,
                        cwd=REPO_ROOT,
                        env=self.env,
                        stdout=out,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                )
        return self

    @property
    def sids(self) -> set[int]:
        return {p.pid for p in self.procs}

    def pids(self) -> list[int]:
        return session_pids(self.sids)

    def __exit__(self, *exc) -> None:
        deadline = time.monotonic() + KILL_WAIT_S
        while pids := self.pids():
            if time.monotonic() > deadline:
                raise RuntimeError(f"{self.label}: processes {pids} survived SIGKILL")
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    continue
            for p in self.procs:
                if p.poll() is None:
                    p.wait(KILL_WAIT_S)
            time.sleep(POLL_S)


def drain(lane: Lane, args: tuple, timeout_s: float, log: Path) -> dict:
    """Enqueue lane.n tasks with no worker running, then start the worker and take
    the completion rate between 10% and 90% done, which leaves out start up and
    the straggler tail. CPU is sampled over the same window."""
    r = connect()
    reset(r)
    redis_pid = [r.info("server")["process_id"]]
    n = lane.n
    enqueue(lane, [(lane.task, args)] * n)
    samples = []
    with lane.worker(log) as worker:
        start = time.monotonic()
        while True:
            done = int(r.get(DONE_KEY) or 0)
            samples.append(
                (
                    time.monotonic(),
                    done,
                    cpu_seconds(worker.pids()),
                    cpu_seconds(redis_pid),
                )
            )
            if done >= n:
                break
            if samples[-1][0] - start > timeout_s:
                raise RuntimeError(
                    f"{lane.label}: {done} of {n} done after {timeout_s}s, see {log}"
                )
            time.sleep(POLL_S)
    window = [s for s in samples if n * 0.1 <= s[1] <= n * 0.9]
    if len(window) < 3:
        raise RuntimeError(
            f"{lane.label}: drained {n} tasks in under 3 polls, raise the task count"
        )
    (t0, d0, w0, r0), (t1, d1, w1, r1) = window[0], window[-1]
    span = t1 - t0
    return {
        "rate": (d1 - d0) / span,
        "worker_cores": (w1 - w0) / span,
        "redis_cores": (r1 - r0) / span,
    }


def ci95(values: list[float]) -> tuple[float, float, float]:
    mean = statistics.mean(values)
    if len(values) < 2:
        return mean, math.nan, math.nan
    df = len(values) - 1
    half = (
        (T95[df - 1] if df <= len(T95) else 1.96)
        * statistics.stdev(values)
        / math.sqrt(len(values))
    )
    return mean, mean - half, mean + half


def paired(
    lanes: list[Lane], args: tuple, rounds: int, timeout_s: float, log: Path
) -> list[dict]:
    """Run every lane once per round, rotating the order each round so slow
    drift on a shared box spreads over all lanes instead of landing on
    whichever runs last. Each lane is compared with the first through the
    mean of its per round ratios and that mean's 95% interval."""
    runs: dict[str, list[dict]] = {lane.label: [] for lane in lanes}
    for i in range(rounds):
        for lane in lanes[i % len(lanes) :] + lanes[: i % len(lanes)]:
            result = drain(lane, args, timeout_s, log)
            runs[lane.label].append(result)
            print(
                f"round {i + 1}/{rounds}  {lane.label}: {result['rate']:,.0f}/s",
                file=sys.stderr,
                flush=True,
            )
    base = [x["rate"] for x in runs[lanes[0].label]]
    rows = []
    for lane in lanes:
        got = runs[lane.label]
        ratio, lo, hi = ci95([x["rate"] / b for x, b in zip(got, base, strict=True)])
        rows.append(
            {
                "label": lane.label,
                "rate": statistics.mean(x["rate"] for x in got),
                "ratio": ratio,
                "lo": lo,
                "hi": hi,
                "worker_cores": statistics.mean(x["worker_cores"] for x in got),
                "redis_cores": statistics.mean(x["redis_cores"] for x in got),
            }
        )
    return rows


def paired_table(rows: list[dict], base: str) -> str:
    lines = [
        f"| lane | tasks/s | x {base} | 95% interval | verdict | worker cores | redis cores |",
        "|---|---:|---:|---|---|---:|---:|",
    ]
    for row in rows:
        if math.isnan(row["lo"]) or row["label"] == base:
            interval, verdict = "", ""
        else:
            interval = f"{row['lo']:.3f} to {row['hi']:.3f}"
            verdict = (
                "noise"
                if row["lo"] <= 1 <= row["hi"]
                else ("faster" if row["ratio"] > 1 else "slower")
            )
        lines.append(
            f"| {row['label']} | {row['rate']:,.0f} | {row['ratio']:.3f} | {interval} | {verdict}"
            f" | {row['worker_cores']:.2f} | {row['redis_cores']:.2f} |"
        )
    return "\n".join(lines)


def pss_mib(pids: list[int]) -> float:
    """PSS, not RSS: forked children share copy on write pages, and RSS counts
    each shared page once per child."""
    total_kb = 0
    for pid in pids:
        try:
            with open(f"/proc/{pid}/smaps_rollup") as f:
                total_kb += next(
                    int(line.split()[1]) for line in f if line.startswith("Pss:")
                )
        except OSError:
            continue
    return total_kb / 1024


def log_path(name: str) -> Path:
    path = Path(os.environ.get("BENCH_LOG_DIR", "/tmp")) / f"bench-{name}.log"
    path.write_text("")
    return path
