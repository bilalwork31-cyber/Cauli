"""Web integration benchmark: what an enqueue costs inside a real Django or
FastAPI request, how long the task then takes to finish, and whether every
request's task ran exactly once.

Per arm and rep, three phases on a freshly truncated table:

1. saturation: closed loop, VUS clients, no worker running. Request rate and
   latency are the web tier's alone; the enqueue arms leave a backlog.
2. drain: start the worker, wait for the backlog, check exactly once.
3. open loop: RATE requests per second with the worker live, giving request
   latency under a fixed load and end to end latency (request start to task
   end) from the timestamps tasks push to Redis.

Arms run in a different order every rep, so drift on a shared box spreads
across arms instead of landing on whichever runs last (bench/README.md).

Run from bench/:  python3 -m web.campaign --reps 3
"""

import argparse
import json
import os
import random
import signal
import statistics
import subprocess
import sys
import sysconfig
import tempfile
import time
import urllib.request
from pathlib import Path

import psycopg
import redis

from common import REDIS_URL
from web.common import LATENCY_KEY, SCHEMA_SQL
from workloads import PG_DSN

BENCH_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BENCH_DIR.parent
PORT = int(os.environ.get("BENCH_WEB_PORT", "8100"))
WEB_PROCS = int(os.environ.get("BENCH_WEB_PROCS", "4"))
WORKER_BIN = os.environ.get("CAULI_WORKER_BIN", "cauli-worker")
K6 = os.environ.get("K6_BIN", "k6")
READY_TIMEOUT_S = 30
# A drain that makes no progress for this long is reported as lost work
# rather than waited out: tasks that dead letter never complete.
STALL_S = 15
SETTLE_S = 2

GUNICORN = [
    sys.executable, "-m", "gunicorn", "web.dj_entry:wsgi",
    "-k", "gthread", "-w", str(WEB_PROCS), "--threads", "8",
    "-b", f"127.0.0.1:{PORT}",
]  # fmt: skip
UVICORN = [
    sys.executable, "-m", "uvicorn", "--workers", str(WEB_PROCS), "--loop", "uvloop",
    "--http", "httptools", "--no-access-log", "--log-level", "warning",
    "--host", "127.0.0.1", "--port", str(PORT),
]  # fmt: skip

DJANGO_ASGI = ["env", "BENCH_DJ_CONN_MAX_AGE=0", *UVICORN, "web.dj_entry:asgi"]

ARMS = {
    "dj_wsgi_insert": (GUNICORN, "GET", "/insert", None),
    "dj_wsgi_cauli": (GUNICORN, "GET", "/cauli", "web.dj_tasks:app"),
    "dj_wsgi_cauli_async_task": (
        GUNICORN,
        "GET",
        "/cauli_async_task",
        "web.dj_tasks:app",
    ),
    "dj_wsgi_celery": (GUNICORN, "GET", "/celery", None),
    "dj_asgi_insert": (DJANGO_ASGI, "GET", "/ainsert", None),
    "dj_asgi_cauli": (DJANGO_ASGI, "GET", "/acauli", "web.dj_tasks:app"),
    "fa_insert": (UVICORN + ["web.fa_app:api"], "POST", "/insert", None),
    "fa_cauli": (UVICORN + ["web.fa_app:api"], "POST", "/cauli", "web.fa_tasks:app"),
    "fa_cauli_blocking": (
        UVICORN + ["web.fa_app:api"],
        "POST",
        "/cauli_blocking",
        "web.fa_tasks:app",
    ),  # fmt: skip
}


def child_env() -> dict[str, str]:
    # The worker embeds the system libpython, not this venv, so the venv's
    # site-packages and the editable cauli source go on PYTHONPATH explicitly.
    site = sysconfig.get_path("purelib")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(BENCH_DIR), str(REPO_ROOT / "py"), site])
    env["BENCH_REDIS_URL"] = REDIS_URL
    return env


def start(cmd: list[str], log: Path) -> subprocess.Popen:
    with log.open("ab") as out:
        return subprocess.Popen(
            cmd, cwd=BENCH_DIR, env=child_env(), stdout=out, stderr=subprocess.STDOUT,
            start_new_session=True,
        )  # fmt: skip


def stop(proc: subprocess.Popen) -> None:
    # Kill the whole session and wait for it: a leaked web or worker process
    # competes with every later measurement (bench/README.md).
    for sig, grace in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(grace)
            return
        except subprocess.TimeoutExpired:
            continue
    raise RuntimeError(f"process group {proc.pid} survived SIGKILL")


def wait_ready() -> None:
    deadline = time.monotonic() + READY_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"web server on port {PORT} not ready after {READY_TIMEOUT_S}s")


def k6(method: str, path: str, mode: str, args: argparse.Namespace) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as summary:
        summary_path = summary.name
    env = dict(os.environ, METHOD=method, URL=f"http://127.0.0.1:{PORT}{path}", MODE=mode,
               VUS=str(args.vus), RATE=str(args.rate), DURATION=f"{args.duration}s")  # fmt: skip
    subprocess.run(
        [K6, "run", "--quiet", "--summary-export", summary_path, str(Path(__file__).with_name("load.js"))],
        env=env, check=True, stdout=subprocess.DEVNULL,
    )  # fmt: skip
    metrics = json.loads(Path(summary_path).read_text())["metrics"]
    os.unlink(summary_path)
    duration = metrics["http_req_duration"]
    return {
        "rps": metrics["http_reqs"]["rate"],
        "requests": metrics["http_reqs"]["count"],
        "failed": metrics["http_req_failed"]["passes"],
        "dropped": metrics.get("dropped_iterations", {}).get("count", 0),
        "p50_ms": duration["p(50)"],
        "p99_ms": duration["p(99)"],
        "max_ms": duration["max"],
    }


def job_counts(pg: psycopg.Connection) -> dict:
    row = pg.execute(
        "SELECT count(*), count(*) FILTER (WHERE processed = 0),"
        " count(*) FILTER (WHERE processed > 1) FROM web_job"
    ).fetchone()
    return {"rows": row[0], "lost": row[1], "duplicated": row[2]}


def wait_drained(pg: psycopg.Connection, r: redis.Redis) -> dict:
    started = last_progress = time.monotonic()
    counts = job_counts(pg)
    while counts["lost"] and time.monotonic() - last_progress < STALL_S:
        time.sleep(0.25)
        previous, counts = counts["lost"], job_counts(pg)
        if counts["lost"] < previous:
            last_progress = time.monotonic()
    counts["drain_s"] = round(time.monotonic() - started, 2)
    counts["dead_letters"] = sum(
        r.xlen(k) for k in r.scan_iter("cauli:dlq*") if r.type(k) == b"stream"
    )
    return counts


def percentiles_ms(samples_ns: list[int]) -> dict:
    if not samples_ns:
        return {}
    ordered = sorted(samples_ns)
    pick = lambda q: ordered[min(len(ordered) - 1, int(q * len(ordered)))] / 1e6  # noqa: E731
    return {
        "e2e_p50_ms": pick(0.50),
        "e2e_p99_ms": pick(0.99),
        "e2e_max_ms": ordered[-1] / 1e6,
    }


def run_arm(name: str, args: argparse.Namespace, pg, r, logs: Path) -> dict:
    server_cmd, method, path, worker_app = ARMS[name]
    pg.execute("TRUNCATE web_job RESTART IDENTITY")
    r.flushall()
    server = start(server_cmd, logs / f"{name}.server.log")
    worker = None
    try:
        wait_ready()
        out = {"arm": name, "saturation": k6(method, path, "vus", args)}
        if worker_app is None:
            return out
        worker_cmd = [
            WORKER_BIN,
            "-A",
            worker_app,
            "--redis-url",
            REDIS_URL,
            "-c",
            str(args.concurrency),
        ]
        worker = start(worker_cmd, logs / f"{name}.worker.log")
        out["drain"] = wait_drained(pg, r)
        r.delete(LATENCY_KEY)
        time.sleep(SETTLE_S)
        out["open_loop"] = k6(method, path, "rate", args)
        out["open_loop_drain"] = wait_drained(pg, r)
        out["open_loop"].update(
            percentiles_ms([int(x) for x in r.lrange(LATENCY_KEY, 0, -1)])
        )
        return out
    finally:
        if worker is not None:
            stop(worker)
        stop(server)


def summarize(results: list[dict]) -> str:
    lines = [
        "| arm | sat rps | sat p50 ms | sat p99 ms | open p50 ms | open p99 ms | e2e p50 ms | e2e p99 ms | lost | dup | dlq |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]  # fmt: skip
    for arm in ARMS:
        runs = [x for x in results if x["arm"] == arm]
        if not runs:
            continue
        med = lambda f: statistics.median(f(x) for x in runs)  # noqa: E731
        cell = lambda key, f: f"{med(f):.1f}" if all(key in x for x in runs) else "n/a"  # noqa: E731
        lost = sum(
            x.get(d, {}).get("lost", 0)
            for x in runs
            for d in ("drain", "open_loop_drain")
        )
        dup = sum(
            x.get(d, {}).get("duplicated", 0)
            for x in runs
            for d in ("drain", "open_loop_drain")
        )
        dlq = max(x.get("open_loop_drain", {}).get("dead_letters", 0) for x in runs)
        lines.append(
            f"| {arm} | {med(lambda x: x['saturation']['rps']):.0f}"
            f" | {med(lambda x: x['saturation']['p50_ms']):.1f} | {med(lambda x: x['saturation']['p99_ms']):.1f}"
            f" | {cell('open_loop', lambda x: x['open_loop']['p50_ms'])} | {cell('open_loop', lambda x: x['open_loop']['p99_ms'])}"
            f" | {cell('open_loop', lambda x: x['open_loop'].get('e2e_p50_ms', float('nan')))}"
            f" | {cell('open_loop', lambda x: x['open_loop'].get('e2e_p99_ms', float('nan')))}"
            f" | {lost if 'drain' in runs[0] else 'n/a'} | {dup if 'drain' in runs[0] else 'n/a'} | {dlq if 'drain' in runs[0] else 'n/a'} |"
        )  # fmt: skip
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration", type=int, default=15, help="seconds per k6 phase")
    parser.add_argument("--vus", type=int, default=64, help="closed loop clients")
    parser.add_argument(
        "--rate", type=int, default=800, help="open loop requests per second"
    )
    parser.add_argument("--concurrency", type=int, default=64, help="cauli-worker -c")
    parser.add_argument("--arms", default=",".join(ARMS), help="comma separated subset")
    parser.add_argument("--out", type=Path, default=Path("web-results.jsonl"))
    args = parser.parse_args()
    arms = args.arms.split(",")
    unknown = set(arms) - set(ARMS)
    if unknown:
        parser.error(f"unknown arms: {sorted(unknown)}")

    logs = Path(tempfile.mkdtemp(prefix="cauli-web-"))
    r = redis.Redis.from_url(REDIS_URL)
    r.ping()
    results = []
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        pg.execute(SCHEMA_SQL)
        for rep in range(args.reps):
            order = arms[:]
            random.Random(rep).shuffle(order)
            for name in order:
                result = run_arm(name, args, pg, r, logs) | {"rep": rep}
                results.append(result)
                with args.out.open("a") as out:
                    out.write(json.dumps(result) + "\n")
                print(json.dumps(result), flush=True)
    print(f"\nlogs: {logs}\n")
    print(summarize(results))


if __name__ == "__main__":
    main()
