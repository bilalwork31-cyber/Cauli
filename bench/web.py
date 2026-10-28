"""Enqueue inside real Django and FastAPI requests under k6: request rate, end
to end task latency, and whether every request's task ran exactly once.

Per arm and rep, on a freshly truncated table: closed loop saturation with no
worker running, then a drain with the worker started, then an open loop at a
fixed rate with the worker live. Arm order is shuffled every rep.
"""

import argparse
import json
import os
import random
import statistics
import subprocess
import tempfile
import time
import urllib.request

import psycopg

from bench import PG_DSN, REDIS_URL, WEB_LATENCY_KEY
from bench.runner import CAULI_WORKER, PYTHON, Worker, connect, log_path

PORT = int(os.environ.get("BENCH_WEB_PORT", "8100"))
K6 = os.environ.get("K6_BIN", "k6")
WEB_PROCS = "4"
READY_TIMEOUT_S = 30
# A drain with no progress for this long is lost work, not slowness: a task
# that dead letters never completes.
STALL_S = 15
SETTLE_S = 2
SCHEMA_SQL = (
    "CREATE TABLE IF NOT EXISTS web_job (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL,"
    " processed INTEGER NOT NULL DEFAULT 0)"
)
# MODE=vus is closed loop saturation; MODE=rate is open loop, so a slow server
# shows up as latency and dropped iterations instead of a lower load.
K6_SCRIPT = """
import http from "k6/http";
const scenario = __ENV.MODE === "rate"
  ? { executor: "constant-arrival-rate", rate: Number(__ENV.RATE), timeUnit: "1s", duration: __ENV.DURATION,
      preAllocatedVUs: 200, maxVUs: 2000 }
  : { executor: "constant-vus", vus: Number(__ENV.VUS), duration: __ENV.DURATION };
export const options = { scenarios: { load: scenario }, summaryTrendStats: ["p(50)", "p(99)", "max"],
  discardResponseBodies: true };
export default function () { http.request(__ENV.METHOD, __ENV.URL); }
"""

GUNICORN = [
    PYTHON,
    "-m",
    "gunicorn",
    "bench.apps.django_site:wsgi",
    "-k",
    "gthread",
    "-w",
    WEB_PROCS,
    "--threads",
    "8",
    "-b",
    f"127.0.0.1:{PORT}",
]
UVICORN = [
    PYTHON,
    "-m",
    "uvicorn",
    "--workers",
    WEB_PROCS,
    "--loop",
    "uvloop",
    "--http",
    "httptools",
    "--no-access-log",
    "--log-level",
    "warning",
    "--host",
    "127.0.0.1",
    "--port",
    str(PORT),
]
DJANGO = "bench.apps.django_site:app"
FASTAPI = "bench.apps.fastapi_site:app"
ASGI_ENV = {"BENCH_DJ_CONN_MAX_AGE": "0"}
# arm -> (server command, server env, method, path, cauli app or None for no worker)
ARMS = {
    "dj_wsgi_insert": (GUNICORN, {}, "GET", "/insert", None),
    "dj_wsgi_cauli": (GUNICORN, {}, "GET", "/cauli", DJANGO),
    "dj_wsgi_cauli_async_task": (GUNICORN, {}, "GET", "/cauli_async_task", DJANGO),
    "dj_wsgi_celery": (GUNICORN, {}, "GET", "/celery", None),
    "dj_asgi_insert": (
        [*UVICORN, "bench.apps.django_site:asgi"],
        ASGI_ENV,
        "GET",
        "/ainsert",
        None,
    ),
    "dj_asgi_cauli": (
        [*UVICORN, "bench.apps.django_site:asgi"],
        ASGI_ENV,
        "GET",
        "/acauli",
        DJANGO,
    ),
    "fa_insert": (
        [*UVICORN, "bench.apps.fastapi_site:api"],
        {},
        "POST",
        "/insert",
        None,
    ),
    "fa_cauli": (
        [*UVICORN, "bench.apps.fastapi_site:api"],
        {},
        "POST",
        "/cauli",
        FASTAPI,
    ),
    "fa_cauli_blocking": (
        [*UVICORN, "bench.apps.fastapi_site:api"],
        {},
        "POST",
        "/cauli_blocking",
        FASTAPI,
    ),
}


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
    with tempfile.TemporaryDirectory() as tmp:
        summary = os.path.join(tmp, "summary.json")
        env = dict(
            os.environ,
            METHOD=method,
            URL=f"http://127.0.0.1:{PORT}{path}",
            MODE=mode,
            VUS=str(args.vus),
            RATE=str(args.rate),
            DURATION=f"{args.duration}s",
        )
        subprocess.run(
            [K6, "run", "--quiet", "--summary-export", summary, "-"],
            input=K6_SCRIPT,
            text=True,
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        with open(summary) as f:
            metrics = json.load(f)["metrics"]
    duration = metrics["http_req_duration"]
    return {
        "rps": metrics["http_reqs"]["rate"],
        "failed": metrics["http_req_failed"]["passes"],
        "dropped": metrics.get("dropped_iterations", {}).get("count", 0),
        "p50_ms": duration["p(50)"],
        "p99_ms": duration["p(99)"],
    }


def job_counts(pg: psycopg.Connection) -> dict:
    rows, lost, duplicated = pg.execute(
        "SELECT count(*), count(*) FILTER (WHERE processed = 0), count(*) FILTER (WHERE processed > 1) FROM web_job"
    ).fetchone()
    return {"rows": rows, "lost": lost, "duplicated": duplicated}


def wait_drained(pg: psycopg.Connection, r) -> dict:
    last_progress = time.monotonic()
    counts = job_counts(pg)
    while counts["lost"] and time.monotonic() - last_progress < STALL_S:
        time.sleep(0.25)
        previous, counts = counts["lost"], job_counts(pg)
        if counts["lost"] < previous:
            last_progress = time.monotonic()
    counts["dead_letters"] = sum(
        r.xlen(k) for k in r.scan_iter("cauli:dlq*") if r.type(k) == b"stream"
    )
    return counts


def e2e_ms(samples_ns: list[int]) -> dict:
    if not samples_ns:
        return {}
    ordered = sorted(samples_ns)
    return {
        f"e2e_p{q}_ms": ordered[min(len(ordered) - 1, len(ordered) * q // 100)] / 1e6
        for q in (50, 99)
    }


def run_arm(name: str, args: argparse.Namespace, pg: psycopg.Connection, r) -> dict:
    server_cmd, server_env, method, path, worker_app = ARMS[name]
    pg.execute("TRUNCATE web_job RESTART IDENTITY")
    r.flushall()
    with Worker(f"{name} server", [server_cmd], log_path("web-server"), env=server_env):
        wait_ready()
        out = {"arm": name, "saturation": k6(method, path, "vus", args)}
        if worker_app is None:
            return out
        worker_cmd = [
            CAULI_WORKER,
            "-A",
            worker_app,
            "--redis-url",
            REDIS_URL,
            "-c",
            str(args.concurrency),
        ]
        with Worker(f"{name} worker", [worker_cmd], log_path("web-worker")):
            out["drain"] = wait_drained(pg, r)
            r.delete(WEB_LATENCY_KEY)
            time.sleep(SETTLE_S)
            out["open_loop"] = k6(method, path, "rate", args)
            out["open_drain"] = wait_drained(pg, r)
            out["open_loop"] |= e2e_ms(
                [int(x) for x in r.lrange(WEB_LATENCY_KEY, 0, -1)]
            )
    return out


def cell(runs: list[dict], phase: str, key: str) -> str:
    values = [x[phase][key] for x in runs if key in x.get(phase, {})]
    return f"{statistics.median(values):,.1f}" if values else ""


def summarize(results: list[dict]) -> str:
    lines = [
        "| arm | sat req/s | sat p99 ms | open p50 ms | open p99 ms | e2e p50 ms | e2e p99 ms | lost | dup | dlq |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm in ARMS:
        runs = [x for x in results if x["arm"] == arm]
        if not runs:
            continue
        checks = ["", "", ""]
        if "drain" in runs[0]:
            drains = [x[d] for x in runs for d in ("drain", "open_drain")]
            checks = [
                str(sum(d[k] for d in drains))
                for k in ("lost", "duplicated", "dead_letters")
            ]
        lines.append(
            f"| {arm} | {cell(runs, 'saturation', 'rps')} | {cell(runs, 'saturation', 'p99_ms')}"
            f" | {cell(runs, 'open_loop', 'p50_ms')} | {cell(runs, 'open_loop', 'p99_ms')}"
            f" | {cell(runs, 'open_loop', 'e2e_p50_ms')} | {cell(runs, 'open_loop', 'e2e_p99_ms')}"
            f" | {' | '.join(checks)} |"
        )
    return "\n".join(lines)


def run(args: argparse.Namespace) -> None:
    unknown = set(args.arms) - set(ARMS)
    if unknown:
        raise SystemExit(
            f"unknown arms {sorted(unknown)}, expected some of {list(ARMS)}"
        )
    r = connect()
    results = []
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        pg.execute(SCHEMA_SQL)
        for rep in range(args.reps):
            order = list(args.arms)
            random.Random(rep).shuffle(order)
            for name in order:
                results.append(run_arm(name, args, pg, r))
                print(json.dumps(results[-1]), flush=True)
    print()
    print(summarize(results))


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--duration", type=int, default=15, help="seconds per k6 phase")
    parser.add_argument("--vus", type=int, default=64, help="closed loop clients")
    parser.add_argument(
        "--rate", type=int, default=500, help="open loop requests per second"
    )
    parser.add_argument("--concurrency", type=int, default=64, help="cauli-worker -c")
    parser.add_argument(
        "--arms",
        type=lambda s: s.split(","),
        default=list(ARMS),
        help="comma separated subset",
    )
