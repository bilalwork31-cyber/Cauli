"""Settings and task bodies shared by the harness and every framework's app.

Kept import light on purpose: every worker process under test imports this,
and the memory benchmark measures those processes.
"""

import os
import time

REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://127.0.0.1:6395/0")
PG_DSN = os.environ.get("BENCH_PG_DSN", "postgresql://bench:bench@127.0.0.1:5432/bench")

DONE_KEY = "bench:done"
STARTED_KEY = "bench:started"
EXEC_KEY = "bench:executions"
# redis-py caps a pool at 100 connections; past that a task body raises
# MaxConnectionsError, which reads as a worker stall.
REDIS_POOL_MAX = 4096
HOLD_S = 3600

# The web benchmark: one web_job row per request, its task bumps processed.
WEB_LATENCY_KEY = "web:e2e_ns"
WEB_PAYLOAD = "x" * 200


def cpu_burn(ms: float) -> int:
    """Hold the CPU, and the GIL, for ``ms`` milliseconds of wall time, so a
    task size means the same thing on every machine."""
    deadline = time.perf_counter() + ms / 1000
    x = 0
    while time.perf_counter() < deadline:
        x = (x * 1103515245 + 12345) & 0x7FFFFFFF
    return x
