"""Shared task bodies, imported by every framework's tasks_*.py so the exact
same work runs under every stack. See CLAIMS.md for which claim each maps to.
"""

import os
import time


def cpu_burn(ms):
    """Hold the CPU (and the GIL, if any) for approximately `ms` milliseconds.

    A perf_counter busy-wait rather than a fixed iteration count: makes the
    duration reproducible across machines instead of CPU-speed-dependent.
    """
    deadline = time.perf_counter() + ms / 1000.0
    x = 0
    while time.perf_counter() < deadline:
        x = (x * 1103515245 + 12345) & 0x7FFFFFFF
    return x


PG_DSN = os.environ.get("BENCH_PG_DSN", "postgresql://bench:bench@127.0.0.1:5432/bench")
PG_INSERT_SQL = "INSERT INTO bench_io (payload) VALUES (%s)"
PG_PAYLOAD = "x" * 200
# Per WORKER PROCESS, not per fleet. Every lane here builds its pool inside
# each process, so the real connection count is `procs x PG_POOL_MAX` and it is
# Postgres's max_connections (400 on the bench box, 3 reserved) that binds.
# cauli's own contrib/sqlalchemy.py states the same rule for user code. It is
# env overridable so a lane running many processes can size its share down
# without changing the single process lanes, which is what the SQLAlchemy
# comparison needs: 8 processes x 100 would ask for 800.
PG_POOL_MAX = int(os.environ.get("BENCH_PG_POOL_MAX", "100"))

# Adversarial mixed workload (CLAIMS.md #4): a calibrated busy-loop stands in
# for "parse a 500KB JSON body after an await" -- same GIL-holding effect,
# more reproducible timing than depending on a real parser's throughput.
POISON_BURST_MS = 50

