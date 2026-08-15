"""Shared config for the framework-throughput benchmark. See RESULTS.md."""

import os

REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://127.0.0.1:6395/0")
DONE_KEY = "bench:done"
