"""Shared config for the framework-throughput benchmark. See RESULTS.md."""

import os

REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://127.0.0.1:6395/0")
DONE_KEY = "bench:done"
# redis-py defaults to 100 connections per pool. Every slot above that raised
# MaxConnectionsError inside the task body, which once read as a worker stall.
REDIS_POOL_MAX = 4096
