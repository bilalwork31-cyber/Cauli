"""Canonical benchmark workloads. Imported by BOTH stacks (Celery and cauli).

Exactly one implementation per workload so the comparison measures the two
runtimes, not two different task bodies.

Fairness notes:
- io_call() uses plain requests.get (new connection per call) on both stacks.
- io_call_async() is the cauli async variant: a minimal HTTP/1.1 keepalive
  pool on raw asyncio streams, one pool per event loop (see the deviation
  note at the pool class below for why it is not httpx).
- cpu_call() is pure hashlib C code (pbkdf2_hmac holds one core busy either
  way) and is identical on both stacks.
"""

import asyncio
import hashlib
import os
from urllib.parse import urlsplit

import requests

MOCK_API = os.environ.get("BENCH_MOCK_API", "http://127.0.0.1:8077/io")

# PBKDF2 iterations calibrated with calibrate.py on the benchmark machine
# (WSL2 Ubuntu-24.04, 6 cores, Python 3.12.3, hashlib/OpenSSL pbkdf2_hmac).
# Measured 2026-07-20: 94000 iterations = 51.0 ms median per call (target 50 ms,
# ~19.6 calls/sec/core; medians of 3x15 reps: 50.9/52.0/51.0 ms). Re-run
# calibrate.py and update this constant if the benchmark machine changes.
#
# Overridable via BENCH_CPU_ITER so a scenario can sweep TASK SIZE, which is
# its own regime axis: at ~51 ms per task the runtime's per-task overhead is
# ~2% of the work and both stacks converge on being core bound, so that single
# size cannot distinguish their dispatch costs at all. Shrinking the task makes
# per-task overhead the dominant term. BOTH stacks import this module, so any
# size stays exactly as apples-to-apples as the default.
# Reference points on this machine: 94000 = 51 ms, 3700 = ~2 ms, 920 = ~0.5 ms.
CPU_ITER = int(os.environ.get("BENCH_CPU_ITER", "94000"))


def io_call() -> int:
    """Sync IO workload: one HTTP GET against the local mock API (50ms delay).

    Deliberately plain requests.get, no Session: identical cost on both stacks.
    """
    r = requests.get(MOCK_API, timeout=10)
    return r.status_code


def cpu_call() -> str:
    """CPU workload: PBKDF2 calibrated to ~50ms of single core work.

    Returns 8 hex chars (JSON serializable, same tiny result on both stacks).
    """
    d = hashlib.pbkdf2_hmac("sha256", b"password", b"salt", CPU_ITER)
    return d[:4].hex()


# ---------------------------------------------------------------------------
# Async IO variant (used only by the cauli async task).
#
# DELIBERATE DEVIATION, measured on this machine (2026-07-20): the originally
# planned module level httpx.AsyncClient tops out near 300 requests/sec of
# client side event loop CPU and ANTI scales with concurrency (460 rps at 1 in
# flight, 143 rps at 100, 292 rps even sharded over 20 clients). Using it
# would benchmark the httpx library, not the worker runtime. This minimal
# HTTP/1.1 keepalive pool on raw asyncio streams costs ~0.15ms of loop CPU per
# call and was measured at 5099 rps with 300 in flight against the 50ms
# endpoint (8465 rps against ms=0), so the runtime under test remains the
# bottleneck. One pool per running event loop, connections reused LIFO,
# created on demand (in flight count is gated by cauli --io-concurrency).
# ---------------------------------------------------------------------------
class _AsyncHTTPPool:
    """Minimal HTTP/1.1 GET client with keepalive connection reuse."""

    def __init__(self, url: str):
        u = urlsplit(url)
        self.host = u.hostname or "127.0.0.1"
        self.port = u.port or 80
        path = (u.path or "/") + (("?" + u.query) if u.query else "")
        self._req = (
            f"GET {path} HTTP/1.1\r\nhost: {self.host}:{self.port}\r\n"
            f"connection: keep-alive\r\n\r\n"
        ).encode()
        self._idle = []

    async def _open(self):
        return await asyncio.open_connection(self.host, self.port)

    async def _roundtrip(self, conn) -> int:
        reader, writer = conn
        writer.write(self._req)
        await writer.drain()
        line = await reader.readline()
        if not line.startswith(b"HTTP/1.1 "):
            raise ConnectionError(f"bad status line: {line!r}")
        status = int(line.split(b" ", 2)[1])
        clen = 0
        while True:
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
            if h.lower().startswith(b"content-length:"):
                clen = int(h.split(b":", 1)[1])
        if clen:
            await reader.readexactly(clen)
        return status

    @staticmethod
    def _close(conn) -> None:
        try:
            conn[1].close()
        except Exception:
            pass

    async def get(self, timeout: float = 10.0) -> int:
        reused = bool(self._idle)
        conn = self._idle.pop() if reused else await self._open()
        try:
            status = await asyncio.wait_for(self._roundtrip(conn), timeout)
        except Exception:
            self._close(conn)
            if not reused:
                raise
            # pooled connection went stale (server keepalive close); one retry
            conn = await self._open()
            try:
                status = await asyncio.wait_for(self._roundtrip(conn), timeout)
            except Exception:
                self._close(conn)
                raise
        self._idle.append(conn)
        return status


_async_pools = {}


async def io_call_async() -> int:
    loop = asyncio.get_running_loop()
    pool = _async_pools.get(id(loop))
    if pool is None:
        pool = _AsyncHTTPPool(MOCK_API)
        _async_pools[id(loop)] = pool
    return await pool.get()
