"""Verify mock_api.py is never the benchmark bottleneck: must sustain >2000 rps.

Uses common._AsyncHTTPPool, the exact client the rupy async task uses, so this
verifies both the server capacity AND the benchmark's async client path:
  phase 1: /io       (50ms simulated latency), 300 concurrent  -> must be >2000 rps
  phase 2: /io?ms=0  (no sleep), 100 concurrent                -> raw ceiling
  phase 3: reference only: single httpx.AsyncClient, 100 concurrent. This is
           the client the harness deliberately does NOT use for the async
           workload (httpx tops out around 300 rps of client side loop CPU on
           this machine, see common.py deviation note).

PASS requires phase 1 > 2000 rps. Exit code 0 on pass, 1 on fail.

Run (mock_api.py must already be up):  python verify_api.py
"""
import asyncio
import sys
import time

from common import _AsyncHTTPPool

BASE = "http://127.0.0.1:8077/io"


async def pool_phase(name: str, url: str, conc: int, seconds: float) -> float:
    pool = _AsyncHTTPPool(url)

    async def worker(stop_at: float, counter: list) -> None:
        while time.perf_counter() < stop_at:
            status = await pool.get()
            if status != 200:
                raise RuntimeError(f"mock api returned {status}")
            counter[0] += 1

    # connection warmup, not measured
    await asyncio.gather(*(pool.get() for _ in range(min(conc, 100))))
    counter = [0]
    t0 = time.perf_counter()
    await asyncio.gather(*(worker(t0 + seconds, counter) for _ in range(conc)))
    dt = time.perf_counter() - t0
    rps = counter[0] / dt
    print(f"{name}: {counter[0]} requests in {dt:.1f}s -> {rps:.0f} rps")
    return rps


async def httpx_reference(conc: int, seconds: float) -> float:
    import httpx

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=conc + 10, max_keepalive_connections=conc + 10),
        timeout=10.0,
    ) as client:
        counter = [0]

        async def worker(stop_at: float) -> None:
            while time.perf_counter() < stop_at:
                r = await client.get(BASE + "?ms=0")
                assert r.status_code == 200
                counter[0] += 1

        t0 = time.perf_counter()
        await asyncio.gather(*(worker(t0 + seconds) for _ in range(conc)))
        dt = time.perf_counter() - t0
        rps = counter[0] / dt
        print(f"phase3 httpx reference (NOT used by the harness), {conc} conc: "
              f"{counter[0]} requests in {dt:.1f}s -> {rps:.0f} rps")
        return rps


async def main() -> int:
    r1 = await pool_phase("phase1 /io 50ms, 300 conc", BASE, 300, 12.0)
    r2 = await pool_phase("phase2 /io?ms=0 raw, 100 conc", BASE + "?ms=0", 100, 8.0)
    try:
        await httpx_reference(100, 5.0)
    except ImportError:
        print("phase3 httpx reference skipped (httpx not installed)")
    ok = r1 > 2000.0
    print(f"RESULT: {'PASS' if ok else 'FAIL'} "
          f"(phase1 {r1:.0f} rps, need >2000; raw ceiling {r2:.0f} rps)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
