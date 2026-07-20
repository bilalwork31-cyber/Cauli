"""Sanity checks for fake_graph.py (verification step 1).

  1. latency + error injection: 400 POSTs at concurrency 40; per-request
     latency must sit in the 200-500ms window (+client overhead), injected
     errors ~2% (accept 0.25%-6% at n=400), split between 500 and 429.
  2. burst throughput: 4000 POSTs at concurrency 1000 must exceed 2000 rps
     (latency-bound floor is conc/0.5s = 2000; CPU headroom shows above it).

Usage: python verify_fake_graph.py   (FAKE_GRAPH_URL env, default :8078)
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from graph_async import AsyncGraphPool  # noqa: E402

BASE = os.environ.get("FAKE_GRAPH_URL", "http://127.0.0.1:8078")


async def one(pool, sem, out):
    async with sem:
        t = time.perf_counter()
        try:
            status = await pool.post_json(
                {"campaign_id": "v", "recipient_id": "r", "page_id": "p"},
                timeout=30.0)
        except Exception as e:
            out.append((None, time.perf_counter() - t, repr(e)))
            return
        out.append((status, time.perf_counter() - t, None))


async def run(n, conc):
    # one pool per <conc> is fine; pool grows connections on demand
    pool = AsyncGraphPool(BASE)
    sem = asyncio.Semaphore(conc)
    out = []
    t0 = time.perf_counter()
    await asyncio.gather(*[one(pool, sem, out) for _ in range(n)])
    wall = time.perf_counter() - t0
    return out, wall


def main():
    fail = 0

    out, wall = asyncio.run(run(400, 40))
    lats = [dt for st, dt, err in out if st is not None]
    st200 = sum(1 for st, _, _ in out if st == 200)
    st500 = sum(1 for st, _, _ in out if st == 500)
    st429 = sum(1 for st, _, _ in out if st == 429)
    errs = st500 + st429
    excs = sum(1 for st, _, _ in out if st is None)
    lats.sort()
    lo, hi = lats[0], lats[-1]
    p50 = lats[len(lats) // 2]
    print(f"[verify_fake_graph] latency check: n=400 conc=40 wall={wall:.1f}s "
          f"min={lo*1000:.0f}ms p50={p50*1000:.0f}ms max={hi*1000:.0f}ms")
    print(f"[verify_fake_graph] statuses: 200={st200} 500={st500} 429={st429} "
          f"exceptions={excs} error_rate={errs/400:.3f}")
    if not (0.190 <= lo and hi <= 0.900):
        print("FAIL: latency outside 200-500ms window (+overhead)")
        fail = 1
    if not (1 <= errs <= 24):
        print("FAIL: injected error count outside 0.25%-6% band")
        fail = 1
    if excs:
        print("FAIL: transport exceptions during latency check")
        fail = 1

    out, wall = asyncio.run(run(4000, 1000))
    ok = sum(1 for st, _, _ in out if st is not None)
    rps = ok / wall
    print(f"[verify_fake_graph] burst: n=4000 conc=1000 wall={wall:.2f}s "
          f"rps={rps:.0f} completed={ok}")
    if rps < 2000:
        print("FAIL: burst below 2000 rps")
        fail = 1

    print("[verify_fake_graph] PASS" if fail == 0 else "[verify_fake_graph] FAIL")
    return fail


if __name__ == "__main__":
    sys.exit(main())
