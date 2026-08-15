"""Enqueue N jobs onto a raw Redis list, no framework (drain-rate setup phase). See RESULTS.md."""

import asyncio
import sys
import time

import redis.asyncio as aredis

from common import REDIS_URL

QUEUE_KEY = "bench:raw:queue"


async def run(n):
    r = aredis.Redis.from_url(REDIS_URL)
    t0 = time.perf_counter()
    for _ in range(n):
        await r.lpush(QUEUE_KEY, b"1")
    dt = time.perf_counter() - t0
    print(f"enqueued {n} in {dt:.2f}s ({n / dt:.1f}/s)")
    await r.aclose()


def main():
    n = int(sys.argv[1])
    asyncio.run(run(n))


if __name__ == "__main__":
    main()
