"""Raw asyncio + redis worker, no framework: the throughput ceiling every
lane in this suite is bounded by. No serialization, no retries, no acks.
See RESULTS.md.

batch=1 (default) is one BRPOP + one INCR per task -- naive, and NOT a fair
ceiling on its own: cauli's own fetch loop batches reads (--batch 16
default), so an unbatched "ceiling" can end up slower than the frameworks
it's meant to bound. batch>1 uses `LPOP key count` (bulk pop, Redis 6.2+) and
one INCRBY per batch instead of one INCR per task, matching that technique.

Usage: python3 raw_asyncio_worker.py [concurrency] [batch]
"""

import asyncio
import sys

import redis.asyncio as aredis

from common import DONE_KEY, REDIS_URL

QUEUE_KEY = "bench:raw:queue"


async def consumer(r):
    while True:
        item = await r.brpop(QUEUE_KEY, timeout=1)
        if item is None:
            continue
        await r.incr(DONE_KEY)


async def consumer_batched(r, batch_size):
    while True:
        items = await r.lpop(QUEUE_KEY, batch_size)
        if not items:
            await asyncio.sleep(0.001)
            continue
        await r.incrby(DONE_KEY, len(items))


async def run(concurrency, batch_size):
    r = aredis.Redis.from_url(REDIS_URL, max_connections=concurrency + 8)
    if batch_size <= 1:
        await asyncio.gather(*(consumer(r) for _ in range(concurrency)))
    else:
        await asyncio.gather(*(consumer_batched(r, batch_size) for _ in range(concurrency)))


def main():
    concurrency = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    asyncio.run(run(concurrency, batch_size))


if __name__ == "__main__":
    main()
