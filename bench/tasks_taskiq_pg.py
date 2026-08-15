"""I/O-bound task, taskiq: one real Postgres INSERT per task.
Run with: taskiq worker tasks_taskiq_pg:broker --workers ... --max-async-tasks ...
"""

import redis.asyncio as aredis
from psycopg_pool import AsyncConnectionPool
from taskiq_redis import ListQueueBroker

from common import DONE_KEY, REDIS_URL
from workloads import PG_DSN, PG_INSERT_SQL, PG_PAYLOAD, PG_POOL_MAX

broker = ListQueueBroker(url=REDIS_URL, queue_name="fwbench_pg")
_r = aredis.Redis.from_url(REDIS_URL)
_pool = AsyncConnectionPool(
    PG_DSN, min_size=2, max_size=PG_POOL_MAX, kwargs={"autocommit": True}, open=False
)


@broker.task
async def insert():
    await _pool.open()  # safe to call repeatedly; see tasks_cauli_async_pg.py
    async with _pool.connection() as conn:
        await conn.execute(PG_INSERT_SQL, (PG_PAYLOAD,))
    await _r.incr(DONE_KEY)
