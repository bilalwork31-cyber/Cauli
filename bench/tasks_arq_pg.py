"""I/O-bound task, arq: one real Postgres INSERT per task.
Run with: arq tasks_arq_pg:WorkerSettings
"""

import redis.asyncio as aredis
from arq.connections import RedisSettings
from psycopg_pool import AsyncConnectionPool

from common import DONE_KEY, REDIS_URL
from workloads import PG_DSN, PG_INSERT_SQL, PG_PAYLOAD, PG_POOL_MAX

_r = aredis.Redis.from_url(REDIS_URL)
redis_settings = RedisSettings.from_dsn(REDIS_URL)
_pool = AsyncConnectionPool(
    PG_DSN, min_size=2, max_size=PG_POOL_MAX, kwargs={"autocommit": True}, open=False
)


async def insert(ctx):
    await _pool.open()  # safe to call repeatedly; see tasks_cauli_async_pg.py
    async with _pool.connection() as conn:
        await conn.execute(PG_INSERT_SQL, (PG_PAYLOAD,))
    await _r.incr(DONE_KEY)


class WorkerSettings:
    functions = [insert]
    redis_settings = redis_settings
    # See tasks_arq.py: default poll_delay=0.5s/max_jobs=10 caps throughput
    # far below what the Postgres pool itself can do.
    poll_delay = 0.01
    max_jobs = 200
