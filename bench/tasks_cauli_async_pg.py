"""I/O-bound task, cauli async: one real Postgres INSERT per task, via
psycopg3's native asyncio pool. Run with:
cauli-worker -A tasks_cauli_async_pg:app -c ...
"""

import redis.asyncio as aredis
from cauli import Cauli
from psycopg_pool import AsyncConnectionPool

from common import DONE_KEY, REDIS_URL
from workloads import PG_DSN, PG_INSERT_SQL, PG_PAYLOAD, PG_POOL_MAX

app = Cauli(redis_url=REDIS_URL)
_r = aredis.Redis.from_url(REDIS_URL)
# prepare_threshold=None: see tasks_cauli_sync_pg.py -- required for
# pgbouncer transaction-pooling compatibility, ~3% cost direct-to-Postgres.
_pool = AsyncConnectionPool(
    PG_DSN,
    min_size=2,
    max_size=PG_POOL_MAX,
    kwargs={"autocommit": True, "prepare_threshold": None},
    open=False,
)


@app.task(store_result=False, max_retries=0)
async def insert():
    # open() is safe to call repeatedly on an already-open pool (its own
    # internal lock guards this); simplest way to open lazily on the
    # worker's asyncio loop instead of at import time, when no loop runs yet.
    await _pool.open()
    async with _pool.connection() as conn:
        await conn.execute(PG_INSERT_SQL, (PG_PAYLOAD,))
    await _r.incr(DONE_KEY)
