"""I/O-bound task, cauli sync: one real Postgres INSERT per task.
Run with: cauli-worker -A tasks_cauli_sync_pg:app -c ...

prepare_threshold=None disables psycopg3's automatic server-side prepared
statements: harmless direct-to-Postgres (~3% slower, measured), required
when BENCH_PG_DSN points at pgbouncer in transaction-pooling mode -- a
prepared statement lives on whichever backend connection created it, and
transaction pooling can hand the next query to a different one, which then
doesn't have it ("prepared statement ... does not exist"). Set
unconditionally so this file works identically either way.
"""

import redis
from cauli import Cauli
from psycopg_pool import ConnectionPool

from common import DONE_KEY, REDIS_URL
from workloads import PG_DSN, PG_INSERT_SQL, PG_PAYLOAD, PG_POOL_MAX

app = Cauli(redis_url=REDIS_URL)
_r = redis.Redis.from_url(REDIS_URL)
_pool = ConnectionPool(
    PG_DSN, min_size=2, max_size=PG_POOL_MAX, kwargs={"autocommit": True, "prepare_threshold": None}
)


@app.task(store_result=False, max_retries=0)
def insert():
    with _pool.connection() as conn:
        conn.execute(PG_INSERT_SQL, (PG_PAYLOAD,))
    _r.incr(DONE_KEY)
