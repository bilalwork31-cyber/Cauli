"""I/O-bound task, cauli sync: one real Postgres INSERT per task.
Run with: cauli-worker -A tasks_cauli_sync_pg:app -c ...
"""

import redis
from cauli import Cauli
from psycopg_pool import ConnectionPool

from common import DONE_KEY, REDIS_URL
from workloads import PG_DSN, PG_INSERT_SQL, PG_PAYLOAD, PG_POOL_MAX

app = Cauli(redis_url=REDIS_URL)
_r = redis.Redis.from_url(REDIS_URL)
_pool = ConnectionPool(PG_DSN, min_size=2, max_size=PG_POOL_MAX, kwargs={"autocommit": True})


@app.task(store_result=False, max_retries=0)
def insert():
    with _pool.connection() as conn:
        conn.execute(PG_INSERT_SQL, (PG_PAYLOAD,))
    _r.incr(DONE_KEY)
