"""I/O-bound task, dramatiq: one real Postgres INSERT per task.
Run with: python3 -m dramatiq tasks_dramatiq_pg --processes ... --threads ...
"""

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker
from psycopg_pool import ConnectionPool

from common import DONE_KEY, REDIS_URL
from workloads import PG_DSN, PG_INSERT_SQL, PG_PAYLOAD, PG_POOL_MAX

broker = RedisBroker(url=REDIS_URL)
dramatiq.set_broker(broker)
_r = redis.Redis.from_url(REDIS_URL)
# open=False + open() on first use: safe whether --processes forks or spawns
# (open() is idempotent -- see tasks_cauli_sync_pg.py).
_pool = ConnectionPool(PG_DSN, min_size=1, max_size=PG_POOL_MAX, kwargs={"autocommit": True}, open=False)


@dramatiq.actor
def insert():
    _pool.open()
    with _pool.connection() as conn:
        conn.execute(PG_INSERT_SQL, (PG_PAYLOAD,))
    _r.incr(DONE_KEY)
