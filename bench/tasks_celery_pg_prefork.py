"""I/O-bound task, Celery prefork baseline: one real Postgres INSERT per
task, one OS process per in-flight task (the strawman this suite calls out
explicitly -- see tasks_celery_pg_gevent.py for Celery's actual best shot at
this workload). Run with: celery -A tasks_celery_pg_prefork worker -P prefork -c ...
"""

import redis
from celery import Celery
from celery.signals import worker_process_init
from psycopg_pool import ConnectionPool

from common import DONE_KEY, REDIS_URL
from workloads import PG_DSN, PG_INSERT_SQL, PG_PAYLOAD

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)

# open=False: the pool must not open connections in the master process --
# prefork children would inherit those sockets across fork() and corrupt
# them. Opened for real in worker_process_init, which fires once per child
# AFTER it forks, not at module import time (which runs in the master).
_pool = ConnectionPool(PG_DSN, min_size=1, max_size=4, kwargs={"autocommit": True}, open=False)


@worker_process_init.connect
def _open_pool(**kwargs):
    _pool.open()


@app.task(ignore_result=True)
def insert():
    with _pool.connection() as conn:
        conn.execute(PG_INSERT_SQL, (PG_PAYLOAD,))
    _r.incr(DONE_KEY)
