"""I/O-bound task, Celery's best shot at this workload: gevent pool +
psycogreen (patches psycopg2's C extension to yield to other greenlets
during a blocking query instead of blocking the whole process), so many
in-flight queries share one process the way cauli's sync IO lane does.
Benchmarking prefork for I/O concurrency (see tasks_celery_pg_prefork.py) is
the honest baseline, not the comparison Celery should be judged on.

Monkey-patching must happen before anything else imports socket/ssl/psycopg2.
Run with: celery -A tasks_celery_pg_gevent worker -P gevent -c ...
"""

from gevent import monkey

monkey.patch_all()
from psycogreen.gevent import patch_psycopg  # noqa: E402

patch_psycopg()

import psycopg2  # noqa: E402
import psycopg2.pool  # noqa: E402
import redis  # noqa: E402
from celery import Celery  # noqa: E402

from common import DONE_KEY, REDIS_URL  # noqa: E402
from workloads import PG_DSN, PG_PAYLOAD  # noqa: E402

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)

# -P gevent still runs one OS process (no fork per task), so a plain
# module-level pool is fine here -- unlike prefork, there is no fork
# happening after this module is imported.
_pg_pool = psycopg2.pool.ThreadedConnectionPool(2, 100, PG_DSN)


@app.task(ignore_result=True)
def insert():
    conn = _pg_pool.getconn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("INSERT INTO bench_io (payload) VALUES (%s)", (PG_PAYLOAD,))
    finally:
        _pg_pool.putconn(conn)
    _r.incr(DONE_KEY)
