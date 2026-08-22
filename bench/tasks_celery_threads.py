"""No-op task, Celery's `-P threads` pool: real OS threads instead of
gevent's cooperative greenlets or prefork's forked processes. No monkey
patching needed -- redis-py's socket calls release the GIL during the
actual syscall, so worker threads can genuinely overlap on I/O, and the
main consumer thread hands work to the pool without needing a blocking
call to coincide with a yield point (gevent's failure mode -- see
RESULTS.md Claim 2).

Run with: celery -A tasks_celery_threads worker -P threads -c 100
"""

import redis
from celery import Celery

from common import DONE_KEY, REDIS_URL

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)


@app.task(ignore_result=True)
def noop():
    _r.incr(DONE_KEY)


@app.task(ignore_result=True)
def hold():
    import time

    time.sleep(3600)
