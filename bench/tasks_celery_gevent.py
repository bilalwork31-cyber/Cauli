"""No-op task, Celery's actual high-concurrency I/O answer: gevent pool,
`-c 1000` in one process. Every experienced Celery operator running
high-concurrency I/O reaches for this, not prefork -- benchmarking prefork
alone for Claims 1/2 would understate Celery's real memory story. Monkey
patching must happen before anything else imports socket/redis.

Run with: celery -A tasks_celery_gevent worker -P gevent -c 1000
"""

from gevent import monkey

monkey.patch_all()

import redis  # noqa: E402
from celery import Celery  # noqa: E402

from common import DONE_KEY, REDIS_URL  # noqa: E402

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
