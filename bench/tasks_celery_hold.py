"""Long-sleeping task, Celery: stays in-flight so RSS/PSS can be measured
at a known steady concurrency. Run with: celery -A tasks_celery_hold worker -P prefork -c ...
"""

import time

from celery import Celery

from common import REDIS_URL

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True


@app.task(ignore_result=True)
def hold():
    time.sleep(3600)
