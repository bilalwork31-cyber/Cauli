"""Segfault blast-radius test, Celery prefork: `segfault` crashes only the
ONE OS process handling it -- expected to be naturally resilient, each
task already runs in its own process. See CLAIMS.md #4.

Run with: celery -A tasks_celery_segfault worker -P prefork -c ...
"""

import ctypes
import time

from celery import Celery

from common import REDIS_URL

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True


@app.task(ignore_result=True)
def hold():
    time.sleep(3600)


@app.task(ignore_result=True)
def segfault():
    ctypes.string_at(0)  # reads from a null pointer -- reliably segfaults
