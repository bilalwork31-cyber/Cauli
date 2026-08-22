"""I/O-bound task, Celery sync: same Django ORM insert as
tasks_cauli_sync_django.py, wired for Celery prefork -- the flagship
real-world pairing cauli wants to replace. Run with:
celery -A tasks_celery_django worker -c 4 -P prefork --prefetch-multiplier=1
"""

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_settings")
import django  # noqa: E402

django.setup()

import redis  # noqa: E402
from celery import Celery  # noqa: E402

from common import DONE_KEY, REDIS_URL  # noqa: E402
from djapp.models import BenchIo  # noqa: E402
from workloads import PG_PAYLOAD  # noqa: E402

app = Celery("fwbench", broker=REDIS_URL, backend=None)
app.conf.task_ignore_result = True
_r = redis.Redis.from_url(REDIS_URL)


@app.task(ignore_result=True)
def insert():
    BenchIo.objects.create(payload=PG_PAYLOAD)
    _r.incr(DONE_KEY)
