"""I/O-bound task, cauli sync: one real Postgres INSERT per task, through
Django's ORM instead of raw psycopg3 (see tasks_cauli_sync_pg.py for the
raw-driver baseline this is compared against) -- query building, model
instantiation and Django's own connection handling included, since that's
what actually runs in a Django app's task bodies.

Uses cauli's official `cauli.contrib.django.django_app()` integration, not
a bare `django.setup()` -- that matters here specifically: `django_app()`
installs `close_old_connections()` before/after every task (Celery-fixup
parity), which is exactly what an earlier version of this file lacked when
the connection-exhaustion finding in RESULTS.md's Claim 5 was first
measured. Kept here as the correct, representative integration rather than
the naive one, per that finding's own conclusion.

Run with: cauli-worker -A tasks_cauli_sync_django:app -c ...
"""

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_settings")
import django  # noqa: E402

django.setup()  # must run before importing djapp.models below

import redis  # noqa: E402
from cauli.contrib.django import django_app  # noqa: E402

from common import DONE_KEY, REDIS_URL  # noqa: E402
from djapp.models import BenchIo  # noqa: E402
from workloads import PG_PAYLOAD  # noqa: E402

app = django_app(redis_url=REDIS_URL)  # apps already ready; installs the DB hooks
_r = redis.Redis.from_url(REDIS_URL)


@app.task(store_result=False, max_retries=0)
def insert():
    BenchIo.objects.create(payload=PG_PAYLOAD)
    _r.incr(DONE_KEY)
