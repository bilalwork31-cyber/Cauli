"""Idempotent django.setup() for every entrypoint (worker apps, driver).

Import this FIRST in any module that touches the ORM. The cauli worker points
--app at a module that imports this before defining tasks, so the single cauli
process pays the full Django app import exactly once; every Celery fork pays
it per process.
"""

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "benchsite.settings")

import django  # noqa: E402
from django.apps import apps  # noqa: E402

if not apps.ready:
    django.setup()
