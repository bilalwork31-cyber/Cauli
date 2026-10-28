"""Django settings for the web benchmark: the bench ORM settings plus what a
served project needs (URLs, hosts, no debug)."""

import os

from django_settings import *  # noqa: F403
from django_settings import DATABASES

DEBUG = False
ALLOWED_HOSTS = ["*"]
ROOT_URLCONF = "web.dj_views"
MIDDLEWARE: list[str] = []

# Under ASGI every request's sync ORM work runs on a fresh thread, so a
# persistent connection is never reused and the server climbed to 385 of 400
# Postgres backends in 15 s, starving the worker. Django's documentation says
# to disable persistent connections under ASGI; the campaign sets 0 there.
DATABASES["default"]["CONN_MAX_AGE"] = int(
    os.environ.get("BENCH_DJ_CONN_MAX_AGE", "60")
)
