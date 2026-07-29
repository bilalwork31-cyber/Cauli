"""Minimal Django settings for the cauli Django integration tests.

Reads the throwaway Postgres/Redis endpoints from env vars set by the
test_django.py fixtures (and passed through to the spawned worker).
"""

import os

SECRET_KEY = "cauli-itest-not-a-secret"
USE_TZ = True
DEBUG = False

INSTALLED_APPS = ["django_site.dapp"]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("CAULI_ITEST_PG_DB", "cauli_itest"),
        "USER": os.environ.get("CAULI_ITEST_PG_USER", "cauli"),
        "PASSWORD": "",
        "HOST": os.environ.get("CAULI_ITEST_PG_HOST", "127.0.0.1"),
        "PORT": os.environ.get("CAULI_ITEST_PG_PORT", "54329"),
        # The two settings the DB lifecycle hooks exist to honor: persistent
        # connections (CONN_MAX_AGE) and stale-connection replacement
        # (CONN_HEALTH_CHECKS, checked by close_old_connections).
        "CONN_MAX_AGE": int(os.environ.get("CAULI_ITEST_CONN_MAX_AGE", "600")),
        "CONN_HEALTH_CHECKS": True,
        "OPTIONS": {
            # Lets tests count THIS process's connections in pg_stat_activity.
            "application_name": os.environ.get("CAULI_ITEST_APPNAME", "cauli-itest"),
        },
    }
}

# Read by cauli.contrib.django.django_app().
CAULI_REDIS_URL = os.environ.get("CAULI_REDIS_URL", "redis://127.0.0.1:6395/0")
CAULI_DEFAULT_QUEUE = "django"
CAULI_RESULT_TTL = 600
