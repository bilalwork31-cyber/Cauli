"""Minimal Django settings -- just enough to run the ORM against the
existing bench Postgres role/db, no admin/auth/sessions/migrations.
"""

from urllib.parse import urlparse

from workloads import PG_DSN

_u = urlparse(PG_DSN)

SECRET_KEY = "bench-only-not-a-real-secret"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
INSTALLED_APPS = ["djapp"]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _u.path.lstrip("/"),
        "USER": _u.username,
        "PASSWORD": _u.password,
        "HOST": _u.hostname,
        "PORT": _u.port or 5432,
        # Django's own documented default recommendation for persistent
        # connections. 0 (close after every request/task) was the setting
        # used before this file went through cauli.contrib.django's
        # official close_old_connections() fixup -- with the fixup actually
        # running, 0 makes every task open a brand new connection, which is
        # correct but needlessly slow; a positive age lets the fixup do its
        # real job (evict genuinely stale connections) while reusing warm
        # ones in between.
        "CONN_MAX_AGE": 60,
    }
}
