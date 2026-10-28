"""WSGI and ASGI callables for gunicorn and uvicorn."""

import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "web.dj_settings")

from django.core.asgi import get_asgi_application  # noqa: E402
from django.core.wsgi import get_wsgi_application  # noqa: E402

wsgi = get_wsgi_application()
asgi = get_asgi_application()
