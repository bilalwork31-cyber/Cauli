"""WSGI config for the django_real benchmark site."""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "benchsite.settings")

application = get_wsgi_application()
