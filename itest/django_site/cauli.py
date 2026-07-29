"""The documented cauli-in-Django setup path, exercised for real by the tests:

    app = django_app(...)      # settings-driven config + DB lifecycle hooks
    autodiscover_tasks(app)    # imports <app>.tasks across INSTALLED_APPS

The worker runs `cauli-worker --app django_site.cauli:app`.
"""

import os
import threading

from cauli.contrib.django import autodiscover_tasks, django_app

app = django_app("django_site.settings")
autodiscover_tasks(app)

# Extra file-marker hooks so test_django.py can PROVE the lifecycle hooks fire
# in the same thread/process as the task itself, on all three execution paths
# (sync pool thread, asyncio loop thread, forked cpu child).
_hooklog = os.environ.get("CAULI_ITEST_HOOKLOG")
if _hooklog:

    def _mark(phase: str) -> None:
        with open(_hooklog, "a") as f:
            f.write(f"{phase} {os.getpid()} {threading.get_ident()}\n")

    @app.before_task
    def _mark_before() -> None:
        _mark("before")

    @app.after_task
    def _mark_after() -> None:
        _mark("after")
