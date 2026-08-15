"""Segfault blast-radius test, cauli async, NAIVE: `segfault` crashes the
SAME process running every `hold` task. If cauli's single-process model has
a shared-fate problem, this is where it shows up hardest -- a crash should
kill every in-flight `hold` task along with it. See CLAIMS.md #4 and
tasks_cauli_async_segfault_fixed.py (the correct, isolated version).

Run with: cauli-worker -A tasks_cauli_async_segfault:app -c ...
"""

import asyncio
import ctypes

from cauli import Cauli

from common import REDIS_URL

app = Cauli(redis_url=REDIS_URL)


@app.task(store_result=False, max_retries=0, timeout=3600)
async def hold():
    await asyncio.sleep(3600)


@app.task(store_result=False, max_retries=0)
async def segfault():
    ctypes.string_at(0)  # reads from a null pointer -- reliably segfaults
