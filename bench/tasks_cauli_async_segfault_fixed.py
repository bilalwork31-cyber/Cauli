"""Segfault blast-radius test, cauli async, FIXED: `segfault` is kind="cpu"
-- isolated in a forked child. Contrast against tasks_cauli_async_segfault.py:
if the `hold` tasks survive here, the naive version's blast radius was a
routing mistake (running risky/unstable code on the shared process), not an
unfixable architectural flaw. See CLAIMS.md #4.

Run with: cauli-worker -A tasks_cauli_async_segfault_fixed:app -c ...
"""

import asyncio
import ctypes

from cauli import Cauli

from common import REDIS_URL

app = Cauli(redis_url=REDIS_URL)


@app.task(store_result=False, max_retries=0, timeout=3600)
async def hold():
    await asyncio.sleep(3600)


@app.task(kind="cpu", store_result=False, max_retries=0)
def segfault():
    ctypes.string_at(0)  # reads from a null pointer -- reliably segfaults
