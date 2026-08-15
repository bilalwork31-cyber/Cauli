"""Long-sleeping task, cauli async: stays in-flight so RSS/PSS can be
measured at a known steady concurrency. Run with:
cauli-worker -A tasks_cauli_async_hold:app -c ...
"""

import asyncio

from cauli import Cauli

from common import REDIS_URL

app = Cauli(redis_url=REDIS_URL)


@app.task(store_result=False, max_retries=0, timeout=3600)
async def hold():
    await asyncio.sleep(3600)
