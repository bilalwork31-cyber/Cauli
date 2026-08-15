"""No-op async task, cauli. Run with: cauli-worker -A bench.tasks_cauli_async:app -c ..."""

import redis.asyncio as aredis
from cauli import Cauli

from common import DONE_KEY, REDIS_URL

app = Cauli(redis_url=REDIS_URL)
_r = aredis.Redis.from_url(REDIS_URL)


@app.task(store_result=False, max_retries=0)
async def noop():
    await _r.incr(DONE_KEY)
