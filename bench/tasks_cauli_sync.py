"""No-op sync task, cauli. Run with: cauli-worker -A bench.tasks_cauli_sync:app -c ..."""

import redis
from cauli import Cauli

from common import DONE_KEY, REDIS_URL

app = Cauli(redis_url=REDIS_URL)
_r = redis.Redis.from_url(REDIS_URL)


@app.task(store_result=False, max_retries=0)
def noop():
    _r.incr(DONE_KEY)
