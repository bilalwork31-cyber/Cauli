"""CPU-bound task, cauli, kind="cpu" (forked children -- true multicore).
Run with: cauli-worker -A tasks_cauli_cpu:app -c ...
"""

import redis
from cauli import Cauli

from common import DONE_KEY, REDIS_URL
from workloads import cpu_burn

app = Cauli(redis_url=REDIS_URL)
_r = redis.Redis.from_url(REDIS_URL)


@app.task(kind="cpu", store_result=False, max_retries=0)
def burn(ms):
    cpu_burn(ms)
    _r.incr(DONE_KEY)
