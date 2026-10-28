"""Tasks for bench/free_threaded.py: the same CPU bound body on the sync
thread lane and the cpu process lane, plus a noop, so a GIL build and a free
threaded build can be compared on identical work."""

import redis
from cauli import Cauli

from common import DONE_KEY, REDIS_URL

app = Cauli(redis_url=REDIS_URL)
_r = redis.Redis.from_url(REDIS_URL)
# About 2 ms of pure Python per call on the reference box (2.51 ms on 3.12,
# 2.09 ms on 3.14t); free_threaded.py measures it on each interpreter.
BURN_LOOPS = 40_000


def burn():
    x = 0
    for i in range(BURN_LOOPS):
        x += i * i
    return x


@app.task(store_result=False, max_retries=0)
def noop():
    _r.incr(DONE_KEY)


@app.task(store_result=False, max_retries=0)
def burn_thread():
    burn()
    _r.incr(DONE_KEY)


@app.task(kind="cpu", store_result=False, max_retries=0)
def burn_cpu():
    burn()
    _r.incr(DONE_KEY)
