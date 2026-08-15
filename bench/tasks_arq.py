"""No-op async task, arq. Run with: arq tasks_arq:WorkerSettings"""

import redis.asyncio as aredis
from arq.connections import RedisSettings

from common import DONE_KEY, REDIS_URL

_r = aredis.Redis.from_url(REDIS_URL)
redis_settings = RedisSettings.from_dsn(REDIS_URL)


async def noop(ctx):
    await _r.incr(DONE_KEY)


class WorkerSettings:
    functions = [noop]
    redis_settings = redis_settings
    # Defaults (poll_delay=0.5s, max_jobs=10) cap throughput at roughly
    # max_jobs/poll_delay regardless of task speed -- measured 26.9/s on a
    # true no-op with defaults. No CLI flag for this; only settable here.
    poll_delay = 0.01
    max_jobs = 200
