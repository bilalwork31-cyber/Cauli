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
