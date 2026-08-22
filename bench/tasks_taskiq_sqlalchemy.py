"""I/O-bound task, taskiq: same SQLAlchemy async ORM insert as
tasks_cauli_async_sqlalchemy.py -- taskiq is FastAPI's most natural async
task-queue pairing, so this is the flagship comparison for the async/ORM
side, same role tasks_celery_django.py plays for sync/ORM.

Run with: taskiq worker tasks_taskiq_sqlalchemy:broker --workers ... --max-async-tasks ...
"""

import redis.asyncio as aredis
from sqlalchemy.ext.asyncio import AsyncSession
from taskiq_redis import ListQueueBroker

from common import DONE_KEY, REDIS_URL
from sqla_models import BenchIo, make_engine
from workloads import PG_PAYLOAD, PG_POOL_MAX

broker = ListQueueBroker(url=REDIS_URL, queue_name="fwbench_sqla")
_r = aredis.Redis.from_url(REDIS_URL)
_engine = None


@broker.task
async def insert():
    global _engine
    if _engine is None:
        _engine = make_engine(PG_POOL_MAX)
    async with AsyncSession(_engine) as session:
        session.add(BenchIo(payload=PG_PAYLOAD))
        await session.commit()
    await _r.incr(DONE_KEY)
