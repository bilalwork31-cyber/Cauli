"""I/O-bound task, cauli async: one real Postgres INSERT per task, through
SQLAlchemy 2.0's async ORM instead of raw psycopg3 (see
tasks_cauli_async_pg.py for the raw-driver baseline this is compared
against) -- the ORM layer most FastAPI + Postgres apps actually use.

Run with: cauli-worker -A tasks_cauli_async_sqlalchemy:app -c ...
"""

import redis.asyncio as aredis
from cauli import Cauli
from sqlalchemy.ext.asyncio import AsyncSession

from common import DONE_KEY, REDIS_URL
from sqla_models import BenchIo, make_engine
from workloads import PG_PAYLOAD, PG_POOL_MAX

app = Cauli(redis_url=REDIS_URL)
_r = aredis.Redis.from_url(REDIS_URL)
# Built lazily on first task, same reason as tasks_cauli_async_pg.py's
# pool: no asyncio loop exists yet at import time.
_engine = None


@app.task(store_result=False, max_retries=0)
async def insert():
    global _engine
    if _engine is None:
        _engine = make_engine(PG_POOL_MAX)
    async with AsyncSession(_engine) as session:
        session.add(BenchIo(payload=PG_PAYLOAD))
        await session.commit()
    await _r.incr(DONE_KEY)
