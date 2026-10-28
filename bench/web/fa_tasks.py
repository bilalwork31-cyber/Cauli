"""The cauli app and task for the FastAPI side, built the documented way:
sqlalchemy_app() for a session per task, at its default pool."""

import time

import redis.asyncio as aredis
from cauli.contrib.sqlalchemy import get_session, sqlalchemy_app
from sqlalchemy import update

from common import REDIS_URL
from web.common import LATENCY_KEY
from web.fa_models import ASYNC_PG_DSN, WebJob

app = sqlalchemy_app(ASYNC_PG_DSN, redis_url=REDIS_URL)
_latency_redis = None


@app.task(store_result=False, max_retries=0)
async def process_job(job_id, t0_ns):
    global _latency_redis
    session = get_session()
    updated = await session.execute(
        update(WebJob).where(WebJob.id == job_id).values(processed=WebJob.processed + 1)
    )
    if updated.rowcount != 1:
        raise LookupError(f"web_job {job_id} not visible to the worker")
    await session.commit()
    if _latency_redis is None:
        _latency_redis = aredis.Redis.from_url(REDIS_URL)
    await _latency_redis.rpush(LATENCY_KEY, time.time_ns() - t0_ns)
