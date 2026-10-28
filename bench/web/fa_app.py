"""FastAPI side. Each endpoint writes one row through the async ORM; the
enqueue arms then hand its id to a task."""

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from web.common import PAYLOAD, WEB_POOL
from web.fa_models import ASYNC_PG_DSN, WebJob
from web.fa_tasks import app as cauli_app
from web.fa_tasks import process_job

engine = create_async_engine(ASYNC_PG_DSN, pool_size=WEB_POOL, max_overflow=0)
Session = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def lifespan(_api):
    yield
    await cauli_app.aclose()
    await engine.dispose()


api = FastAPI(lifespan=lifespan)


async def _insert() -> int:
    async with Session() as session:
        job = WebJob(payload=PAYLOAD)
        session.add(job)
        await session.commit()
        return job.id


@api.get("/health")
async def health():
    return "ok"


@api.post("/insert")
async def insert():
    return {"id": await _insert()}


@api.post("/cauli")
async def cauli():
    t0 = time.time_ns()
    job_id = await _insert()
    await process_job.adelay(job_id, t0)
    return {"id": job_id}


@api.post("/cauli_blocking")
async def cauli_blocking():
    # The mistake adelay() exists to prevent: a blocking Redis round trip on
    # the event loop thread. Measured so the cost has a number.
    t0 = time.time_ns()
    job_id = await _insert()
    process_job.delay(job_id, t0)
    return {"id": job_id}
