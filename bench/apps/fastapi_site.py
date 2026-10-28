"""The FastAPI app for the web benchmark: SQLAlchemy model, the cauli app
built with sqlalchemy_app(), its task and the endpoints."""

import os
import time
from contextlib import asynccontextmanager

import redis.asyncio as aredis
from cauli.contrib.sqlalchemy import get_session, sqlalchemy_app
from fastapi import FastAPI
from sqlalchemy import Integer, Text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from bench import PG_DSN, REDIS_URL, WEB_LATENCY_KEY, WEB_PAYLOAD

ASYNC_PG_DSN = PG_DSN.replace("postgresql://", "postgresql+psycopg://", 1)
# Connections per web process: 4 processes x 8 keeps the web tier far under
# Postgres's max_connections.
WEB_POOL = int(os.environ.get("BENCH_WEB_POOL", "8"))


class Base(DeclarativeBase):
    pass


class WebJob(Base):
    __tablename__ = "web_job"

    id: Mapped[int] = mapped_column(primary_key=True)
    payload: Mapped[str] = mapped_column(Text)
    processed: Mapped[int] = mapped_column(Integer, default=0)


app = sqlalchemy_app(ASYNC_PG_DSN, redis_url=REDIS_URL)
_latency = aredis.Redis.from_url(REDIS_URL)


@app.task(store_result=False, max_retries=0)
async def process_job(job_id, t0_ns):
    session = get_session()
    updated = await session.execute(
        update(WebJob).where(WebJob.id == job_id).values(processed=WebJob.processed + 1)
    )
    if updated.rowcount != 1:
        raise LookupError(f"web_job {job_id} not visible to the worker")
    await session.commit()
    await _latency.rpush(WEB_LATENCY_KEY, time.time_ns() - t0_ns)


engine = create_async_engine(ASYNC_PG_DSN, pool_size=WEB_POOL, max_overflow=0)
Session = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def lifespan(_api):
    yield
    await app.aclose()
    await engine.dispose()


api = FastAPI(lifespan=lifespan)


async def _insert() -> int:
    async with Session() as session:
        job = WebJob(payload=WEB_PAYLOAD)
        session.add(job)
        await session.commit()
        return job.id


@api.get("/health")
async def health():
    return os.getpid()


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
    # the event loop thread, measured so its cost has a number.
    t0 = time.time_ns()
    job_id = await _insert()
    process_job.delay(job_id, t0)
    return {"id": job_id}
