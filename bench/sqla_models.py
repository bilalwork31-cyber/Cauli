"""SQLAlchemy 2.0 declarative model mapped onto the same bench_io table
every other PG lane in this suite writes to -- the async-ORM counterpart
to djapp/models.py's Django ORM, both real ORMs instead of raw driver
calls, one representing Django's sync story, one the SQLAlchemy-async
story most FastAPI + Postgres apps actually use.
"""

from sqlalchemy import DateTime, Text, func
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from workloads import PG_DSN

# psycopg3 speaks async natively; SQLAlchemy's postgresql+psycopg dialect
# picks the async driver automatically under create_async_engine, so this
# needs no extra driver (asyncpg) beyond what's already installed.
ASYNC_PG_DSN = PG_DSN.replace("postgresql://", "postgresql+psycopg://", 1)


class Base(DeclarativeBase):
    pass


class BenchIo(Base):
    __tablename__ = "bench_io"

    id: Mapped[int] = mapped_column(primary_key=True)
    payload: Mapped[str] = mapped_column(Text)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now())


def make_engine(pool_max):
    """Up to ``pool_max`` REUSABLE connections, matching the shape the raw
    psycopg lanes use: ``AsyncConnectionPool(min_size=2, max_size=pool_max)``.

    This used to read ``pool_size=2, max_overflow=pool_max - 2``, which looks
    like the same thing and is not. SQLAlchemy keeps exactly ``pool_size``
    connections pooled; every OVERFLOW connection is closed when it is
    returned rather than kept, so above two concurrent tasks nearly every
    insert paid a fresh TCP connect plus SCRAM handshake. psycopg's
    ``min_size`` is an idle floor, not a cap, so the two parameters do not
    correspond and translating one to the other by name was wrong.

    This single line is why the SQLAlchemy lane reported cauli and taskiq
    within a few percent of each other and of a framework-free probe: none of
    them was the bottleneck, the connection churn was. Any number measured
    through the old engine says nothing about the framework above it.
    """
    return create_async_engine(ASYNC_PG_DSN, pool_size=pool_max, max_overflow=0)
