"""The documented cauli.contrib.sqlalchemy setup, run by test_sqlalchemy.py:

    engine = create_async_engine(dsn, ...)
    app = install_sqlalchemy_session(Cauli(...), engine)

Postgres coordinates come from CAULI_ITEST_PG_* env vars; defaults are the
"bench" role and database on the default port.
"""

import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from cauli import Cauli
from cauli.contrib.sqlalchemy import get_session, install_sqlalchemy_session

_host = os.environ.get("CAULI_ITEST_PG_HOST", "127.0.0.1")
_port = os.environ.get("CAULI_ITEST_PG_PORT", "5432")
_db = os.environ.get("CAULI_ITEST_PG_DB", "bench")
_user = os.environ.get("CAULI_ITEST_PG_USER", "bench")
_password = os.environ.get("CAULI_ITEST_PG_PASSWORD", "bench")
_appname = os.environ.get("CAULI_ITEST_APPNAME", "cauli-itest-sqlalchemy")

_dsn = f"postgresql+psycopg://{_user}:{_password}@{_host}:{_port}/{_db}"
_engine = create_async_engine(_dsn, connect_args={"application_name": _appname})

app = install_sqlalchemy_session(Cauli(default_queue="sqlalchemy"), _engine)


@app.task(max_retries=0)
async def session_probe(hold_seconds: float):
    """Holds its session's connection for hold_seconds, then reports its backend.

    max_retries=0 so an exhausted pool fails the task instead of retrying.
    """
    session = get_session()
    await session.execute(text("select pg_sleep(:s)"), {"s": hold_seconds})
    pid = (await session.execute(text("select pg_backend_pid()"))).scalar_one()
    return {"pid": pid}
