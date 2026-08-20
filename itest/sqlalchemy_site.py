"""The documented cauli.contrib.sqlalchemy setup path, exercised for real
by test_sqlalchemy.py:

    engine = create_async_engine(dsn, ...)
    app = install_sqlalchemy_session(Cauli(...), engine)

The worker runs `cauli-worker --app sqlalchemy_site:app`. Postgres coordinates
come from CAULI_ITEST_PG_* env vars, mirroring the CAULI_ITEST_PG_* threading
django_site/settings.py uses for its own DATABASES dict, so test_sqlalchemy.py
can point one run's engine at a uniquely tagged application_name without
editing this file. Defaults match the audit's own bench role/database on
Postgres's default port, so this module also works stood up by hand.
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
    """Holds its own session's connection for hold_seconds, then reports the
    real Postgres backend that served it. max_retries=0: a pool exhausted by
    a leaked session must surface as a failed task, not be quietly retried
    into passing later, the same reasoning django_site/dapp/tasks.py uses.
    """
    session = get_session()
    await session.execute(text("select pg_sleep(:s)"), {"s": hold_seconds})
    pid = (await session.execute(text("select pg_backend_pid()"))).scalar_one()
    return {"pid": pid}
