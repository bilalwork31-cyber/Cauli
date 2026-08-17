"""Alias for :mod:`cauli.contrib.sqlalchemy`, kept so existing imports work.

The implementation moved before the 1.0 API freeze because nothing in it was
ever FastAPI specific: it imports no web framework, it is async SQLAlchemy
session lifecycle, and it serves Starlette, Litestar or a bare asyncio app
identically. A Litestar user was never going to look for it under this name,
and a genuine FastAPI integration later, with on commit enqueue and request
helpers, will want this namespace for itself.

Every name here is the same object as in :mod:`cauli.contrib.sqlalchemy`, a
reexport and not a second copy, so the two import paths share one ContextVar
and mixing them in one process is safe. New code should import from
``cauli.contrib.sqlalchemy``; ``fastapi_app`` is the pre rename name of
:func:`~cauli.contrib.sqlalchemy.sqlalchemy_app`.
"""

from cauli.contrib.sqlalchemy import (
    get_session,
    install_sqlalchemy_session,
    sqlalchemy_app,
)

fastapi_app = sqlalchemy_app

__all__ = [
    "fastapi_app",
    "get_session",
    "install_sqlalchemy_session",
    "sqlalchemy_app",
]
