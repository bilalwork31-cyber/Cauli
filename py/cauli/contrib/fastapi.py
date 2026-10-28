"""Alias for :mod:`cauli.contrib.sqlalchemy`; import from there in new code.

Every name is the same object, so both paths share one ContextVar and can be
mixed in one process. ``fastapi_app`` is ``sqlalchemy_app``.
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
