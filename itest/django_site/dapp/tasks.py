"""Tasks discovered by autodiscover_tasks() across INSTALLED_APPS.

One ORM task and one no-DB probe per execution path (sync thread pool,
embedded asyncio loop, forked cpu child)."""

import asyncio
import os
import threading

from asgiref.sync import sync_to_async
from django.db import connection

from django_site.cauli import app
from django_site.dapp.models import Item


def _ctx(extra):
    return {"pid": os.getpid(), "tid": threading.get_ident(), **extra}


# max_retries=0 on every ORM task: a stale-connection failure must surface as
# a task failure, not be quietly healed by a retry (the restart-survival test
# depends on the FIRST attempt succeeding). timeout=20 keeps the §4.4
# invariant (visibility_timeout 30 > task timeout) satisfied.


@app.task(max_retries=0, timeout=20)
def db_add(name):
    Item.objects.create(name=name)
    return _ctx({"count": Item.objects.filter(name=name).count()})


@app.task(max_retries=0, timeout=20)
async def adb_add(name):
    # Django's async ORM interface: sync DB work runs in asgiref's
    # thread-sensitive executor, which is exactly where the contrib hook
    # closes old connections for async tasks.
    await Item.objects.acreate(name=name)
    return _ctx({"count": await Item.objects.filter(name=name).acount()})


def _orm_thread_and_backend():
    with connection.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        return threading.current_thread().name, cur.fetchone()[0]


@app.task(max_retries=0, timeout=20)
async def adb_sticky(name):
    # Where each async ORM call ran: asgiref thread and Postgres backend.
    # The sleeps are real suspensions, so other tasks interleave.
    seen = []
    item = await Item.objects.acreate(name=name)
    seen.append(await sync_to_async(_orm_thread_and_backend)())
    await asyncio.sleep(0.05)
    item = await Item.objects.aget(pk=item.pk)
    seen.append(await sync_to_async(_orm_thread_and_backend)())
    await asyncio.sleep(0.05)
    await Item.objects.filter(pk=item.pk).aupdate(name=f"{name}-updated")
    seen.append(await sync_to_async(_orm_thread_and_backend)())
    return seen


@app.task(kind="cpu", max_retries=0, timeout=20)
def cpu_db_add(name):
    Item.objects.create(name=name)
    return _ctx({"count": Item.objects.filter(name=name).count()})


@app.task()
def probe(x):
    return _ctx({"x": x})


@app.task()
async def aprobe(x):
    return _ctx({"x": x})


@app.task(kind="cpu")
def cpu_probe(x):
    return _ctx({"x": x})
