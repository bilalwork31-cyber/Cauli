"""Fixture app imported by the rupy._exec child in subprocess tests.

The child is spawned with cwd = this directory, so `--app exec_fixture_app:app`
resolves via the CWD sys.path entry. The redis URL points at a dead port on
purpose: _exec must never touch redis.
"""
import asyncio
import time

from rupy import Retry, Rupy

app = Rupy(redis_url="redis://127.0.0.1:1/0")


@app.task(name="add", kind="cpu")
def add(a, b):
    return a + b


@app.task(name="boom", kind="cpu")
def boom():
    raise ValueError("kaboom")


@app.task(name="bigfail", kind="cpu")
def bigfail():
    raise ValueError("x" * 20000)


@app.task(name="sleepy", kind="cpu")
def sleepy(seconds):
    time.sleep(seconds)
    return "done"


@app.task(name="retryme", kind="cpu")
def retryme(countdown=None):
    raise Retry(countdown)


@app.task(name="unser", kind="cpu")
def unser():
    return {1, 2, 3}  # a set is not JSON serializable


@app.task(name="noisy", kind="cpu")
def noisy():
    print("this print must NOT corrupt the pipe protocol")
    return "quiet"


@app.task(name="aadd", kind="cpu")
async def aadd(a, b):
    await asyncio.sleep(0)
    return a + b
