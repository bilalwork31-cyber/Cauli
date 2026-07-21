from __future__ import annotations

import shutil
import subprocess
import time

import pytest
import redis as redis_lib

from cauli import Cauli

REDIS_PORT = 6391  # throwaway test instance; never the shared 6379
REDIS_URL = f"redis://127.0.0.1:{REDIS_PORT}/0"


def _try_connect(url: str) -> redis_lib.Redis | None:
    client = redis_lib.Redis.from_url(
        url, socket_connect_timeout=0.25, socket_timeout=5
    )
    try:
        client.ping()
    except Exception:
        return None
    return client


@pytest.fixture(scope="session")
def redis_url():
    """Start (if needed) a throwaway redis-server on REDIS_PORT for the session."""
    client = _try_connect(REDIS_URL)
    proc = None
    if client is None:
        server = shutil.which("redis-server")
        if server is None:
            pytest.skip("redis-server not installed")
        proc = subprocess.Popen(
            [server, "--port", str(REDIS_PORT), "--save", "", "--appendonly", "no"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while client is None and time.monotonic() < deadline:
            time.sleep(0.05)
            client = _try_connect(REDIS_URL)
        if client is None:
            proc.terminate()
            pytest.fail(f"could not start redis-server on port {REDIS_PORT}")
    yield REDIS_URL
    try:
        client.close()
    finally:
        if proc is not None:
            proc.terminate()
            proc.wait(timeout=5)


@pytest.fixture()
def redis_client(redis_url):
    """Fresh (flushed) client per test against the throwaway instance."""
    client = redis_lib.Redis.from_url(redis_url)
    client.flushall()
    yield client
    client.close()


@pytest.fixture()
def app(redis_url, redis_client) -> Cauli:
    """A Cauli app bound to the flushed throwaway redis."""
    return Cauli(redis_url=redis_url)
