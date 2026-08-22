"""The client half of ``--max-envelope-bytes``: an oversize envelope is refused
at the call site instead of hanging ``AsyncResult.get()`` forever.

A worker discards an entry larger than its ``--max-envelope-bytes`` BEFORE
parsing it, so it never learns the task id, never writes ``cauli:result:{id}``,
and a ``.get()`` with no timeout polls until the process is killed.
"""

from __future__ import annotations

import asyncio
import re

import pytest

from cauli import AsyncCauli, Cauli, _codec
from cauli.app import _DEFAULT_MAX_ENVELOPE_BYTES


def test_default_matches_the_worker_flag_default(app):
    assert _DEFAULT_MAX_ENVELOPE_BYTES == 1_048_576
    assert app.max_envelope_bytes == _DEFAULT_MAX_ENVELOPE_BYTES


@pytest.mark.parametrize("bad", [0, -1])
def test_non_positive_limit_is_rejected_at_construction(redis_url, bad):
    with pytest.raises(ValueError, match="max_envelope_bytes must be > 0"):
        Cauli(redis_url=redis_url, max_envelope_bytes=bad)


def test_oversize_delay_raises_and_publishes_nothing(redis_url, redis_client):
    app = Cauli(redis_url=redis_url, max_envelope_bytes=512)

    @app.task(name="big")
    def big(blob):
        return len(blob)

    with pytest.raises(ValueError) as excinfo:
        big.delay("x" * 4096)

    msg = str(excinfo.value)
    assert "'big'" in msg, "the message must name the task"
    assert "512 byte limit" in msg, "the message must name the limit"
    assert "--max-envelope-bytes" in msg, "the message must name the worker flag"
    measured = re.search(r"envelope is (\d+) bytes", msg)
    assert measured and int(measured.group(1)) > 4096, "the measured size is named"

    assert redis_client.exists("cauli:q:default") == 0
    assert redis_client.exists("cauli:delayed:default") == 0


def test_oversize_delayed_call_never_reaches_the_zset(redis_url, redis_client):
    app = Cauli(redis_url=redis_url, max_envelope_bytes=512)

    @app.task(name="big.later")
    def later(blob):
        return blob

    with pytest.raises(ValueError, match="byte limit"):
        later.apply_async(("x" * 4096,), countdown=60)

    assert redis_client.exists("cauli:delayed:default") == 0


def test_the_limit_is_inclusive_and_off_by_one_exact(redis_url, redis_client):
    app = Cauli(redis_url=redis_url)

    @app.task(name="edge")
    def edge(blob):
        return blob

    payload = "y" * 100
    envelope, _queue, _fire = app.make_envelope("edge", (payload,), task=edge)
    exact = len(_codec.encode(envelope))

    app.max_envelope_bytes = exact
    edge.delay(payload)
    assert len(redis_client.xrange("cauli:q:default")) == 1, "== limit must publish"

    app.max_envelope_bytes = exact - 1
    with pytest.raises(ValueError, match="byte limit"):
        edge.delay(payload)
    assert len(redis_client.xrange("cauli:q:default")) == 1, "nothing extra written"


def test_a_normal_payload_is_unaffected(app, redis_client):
    @app.task(name="ordinary")
    def ordinary(blob):
        return blob

    ordinary.delay("z" * 10_000)
    assert len(redis_client.xrange("cauli:q:default")) == 1


def test_async_enqueue_refuses_the_same_payload(redis_url, redis_client):
    app = AsyncCauli(redis_url=redis_url, max_envelope_bytes=512)

    @app.task(name="big.async")
    async def big(blob):
        return blob

    async def main():
        try:
            with pytest.raises(ValueError, match="byte limit"):
                await big.adelay("x" * 4096)
        finally:
            await app.aclose()

    asyncio.run(main())
    assert redis_client.exists("cauli:q:default") == 0
