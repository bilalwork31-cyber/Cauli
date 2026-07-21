"""Unit tests for the stage-2 persister (scenario C verification step).

Run: BENCH_REDIS_PORT=6393 ~/rupy-bench-venv/bin/pytest test_persist.py -v
Needs: redis on 6393 and Postgres per pg_setup.sh.
"""

import pytest

import persist_common as pc
import store


def _rec(i, rid=None, sent_at=1000):
    return {
        "recipient_id": rid or f"tr{i:05d}",
        "campaign_id": "tc",
        "page_id": "p0",
        "message_id": f"m{i}",
        "sent_at_ms": sent_at,
        "enqueued_first_ms": 500,
    }


@pytest.fixture(autouse=True)
def clean():
    pc.results_conn().flushdb()
    store.flush_store_db()
    pc.ensure_schema()
    pc.truncate()
    yield
    pc.results_conn().flushdb()
    store.flush_store_db()


def test_upsert_idempotent():
    # 10 records, one duplicated recipient_id -> 9 unique rows
    recs = [_rec(i) for i in range(9)] + [_rec(99, rid="tr00000")]
    for r in recs:
        pc.push_result(r)
    assert pc.backlog_len() == 10
    assert pc.drain_once(500) == 10
    assert pc.backlog_len() == 0
    assert pc.pg_count() == 9
    # replay the exact same batch: ON CONFLICT keeps it at 9
    for r in recs:
        pc.push_result(r)
    assert pc.drain_once(500) == 10
    assert pc.pg_count() == 9
    # lags recorded once per drained row (batch-commit granularity)
    assert len(pc.collect_lags()) == 20
    assert sum(pc.persist_timeline().values()) == 20


def test_lpop_batch_drain():
    for i in range(1200):
        pc.push_result(_rec(i))
    assert pc.drain_once(500) == 500
    assert pc.drain_once(500) == 500
    assert pc.drain_once(500) == 200
    assert pc.drain_once(500) == 0
    assert pc.backlog_len() == 0
    assert pc.pg_count() == 1200


def test_chain_logic():
    calls = []
    reenq = calls.append
    # backlog remains after a full drain -> immediate re-enqueue (None)
    for i in range(600):
        pc.push_result(_rec(i))
    store.set_bg_active(True)
    pc.drain_and_chain(reenq)
    assert calls == [None]
    # empty backlog but campaign running -> delayed poll (1.0)
    pc.drain_and_chain(reenq)
    assert calls == [None, 1.0]
    # empty backlog, campaign over -> chain stops
    store.set_bg_active(False)
    pc.drain_and_chain(reenq)
    assert calls == [None, 1.0]
    assert pc.pg_count() == 600
