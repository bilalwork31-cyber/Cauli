"""Unit tests for store.py invariants. Run against a throwaway redis:

    BENCH_REDIS_PORT=6393 ~/rupy-bench-venv/bin/pytest test_store.py -v

Covers: claim_batch atomicity under 8 concurrent claimers (no double claim),
orphan reclaim after lease expiry, and the duplicates tripwire firing on a
double-send attempt.
"""
import threading
import time

import pytest

import store


@pytest.fixture(autouse=True)
def clean_db():
    store.conn().ping()
    store.flush_store_db()
    yield
    store.flush_store_db()


def test_seed_shapes():
    store.seed_campaign("t0", 10, 3)
    assert store.campaign_total("t0") == 10
    counts = store.count_by_status("t0")
    assert counts == {"pending": 10}
    rows = store.collect_rows("t0")
    pages = {r["page_id"] for r in rows}
    assert pages == {"p0", "p1", "p2"}


def test_claim_batch_atomic_under_concurrency():
    """8 concurrent claimers, no rid handed out twice (SKIP LOCKED analog)."""
    n = 400
    store.seed_campaign("t1", n, 5)
    claimed = []
    lock = threading.Lock()

    def worker():
        empty = 0
        while empty < 3:
            got = store.claim_batch("t1", limit=25)   # default 60s lease
            if got:
                with lock:
                    claimed.extend(rid for rid, _ in got)
                empty = 0
            else:
                empty += 1

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert len(claimed) == n, f"claimed {len(claimed)} != {n}"
    assert len(set(claimed)) == n, "a recipient was double-claimed"
    counts = store.count_by_status("t1")
    assert counts.get("queued") == n


def test_orphan_reclaim_after_lease_expiry():
    store.seed_campaign("t2", 5, 1)
    first = store.claim_batch("t2", limit=10, lease_ms=150)
    assert len(first) == 5
    # while leased: nothing to claim
    assert store.claim_batch("t2", limit=10, lease_ms=150) == []
    time.sleep(0.25)
    # lease expired: same rows come back (orphan reclaim)
    second = store.claim_batch("t2", limit=10, lease_ms=150)
    assert sorted(r for r, _ in second) == sorted(r for r, _ in first)
    # a terminal row is NOT reclaimed
    rid = first[0][0]
    store.mark_result("t2", rid, "sent", attempts=1,
                      sent_at_ms=store.now_ms())
    time.sleep(0.25)
    third = store.claim_batch("t2", limit=10, lease_ms=150)
    assert rid not in [r for r, _ in third]
    assert len(third) == 4


def test_retry_row_becomes_due_again():
    store.seed_campaign("t3", 1, 1)
    [(rid, _)] = store.claim_batch("t3", limit=1)
    store.mark_result("t3", rid, "retry", attempts=1,
                      next_due_ms=store.now_ms() + 100)
    assert store.claim_batch("t3", limit=1) == []      # not due yet
    time.sleep(0.15)
    again = store.claim_batch("t3", limit=1)
    assert [r for r, _ in again] == [rid]
    assert store.count_by_status("t3") == {"queued": 1}


def test_duplicate_tripwire_fires_on_double_send_attempt():
    store.seed_campaign("t4", 1, 1)
    [(rid, _)] = store.claim_batch("t4", limit=1)
    # first (legitimate) send
    assert store.acquire_lock("t4", rid)
    assert store.begin_send("t4", rid) == 1
    assert store.record_send_attempt("t4", rid) is True    # POST allowed
    store.set_sent_flag("t4", rid)
    store.mark_result("t4", rid, "sent", attempts=1, sent_at_ms=store.now_ms())
    assert store.duplicates("t4") == 0
    # a second send attempt on the same recipient (guard chain bypassed)
    assert store.acquire_lock("t4", rid)                   # lock was released
    assert store.sent_flag_exists("t4", rid)               # silent guard sees it
    assert store.record_send_attempt("t4", rid) is False   # tripwire blocks
    assert store.duplicates("t4") == 1                     # ...and counts it
    # enqueued_first_ms was stamped once, on first claim
    row = store.collect_rows("t4")[0]
    assert row["status"] == "sent"
    assert row["enqueued_first_ms"] > 0


def test_lock_is_exclusive():
    store.seed_campaign("t5", 1, 1)
    assert store.acquire_lock("t5", "r000000")
    assert not store.acquire_lock("t5", "r000000")
    store.release_lock("t5", "r000000")
    assert store.acquire_lock("t5", "r000000")
