"""Scenario C send paths: identical guard chain and retry ladder to
campaign_common (same lock -> attempts -> flag -> tripwire -> POST -> classify
logic, same knobs), plus stage-2: capture message_id from the fake Graph
response and RPUSH a persist record on every successful send.

ADDITIVE module: campaign_common.py is NOT modified (the A/B suite executes
from it). Shared pieces are imported from it (page semaphores, backoffs,
failure classification, dispatch_tick) so semantics cannot drift.

Record-vs-flag ordering: push the persist record FIRST, then set the sent
flag. A crash in between re-sends the recipient (at-least-once) and Postgres
ON CONFLICT dedups the extra record; the reverse order could lose a record
forever and stall pg_count == sent.
"""
import asyncio
import concurrent.futures
import json
import time

import requests

import campaign_common as cc
import campconfig
import graph_async_c
import persist_common
import store
import store_async

CONFIG = campconfig.CONFIG
SEND_URL = campconfig.GRAPH_URL + "/me/messages"

dispatch_tick = cc.dispatch_tick        # unchanged production-quirk dispatch


def _record(cid, rid, page_id, message_id, sent_at_ms, enq_first_ms):
    return {"recipient_id": rid, "campaign_id": cid, "page_id": page_id,
            "message_id": message_id, "sent_at_ms": sent_at_ms,
            "enqueued_first_ms": enq_first_ms}


# ------------------------------------------------------------------ sync path -
def _post_once_sync_c(cid, rid, page_id):
    """-> ('ok'|'fail'|'blocked', retryable, message_id|None)."""
    if not store.record_send_attempt(cid, rid):
        return "blocked", False, None
    try:
        resp = requests.post(
            SEND_URL,
            json={"campaign_id": cid, "recipient_id": rid, "page_id": page_id},
            timeout=30)
    except requests.RequestException:
        return "fail", True, None
    if resp.status_code == 200:
        try:
            mid = resp.json().get("message_id", "unknown")
        except ValueError:
            mid = "unknown"
        return "ok", False, mid
    return "fail", (resp.status_code == 429 or resp.status_code >= 500), None


def _send_one_sync_c(cid, rid, page_id):
    with cc._page_sem(page_id):
        if not store.acquire_lock(cid, rid):
            store.incr_counter(cid, "lock_skips")
            return {"rid": rid, "outcome": "lock_skip"}
        attempts = store.begin_send(cid, rid)
        if store.sent_flag_exists(cid, rid):
            return {"rid": rid, "outcome": "already_sent", "attempts": attempts}
        for i in range(cc.IN_PROCESS_TRIES):
            res, retryable, mid = _post_once_sync_c(cid, rid, page_id)
            if res == "ok":
                sent_at = cc.now_ms()
                enq = int(store.conn().hget(store.k_hash(cid, rid),
                                            "enqueued_first_ms") or 0)
                persist_common.push_result(
                    _record(cid, rid, page_id, mid, sent_at, enq))
                store.set_sent_flag(cid, rid)
                if CONFIG["SEND_DELAY"] > 0:
                    time.sleep(CONFIG["SEND_DELAY"])
                return {"rid": rid, "outcome": "sent", "attempts": attempts,
                        "sent_at_ms": sent_at}
            if res == "blocked":
                return {"rid": rid, "outcome": "already_sent",
                        "attempts": attempts}
            if not retryable:
                break
            if i < cc.IN_PROCESS_TRIES - 1:
                store.incr_counter(cid, "http_retries")
                time.sleep(cc.BACKOFFS[i] * CONFIG["RETRY_SCALE"])
        return {"rid": rid, "attempts": attempts, **cc._failure_fields(attempts)}


def send_batch_sync_c(campaign_id, batch):
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONFIG["CONCURRENT_GLOBAL"]) as ex:
        futs = [ex.submit(_send_one_sync_c, campaign_id, rid, page)
                for rid, page in batch]
        results = [f.result() for f in futs]
    store.mark_results(campaign_id, results)
    return cc._batch_summary(batch, results)


# ----------------------------------------------------------------- async path -
async def _post_once_async_c(cid, rid, page_id):
    if not await store_async.record_send_attempt(cid, rid):
        return "blocked", False, None
    pool = graph_async_c.get_pool(campconfig.GRAPH_URL)
    try:
        status, body = await pool.post_json(
            {"campaign_id": cid, "recipient_id": rid, "page_id": page_id},
            timeout=30.0)
    except Exception:
        return "fail", True, None
    if status == 200:
        try:
            mid = json.loads(body).get("message_id", "unknown")
        except ValueError:
            mid = "unknown"
        return "ok", False, mid
    return "fail", (status == 429 or status >= 500), None


async def _send_one_async_c(cid, rid, page_id):
    async with cc._apage_sem(page_id):
        if not await store_async.acquire_lock(cid, rid):
            await store_async.incr_counter(cid, "lock_skips")
            return {"rid": rid, "outcome": "lock_skip"}
        attempts = await store_async.begin_send(cid, rid)
        if await store_async.sent_flag_exists(cid, rid):
            return {"rid": rid, "outcome": "already_sent", "attempts": attempts}
        for i in range(cc.IN_PROCESS_TRIES):
            res, retryable, mid = await _post_once_async_c(cid, rid, page_id)
            if res == "ok":
                sent_at = cc.now_ms()
                enq = int(await store_async._conn().hget(
                    store.k_hash(cid, rid), "enqueued_first_ms") or 0)
                await persist_common.push_result_async(
                    _record(cid, rid, page_id, mid, sent_at, enq))
                await store_async.set_sent_flag(cid, rid)
                if CONFIG["SEND_DELAY"] > 0:
                    await asyncio.sleep(CONFIG["SEND_DELAY"])
                return {"rid": rid, "outcome": "sent", "attempts": attempts,
                        "sent_at_ms": sent_at}
            if res == "blocked":
                return {"rid": rid, "outcome": "already_sent",
                        "attempts": attempts}
            if not retryable:
                break
            if i < cc.IN_PROCESS_TRIES - 1:
                await store_async.incr_counter(cid, "http_retries")
                await asyncio.sleep(cc.BACKOFFS[i] * CONFIG["RETRY_SCALE"])
        return {"rid": rid, "attempts": attempts, **cc._failure_fields(attempts)}


async def send_batch_async_c(campaign_id, batch):
    results = await asyncio.gather(
        *[_send_one_async_c(campaign_id, rid, page) for rid, page in batch])
    store.mark_results(campaign_id, results)
    return cc._batch_summary(batch, results)
