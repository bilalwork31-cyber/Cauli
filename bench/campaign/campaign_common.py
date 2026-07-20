"""ONE campaign implementation imported by BOTH stacks (Celery and rupy).

dispatch_tick:  quota = max(1, APP_MAX_PER_MINUTE // active_campaigns) applied
                PER TICK (production quirk, kept), claim capped additionally by
                MAX_BATCHES_PER_DISPATCH * BATCH_SIZE, chunked into BATCH_SIZE
                batches. Queue = campaign_short if campaign total <= 500 else
                campaign_long. The caller (stack wrapper) enqueues the batches.

send flow (identical semantics sync/async), per recipient INSIDE the per-page
semaphore (CONCURRENT_PER_PAGE, module-global per process / per event loop):
  1. intent lock SET NX EX 300 (held->lock_skip, row stays leased, reclaimed
     on lease expiry),
  2. attempts += 1, status=sending,
  3. sent flag check (silent skip -> already_sent: guard working, NOT a
     duplicate),
  4. up to 1+3 POSTs to the fake Graph API, timeout 30s; retry on 5xx/429/
     timeout with backoffs 1s,2s,4s * RETRY_SCALE (each retry counted in the
     http_retries counter); every actual POST goes through the
     record_send_attempt tripwire (duplicates counter),
  5. success: sent flag up immediately, sent_at stamped, then
     time.sleep(SEND_DELAY) still inside the semaphore (per-page pacing),
  6. final failure: attempts < MAX_ATTEMPTS -> status retry with next_due =
     now + (min(90, 8*2^(attempts-1)) + rand(1,15)) * BACKOFF_SCALE seconds,
     else failed.
Batch ends with ONE grouped store.mark_results (sent flags were already set
at send time, so duplicate protection never waits on the grouped write).

Sync: ThreadPoolExecutor(CONCURRENT_GLOBAL) per batch + threading.Semaphore
per page. Async: per-page asyncio.Semaphore; GLOBAL slots come from rupy's
--io-concurrency admission gate itself; HTTP via graph_async (raw asyncio
keepalive pool), redis via store_async.
"""
import asyncio
import concurrent.futures
import random
import threading
import time

import requests

import campconfig
import graph_async
import store
import store_async

CONFIG = campconfig.CONFIG
SEND_URL = campconfig.GRAPH_URL + "/me/messages"
IN_PROCESS_TRIES = 4            # 1 initial + up to 3 retries
BACKOFFS = (1.0, 2.0, 4.0)      # seconds, scaled by RETRY_SCALE


def now_ms():
    return int(time.time() * 1000)


# ------------------------------------------------------------------ dispatch -
def dispatch_tick(campaign_id):
    """Claim due rows, chunk into batches. Caller enqueues send tasks."""
    active = store.active_campaigns()
    quota = max(1, CONFIG["APP_MAX_PER_MINUTE"] // active)
    limit = min(quota, CONFIG["MAX_BATCHES_PER_DISPATCH"] * CONFIG["BATCH_SIZE"])
    claimed = store.claim_batch(campaign_id, now_ms(), limit)
    bs = CONFIG["BATCH_SIZE"]
    batches = [claimed[i:i + bs] for i in range(0, len(claimed), bs)]
    total = store.campaign_total(campaign_id)
    queue = "campaign_short" if total <= 500 else "campaign_long"
    return {"batches": batches, "queue": queue, "claimed": len(claimed)}


# ------------------------------------------------------------- failure paths -
def _failure_fields(attempts):
    if attempts < CONFIG["MAX_ATTEMPTS"]:
        delay_s = (min(90.0, 8.0 * (2.0 ** (attempts - 1)))
                   + random.uniform(1.0, 15.0)) * CONFIG["BACKOFF_SCALE"]
        return {"outcome": "retry", "next_due_ms": now_ms() + int(delay_s * 1000)}
    return {"outcome": "failed"}


# ------------------------------------------------------------------ sync path -
_page_sems = {}
_page_sems_lock = threading.Lock()


def _page_sem(page_id):
    sem = _page_sems.get(page_id)
    if sem is None:
        with _page_sems_lock:
            sem = _page_sems.get(page_id)
            if sem is None:
                sem = threading.Semaphore(CONFIG["CONCURRENT_PER_PAGE"])
                _page_sems[page_id] = sem
    return sem


def _post_once_sync(cid, rid, page_id):
    """One POST through the tripwire. -> ('ok'|'fail'|'blocked', retryable)."""
    if not store.record_send_attempt(cid, rid):
        return "blocked", False
    try:
        resp = requests.post(
            SEND_URL,
            json={"campaign_id": cid, "recipient_id": rid, "page_id": page_id},
            timeout=30)
    except requests.RequestException:
        return "fail", True
    if resp.status_code == 200:
        return "ok", False
    return "fail", (resp.status_code == 429 or resp.status_code >= 500)


def _send_one_sync(cid, rid, page_id):
    with _page_sem(page_id):
        if not store.acquire_lock(cid, rid):
            store.incr_counter(cid, "lock_skips")
            return {"rid": rid, "outcome": "lock_skip"}
        attempts = store.begin_send(cid, rid)
        if store.sent_flag_exists(cid, rid):
            return {"rid": rid, "outcome": "already_sent", "attempts": attempts}
        for i in range(IN_PROCESS_TRIES):
            res, retryable = _post_once_sync(cid, rid, page_id)
            if res == "ok":
                sent_at = now_ms()
                store.set_sent_flag(cid, rid)
                time.sleep(CONFIG["SEND_DELAY"])
                return {"rid": rid, "outcome": "sent", "attempts": attempts,
                        "sent_at_ms": sent_at}
            if res == "blocked":
                return {"rid": rid, "outcome": "already_sent",
                        "attempts": attempts}
            if not retryable:
                break
            if i < IN_PROCESS_TRIES - 1:
                store.incr_counter(cid, "http_retries")
                time.sleep(BACKOFFS[i] * CONFIG["RETRY_SCALE"])
        return {"rid": rid, "attempts": attempts, **_failure_fields(attempts)}


def send_batch_sync(campaign_id, batch):
    """batch: [[rid, page_id], ...]. Called by Celery AND rupy sync tasks."""
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONFIG["CONCURRENT_GLOBAL"]) as ex:
        futs = [ex.submit(_send_one_sync, campaign_id, rid, page)
                for rid, page in batch]
        results = [f.result() for f in futs]
    store.mark_results(campaign_id, results)
    return _batch_summary(batch, results)


# ----------------------------------------------------------------- async path -
_apage_sems = {}


def _apage_sem(page_id):
    key = (id(asyncio.get_running_loop()), page_id)
    sem = _apage_sems.get(key)
    if sem is None:
        sem = asyncio.Semaphore(CONFIG["CONCURRENT_PER_PAGE"])
        _apage_sems[key] = sem
    return sem


async def _post_once_async(cid, rid, page_id):
    if not await store_async.record_send_attempt(cid, rid):
        return "blocked", False
    pool = graph_async.get_pool(campconfig.GRAPH_URL)
    try:
        status = await pool.post_json(
            {"campaign_id": cid, "recipient_id": rid, "page_id": page_id},
            timeout=30.0)
    except Exception:
        return "fail", True
    if status == 200:
        return "ok", False
    return "fail", (status == 429 or status >= 500)


async def _send_one_async(cid, rid, page_id):
    async with _apage_sem(page_id):
        if not await store_async.acquire_lock(cid, rid):
            await store_async.incr_counter(cid, "lock_skips")
            return {"rid": rid, "outcome": "lock_skip"}
        attempts = await store_async.begin_send(cid, rid)
        if await store_async.sent_flag_exists(cid, rid):
            return {"rid": rid, "outcome": "already_sent", "attempts": attempts}
        for i in range(IN_PROCESS_TRIES):
            res, retryable = await _post_once_async(cid, rid, page_id)
            if res == "ok":
                sent_at = now_ms()
                await store_async.set_sent_flag(cid, rid)
                await asyncio.sleep(CONFIG["SEND_DELAY"])
                return {"rid": rid, "outcome": "sent", "attempts": attempts,
                        "sent_at_ms": sent_at}
            if res == "blocked":
                return {"rid": rid, "outcome": "already_sent",
                        "attempts": attempts}
            if not retryable:
                break
            if i < IN_PROCESS_TRIES - 1:
                await store_async.incr_counter(cid, "http_retries")
                await asyncio.sleep(BACKOFFS[i] * CONFIG["RETRY_SCALE"])
        return {"rid": rid, "attempts": attempts, **_failure_fields(attempts)}


async def send_batch_async(campaign_id, batch):
    """Same logic as send_batch_sync on asyncio (rupy async variant)."""
    results = await asyncio.gather(
        *[_send_one_async(campaign_id, rid, page) for rid, page in batch])
    store.mark_results(campaign_id, results)   # one sync pipeline per batch
    return _batch_summary(batch, results)


# ------------------------------------------------------------------- summary --
def _batch_summary(batch, results):
    counts = {}
    for r in results:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
    return {"batch": len(batch), **counts}
