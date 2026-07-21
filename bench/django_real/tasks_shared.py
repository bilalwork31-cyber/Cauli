"""ONE task-logic implementation imported by BOTH stacks (Celery and cauli),
adapted from bench/campaign/campaign_common_c.py with the Django ORM replacing
the redis recipient store.

Same recipe as scenario C: dispatch claims + chunks into batches of
BATCH_SIZE; per recipient inside the per-page semaphore (CONCURRENT_PER_PAGE,
reused from campaign_common): intent lock -> attempts -> sent-flag check ->
tripwire -> POST (1 + up to 3 in-process HTTP retries, backoffs 1/2/4s) ->
classify; production backoff min(90, 8*2^(n-1)) + rand(1,15)s at batch level,
capped at MAX_ATTEMPTS. Successful sends push a persist record to redis
results_raw FIRST, then set the sent flag (crash between = re-send +
ON CONFLICT dedup, never a lost record) - identical ordering to C.

Both stacks run the SYNC send path because the Django ORM is synchronous
(documented deviation from run C where cauli used the async redis variant).
The per-batch ThreadPoolExecutor of the C variant is replaced by ONE
process-global executor + identical semaphore caps: per-batch concurrency is
still CONCURRENT_GLOBAL(15) on Celery (pool size 15, one batch per child at a
time under prefetch=1) and per-page is still CONCURRENT_PER_PAGE(3)
everywhere. Long-lived threads keep Django's per-thread Postgres connections
persistent instead of churning 15 new connections per batch, which no real
deployment would do. Pool size per process comes from DJ_SEND_POOL (Celery
children: 15; cauli single process: 240, its many-batches-in-one-process
concurrency budget).

Persist stage keeps the C shape: send results buffered to redis (db 4) via
persist_common.push_result, drained in LPOP-500 batches by self-chaining
persister tasks, bulk-inserted with ignore_conflicts into SendLog. Lag/
timeline bookkeeping reuses persist_common's redis keys so driver reporting
matches run C.

Background fill mirrors bgfill_common paces exactly: ghost_job = 50 x
{1 GET /conversations + 1s sleep} now also writing a BackfillJob ORM row per
iteration; webhook_drain_tick = claim up to 50 inbox rows (FOR UPDATE SKIP
LOCKED), 10ms fake processing each, 5% injected failures with 2^attempts
seconds backoff, dead-letter at 5 attempts - rows in WebhookInboxItem instead
of a redis list.
"""

import concurrent.futures
import json
import os
import socket
import threading
import time

import requests
from django.db import transaction
from django.db.models import F

import django_boot  # noqa: F401
from campaigns.models import BackfillJob, SendLog, WebhookInboxItem

import campaign_common as cc  # shared semaphores/backoffs/summary
import campconfig
import bgfill_common  # pace constants only (no redis-list use)
import orm_store
import persist_common  # redis buffer + lag/timeline bookkeeping
import store as redis_store  # bench counters / bg flags (redis db 3)

CONFIG = campconfig.CONFIG
SEND_URL = campconfig.GRAPH_URL + "/me/messages"
CONV_URL = campconfig.GRAPH_URL + "/conversations"
now_ms = orm_store.now_ms


# ------------------------------------------------------------------ dispatch -
def dispatch_tick(campaign_id):
    """Claim due rows via the ORM, chunk into batches. Caller enqueues."""
    active = orm_store.active_campaigns()
    quota = max(1, CONFIG["APP_MAX_PER_MINUTE"] // active)
    limit = min(quota, CONFIG["MAX_BATCHES_PER_DISPATCH"] * CONFIG["BATCH_SIZE"])
    claimed = orm_store.claim_batch(campaign_id, now_ms(), limit)
    bs = CONFIG["BATCH_SIZE"]
    batches = [claimed[i : i + bs] for i in range(0, len(claimed), bs)]
    total = orm_store.campaign_total(campaign_id)
    queue = "campaign_short" if total <= 500 else "campaign_long"
    return {"batches": batches, "queue": queue, "claimed": len(claimed)}


# ------------------------------------------------------------------ send path
_pool = None
_pool_lock = threading.Lock()


def send_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                size = int(
                    os.environ.get("DJ_SEND_POOL", str(CONFIG["CONCURRENT_GLOBAL"]))
                )
                _pool = concurrent.futures.ThreadPoolExecutor(
                    max_workers=size, thread_name_prefix="send"
                )
    return _pool


def _record(cid, rid, page_id, message_id, sent_at_ms, enq_first_ms):
    return {
        "recipient_id": rid,
        "campaign_id": cid,
        "page_id": page_id,
        "message_id": message_id,
        "sent_at_ms": sent_at_ms,
        "enqueued_first_ms": enq_first_ms,
    }


def _post_once(cid, rid, page_id):
    """One POST through the DB tripwire. -> ('ok'|'fail'|'blocked', retryable,
    message_id|None). Same classification as campaign_common_c."""
    if not orm_store.record_send_attempt(cid, rid):
        return "blocked", False, None
    try:
        resp = requests.post(
            SEND_URL,
            json={"campaign_id": cid, "recipient_id": rid, "page_id": page_id},
            timeout=30,
        )
    except requests.RequestException:
        return "fail", True, None
    if resp.status_code == 200:
        try:
            mid = resp.json().get("message_id", "unknown")
        except ValueError:
            mid = "unknown"
        return "ok", False, mid
    return "fail", (resp.status_code == 429 or resp.status_code >= 500), None


def _send_one(cid, rid, page_id):
    with cc._page_sem(page_id):
        if not orm_store.acquire_lock(cid, rid):
            redis_store.incr_counter(cid, "lock_skips")
            return {"rid": rid, "outcome": "lock_skip"}
        attempts, flag, enq_first = orm_store.begin_send(rid)
        if flag:
            return {"rid": rid, "outcome": "already_sent", "attempts": attempts}
        for i in range(cc.IN_PROCESS_TRIES):
            res, retryable, mid = _post_once(cid, rid, page_id)
            if res == "ok":
                sent_at = now_ms()
                persist_common.push_result(
                    _record(cid, rid, page_id, mid, sent_at, enq_first)
                )
                orm_store.set_sent_flag(rid)
                if CONFIG["SEND_DELAY"] > 0:
                    time.sleep(CONFIG["SEND_DELAY"])
                return {
                    "rid": rid,
                    "outcome": "sent",
                    "attempts": attempts,
                    "sent_at_ms": sent_at,
                }
            if res == "blocked":
                return {"rid": rid, "outcome": "already_sent", "attempts": attempts}
            if not retryable:
                break
            if i < cc.IN_PROCESS_TRIES - 1:
                redis_store.incr_counter(cid, "http_retries")
                time.sleep(cc.BACKOFFS[i] * CONFIG["RETRY_SCALE"])
        return {"rid": rid, "attempts": attempts, **cc._failure_fields(attempts)}


def send_batch(campaign_id, batch):
    """batch: [[rid, page_id], ...]. Same function on Celery AND cauli."""
    pool = send_pool()
    futs = [pool.submit(_send_one, campaign_id, rid, page) for rid, page in batch]
    results = [f.result() for f in futs]
    orm_store.mark_results(campaign_id, results)
    return cc._batch_summary(batch, results)


# ------------------------------------------------------------------ persist --
def persist_drain_once(limit=persist_common.DRAIN_LIMIT):
    """LPOP up to limit records from redis, bulk-insert into SendLog with
    ignore_conflicts (= ON CONFLICT DO NOTHING). Same lag/timeline redis
    bookkeeping as persist_common.drain_once."""
    raw = persist_common.results_conn().lpop(persist_common.RESULTS_KEY, limit)
    if not raw:
        return 0
    recs = [json.loads(x) for x in raw]
    SendLog.objects.bulk_create(
        [
            SendLog(
                recipient_rid=r["recipient_id"],
                campaign_cid=r["campaign_id"],
                page_id=r["page_id"],
                message_id=r["message_id"],
                sent_at_ms=r["sent_at_ms"],
                enqueued_first_ms=r["enqueued_first_ms"],
            )
            for r in recs
        ],
        ignore_conflicts=True,
    )
    now = int(time.time() * 1000)
    pipe = persist_common.results_conn().pipeline(transaction=False)
    for r in recs:
        pipe.rpush(persist_common.LAGS_KEY, now - int(r["sent_at_ms"]))
    pipe.hincrby(persist_common.TIMELINE_KEY, str(now // 10000), len(recs))
    pipe.incrby("persist:drained", len(recs))
    pipe.execute()
    return len(recs)


def persist_drain_and_chain(reenqueue):
    """Same chaining contract as persist_common.drain_and_chain."""
    n = persist_drain_once()
    if persist_common.backlog_len() > 0:
        reenqueue(None)  # more work: chain immediately
    elif redis_store.bg_active():
        reenqueue(1.0)  # idle but campaign still running: poll in 1s
    return n


# ------------------------------------------------------------------ bg fill --
def ghost_job():
    """bgfill_common.ghost_job pacing (50 x GET + 1s sleep) writing its
    progress to a BackfillJob ORM row each iteration."""
    job = BackfillJob.objects.create(
        worker=f"{socket.gethostname()}:{os.getpid()}", started_ms=now_ms()
    )
    ok = 0
    for i in range(bgfill_common.GHOST_ITERATIONS):
        try:
            requests.get(CONV_URL, params={"page": i}, timeout=30)
            BackfillJob.objects.filter(pk=job.pk).update(
                pages_fetched=F("pages_fetched") + 1
            )
            redis_store.incr_bg("ghost_calls")
            ok += 1
        except requests.RequestException:
            BackfillJob.objects.filter(pk=job.pk).update(errors=F("errors") + 1)
            redis_store.incr_bg("ghost_errors")
        time.sleep(1.0)
    BackfillJob.objects.filter(pk=job.pk).update(finished=True)
    redis_store.incr_bg("ghost_runs")
    return ok


def seed_webhook_inbox(n=500):
    WebhookInboxItem.objects.all().delete()
    WebhookInboxItem.objects.bulk_create(
        [
            WebhookInboxItem(
                item_key=f"w{i:05d}",
                status="pending",
                attempts=0,
                next_due_ms=0,
                payload={"seq": i},
            )
            for i in range(n)
        ],
        batch_size=1000,
    )


def webhook_drain_tick():
    """bgfill_common.webhook_drain_tick over the ORM: claim up to 50 due rows
    with FOR UPDATE SKIP LOCKED, 10ms fake processing, 5% injected failures
    with 2^attempts seconds backoff, dead at 5 attempts."""
    import random

    now = now_ms()
    with transaction.atomic():
        rows = list(
            WebhookInboxItem.objects.select_for_update(skip_locked=True)
            .filter(status="pending", next_due_ms__lte=now)
            .order_by("next_due_ms")[: bgfill_common.WEBHOOK_CLAIM]
            .values_list("pk", "attempts")
        )
        if rows:
            WebhookInboxItem.objects.filter(pk__in=[r[0] for r in rows]).update(
                status="processing"
            )
    processed = retried = dead = 0
    for pk, attempts in rows:
        time.sleep(bgfill_common.WEBHOOK_ROW_S)  # fake processing
        if random.random() < bgfill_common.WEBHOOK_FAIL_RATE:
            attempts += 1
            redis_store.incr_bg("webhook_failures")
            if attempts >= bgfill_common.WEBHOOK_MAX_ATTEMPTS:
                WebhookInboxItem.objects.filter(pk=pk).update(
                    status="dead", attempts=attempts
                )
                redis_store.incr_bg("webhook_dead")
                dead += 1
            else:
                backoff_ms = int((2**attempts) * 1000)
                WebhookInboxItem.objects.filter(pk=pk).update(
                    status="pending",
                    attempts=attempts,
                    next_due_ms=now_ms() + backoff_ms,
                )
                retried += 1
        else:
            WebhookInboxItem.objects.filter(pk=pk).update(status="done")
            redis_store.incr_bg("webhook_processed")
            processed += 1
    return {
        "claimed": len(rows),
        "processed": processed,
        "retried": retried,
        "dead": dead,
    }
