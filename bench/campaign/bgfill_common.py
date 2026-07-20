"""Background-fill noise, ONE implementation for both stacks.

ghost_job:           50 iterations of { GET /conversations (1 call), sleep 1s }
                     (~60-75s per run). The stack wrappers re-enqueue it while
                     the campaign runs (bg:run flag set by the driver).
webhook_drain_tick:  claim up to 50 rows from the seeded webhook inbox list,
                     10ms fake processing each, 5% injected failures with
                     exponential backoff (2^attempts seconds via a delayed
                     zset), dead letter at 5 attempts.

Counters land in bg:ctr:* (db 3) so the driver can report that the noise
actually ran.
"""
import json
import random
import time

import requests

import campconfig
import store

GHOST_ITERATIONS = 50
CONV_URL = campconfig.GRAPH_URL + "/conversations"

WEBHOOK_INBOX = "bg:webhook:inbox"
WEBHOOK_DELAYED = "bg:webhook:delayed"
WEBHOOK_DEAD = "bg:webhook:dead"
WEBHOOK_FAIL_RATE = 0.05
WEBHOOK_MAX_ATTEMPTS = 5
WEBHOOK_CLAIM = 50
WEBHOOK_ROW_S = 0.010


def ghost_job():
    ok = 0
    for i in range(GHOST_ITERATIONS):
        try:
            requests.get(CONV_URL, params={"page": i}, timeout=30)
            store.incr_bg("ghost_calls")
            ok += 1
        except requests.RequestException:
            store.incr_bg("ghost_errors")
        time.sleep(1.0)
    store.incr_bg("ghost_runs")
    return ok


def seed_webhook_inbox(n=500):
    r = store.conn()
    r.delete(WEBHOOK_INBOX, WEBHOOK_DELAYED, WEBHOOK_DEAD)
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        pipe.rpush(WEBHOOK_INBOX, json.dumps({"id": f"w{i:05d}", "attempts": 0}))
    pipe.execute()


def webhook_drain_tick():
    r = store.conn()
    now = int(time.time() * 1000)
    # move due retries back into the inbox (ZREM winner pushes, drain-safe)
    due = r.zrangebyscore(WEBHOOK_DELAYED, "-inf", now, start=0, num=200)
    for m in due:
        if r.zrem(WEBHOOK_DELAYED, m):
            r.rpush(WEBHOOK_INBOX, m)
    rows = r.lpop(WEBHOOK_INBOX, WEBHOOK_CLAIM) or []
    processed = retried = dead = 0
    for raw in rows:
        row = json.loads(raw)
        time.sleep(WEBHOOK_ROW_S)                 # fake processing
        if random.random() < WEBHOOK_FAIL_RATE:   # injected failure
            row["attempts"] += 1
            store.incr_bg("webhook_failures")
            if row["attempts"] >= WEBHOOK_MAX_ATTEMPTS:
                r.rpush(WEBHOOK_DEAD, json.dumps(row))
                store.incr_bg("webhook_dead")
                dead += 1
            else:
                backoff_ms = int((2 ** row["attempts"]) * 1000)
                r.zadd(WEBHOOK_DELAYED, {json.dumps(row): now + backoff_ms})
                retried += 1
        else:
            store.incr_bg("webhook_processed")
            processed += 1
    return {"claimed": len(rows), "processed": processed,
            "retried": retried, "dead": dead}


def webhook_counts():
    """Driver-side snapshot of bg-fill activity."""
    r = store.conn()
    return {
        "ghost_runs": store.get_bg("ghost_runs"),
        "ghost_calls": store.get_bg("ghost_calls"),
        "ghost_errors": store.get_bg("ghost_errors"),
        "webhook_processed": store.get_bg("webhook_processed"),
        "webhook_failures": store.get_bg("webhook_failures"),
        "webhook_dead": store.get_bg("webhook_dead"),
        "webhook_inbox_left": r.llen(WEBHOOK_INBOX),
        "webhook_delayed_left": r.zcard(WEBHOOK_DELAYED),
    }
