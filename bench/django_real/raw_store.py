"""Raw-SQL + redis data layer for django_real (selected by DJ_DATA_LAYER=raw).

Same guard-chain contract as orm_store (claim -> intent lock -> attempts ->
sent-flag check -> tripwire -> POST -> outcome), used IDENTICALLY by both
stacks, but the per-send Postgres work is moved OUT of the send hot path:

  1. claim_batch is ONE raw SQL statement per claim call:
     UPDATE ... WHERE rid IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING,
     with the exact due/orphan predicates and 10-minute lease semantics of
     orm_store.claim_batch. RETURNING also carries enqueued_first_ms so the
     send path never needs to read Postgres for the wait metric.
  2. The intent lock is redis: SET cauli:intent:{rid} NX EX 900 (store db 3)
     instead of a conditional UPDATE on Recipient.lock_until_ms. On lock
     failure the sender checks the sent flag: flag up -> already_sent repair
     record; flag down -> lock_skip (row stays leased, reclaimed on expiry),
     the same outcomes the ORM layer produces.
  3. The sent flag is redis cauli:sent:{rid} (EX 86400); the pre-POST
     tripwire is the same Lua contract as bench/campaign/store.py: flag
     already up at POST time -> INCR the campaign duplicates counter and
     refuse the send. The SendLog unique constraint stays as the final
     integrity backstop. Attempts live in redis (INCR cauli:att:{rid}) and
     are persisted onto the row by the persister.
  4. Every send outcome (sent / already_sent / retry / failed) is LPUSHed to
     the shared results_raw list (redis db 4, same key as persist_common so
     backlog / lag / drained reporting is unchanged). The send hot path
     touches NO Postgres at all.
  5. Bg persister tasks RPOP batches of DJ_PERSIST_BATCH (default 50; LPUSH
     head + RPOP tail = FIFO) and apply them with raw executemany:
     INSERT SendLog ON CONFLICT DO NOTHING first (the integrity anchor),
     then UPDATE campaigns_recipient status/attempts/sent flags. Rows are
     sorted by rid inside each statement group so concurrent persisters
     acquire row locks in a consistent order.

Postgres is written only by the dispatcher claim and the persister; redis
carries locks, flags, attempts and the outcome buffer.
"""

import json
import os
import time

from django.db import connection

import django_boot  # noqa: F401  (apps must be loaded before models/SQL)

import persist_common  # results buffer (db 4) + lag/timeline bookkeeping
import store as redis_store  # bench counters / dup counter / bg flags (db 3)

INTENT_TTL_S = 900  # SET cauli:intent:{rid} NX EX 900
SENT_TTL_S = 86400
DRAIN_LIMIT = int(os.environ.get("DJ_PERSIST_BATCH", "50"))


def now_ms():
    return int(time.time() * 1000)


def k_intent(rid):
    return f"cauli:intent:{rid}"


def k_sent(rid):
    return f"cauli:sent:{rid}"


def k_att(rid):
    return f"cauli:att:{rid}"


# ------------------------------------------------------------------ claiming -
CLAIM_SQL = """
UPDATE campaigns_recipient AS r
   SET status = 'queued',
       lease_until_ms = %(lease_until)s,
       enqueued_first_ms = CASE WHEN r.enqueued_first_ms = 0
                                THEN %(now)s ELSE r.enqueued_first_ms END
 WHERE r.rid IN (
       SELECT rid
         FROM campaigns_recipient
        WHERE campaign_id = %(cid)s
          AND ((status IN ('pending', 'retry') AND next_due_ms <= %(now)s)
            OR (status IN ('queued', 'sending') AND lease_until_ms <= %(now)s))
        ORDER BY next_due_ms
        LIMIT %(limit)s
          FOR UPDATE SKIP LOCKED)
RETURNING r.rid, r.page_id, r.enqueued_first_ms
"""


def claim_batch(cid, at_ms=None, limit=50, lease_ms=None):
    """One-statement SKIP LOCKED claim. Returns [(rid, page_id, enq_ms), ...]
    (the extra enqueued_first_ms rides along so senders never read Postgres).
    """
    import campconfig

    now = at_ms if at_ms is not None else now_ms()
    lease = lease_ms if lease_ms is not None else campconfig.CONFIG["LEASE_MS"]
    with connection.cursor() as cur:
        cur.execute(
            CLAIM_SQL,
            {
                "cid": cid,
                "now": now,
                "lease_until": now + int(lease),
                "limit": int(limit),
            },
        )
        rows = [(r[0], r[1], int(r[2])) for r in cur.fetchall()]
    if rows:
        redis_store.incr_counter(cid, "claimed_total", len(rows))
    return rows


# ------------------------------------------------------------------ send path
# Tripwire: KEYS[1]=sent flag  KEYS[2]=duplicates counter. 1 = ok to POST.
ATTEMPT_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('INCR', KEYS[2])
  return 0
end
return 1
"""

_attempt_script = None


def acquire_lock(rid):
    """Intent lock: SET cauli:intent:{rid} NX EX 900."""
    return bool(redis_store.conn().set(k_intent(rid), "1", nx=True, ex=INTENT_TTL_S))


def release_lock(rid):
    redis_store.conn().delete(k_intent(rid))


def sent_flag_exists(rid):
    return redis_store.conn().exists(k_sent(rid)) == 1


def begin_send(rid):
    """attempts += 1 (redis; persisted onto the row by the persister)."""
    return int(redis_store.conn().incr(k_att(rid)))


def record_send_attempt(cid, rid):
    """Tripwire before an actual POST. False = sent flag already up, campaign
    duplicates counter incremented, caller MUST NOT send."""
    global _attempt_script
    if _attempt_script is None:
        _attempt_script = redis_store.conn().register_script(ATTEMPT_LUA)
    return int(_attempt_script(keys=[k_sent(rid), redis_store.k_dup(cid)])) == 1


def set_sent_flag(rid):
    redis_store.conn().set(k_sent(rid), "1", ex=SENT_TTL_S)


def push_outcome(record):
    """LPUSH the outcome record to the shared results_raw list (redis db 4)."""
    persist_common.results_conn().lpush(
        persist_common.RESULTS_KEY, json.dumps(record, separators=(",", ":"))
    )


# ------------------------------------------------------------------ persist --
LOG_SQL = (
    "INSERT INTO campaigns_sendlog (recipient_rid, campaign_cid, page_id, "
    "message_id, sent_at_ms, enqueued_first_ms) VALUES (%s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (recipient_rid) DO NOTHING"
)
SENT_SQL = (
    "UPDATE campaigns_recipient SET status = 'sent', attempts = %s, "
    "sent_at_ms = %s, sent_flag = true, lease_until_ms = 0, lock_until_ms = 0 "
    "WHERE rid = %s"
)
ALREADY_SQL = (
    "UPDATE campaigns_recipient SET status = 'sent', sent_flag = true, "
    "lease_until_ms = 0, lock_until_ms = 0 WHERE rid = %s"
)
RETRY_SQL = (
    "UPDATE campaigns_recipient SET status = 'retry', attempts = %s, "
    "next_due_ms = %s, lease_until_ms = 0 WHERE rid = %s"
)
FAIL_SQL = (
    "UPDATE campaigns_recipient SET status = %s, attempts = %s, "
    "lease_until_ms = 0 WHERE rid = %s"
)


def persist_drain_once(limit=None):
    """RPOP up to limit outcome records (FIFO against the LPUSH producer),
    apply with raw executemany: SendLog insert FIRST (crash between = row
    reclaimed later, sent flag blocks the resend, already_sent repairs the
    status), then the recipient row updates. Lag/timeline bookkeeping uses the
    same redis keys as persist_common so driver reporting is unchanged."""
    n = int(limit) if limit else DRAIN_LIMIT
    raw = persist_common.results_conn().rpop(persist_common.RESULTS_KEY, n)
    if not raw:
        return 0
    recs = [json.loads(x) for x in raw]
    sent, already, retry, failed = [], [], [], []
    for r in recs:
        o = r["outcome"]
        if o == "sent":
            sent.append(r)
        elif o == "already_sent":
            already.append(r)
        elif o == "retry":
            retry.append(r)
        else:  # failed / skipped
            failed.append(r)
    key = lambda r: r["recipient_id"]  # noqa: E731  (consistent lock order)
    with connection.cursor() as cur:
        if sent:
            sent.sort(key=key)
            cur.executemany(
                LOG_SQL,
                [
                    (
                        r["recipient_id"],
                        r["campaign_id"],
                        r["page_id"],
                        r["message_id"],
                        r["sent_at_ms"],
                        r["enqueued_first_ms"],
                    )
                    for r in sent
                ],
            )
            cur.executemany(
                SENT_SQL,
                [
                    (r["attempts"], r["sent_at_ms"], r["recipient_id"])
                    for r in sent
                ],
            )
        if already:
            already.sort(key=key)
            cur.executemany(ALREADY_SQL, [(r["recipient_id"],) for r in already])
        if retry:
            retry.sort(key=key)
            cur.executemany(
                RETRY_SQL,
                [
                    (r["attempts"], r["next_due_ms"], r["recipient_id"])
                    for r in retry
                ],
            )
        if failed:
            failed.sort(key=key)
            cur.executemany(
                FAIL_SQL,
                [(r["outcome"], r["attempts"], r["recipient_id"]) for r in failed],
            )
    now = int(time.time() * 1000)
    pipe = persist_common.results_conn().pipeline(transaction=False)
    for r in sent:
        pipe.rpush(persist_common.LAGS_KEY, now - int(r["sent_at_ms"]))
    if sent:
        pipe.hincrby(persist_common.TIMELINE_KEY, str(now // 10000), len(sent))
    pipe.incrby("persist:drained", len(recs))
    pipe.execute()
    return len(recs)
