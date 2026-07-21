"""Stage-2 persister (scenario C), ONE implementation for both stacks.

Successful sends RPUSH a compact JSON record onto redis list `results_raw`
(suite redis, its own db, default 4). The persister task drains it:
LPOP 500, bulk INSERT into Postgres with ON CONFLICT (recipient_id) DO
NOTHING (idempotent, mirrors the production external_message_id dedup),
records per-row persist lag (commit-batch granularity) and a persists/s
timeline, then self-chains while work remains (like ghost_job): immediate
re-enqueue when backlog remains, 1s-delayed re-enqueue while the campaign is
still running (bg:run flag), stop otherwise.

PG access: psycopg2, one short-lived connection per drain call (thread-safe
under cauli's thread pool and celery prefork without shared-state care).
"""

import json
import time

import psycopg2
from psycopg2.extras import execute_values
import redis

import campconfig
import store

RESULTS_DB = int(__import__("os").environ.get("CAMPAIGN_RESULTS_DB", "4"))
RESULTS_KEY = "results_raw"
LAGS_KEY = "persist:lags"
TIMELINE_KEY = "persist:timeline"  # hash: abs_10s_bucket -> rows persisted
DRAIN_LIMIT = 500

PG_DSN = __import__("os").environ.get(
    "CAMPAIGN_PG_DSN", "host=127.0.0.1 port=5432 dbname=bench user=bench password=bench"
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS messages (
    recipient_id      text PRIMARY KEY,
    campaign_id       text,
    page_id           text,
    message_id        text,
    sent_at_ms        bigint,
    enqueued_first_ms bigint
)
"""

_client = None


def results_conn():
    global _client
    if _client is None:
        _client = redis.Redis(
            host="127.0.0.1",
            port=campconfig.REDIS_PORT,
            db=RESULTS_DB,
            decode_responses=True,
        )
    return _client


_aclients = {}


def results_aconn():
    import asyncio
    import redis.asyncio as aredis

    loop = asyncio.get_running_loop()
    c = _aclients.get(id(loop))
    if c is None:
        c = aredis.Redis(
            host="127.0.0.1",
            port=campconfig.REDIS_PORT,
            db=RESULTS_DB,
            decode_responses=True,
        )
        _aclients[id(loop)] = c
    return c


def push_result(record):
    results_conn().rpush(RESULTS_KEY, json.dumps(record, separators=(",", ":")))


async def push_result_async(record):
    await results_aconn().rpush(RESULTS_KEY, json.dumps(record, separators=(",", ":")))


# --------------------------------------------------------------------- pg ----
def pg_conn():
    return psycopg2.connect(PG_DSN)


def ensure_schema():
    c = pg_conn()
    try:
        with c, c.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    finally:
        c.close()


def truncate():
    c = pg_conn()
    try:
        with c, c.cursor() as cur:
            cur.execute("TRUNCATE messages")
    finally:
        c.close()


def pg_count():
    c = pg_conn()
    try:
        with c, c.cursor() as cur:
            cur.execute("SELECT count(*) FROM messages")
            return int(cur.fetchone()[0])
    finally:
        c.close()


# ------------------------------------------------------------------- drain ---
def backlog_len():
    return results_conn().llen(RESULTS_KEY)


def drain_once(limit=DRAIN_LIMIT):
    """LPOP up to limit records, bulk-upsert into pg. Returns rows drained."""
    raw = results_conn().lpop(RESULTS_KEY, limit)
    if not raw:
        return 0
    recs = [json.loads(x) for x in raw]
    c = pg_conn()
    try:
        with c, c.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO messages (recipient_id, campaign_id, page_id, "
                "message_id, sent_at_ms, enqueued_first_ms) VALUES %s "
                "ON CONFLICT (recipient_id) DO NOTHING",
                [
                    (
                        r["recipient_id"],
                        r["campaign_id"],
                        r["page_id"],
                        r["message_id"],
                        r["sent_at_ms"],
                        r["enqueued_first_ms"],
                    )
                    for r in recs
                ],
            )
    finally:
        c.close()
    now = int(time.time() * 1000)
    pipe = results_conn().pipeline(transaction=False)
    for r in recs:
        pipe.rpush(LAGS_KEY, now - int(r["sent_at_ms"]))
    pipe.hincrby(TIMELINE_KEY, str(now // 10000), len(recs))
    pipe.incrby("persist:drained", len(recs))
    pipe.execute()
    return len(recs)


def drain_and_chain(reenqueue):
    """Persister task body. reenqueue(countdown_s or None) re-enqueues self."""
    n = drain_once()
    if backlog_len() > 0:
        reenqueue(None)  # more work: chain immediately
    elif store.bg_active():
        reenqueue(1.0)  # idle but campaign still running: poll in 1s
    return n


# ---------------------------------------------------------------- reporting --
def collect_lags():
    r = results_conn()
    total = r.llen(LAGS_KEY)
    out = []
    for off in range(0, total, 50000):
        out.extend(float(x) for x in r.lrange(LAGS_KEY, off, off + 49999))
    return out


def persist_timeline():
    """{abs_10s_bucket(int): rows(int)} from the drain-side counter."""
    return {int(k): int(v) for k, v in results_conn().hgetall(TIMELINE_KEY).items()}
