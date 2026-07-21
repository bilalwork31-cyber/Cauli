"""Recipient store: Redis mimic of the production Postgres recipient table.

Shared by BOTH stacks (Celery and cauli). Lives in db STORE_DB (3) of the suite
redis so it never collides with broker (db 0) or celery backend (db 1).

Key layout:
  campaign:{cid}:r:{rid}    hash  {status, attempts, page_id, next_due_ms,
                                   lease_until_ms, sent_at_ms, enqueued_first_ms}
                                  status: pending|retry|queued|sending|sent|failed|skipped
  campaign:{cid}:due        zset  pending/retry rows, score = next_due_ms
  campaign:{cid}:leased     zset  claimed rows, score = lease_until_ms
  campaign:{cid}:ids        set   all recipient ids (driver counting)
  campaign:{cid}:meta       hash  {total, n_pages, seeded_at_ms}
  campaign:{cid}:duplicates str   TRIPWIRE counter, must stay 0 (see below)
  campaign:{cid}:ctr:{name} str   counters (http_retries, lock_skips, ...)
  send:lock:{cid}:{rid}     str   intent lock  SET NX EX 300
  send:sent:{cid}:{rid}     str   sent flag (blocks any re-send), EX 86400
  campaigns:active          set   active campaigns (dispatch quota divisor)
  bg:run                    str   background-fill "campaign running" flag
  bg:ctr:{name}             str   background-fill counters

Duplicate-protection contract (mirror of production):
  1. claim_batch() is the SKIP LOCKED analog: ONE atomic Lua EVAL pops due
     pending/retry rows plus expired-lease orphans, so concurrent dispatchers
     can never double-claim a live lease.
  2. The intent lock serializes senders per recipient (redelivery safe).
  3. The sent flag is checked under the lock; if present the sender skips
     silently (outcome "already_sent") - the guard WORKING is not a duplicate.
  4. record_send_attempt() is the tripwire every actual POST call site must
     pass through first: if the sent flag is already up THERE, the duplicates
     counter increments and the send is refused. Nonzero duplicates means the
     guard chain was bypassed; the driver asserts it is 0.
"""

import time

import redis

import campconfig

PORT = campconfig.REDIS_PORT
DB = campconfig.STORE_DB
LOCK_TTL_S = 300
SENT_TTL_S = 86400
ACTIVE_SET = "campaigns:active"
BG_RUN_KEY = "bg:run"

_client = None
_scripts = {}
_ids_cache = {}


def conn():
    global _client
    if _client is None:
        _client = redis.Redis(host="127.0.0.1", port=PORT, db=DB, decode_responses=True)
    return _client


def now_ms():
    return int(time.time() * 1000)


def k_hash(cid, rid):
    return f"campaign:{cid}:r:{rid}"


def k_due(cid):
    return f"campaign:{cid}:due"


def k_leased(cid):
    return f"campaign:{cid}:leased"


def k_ids(cid):
    return f"campaign:{cid}:ids"


def k_meta(cid):
    return f"campaign:{cid}:meta"


def k_dup(cid):
    return f"campaign:{cid}:duplicates"


def k_ctr(cid, name):
    return f"campaign:{cid}:ctr:{name}"


def k_lock(cid, rid):
    return f"send:lock:{cid}:{rid}"


def k_flag(cid, rid):
    return f"send:sent:{cid}:{rid}"


# ---------------------------------------------------------------------------
# Lua: atomic claim (SKIP LOCKED analog) + orphan reclaim, single EVAL.
# KEYS[1]=due zset  KEYS[2]=leased zset
# ARGV[1]=now_ms ARGV[2]=limit ARGV[3]=lease_ms ARGV[4]=hash key prefix
# Returns flat [rid, page_id, rid, page_id, ...].
# ---------------------------------------------------------------------------
CLAIM_LUA = """
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])
local pre = ARGV[4]
local out = {}
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, limit)
for _, rid in ipairs(expired) do
  if (#out / 2) >= limit then break end
  local h = pre .. rid
  local st = redis.call('HGET', h, 'status')
  if st == 'queued' or st == 'sending' then
    redis.call('HSET', h, 'status', 'queued', 'lease_until_ms', now + lease)
    if redis.call('HGET', h, 'enqueued_first_ms') == '0' then
      redis.call('HSET', h, 'enqueued_first_ms', now)
    end
    redis.call('ZADD', KEYS[2], now + lease, rid)
    out[#out + 1] = rid
    out[#out + 1] = redis.call('HGET', h, 'page_id') or ''
  else
    redis.call('ZREM', KEYS[2], rid)
  end
end
local remain = limit - (#out / 2)
if remain > 0 then
  local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, remain)
  for _, rid in ipairs(due) do
    local h = pre .. rid
    redis.call('ZREM', KEYS[1], rid)
    redis.call('HSET', h, 'status', 'queued', 'lease_until_ms', now + lease)
    if redis.call('HGET', h, 'enqueued_first_ms') == '0' then
      redis.call('HSET', h, 'enqueued_first_ms', now)
    end
    redis.call('ZADD', KEYS[2], now + lease, rid)
    out[#out + 1] = rid
    out[#out + 1] = redis.call('HGET', h, 'page_id') or ''
  end
end
return out
"""

# Tripwire: KEYS[1]=sent flag  KEYS[2]=duplicates counter. 1=ok to POST.
ATTEMPT_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('INCR', KEYS[2])
  return 0
end
return 1
"""


def _script(name, src):
    s = _scripts.get(name)
    if s is None:
        s = conn().register_script(src)
        _scripts[name] = s
    return s


# ------------------------------------------------------------------ seeding --
def seed_campaign(cid, n, n_pages, at_ms=None):
    """Seed n recipients round-robin over n_pages pages, all due immediately."""
    r = conn()
    now = at_ms if at_ms is not None else now_ms()
    pipe = r.pipeline(transaction=False)
    for i in range(n):
        rid = f"r{i:06d}"
        page = f"p{i % n_pages}"
        pipe.hset(
            k_hash(cid, rid),
            mapping={
                "status": "pending",
                "attempts": 0,
                "page_id": page,
                "next_due_ms": now,
                "lease_until_ms": 0,
                "sent_at_ms": 0,
                "enqueued_first_ms": 0,
            },
        )
        pipe.zadd(k_due(cid), {rid: now})
        pipe.sadd(k_ids(cid), rid)
        if len(pipe) >= 3000:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    r.hset(k_meta(cid), mapping={"total": n, "n_pages": n_pages, "seeded_at_ms": now})
    r.sadd(ACTIVE_SET, cid)
    _ids_cache.pop(cid, None)


def flush_store_db():
    conn().flushdb()
    _ids_cache.clear()


# ------------------------------------------------------------------ claiming -
def claim_batch(cid, at_ms=None, limit=50, lease_ms=None):
    """Atomically claim up to limit due rows (+ expired-lease orphans).

    Sets status=queued, lease_until=now+lease, stamps enqueued_first_ms on
    first claim. Returns [(rid, page_id), ...].
    """
    now = at_ms if at_ms is not None else now_ms()
    lease = lease_ms if lease_ms is not None else campconfig.CONFIG["LEASE_MS"]
    flat = _script("claim", CLAIM_LUA)(
        keys=[k_due(cid), k_leased(cid)],
        args=[now, int(limit), int(lease), f"campaign:{cid}:r:"],
    )
    pairs = [(flat[i], flat[i + 1]) for i in range(0, len(flat), 2)]
    if pairs:
        conn().incrby(k_ctr(cid, "claimed_total"), len(pairs))
    return pairs


# ------------------------------------------------------------------ send path
def acquire_lock(cid, rid):
    return bool(conn().set(k_lock(cid, rid), "1", nx=True, ex=LOCK_TTL_S))


def release_lock(cid, rid):
    conn().delete(k_lock(cid, rid))


def begin_send(cid, rid):
    """Increment attempts, set status=sending. Returns the new attempts."""
    pipe = conn().pipeline(transaction=False)
    pipe.hincrby(k_hash(cid, rid), "attempts", 1)
    pipe.hset(k_hash(cid, rid), "status", "sending")
    res = pipe.execute()
    return int(res[0])


def sent_flag_exists(cid, rid):
    return conn().exists(k_flag(cid, rid)) == 1


def record_send_attempt(cid, rid):
    """Tripwire before an actual POST. False = flag already up, duplicates
    counter incremented, caller MUST NOT send."""
    return (
        int(
            _script("attempt", ATTEMPT_LUA)(
                keys=[k_flag(cid, rid), k_dup(cid)], args=[]
            )
        )
        == 1
    )


def set_sent_flag(cid, rid):
    conn().set(k_flag(cid, rid), "1", ex=SENT_TTL_S)


# ------------------------------------------------------------------ marking --
def mark_results(cid, results):
    """Grouped outcome application (one pipeline per batch).

    results: [{rid, outcome, attempts, sent_at_ms?, next_due_ms?}, ...]
    outcome: sent | already_sent | retry | failed | skipped
    Ordering per row: leave leased zset last-consistent (zrem leased first,
    hash update, due zadd LAST) so a concurrent claim never sees a half state
    it could double-hand-out.
    """
    r = conn()
    pipe = r.pipeline(transaction=False)
    for res in results:
        rid = res["rid"]
        o = res["outcome"]
        att = res.get("attempts", 0)
        h = k_hash(cid, rid)
        if o == "lock_skip":
            continue
        pipe.zrem(k_leased(cid), rid)
        if o == "sent":
            pipe.hset(
                h,
                mapping={
                    "status": "sent",
                    "attempts": att,
                    "sent_at_ms": res["sent_at_ms"],
                    "lease_until_ms": 0,
                },
            )
            pipe.zrem(k_due(cid), rid)
            pipe.delete(k_lock(cid, rid))
        elif o == "already_sent":
            pipe.hset(h, mapping={"status": "sent", "lease_until_ms": 0})
            pipe.zrem(k_due(cid), rid)
            pipe.delete(k_lock(cid, rid))
            pipe.incr(k_ctr(cid, "already_sent"))
        elif o == "retry":
            pipe.hset(
                h,
                mapping={
                    "status": "retry",
                    "attempts": att,
                    "next_due_ms": res["next_due_ms"],
                    "lease_until_ms": 0,
                },
            )
            pipe.delete(k_lock(cid, rid))
            pipe.zadd(k_due(cid), {rid: res["next_due_ms"]})
        elif o in ("failed", "skipped"):
            pipe.hset(h, mapping={"status": o, "attempts": att, "lease_until_ms": 0})
            pipe.zrem(k_due(cid), rid)
            pipe.delete(k_lock(cid, rid))
        else:
            raise ValueError(f"unknown outcome {o!r}")
    pipe.execute()


def mark_result(cid, rid, outcome, **kw):
    mark_results(cid, [{"rid": rid, "outcome": outcome, **kw}])


# ------------------------------------------------------------------ queries --
def campaign_total(cid):
    return int(conn().hget(k_meta(cid), "total") or 0)


def active_campaigns():
    return max(1, conn().scard(ACTIVE_SET) or 0)


def _ids(cid):
    ids = _ids_cache.get(cid)
    if ids is None:
        ids = sorted(conn().smembers(k_ids(cid)))
        _ids_cache[cid] = ids
    return ids


def count_by_status(cid):
    r = conn()
    ids = _ids(cid)
    counts = {}
    for off in range(0, len(ids), 2000):
        pipe = r.pipeline(transaction=False)
        chunk = ids[off : off + 2000]
        for rid in chunk:
            pipe.hget(k_hash(cid, rid), "status")
        for st in pipe.execute():
            counts[st] = counts.get(st, 0) + 1
    return counts


def collect_rows(cid):
    """Final collection for the driver: one dict per recipient."""
    r = conn()
    ids = _ids(cid)
    fields = ["status", "attempts", "sent_at_ms", "enqueued_first_ms", "page_id"]
    rows = []
    for off in range(0, len(ids), 2000):
        pipe = r.pipeline(transaction=False)
        chunk = ids[off : off + 2000]
        for rid in chunk:
            pipe.hmget(k_hash(cid, rid), fields)
        for rid, vals in zip(chunk, pipe.execute()):
            rows.append(
                {
                    "rid": rid,
                    "status": vals[0],
                    "attempts": int(vals[1] or 0),
                    "sent_at_ms": int(vals[2] or 0),
                    "enqueued_first_ms": int(vals[3] or 0),
                    "page_id": vals[4],
                }
            )
    return rows


def duplicates(cid):
    return int(conn().get(k_dup(cid)) or 0)


def incr_counter(cid, name, n=1):
    conn().incrby(k_ctr(cid, name), n)


def get_counter(cid, name):
    return int(conn().get(k_ctr(cid, name)) or 0)


# ------------------------------------------------------------------ bg fill --
def set_bg_active(active):
    if active:
        conn().set(BG_RUN_KEY, "1")
    else:
        conn().delete(BG_RUN_KEY)


def bg_active():
    return conn().exists(BG_RUN_KEY) == 1


def incr_bg(name, n=1):
    conn().incrby(f"bg:ctr:{name}", n)


def get_bg(name):
    return int(conn().get(f"bg:ctr:{name}") or 0)
