"""Async twins of the store hot-path ops, used ONLY by send_batch_async.

Sync redis calls inside coroutines would block the embedded event loop, so the
async send path uses redis.asyncio for its per-recipient operations. One
client per running event loop (redis.asyncio connections are loop-bound).
Semantics are identical to the sync functions in store.py; batch-end
mark_results stays sync (one pipeline per batch, ~1 RTT).
"""

import asyncio

import redis.asyncio as aredis

import store as S

_clients = {}


def _conn():
    loop = asyncio.get_running_loop()
    c = _clients.get(id(loop))
    if c is None:
        c = aredis.Redis(host="127.0.0.1", port=S.PORT, db=S.DB, decode_responses=True)
        _clients[id(loop)] = c
    return c


async def acquire_lock(cid, rid):
    return bool(await _conn().set(S.k_lock(cid, rid), "1", nx=True, ex=S.LOCK_TTL_S))


async def begin_send(cid, rid):
    pipe = _conn().pipeline(transaction=False)
    pipe.hincrby(S.k_hash(cid, rid), "attempts", 1)
    pipe.hset(S.k_hash(cid, rid), "status", "sending")
    res = await pipe.execute()
    return int(res[0])


async def sent_flag_exists(cid, rid):
    return await _conn().exists(S.k_flag(cid, rid)) == 1


async def record_send_attempt(cid, rid):
    """Tripwire before an actual POST (see store.record_send_attempt)."""
    val = await _conn().eval(S.ATTEMPT_LUA, 2, S.k_flag(cid, rid), S.k_dup(cid))
    return int(val) == 1


async def set_sent_flag(cid, rid):
    await _conn().set(S.k_flag(cid, rid), "1", ex=S.SENT_TTL_S)


async def incr_counter(cid, name, n=1):
    await _conn().incrby(S.k_ctr(cid, name), n)
