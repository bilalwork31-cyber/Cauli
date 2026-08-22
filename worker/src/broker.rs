//! Redis broker primitives: key naming, consumer group setup, the delayed
//! mover Lua script, idempotency guard, and the pipelined completion writes
//! (PROTOCOL §1, §4.1-§4.3, §4.5).

use crate::envelope::ErrorJson;
use anyhow::Result;
use redis::aio::ConnectionManager;

pub fn q_key(queue: &str) -> String {
    format!("cauli:q:{queue}")
}
pub fn delayed_key(queue: &str) -> String {
    format!("cauli:delayed:{queue}")
}
pub fn dlq_key(queue: &str) -> String {
    format!("cauli:dlq:{queue}")
}
pub fn result_key(id: &str) -> String {
    format!("cauli:result:{id}")
}
/// Round constants for `sha256_block`, the first 32 bits of the fractional
/// parts of the cube roots of the first 64 primes (FIPS 180-4 §4.2.2).
#[rustfmt::skip]
const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// One FIPS 180-4 §6.2.2 compression round over a 64-byte block.
fn sha256_block(h: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for (wi, chunk) in w.iter_mut().zip(block.chunks_exact(4)) {
        *wi = u32::from_be_bytes(chunk.try_into().expect("4-byte chunk"));
    }
    for i in 16..64 {
        let a = w[i - 15];
        let b = w[i - 2];
        let s0 = a.rotate_right(7) ^ a.rotate_right(18) ^ (a >> 3);
        let s1 = b.rotate_right(17) ^ b.rotate_right(19) ^ (b >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    // Working state a..h as one array, rotated each round, so the eight
    // registers stay readable without eight single-letter bindings.
    let mut s = *h;
    for (k, wi) in SHA256_K.iter().zip(w.iter()) {
        let s1 = s[4].rotate_right(6) ^ s[4].rotate_right(11) ^ s[4].rotate_right(25);
        let ch = (s[4] & s[5]) ^ (!s[4] & s[6]);
        let t1 = s[7]
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(*k)
            .wrapping_add(*wi);
        let s0 = s[0].rotate_right(2) ^ s[0].rotate_right(13) ^ s[0].rotate_right(22);
        let maj = (s[0] & s[1]) ^ (s[0] & s[2]) ^ (s[1] & s[2]);
        let t2 = s0.wrapping_add(maj);
        s.rotate_right(1); // h<-g, g<-f, ... b<-a; s[0] and s[4] set below
        s[0] = t1.wrapping_add(t2);
        s[4] = s[4].wrapping_add(t1);
    }
    for (hv, sv) in h.iter_mut().zip(s) {
        *hv = hv.wrapping_add(sv);
    }
}

/// SHA-256 (FIPS 180-4) of `data`. Implemented here rather than pulled in as
/// a dependency: it is one call site on a path already dominated by a redis
/// round trip, and the worker's dependency set is deliberately small.
fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut block = [0u8; 64];
    let mut chunks = data.chunks_exact(64);
    for chunk in chunks.by_ref() {
        block.copy_from_slice(chunk);
        sha256_block(&mut h, &block);
    }
    // Padding: 0x80, zeros, then the message length in BITS as a big-endian
    // u64. One extra block when the remainder leaves no room for both.
    let rem = chunks.remainder();
    let mut tail = [0u8; 128];
    tail[..rem.len()].copy_from_slice(rem);
    tail[rem.len()] = 0x80;
    let tail_len = if rem.len() + 9 <= 64 { 64 } else { 128 };
    let bits = (data.len() as u64).wrapping_mul(8);
    tail[tail_len - 8..tail_len].copy_from_slice(&bits.to_be_bytes());
    for chunk in tail[..tail_len].chunks_exact(64) {
        block.copy_from_slice(chunk);
        sha256_block(&mut h, &block);
    }
    let mut out = [0u8; 32];
    for (slot, word) in out.chunks_exact_mut(4).zip(h) {
        slot.copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// Deterministic digest of an app-supplied idempotency_key, hex-encoded.
/// Folds an arbitrary-length, arbitrary-charset string (attacker/app
/// controlled per audit M1) into a bounded, redis-key-safe token:
/// neutralizes cluster hash-tag injection (`{...}`) and unbounded key-size
/// DoS.
///
/// SHA-256 truncated to 128 bits, NOT a fast non-cryptographic hash. A
/// collision here is not a hash-table nuisance, it is silent task loss: the
/// colliding task takes the `Duplicate` branch, gets acked, XDELed and
/// written a duplicate result, and never runs. FNV-1a 64-bit (what this used
/// to be) is trivially invertible, so anyone who can influence one
/// idempotency_key could suppress another tenant's task on demand, and even
/// without an adversary the birthday bound puts accidental collisions in
/// reach of a busy deployment. 128 bits keeps both out of reach while the
/// key stays a fixed 32 hex chars.
fn idemp_digest_hex(s: &str) -> String {
    use std::fmt::Write as _;
    let d = sha256(s.as_bytes());
    let mut out = String::with_capacity(32);
    for b in &d[..16] {
        let _ = write!(out, "{b:02x}"); // writing to a String cannot fail
    }
    out
}

pub fn idemp_key(key: &str) -> String {
    format!("cauli:idemp:{}", idemp_digest_hex(key))
}

/// PROTOCOL §4.3 delayed mover script (verbatim).
pub const MOVER_LUA: &str = r#"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i, e in ipairs(due) do
  -- XADD before ZREM, deliberately. A script is atomic against other
  -- clients but does NOT roll back on its own error: every write it already
  -- made stays committed. Publishing first means a failure here (say the
  -- stream key now holds the wrong type) can only duplicate this entry,
  -- never lose it. The reverse order would remove it from the set with no
  -- guarantee it ever reached the stream. Do not swap these two lines.
  redis.call('XADD', KEYS[2], '*', 'e', e)
  redis.call('ZREM', KEYS[1], e)
end
return #due
"#;

pub async fn ensure_groups(conn: &mut ConnectionManager, queues: &[String]) -> Result<()> {
    for q in queues {
        let r: redis::RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(q_key(q))
            .arg("cauli")
            .arg("0")
            .arg("MKSTREAM")
            .query_async(conn)
            .await;
        match r {
            Ok(_) => {}
            Err(e) if e.to_string().contains("BUSYGROUP") => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Run the §4.3 mover once for one queue. Returns moved count.
pub async fn run_mover(
    conn: &mut ConnectionManager,
    script: &redis::Script,
    queue: &str,
    now_ms: u64,
    limit: usize,
) -> Result<i64> {
    let n: i64 = script
        .key(delayed_key(queue))
        .key(q_key(queue))
        .arg(now_ms)
        .arg(limit)
        .invoke_async(conn)
        .await?;
    Ok(n)
}

/// True if `e` is Redis Cluster's CROSSSLOT. This is a permanent property of
/// a script's declared keys (here, `delayed_key` and `q_key` never share a
/// hash tag), not a transient condition: the same script fails the same way
/// on every future call, so a caller must not log or retry it like an
/// ordinary redis error.
pub fn is_crossslot(e: &anyhow::Error) -> bool {
    e.downcast_ref::<redis::RedisError>()
        .is_some_and(|re| re.kind() == redis::ErrorKind::CrossSlot)
}

/// True if `e` is Redis's NOGROUP: the consumer group named in the command
/// does not exist, because the group or the whole stream key is gone. Matched
/// on the error CODE, not on message text and not by widening the caller's
/// generic error arm: a NOGROUP means the broker dataset was reset under a
/// live connection, and it is the one XREADGROUP failure that never clears by
/// waiting (see `loops::recreate_groups`).
pub fn is_nogroup(e: &redis::RedisError) -> bool {
    e.code() == Some("NOGROUP")
}

/// §4.5 idempotency guard outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum IdempClaim {
    /// Fresh claim: no one held the key. Execute.
    Fresh,
    /// The key is already held by THIS task's own id (a retry re-enqueues the
    /// same id, and a crash-redelivered claim per §4.4 does too). This is our
    /// own earlier claim, not someone else's: proceed with execution (fixes
    /// audit C1 — without this, a task's own retry finds its own claim and
    /// silently resolves as "duplicate" forever, so retry + idempotency_key
    /// could never be used together).
    MineAgain,
    /// The key is held by a DIFFERENT task id: a genuine duplicate.
    /// `claimant` is that id, carried back so a suppressed caller can look up
    /// the claimant's own outcome. Empty only in the race where the key
    /// expired between the failed SET and the GET of its holder.
    Duplicate { claimant: String },
}

/// §4.5 idempotency guard. Atomic via a single Lua script: `SET NX`, and on
/// failure `GET` the existing value to distinguish "my own claim" (proceed)
/// from "someone else's claim" (duplicate) — see `IdempClaim`.
///
/// The PEXPIRE in the "mine again" branch is what extends the lease across a
/// retry or a §4.4 crash redelivery: without it the window stays anchored at
/// the FIRST claim, so a retry chain outlives the key it claimed.
///
/// Returns `{code, holder}`: the holder's task id travels back with the
/// duplicate verdict, since nothing else ever tells a suppressed caller which
/// execution took the key. Empty in the branches that have no other holder to
/// name, so the reply is always a two element array.
const IDEMP_CLAIM_LUA: &str = r#"
local ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if ok then
  return {1, ''}
end
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return {2, ''}
end
return {0, cur or ''}
"#;

/// TTL a claim is actually written with, derived from the execution it
/// guards rather than taken as configured. `idemp_ttl` is one global number
/// and `timeout_ms` is per task, so a plain `idemp_ttl` shorter than the
/// task's own timeout expires the key mid execution and the next attempt
/// claims Fresh, which is exactly the duplicate concurrent run the key
/// exists to prevent. Do not simplify this back to `idemp_ttl`.
fn claim_ttl_s(idemp_ttl_s: u64, timeout_ms: u64) -> u64 {
    let execution_s = timeout_ms
        .saturating_add(crate::exec::BACKSTOP_GRACE_MS)
        .div_ceil(1000);
    idemp_ttl_s.max(execution_s)
}

/// Built once, not per task. `redis::Script::new` computes the script's SHA1
/// on construction, so building it inside `idemp_claim` re-hashed the source
/// on every single idempotent task before the EVALSHA could even be sent.
static IDEMP_CLAIM_SCRIPT: std::sync::LazyLock<redis::Script> =
    std::sync::LazyLock::new(|| redis::Script::new(IDEMP_CLAIM_LUA));

pub async fn idemp_claim(
    conn: &mut ConnectionManager,
    key: &str,
    task_id: &str,
    idemp_ttl_s: u64,
    timeout_ms: u64,
) -> Result<IdempClaim> {
    let script = &*IDEMP_CLAIM_SCRIPT;
    let ttl_s = claim_ttl_s(idemp_ttl_s, timeout_ms);
    let (code, holder): (i64, String) = script
        .key(idemp_key(key))
        .arg(task_id)
        .arg(ttl_s)
        .arg(ttl_s.saturating_mul(1000))
        .invoke_async(conn)
        .await?;
    Ok(match code {
        1 => IdempClaim::Fresh,
        2 => IdempClaim::MineAgain,
        _ => IdempClaim::Duplicate { claimant: holder },
    })
}

/// §4.1 success: [SET result EX ttl]? + XACK + XDEL, one pipeline.
pub async fn finish_success(
    conn: &mut ConnectionManager,
    queue: &str,
    stream_id: &str,
    task_id: &str,
    result_json: Option<&str>, // None when store_result = false
    result_ttl_s: u64,
) -> Result<()> {
    let mut pipe = redis::pipe();
    if let Some(rj) = result_json {
        pipe.cmd("SET")
            .arg(result_key(task_id))
            .arg(rj)
            .arg("EX")
            .arg(result_ttl_s)
            .ignore();
    }
    add_ack_del(&mut pipe, queue, stream_id);
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

/// Duplicate resolution (§4.5): optional duplicate result + XACK + XDEL.
pub async fn finish_duplicate(
    conn: &mut ConnectionManager,
    queue: &str,
    stream_id: &str,
    task_id: &str,
    result_json: Option<&str>,
    result_ttl_s: u64,
) -> Result<()> {
    finish_success(conn, queue, stream_id, task_id, result_json, result_ttl_s).await
}

/// §4.2 retry: ZADD delayed + XACK + XDEL (no result key), one pipeline.
pub async fn finish_retry(
    conn: &mut ConnectionManager,
    queue: &str,
    stream_id: &str,
    envelope_json: &str,
    fire_at_ms: u64,
) -> Result<()> {
    let mut pipe = redis::pipe();
    pipe.cmd("ZADD")
        .arg(delayed_key(queue))
        .arg(fire_at_ms)
        .arg(envelope_json)
        .ignore();
    add_ack_del(&mut pipe, queue, stream_id);
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

/// Cap on each DLQ stream (`cauli:dlq:{queue}`), enforced with approximate
/// XADD MAXLEN below. Unbounded, a long lived worker under a sustained
/// trickle of failures grows the stream forever until Redis runs out of
/// memory, which takes down every queue in the deployment, not just the
/// failing one. 1000 keeps enough recent history to see a failure trend
/// (hours to days at realistic failure rates) while bounding the worst case,
/// every entry near --max-envelope-bytes (default 1 MiB), to roughly 1 GB
/// per queue instead of unbounded. Past the cap the oldest dead letters are
/// dropped: see PROTOCOL.md section 1's key table.
const DLQ_MAXLEN: u64 = 1000;

/// Retention on each DLQ stream key, refreshed by every dead letter write
/// below (`EXPIRE`, so the clock restarts at the most recent failure and a
/// queue that keeps failing keeps its history).
///
/// `DLQ_MAXLEN` alone bounds the stream by COUNT, never by AGE: a queue that
/// dead lettered a handful of tasks once and then went quiet kept the full
/// args and kwargs of every one of them in Redis forever. Those are the same
/// payloads `result_ttl` (default 3600s) expires within the hour, so an
/// operator reading the retention story off `result_ttl` was wrong by
/// several orders of magnitude, and the leftover memory was never reclaimed
/// by anything.
///
/// 7 days rather than `result_ttl`: a dead letter is evidence for a human,
/// and it has to outlive a weekend plus the Monday morning it is read on.
/// The two bounds are complementary — count for a queue failing constantly,
/// age for a queue that failed once — so neither one alone can be dropped.
/// Retention semantics are in PROTOCOL.md section 1's key table.
const DLQ_TTL_S: u64 = 7 * 24 * 60 * 60;

/// DLQ write (final failure §4.2, malformed/unregistered §4, redelivery §4.4):
/// XADD dlq + [SET result]? + XACK + XDEL, one pipeline.
pub async fn finish_dlq(
    conn: &mut ConnectionManager,
    queue: &str,
    stream_id: &str,
    envelope_json: &str,
    reason: &str,
    error: Option<&ErrorJson>,
    result: Option<(&str, &str, u64)>, // (task_id, result_json, ttl_s)
) -> Result<()> {
    let error_field = match error {
        Some(e) => serde_json::to_string(e).unwrap_or_default(),
        None => String::new(),
    };
    let dk = dlq_key(queue);
    let mut pipe = redis::pipe();
    pipe.cmd("XADD")
        .arg(&dk)
        .arg("MAXLEN")
        .arg("~")
        .arg(DLQ_MAXLEN)
        .arg("*")
        .arg("e")
        .arg(envelope_json)
        .arg("reason")
        .arg(reason)
        .arg("error")
        .arg(error_field)
        .ignore();
    // Bound the stream by AGE as well as by count: see DLQ_TTL_S.
    pipe.cmd("EXPIRE").arg(&dk).arg(DLQ_TTL_S).ignore();
    if let Some((task_id, rj, ttl)) = result {
        pipe.cmd("SET")
            .arg(result_key(task_id))
            .arg(rj)
            .arg("EX")
            .arg(ttl)
            .ignore();
    }
    add_ack_del(&mut pipe, queue, stream_id);
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

/// XACK + XDEL for one entry, in their own MULTI/EXEC.
///
/// The pair has to land together or not at all. A pipeline is one write, but
/// a write can tear: if the connection dies after redis has read the XACK and
/// before it reads the XDEL, the entry leaves the pending entries list while
/// staying in the stream. Nothing can ever reach it again, because it is in
/// no PEL, it sits behind the group's last-delivered-id, and nothing XTRIMs
/// `cauli:q:{queue}`, so it stays resident until someone deletes the key by
/// hand. Inside MULTI the tear costs the EXEC instead, and redis discards a
/// transaction whose client disconnects before EXEC: the entry simply stays
/// pending and the §4.4 recovery loop redelivers it, which is the
/// at-least-once behaviour PROTOCOL.md already promises.
///
/// MULTI wraps ONLY this pair, never the surrounding pipeline (so not
/// `Pipeline::atomic()`, which would hoist MULTI to the head of it). Both
/// commands address the single key `cauli:q:{queue}`, so the transaction
/// lives in one slot; the callers' other writes (`cauli:result:{id}`,
/// `cauli:delayed:{queue}`, `cauli:dlq:{queue}`) do not, and a transaction
/// spanning them is CROSSSLOT on a cluster node.
fn add_ack_del(pipe: &mut redis::Pipeline, queue: &str, stream_id: &str) {
    // One allocation, not two: this runs on every single completion (success,
    // duplicate, retry, and DLQ all funnel through here).
    let qk = q_key(queue);
    pipe.cmd("MULTI").ignore();
    pipe.cmd("XACK")
        .arg(&qk)
        .arg("cauli")
        .arg(stream_id)
        .ignore();
    pipe.cmd("XDEL").arg(&qk).arg(stream_id).ignore();
    pipe.cmd("EXEC").ignore();
}

/// Entry id of the group's oldest pending (delivered, unacked) entry, or
/// None when the pending entries list is empty. One `XPENDING key group - +
/// 1`, the same command family the recovery loop already uses.
pub async fn oldest_pending_id(
    conn: &mut ConnectionManager,
    queue: &str,
) -> Result<Option<String>> {
    let r: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
        .arg(q_key(queue))
        .arg("cauli")
        .arg("-")
        .arg("+")
        .arg(1)
        .query_async(conn)
        .await?;
    Ok(r.into_iter().next().map(|(id, ..)| id))
}

/// Entry id of the oldest entry the group has NOT delivered yet (pure
/// backlog), or None when the group is caught up.
///
/// Deliberately anchored on the group's `last-delivered-id` rather than on
/// the stream head: an entry behind that id is either pending (covered by
/// `oldest_pending_id`) or an orphan left behind by an XACK whose XDEL never
/// landed, and an orphan must not be reported as outstanding work forever.
pub async fn oldest_undelivered_id(
    conn: &mut ConnectionManager,
    queue: &str,
) -> Result<Option<String>> {
    let key = q_key(queue);
    let info: redis::streams::StreamInfoGroupsReply = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(&key)
        .query_async(conn)
        .await?;
    let Some(group) = info.groups.iter().find(|g| g.name == "cauli") else {
        return Ok(None); // group gone: the fetch loop's NOGROUP path owns this
    };
    // Exclusive range: the first entry strictly after the last one delivered.
    let r: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(&key)
        .arg(format!("({}", group.last_delivered_id))
        .arg("+")
        .arg("COUNT")
        .arg(1)
        .query_async(conn)
        .await?;
    Ok(r.ids.into_iter().next().map(|e| e.id))
}

/// §4.4 extended XPENDING page: (entry_id, consumer, idle_ms, delivery_count).
/// `start` pages through the PEL: `"-"` for the first page, then `"(<last>"`
/// (exclusive range, XRANGE syntax) to resume after a page's final entry —
/// the recovery loop must not restart from `"-"` within one tick, or entries
/// it skipped (still legitimately running per their own timeout) would make
/// the drain loop spin on the same page forever.
pub async fn xpending_idle(
    conn: &mut ConnectionManager,
    queue: &str,
    min_idle_ms: u64,
    start: &str,
    count: usize,
) -> Result<Vec<(String, String, u64, u64)>> {
    let r: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
        .arg(q_key(queue))
        .arg("cauli")
        .arg("IDLE")
        .arg(min_idle_ms)
        .arg(start)
        .arg("+")
        .arg(count)
        .query_async(conn)
        .await?;
    Ok(r)
}

fn raw_e_field(map: &std::collections::HashMap<String, redis::Value>) -> Option<String> {
    map.get("e")
        .and_then(|v| redis::from_redis_value::<String>(v).ok())
}

/// §4.4 (H1) non-destructive peek at pending entries' envelopes, one
/// pipelined round trip for the whole page: XRANGE by exact id does not
/// touch the PEL (no idle-time reset, no delivery_count bump, no ownership
/// change) — unlike XCLAIM. Used to read each entry's own `timeout_ms`
/// BEFORE deciding whether it is actually stuck (idle long enough relative
/// to ITS OWN timeout, not just the visibility_timeout floor) so a
/// legitimately still-running long task is never reclaimed out from under
/// itself. Per entry: None if it no longer exists (already acked/claimed
/// elsewhere); Some(None) if it exists but has no `e` field.
pub async fn peek_entries(
    conn: &mut ConnectionManager,
    queue: &str,
    entry_ids: &[String],
) -> Result<Vec<Option<Option<String>>>> {
    use redis::streams::StreamRangeReply;
    let qk = q_key(queue);
    let mut pipe = redis::pipe();
    for id in entry_ids {
        pipe.cmd("XRANGE").arg(&qk).arg(id).arg(id);
    }
    let replies: Vec<StreamRangeReply> = pipe.query_async(conn).await?;
    Ok(entry_ids
        .iter()
        .zip(replies)
        .map(|(entry_id, reply)| {
            reply
                .ids
                .iter()
                .find(|sid| sid.id == *entry_id)
                .map(|sid| raw_e_field(&sid.map))
        })
        .collect())
}

/// §4.4 XCLAIM a batch of entries, one pipelined round trip; per entry,
/// returns the envelope field `e` if the claim succeeded and the entry still
/// exists (None means someone else won or the entry vanished). The raw
/// payload is returned even if it is not valid JSON.
pub async fn xclaim_entries(
    conn: &mut ConnectionManager,
    queue: &str,
    consumer: &str,
    min_idle_ms: u64,
    entry_ids: &[String],
) -> Result<Vec<Option<Option<String>>>> {
    use redis::streams::StreamClaimReply;
    let qk = q_key(queue);
    let mut pipe = redis::pipe();
    for id in entry_ids {
        pipe.cmd("XCLAIM")
            .arg(&qk)
            .arg("cauli")
            .arg(consumer)
            .arg(min_idle_ms)
            .arg(id);
    }
    let replies: Vec<StreamClaimReply> = pipe.query_async(conn).await?;
    Ok(entry_ids
        .iter()
        .zip(replies)
        .map(|(entry_id, reply)| {
            reply
                .ids
                .iter()
                .find(|sid| sid.id == *entry_id)
                .map(|sid| raw_e_field(&sid.map))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idemp_key_is_deterministic_and_bounded() {
        // M1: same input -> same key, always, regardless of process/host
        // (idempotency must agree across workers).
        assert_eq!(idemp_key("order-42"), idemp_key("order-42"));
        assert_ne!(idemp_key("order-42"), idemp_key("order-43"));

        // Bounded length regardless of input size or content (neutralizes
        // key-size DoS and cluster hash-tag injection via `{...}`).
        let huge = "x".repeat(1_000_000);
        let hostile = "{tag}".repeat(1000);
        for input in ["", "a", "order-42", &huge, &hostile] {
            let k = idemp_key(input);
            assert!(k.starts_with("cauli:idemp:"));
            assert_eq!(
                k.len(),
                "cauli:idemp:".len() + 32,
                "hash must be a fixed 32 hex chars"
            );
            assert!(
                k[12..].bytes().all(|b| b.is_ascii_hexdigit()),
                "the whole suffix must be hex: {k}"
            );
            assert!(
                !k.contains('{') && !k.contains('}'),
                "no hash-tag characters may survive"
            );
        }
    }

    /// The digest under `idemp_key` must be real SHA-256, not something that
    /// merely looks like hex. A collision is silent task loss (the colliding
    /// task resolves as a duplicate and never runs), so this pins the
    /// implementation against FIPS 180-4 vectors: the empty message, the
    /// single-block "abc" case, and a multi-megabyte input that exercises
    /// the multi-block path and both padding branches.
    #[test]
    fn idemp_digest_is_sha256_not_a_fast_hash() {
        let hex = |d: [u8; 32]| d.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(
            hex(sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(sha256(b"order-42")),
            "3bf8b157c4238eefe5ae4a66eca81c6b887d4dcedb58dd674271859f4dc2edfd"
        );
        assert_eq!(
            hex(sha256(&b"x".repeat(1_000_000))),
            "1b977e9f84f1b26b6ed7f68b0498faee2385ea4125bd29adce4a7d9106ba3134"
        );
        // Lengths straddling both padding boundaries (55/56 = the length
        // field just fits / just does not, 63/64/65 = block edges), the
        // arithmetic most hand written SHA-256 gets wrong.
        for (n, want) in [
            (
                55,
                "fb66d40c3bfff05b0d5af8612d0abfbfacc6f5f26c330bc7ad634f1f44bc20ad",
            ),
            (
                56,
                "4877e564e5e36e367c7c8d59670774becd3350610b6df4c399c9fa9b66da5813",
            ),
            (
                63,
                "a96b8773f21910f6b1fc287629c1533b494d82301420aa3cfe7d8ebbc18ace77",
            ),
            (
                64,
                "ffbf30ab94107b2c14d75cfb455ec94f200400ddc5ce304e0c21894090db055f",
            ),
            (
                65,
                "c4a2649e068ab18f0b332492f541ae0bf011accef2944241c15d13be3aa3e624",
            ),
            (
                119,
                "0ee964660d4956e34132b7b0f5bdc15fd0d365e26186ac9fd97a090d8d5e5508",
            ),
            (
                120,
                "93dd18da6780c736e1a176724e4afb13b035014ce414d9c2675599e3124e41fb",
            ),
        ] {
            assert_eq!(hex(sha256(&b"y".repeat(n))), want, "length {n}");
        }

        // And the key really is the first 128 bits of that digest.
        assert_eq!(
            idemp_key("order-42"),
            format!("cauli:idemp:{}", &hex(sha256(b"order-42"))[..32])
        );
        assert_ne!(idemp_key("order-42"), idemp_key("order-43"));
    }

    #[test]
    fn claim_ttl_never_expires_before_the_execution_it_guards() {
        let grace = crate::exec::BACKSTOP_GRACE_MS;
        // Whichever of the two independent numbers is longer wins.
        assert_eq!(claim_ttl_s(86_400, 300_000), 86_400);
        assert_eq!(
            claim_ttl_s(60, 300_000),
            (300_000 + grace).div_ceil(1000),
            "a 300s task under a 60s idemp_ttl must claim for its execution"
        );
        // Rounds up, never down: a claim one tick short of its own execution
        // is the window this derivation exists to close.
        assert_eq!(claim_ttl_s(0, 1_500), (1_500 + grace).div_ceil(1000));
        assert_eq!(claim_ttl_s(u64::MAX, u64::MAX), u64::MAX);
    }

    /// A throwaway redis-server this test owns, on a port dedicated to
    /// broker.rs's own tests: never :6392 (worker/tests/common), :6391
    /// (py/itest), :6390 (redis_response_timeout.rs), and never :6379.
    struct ThrowawayRedis {
        port: u16,
    }

    impl ThrowawayRedis {
        fn start(port: u16, cluster: bool) -> Self {
            let _ = std::process::Command::new("redis-cli")
                .args(["-p", &port.to_string(), "shutdown", "nosave"])
                .output();
            std::thread::sleep(std::time::Duration::from_millis(150));
            let mut args = vec![
                "--port".to_string(),
                port.to_string(),
                "--save".to_string(),
                String::new(),
                "--appendonly".to_string(),
                "no".to_string(),
                "--daemonize".to_string(),
                "yes".to_string(),
            ];
            if cluster {
                let dir = std::env::temp_dir().join(format!("cauli-test-cluster-{port}"));
                // A stale nodes.conf from an earlier run already claims
                // every slot, and CLUSTER ADDSLOTSRANGE refuses to reclaim
                // an already assigned slot: start genuinely blank every
                // time, not just a fresh process.
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).expect("cluster test dir");
                let conf = dir.join("nodes.conf");
                args.push("--cluster-enabled".to_string());
                args.push("yes".to_string());
                args.push("--cluster-config-file".to_string());
                args.push(conf.to_str().expect("utf8 tmp path").to_string());
            }
            let out = std::process::Command::new("redis-server")
                .args(&args)
                .output()
                .expect("redis-server spawn");
            assert!(out.status.success(), "redis-server failed: {out:?}");
            for _ in 0..50 {
                let ping = std::process::Command::new("redis-cli")
                    .args(["-p", &port.to_string(), "ping"])
                    .output();
                if ping
                    .map(|o| String::from_utf8_lossy(&o.stdout).contains("PONG"))
                    .unwrap_or(false)
                {
                    if cluster {
                        // Single node cluster: claim every slot so ordinary
                        // commands work, then the crossslot check under test
                        // comes purely from KEYS spanning two of them.
                        //
                        // CLUSTER ADDSLOTSRANGE is issued from inside the wait
                        // loop, and the loop reads CLUSTER INFO rather than
                        // redis-cli's exit status, because redis-cli exits 0
                        // even when the server replies with an error. A node
                        // that has just answered PING can still be too early
                        // to accept the command, and under the load of the
                        // full parallel suite it frequently is: the exit
                        // status then says success while no slot was assigned,
                        // and cluster_state stays "fail" forever no matter how
                        // long the loop waits. Retrying until
                        // cluster_slots_assigned reports the full range is
                        // what actually closes that race.
                        let mut became_ok = false;
                        let mut last_info = String::new();
                        for _ in 0..300 {
                            let info = std::process::Command::new("redis-cli")
                                .args(["-p", &port.to_string(), "cluster", "info"])
                                .output()
                                .expect("cluster info");
                            last_info = String::from_utf8_lossy(&info.stdout).into_owned();
                            if last_info.contains("cluster_state:ok") {
                                became_ok = true;
                                break;
                            }
                            if !last_info.contains("cluster_slots_assigned:16384") {
                                let _ = std::process::Command::new("redis-cli")
                                    .args([
                                        "-p",
                                        &port.to_string(),
                                        "cluster",
                                        "addslotsrange",
                                        "0",
                                        "16383",
                                    ])
                                    .output()
                                    .expect("cluster addslotsrange");
                            }
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                        assert!(
                            became_ok,
                            "cluster never reached cluster_state:ok; last CLUSTER INFO:\n{last_info}"
                        );
                    }
                    return Self { port };
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            panic!("redis on {port} did not answer PING");
        }

        fn url(&self) -> String {
            format!("redis://127.0.0.1:{}/0", self.port)
        }
    }

    impl Drop for ThrowawayRedis {
        fn drop(&mut self) {
            let _ = std::process::Command::new("redis-cli")
                .args(["-p", &self.port.to_string(), "shutdown", "nosave"])
                .output();
        }
    }

    /// F1 reproduction. Forces the SECOND operation (XADD, now ordered
    /// first) to error the way the live reproduction did: WRONGTYPE on the
    /// target key. Asserts the actual property that matters: the entry
    /// survives rather than vanishing. Under the old ZREM-then-XADD order
    /// this test fails, the entry is gone from both the set and the stream.
    #[tokio::test]
    async fn mover_lua_creates_before_destroying_on_xadd_error() {
        let redis = ThrowawayRedis::start(6409, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "reorder";
        let member = r#"{"id":"reorder-1","task":"t"}"#;
        let due_at: u64 = 1_000;

        let _: () = redis::cmd("ZADD")
            .arg(delayed_key(queue))
            .arg(due_at)
            .arg(member)
            .query_async(&mut conn)
            .await
            .expect("seed delayed entry");
        // The stream key now holds the wrong type: XADD against it errors.
        let _: () = redis::cmd("SET")
            .arg(q_key(queue))
            .arg("not-a-stream")
            .query_async(&mut conn)
            .await
            .expect("corrupt stream key");

        let script = redis::Script::new(MOVER_LUA);
        let err = run_mover(&mut conn, &script, queue, due_at + 1, 128)
            .await
            .expect_err("WRONGTYPE must surface, not be swallowed");
        assert!(
            err.to_string().to_uppercase().contains("WRONGTYPE"),
            "unexpected error: {err}"
        );

        let score: Option<f64> = redis::cmd("ZSCORE")
            .arg(delayed_key(queue))
            .arg(member)
            .query_async(&mut conn)
            .await
            .expect("zscore");
        assert_eq!(
            score,
            Some(due_at as f64),
            "entry must survive a mid script XADD failure, not vanish"
        );
    }

    /// F2 reproduction: a genuine single node cluster-enabled redis, CRC16
    /// verified to put cauli:q:myqueue and cauli:delayed:myqueue in
    /// different slots (416 and 439). Asserts the error is both real
    /// CROSSSLOT and correctly classified as non-transient, the two facts
    /// mover_loop's loud-vs-quiet branch depends on.
    #[tokio::test]
    async fn mover_crossslot_is_detected_not_treated_as_transient() {
        let redis = ThrowawayRedis::start(6410, true);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let script = redis::Script::new(MOVER_LUA);
        let err = run_mover(&mut conn, &script, "myqueue", 1_000, 128)
            .await
            .expect_err("cauli:q:{queue} and cauli:delayed:{queue} never share a slot");
        assert!(
            err.to_string().to_uppercase().contains("CROSSSLOT"),
            "expected CROSSSLOT, got: {err}"
        );
        assert!(
            is_crossslot(&err),
            "is_crossslot must recognize the real error mover_loop will see: {err}"
        );
    }

    /// The two facts `loops::fetch_loop` splits its error arm on: a broker
    /// that lost the consumer group answers XREADGROUP with a NOGROUP that
    /// `is_nogroup` recognizes, and `ensure_groups` makes the very same call
    /// succeed again afterwards. Driven against a real redis rather than a
    /// synthetic error, since the point is that the code survives the round
    /// trip through the client.
    #[tokio::test]
    async fn nogroup_is_detected_and_ensure_groups_clears_it() {
        let redis = ThrowawayRedis::start(6422, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queues = vec!["reset".to_string()];
        let read = |conn: &mut ConnectionManager| {
            let key = q_key(&queues[0]);
            let mut conn = conn.clone();
            async move {
                redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg("cauli")
                    .arg("c1")
                    .arg("COUNT")
                    .arg(1)
                    .arg("STREAMS")
                    .arg(key)
                    .arg(">")
                    .query_async::<Option<redis::streams::StreamReadReply>>(&mut conn)
                    .await
            }
        };

        let err = read(&mut conn)
            .await
            .expect_err("no group exists on a fresh dataset");
        assert!(
            is_nogroup(&err),
            "is_nogroup must recognize the real error fetch_loop will see: {err} \
             (code {:?})",
            err.code()
        );
        // Another failure of the very same call must NOT take that branch:
        // to the generic handler every one of them is "XREADGROUP failed".
        let _: () = redis::cmd("SET")
            .arg(q_key(&queues[0]))
            .arg("not a stream")
            .query_async(&mut conn)
            .await
            .expect("set");
        let other = read(&mut conn).await.expect_err("wrong type");
        assert!(
            !is_nogroup(&other),
            "only NOGROUP takes that branch: {other}"
        );
        let _: () = redis::cmd("DEL")
            .arg(q_key(&queues[0]))
            .query_async(&mut conn)
            .await
            .expect("del");

        ensure_groups(&mut conn, &queues).await.expect("recreate");
        read(&mut conn).await.expect("group exists again");
    }

    /// The property the derived TTL exists for: a task whose timeout_ms is
    /// far longer than idemp_ttl still holds its claim for the whole
    /// execution, and a second attempt under the same id (a §4.2 retry or a
    /// §4.4 redelivery) pushes the lease out again instead of inheriting
    /// what the first claim had left.
    #[tokio::test]
    async fn claim_outlives_a_long_execution_and_refreshes_on_mine_again() {
        let redis = ThrowawayRedis::start(6420, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let key = "order-77";
        let task_id = "a".repeat(32);
        let timeout_ms: u64 = 600_000; // a ten minute task
        let idemp_ttl_s: u64 = 60; // one minute, if the claim took it as configured

        assert_eq!(
            idemp_claim(&mut conn, key, &task_id, idemp_ttl_s, timeout_ms)
                .await
                .expect("fresh claim"),
            IdempClaim::Fresh
        );
        let pttl: i64 = redis::cmd("PTTL")
            .arg(idemp_key(key))
            .query_async(&mut conn)
            .await
            .expect("pttl");
        assert!(
            pttl > timeout_ms as i64,
            "claim expires in {pttl}ms, before the {timeout_ms}ms execution it guards"
        );

        // Burn the lease down to what a plain idemp_ttl claim would have had
        // left mid execution, then re-enter as the same task id.
        let _: () = redis::cmd("PEXPIRE")
            .arg(idemp_key(key))
            .arg(5_000)
            .query_async(&mut conn)
            .await
            .expect("shorten lease");
        assert_eq!(
            idemp_claim(&mut conn, key, &task_id, idemp_ttl_s, timeout_ms)
                .await
                .expect("second claim"),
            IdempClaim::MineAgain
        );
        let refreshed: i64 = redis::cmd("PTTL")
            .arg(idemp_key(key))
            .query_async(&mut conn)
            .await
            .expect("pttl after refresh");
        assert!(
            refreshed > timeout_ms as i64,
            "mine again must extend its own lease, got {refreshed}ms"
        );
    }

    /// A dead letter carries the task's full args and kwargs, and nothing
    /// ever read them back out. `DLQ_MAXLEN` bounds the stream by count
    /// only, so a queue that failed a few times and then went quiet used to
    /// hold those payloads in Redis forever. The write must therefore leave
    /// the key with a finite TTL, refreshed by each new dead letter, and the
    /// XACK/XDEL half of the pipeline must still land.
    #[tokio::test]
    async fn dlq_write_bounds_the_stream_by_age_not_only_by_count() {
        let redis = ThrowawayRedis::start(6424, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "dead";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let sid: String = redis::cmd("XADD")
            .arg(q_key(queue))
            .arg("*")
            .arg("e")
            .arg(r#"{"id":"d1","task":"t"}"#)
            .query_async(&mut conn)
            .await
            .expect("seed entry");

        finish_dlq(
            &mut conn,
            queue,
            &sid,
            r#"{"id":"d1","task":"t","args":["secret"]}"#,
            "final_failure",
            None,
            None,
        )
        .await
        .expect("dlq write");

        let ttl: i64 = redis::cmd("TTL")
            .arg(dlq_key(queue))
            .query_async(&mut conn)
            .await
            .expect("ttl");
        assert!(
            ttl > 0 && ttl <= DLQ_TTL_S as i64,
            "dlq must not persist forever, TTL was {ttl}"
        );
        let len: u64 = redis::cmd("XLEN")
            .arg(dlq_key(queue))
            .query_async(&mut conn)
            .await
            .expect("xlen");
        assert_eq!(len, 1, "the dead letter itself must still be written");
        // The EXPIRE must not have displaced the ack/delete half.
        let pending: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(q_key(queue))
            .arg("cauli")
            .arg("-")
            .arg("+")
            .arg(10)
            .query_async(&mut conn)
            .await
            .expect("xpending");
        assert!(pending.is_empty(), "entry must be acked: {pending:?}");

        // A queue that keeps failing keeps its history: the next dead letter
        // pushes the expiry back out rather than inheriting what was left.
        let _: () = redis::cmd("EXPIRE")
            .arg(dlq_key(queue))
            .arg(30)
            .query_async(&mut conn)
            .await
            .expect("shorten");
        finish_dlq(
            &mut conn,
            queue,
            "0-0",
            r#"{"id":"d2","task":"t"}"#,
            "final_failure",
            None,
            None,
        )
        .await
        .expect("second dlq write");
        let refreshed: i64 = redis::cmd("TTL")
            .arg(dlq_key(queue))
            .query_async(&mut conn)
            .await
            .expect("ttl after refresh");
        assert!(
            refreshed > 30,
            "each dead letter must refresh retention, got {refreshed}"
        );
    }

    /// A suppressed caller has to be able to find the execution that took the
    /// key. Nothing ever releases a claim, so once the claimant has been dead
    /// lettered every resubmission is suppressed too, and the id in the
    /// verdict is the only route back to what actually happened.
    #[tokio::test]
    async fn duplicate_verdict_names_the_claimant() {
        let redis = ThrowawayRedis::start(6421, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let key = "order-88";
        let claimant = "a".repeat(32);
        let latecomer = "b".repeat(32);

        assert_eq!(
            idemp_claim(&mut conn, key, &claimant, 60, 1_000)
                .await
                .expect("fresh claim"),
            IdempClaim::Fresh
        );
        assert_eq!(
            idemp_claim(&mut conn, key, &latecomer, 60, 1_000)
                .await
                .expect("duplicate claim"),
            IdempClaim::Duplicate {
                claimant: claimant.clone()
            }
        );
    }

    /// The ack and the delete must be one transaction, and ONLY they may be
    /// in it. A torn write between them acks an entry that stays in the
    /// stream, where no PEL scan and no backlog scan can ever reach it again.
    /// The second half matters just as much: pulling a caller's other key
    /// (here the result key) inside the MULTI would make every completion
    /// CROSSSLOT on a cluster node, so the transaction must name
    /// `cauli:q:{queue}` and nothing else.
    #[test]
    fn ack_and_del_are_one_single_slot_transaction() {
        let mut pipe = redis::pipe();
        pipe.cmd("SET")
            .arg(result_key("t1"))
            .arg("{}")
            .arg("EX")
            .arg(60)
            .ignore();
        add_ack_del(&mut pipe, "q", "1-1");
        let wire = String::from_utf8(pipe.get_packed_pipeline()).expect("utf8 wire");
        let at = |needle: &str| {
            wire.find(needle)
                .unwrap_or_else(|| panic!("{needle} missing from wire: {wire:?}"))
        };
        assert!(
            at("SET") < at("MULTI"),
            "the caller's own writes stay outside the transaction: {wire:?}"
        );
        assert!(
            at("MULTI") < at("XACK") && at("XACK") < at("XDEL") && at("XDEL") < at("EXEC"),
            "ack and delete must be queued between MULTI and EXEC: {wire:?}"
        );
        let body = &wire[at("MULTI")..at("EXEC")];
        assert!(
            !body.contains(&result_key("t1")),
            "no second key may enter the transaction: {body:?}"
        );
        assert_eq!(
            body.matches(&q_key("q")).count(),
            2,
            "both queued commands address the one stream key: {body:?}"
        );
    }

    /// End to end proof that the MULTI/EXEC pair is hand written into the
    /// middle of a pipeline correctly: a miscounted reply would fail the
    /// whole completion, and the completion write is what makes a task
    /// finished. Asserts all three effects of a success write land: the
    /// result key with its TTL, the ack, and the delete.
    #[tokio::test]
    async fn finish_success_acks_deletes_and_writes_the_result() {
        let redis = ThrowawayRedis::start(6425, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "done";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let _: String = redis::cmd("XADD")
            .arg(q_key(queue))
            .arg("*")
            .arg("e")
            .arg(r#"{"id":"s1","task":"t"}"#)
            .query_async(&mut conn)
            .await
            .expect("seed entry");
        // Deliver it so the entry is genuinely in the PEL, the state a real
        // completion acks out of.
        let _: Option<redis::streams::StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("cauli")
            .arg("c1")
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(q_key(queue))
            .arg(">")
            .query_async(&mut conn)
            .await
            .expect("deliver");
        let pending: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(q_key(queue))
            .arg("cauli")
            .arg("-")
            .arg("+")
            .arg(10)
            .query_async(&mut conn)
            .await
            .expect("xpending");
        let sid = pending.first().expect("one delivered entry").0.clone();

        finish_success(&mut conn, queue, &sid, "s1", Some(r#"{"ok":true}"#), 60)
            .await
            .expect("success write");

        let ttl: i64 = redis::cmd("TTL")
            .arg(result_key("s1"))
            .query_async(&mut conn)
            .await
            .expect("ttl");
        assert!(ttl > 0 && ttl <= 60, "result key TTL was {ttl}");
        let pending: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(q_key(queue))
            .arg("cauli")
            .arg("-")
            .arg("+")
            .arg(10)
            .query_async(&mut conn)
            .await
            .expect("xpending after");
        assert!(pending.is_empty(), "entry must be acked: {pending:?}");
        let len: u64 = redis::cmd("XLEN")
            .arg(q_key(queue))
            .query_async(&mut conn)
            .await
            .expect("xlen");
        assert_eq!(len, 0, "the acked entry must also be deleted, not orphaned");
    }
}
