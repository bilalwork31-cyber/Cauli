//! Redis broker primitives: key naming, consumer group setup, the delayed
//! mover Lua script, idempotency guard, and the pipelined completion writes
//! (PROTOCOL §1, §4.1-§4.3, §4.5).

use crate::envelope::ErrorJson;
use anyhow::{anyhow, Result};
use redis::aio::ConnectionManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
    for (wi, chunk) in w.iter_mut().zip(block.as_chunks::<4>().0) {
        *wi = u32::from_be_bytes(*chunk);
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
    let (blocks, rem) = data.as_chunks::<64>();
    for block in blocks {
        sha256_block(&mut h, block);
    }
    // Padding: 0x80, zeros, then the message length in BITS as a big-endian
    // u64. One extra block when the remainder leaves no room for both.
    let mut tail = [0u8; 128];
    tail[..rem.len()].copy_from_slice(rem);
    tail[rem.len()] = 0x80;
    let tail_len = if rem.len() + 9 <= 64 { 64 } else { 128 };
    let bits = (data.len() as u64).wrapping_mul(8);
    tail[tail_len - 8..tail_len].copy_from_slice(&bits.to_be_bytes());
    for block in tail[..tail_len].as_chunks::<64>().0 {
        sha256_block(&mut h, block);
    }
    let mut out = [0u8; 32];
    for (slot, word) in out.as_chunks_mut::<4>().0.iter_mut().zip(h) {
        *slot = word.to_be_bytes();
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
    // Nibble table, not `write!(out, "{b:02x}")`. The formatting machinery cost
    // 391.7ns for these 16 bytes against 31.4ns for the table, i.e. core::fmt
    // was more expensive than the SHA-256 it formats (376ns for a 36 byte key).
    // Byte for byte identical output; this is purely how the digest is spelled.
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let d = sha256(s.as_bytes());
    let mut out = Vec::with_capacity(32);
    for b in &d[..16] {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0x0f) as usize]);
    }
    String::from_utf8(out).expect("hex table is ASCII")
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

/// One buffered completion: the caller's own writes (result SET, retry ZADD,
/// DLQ XADD), the entry to ack, and the channel its flush outcome travels
/// back on. Built by the `finish_*` functions below, drained by `flusher`.
struct AckReq {
    stream_id: String,
    extra: Vec<redis::Cmd>,
    done: oneshot::Sender<FlushOutcome>,
}

/// Shared per-flush result: one pipeline serves up to `--ack-batch`
/// completions, so its single error has to be cloneable to every waiter.
type FlushOutcome = std::result::Result<(), Arc<redis::RedisError>>;

/// §4.1 completion buffers, one flusher task per queue.
///
/// Every completion (success, duplicate, retry, DLQ) used to pay redis one
/// round trip of four commands (MULTI + XACK + XDEL + EXEC). Measured on the
/// bench box, the per-round-trip work (socket read, parse, reply build,
/// write) costs redis's single command-execution thread ~10x the commands
/// themselves, and that thread was the whole system's throughput ceiling.
/// Buffering completions per queue and flushing them as ONE pipeline with
/// ONE multi-id XACK and ONE multi-id XDEL (batching an XACK is 6.2x
/// cheaper per id than acking ids singly, and XDEL batches the same way)
/// cuts redis main-thread CPU per task by ~3-5x. The XDEL stays per-flush,
/// not per-entry, and it stays AT ALL — an earlier revision of this design
/// dropped it and left removal to the §4.1 trim alone, whose boundary is
/// the group's oldest PENDING id: one slow task then retained every
/// completed entry behind it (throughput x that task's duration — 30k/s
/// under a default 300s timeout_ms is nine million entries, gigabytes of
/// redis memory). The batched XDEL frees each completed entry at its own
/// flush, whatever else is still running. No MULTI wraps the pair: the old
/// per-entry MULTI existed because a torn XACK/XDEL stranded the entry
/// forever, and `loops::trim_loop` now reclaims exactly that orphan shape,
/// so a tear costs one trim tick of residue instead of a transaction per
/// completion.
///
/// A flush fires at whichever comes first: `--ack-batch` buffered
/// completions, or `--ack-flush-ms` since the OLDEST buffered one. The
/// window is the at-least-once cost of this design and it is bounded: a
/// worker killed mid-window loses only unflushed acks, at most one buffer
/// per queue, and those entries are simply redelivered (§4.4) and resolved
/// by the §4.5 idempotency guard like any other crash duplicate. It is not
/// a durability cost: `finish_*` do not return until their flush has been
/// answered, so a completion the worker has reported (counters, drain) is
/// on the broker.
///
/// Command order inside one flush pipeline is a correctness property, not a
/// layout choice: every caller's own writes (result SET, retry ZADD, DLQ
/// XADD) are queued BEFORE the batch XACK, so a connection torn mid-flush
/// can apply a task's writes without its ack (redelivered, then resolved as
/// a duplicate) but never the ack without the writes (which would drop a
/// retry or a dead letter on the floor). The batch XDEL comes LAST, after
/// the XACK: torn between them, the entries are acked-but-undeleted, which
/// the trim reclaims. The other order would delete entries still in the
/// PEL, and a pending entry whose payload is gone can never be peeked,
/// claimed or acked by §4.4 — it pins XPENDING forever. `build_flush` owns
/// this order and `extras_precede_ack_precedes_del_on_the_wire` pins it.
#[derive(Clone)]
pub struct AckBufs {
    senders: Arc<HashMap<String, mpsc::Sender<AckReq>>>,
}

impl AckBufs {
    /// One buffer + flusher task per queue. Must be called on a tokio
    /// runtime. `batch`/`flush_ms` come from `--ack-batch`/`--ack-flush-ms`.
    pub fn start(conn: &ConnectionManager, queues: &[String], batch: usize, flush_ms: u64) -> Self {
        let batch = batch.max(1);
        let mut senders = HashMap::new();
        for q in queues {
            // Capacity bounds worker memory if redis stalls; senders then
            // wait in `submit`, exactly where they used to wait on their own
            // completion round trip.
            let (tx, rx) = mpsc::channel(batch.saturating_mul(4).max(256));
            tokio::spawn(flusher(conn.clone(), q.clone(), rx, batch, flush_ms));
            senders.insert(q.clone(), tx);
        }
        Self {
            senders: Arc::new(senders),
        }
    }

    /// Queue one completion and wait for the flush that carries it. The
    /// caller is not done until this returns: the §4.7 drain counts a task
    /// in flight until its ack is on the broker.
    async fn submit(&self, queue: &str, stream_id: &str, extra: Vec<redis::Cmd>) -> Result<()> {
        let Some(tx) = self.senders.get(queue) else {
            // Unreachable in the worker (queues are fixed at startup and the
            // fetch loop only reads them), but a completion must never be
            // silently dropped, so refuse loudly instead of panicking.
            return Err(anyhow!("no completion buffer for queue {queue}"));
        };
        let (done, rx) = oneshot::channel();
        tx.send(AckReq {
            stream_id: stream_id.to_string(),
            extra,
            done,
        })
        .await
        .map_err(|_| anyhow!("completion flusher for queue {queue} is gone"))?;
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(anyhow!("completion flush failed: {e}")),
            Err(_) => Err(anyhow!("completion flush dropped before it was answered")),
        }
    }
}

/// Drain one queue's completion buffer: collect up to `max_batch` requests
/// or until `flush_ms` has passed since the first one, then write them as
/// one pipeline and answer every waiter. Exits when every sender is gone
/// (process teardown); nothing is dropped on the way out, because `submit`
/// holds its sender alive until its own flush is answered.
async fn flusher(
    mut conn: ConnectionManager,
    queue: String,
    mut rx: mpsc::Receiver<AckReq>,
    max_batch: usize,
    flush_ms: u64,
) {
    loop {
        let Some(first) = rx.recv().await else { return };
        let mut batch = vec![first];
        // The deadline anchors on the OLDEST buffered completion, so no ack
        // ever waits more than one window regardless of arrival pattern.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(flush_ms);
        while batch.len() < max_batch {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(req)) => batch.push(req),
                Ok(None) | Err(_) => break, // senders gone, or window elapsed
            }
        }
        let (pipe, dones) = build_flush(&queue, batch);
        let outcome: FlushOutcome = pipe
            .query_async::<()>(&mut conn)
            .await
            .map_err(Arc::new)
            .map(|_: ()| ());
        for done in dones {
            // A receiver gone before the answer means its dispatch task was
            // torn down (process exit); there is no one left to inform.
            let _ = done.send(outcome.clone());
        }
    }
}

/// The wire layout of one flush: every request's own writes first, then one
/// XACK naming every entry, then one XDEL naming the same entries. See the
/// ordering note on `AckBufs`.
fn build_flush(
    queue: &str,
    batch: Vec<AckReq>,
) -> (redis::Pipeline, Vec<oneshot::Sender<FlushOutcome>>) {
    let qk = q_key(queue);
    let mut pipe = redis::pipe();
    let mut xack = redis::cmd("XACK");
    xack.arg(&qk).arg("cauli");
    let mut xdel = redis::cmd("XDEL");
    xdel.arg(&qk);
    let mut dones = Vec::with_capacity(batch.len());
    for req in batch {
        xack.arg(&req.stream_id);
        xdel.arg(&req.stream_id);
        for cmd in req.extra {
            pipe.add_command(cmd).ignore();
        }
        dones.push(req.done);
    }
    pipe.add_command(xack).ignore();
    pipe.add_command(xdel).ignore();
    (pipe, dones)
}

/// §4.1 success: [SET result EX ttl]? + batched XACK + XDEL (via `AckBufs`).
pub async fn finish_success(
    bufs: &AckBufs,
    queue: &str,
    stream_id: &str,
    task_id: &str,
    result_json: Option<&str>, // None when store_result = false
    result_ttl_s: u64,
) -> Result<()> {
    let mut extra = Vec::new();
    if let Some(rj) = result_json {
        let mut set = redis::cmd("SET");
        set.arg(result_key(task_id))
            .arg(rj)
            .arg("EX")
            .arg(result_ttl_s);
        extra.push(set);
    }
    bufs.submit(queue, stream_id, extra).await
}

/// Duplicate resolution (§4.5): optional duplicate result + batched XACK + XDEL.
pub async fn finish_duplicate(
    bufs: &AckBufs,
    queue: &str,
    stream_id: &str,
    task_id: &str,
    result_json: Option<&str>,
    result_ttl_s: u64,
) -> Result<()> {
    finish_success(bufs, queue, stream_id, task_id, result_json, result_ttl_s).await
}

/// §4.2 retry: ZADD delayed + batched XACK + XDEL (no result key). The ZADD
/// rides the same flush pipeline as the ack, queued before it
/// (`build_flush`), so the ack can never land without the reschedule.
pub async fn finish_retry(
    bufs: &AckBufs,
    queue: &str,
    stream_id: &str,
    envelope_json: &str,
    fire_at_ms: u64,
) -> Result<()> {
    let mut zadd = redis::cmd("ZADD");
    zadd.arg(delayed_key(queue))
        .arg(fire_at_ms)
        .arg(envelope_json);
    bufs.submit(queue, stream_id, vec![zadd]).await
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
/// XADD dlq + [SET result]? + batched XACK + XDEL. The dead letter rides the same
/// flush pipeline as the ack, queued before it (`build_flush`), so the ack
/// can never land without the dead letter.
pub async fn finish_dlq(
    bufs: &AckBufs,
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
    let mut extra = Vec::new();
    let mut xadd = redis::cmd("XADD");
    xadd.arg(&dk)
        .arg("MAXLEN")
        .arg("~")
        .arg(DLQ_MAXLEN)
        .arg("*")
        .arg("e")
        .arg(envelope_json)
        .arg("reason")
        .arg(reason)
        .arg("error")
        .arg(error_field);
    extra.push(xadd);
    // Bound the stream by AGE as well as by count: see DLQ_TTL_S.
    let mut expire = redis::cmd("EXPIRE");
    expire.arg(&dk).arg(DLQ_TTL_S);
    extra.push(expire);
    if let Some((task_id, rj, ttl)) = result {
        let mut set = redis::cmd("SET");
        set.arg(result_key(task_id)).arg(rj).arg("EX").arg(ttl);
        extra.push(set);
    }
    bufs.submit(queue, stream_id, extra).await
}

/// §4.1 trim boundary: the id below which EVERY entry in `cauli:q:{queue}`
/// is acked, so `trim_acked` may remove it. None when nothing is safely
/// below anything (no group yet, or nothing ever delivered).
///
/// The boundary is the group's oldest pending id, or, when the PEL is
/// empty, one sequence past its last-delivered-id. Proof that nothing
/// pending or undelivered is ever below the returned id, including against
/// concurrent delivery, acking, claiming, and a second process trimming:
///
/// * ids enter the PEL only via XREADGROUP `>`, which delivers strictly
///   ascending ids, so once `P` = oldest-pending is observed, every id that
///   is pending NOW or LATER is >= `P` (acks only remove; new deliveries are
///   above the last-delivered-id, which `P`'s own delivery already bounded);
///   XCLAIM moves ownership of an existing PEL id, it never adds one below.
/// * with the PEL observed empty, any entry pending later was delivered
///   after that observation, so its id is strictly above last-delivered-id
///   as read BEFORE the PEL probe — which is why this function reads XINFO
///   first and XPENDING second. Do not swap them: read the other way, a
///   delivery landing between the two reads sits below the boundary while
///   pending, and the trim would destroy it. That is silent task loss.
/// * undelivered ids are strictly above last-delivered-id at all times.
///
/// A second worker trimming concurrently computes its own boundary under
/// the same invariants, and XTRIM MINID only ever removes ids strictly
/// below a valid boundary, so concurrent trims are idempotent and safe.
pub async fn acked_below(conn: &mut ConnectionManager, queue: &str) -> Result<Option<String>> {
    let key = q_key(queue);
    // XINFO before XPENDING — see the ordering proof above.
    let info: redis::streams::StreamInfoGroupsReply = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(&key)
        .query_async(conn)
        .await?;
    let Some(group) = info.groups.iter().find(|g| g.name == "cauli") else {
        return Ok(None); // group gone: the fetch loop's NOGROUP path owns this
    };
    if let Some(oldest) = oldest_pending_id(conn, queue).await? {
        return Ok(Some(oldest));
    }
    if group.last_delivered_id == "0-0" {
        return Ok(None); // nothing ever delivered, nothing is provably acked
    }
    Ok(stream_id_after(&group.last_delivered_id))
}

/// The id one sequence number after `id` (`"5-3"` -> `"5-4"`): the smallest
/// id an XTRIM MINID boundary can carry that also removes `id` itself.
/// None for anything that is not `<ms>-<seq>`. At `seq == u64::MAX` it
/// returns the id unchanged (one entry retained, never one destroyed):
/// unreachable in practice, but the conservative direction costs one entry
/// of memory where the other direction would need `ms+1` reasoning for no
/// benefit.
fn stream_id_after(id: &str) -> Option<String> {
    let (ms, seq) = crate::ctx::parse_stream_id(id)?;
    Some(match seq.checked_add(1) {
        Some(next) => format!("{ms}-{next}"),
        None => id.to_string(),
    })
}

/// §4.1 bulk removal of acked entries: `XTRIM cauli:q:{queue} MINID
/// {boundary}` with a boundary from `acked_below`. Replaces the per-entry
/// XDEL the completion path used to pay: entries below the boundary are all
/// acked (or the acked-without-XDEL orphans the old MULTI'd pair existed to
/// prevent — the trim now cleans those too), and MINID removes ids strictly
/// below the boundary, so the pending entry the boundary names survives.
/// Returns the number of entries removed.
pub async fn trim_acked(conn: &mut ConnectionManager, queue: &str, boundary: &str) -> Result<u64> {
    let n: u64 = redis::cmd("XTRIM")
        .arg(q_key(queue))
        .arg("MINID")
        .arg(boundary)
        .query_async(conn)
        .await?;
    Ok(n)
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

    /// The wire value itself, against digests computed by an independent
    /// implementation (`hashlib.sha256(k).hexdigest()[:32]`). The
    /// deterministic/bounded test below only compares keys to other keys, so
    /// it passes just as happily against a wrong digest or a wrong hex
    /// encoder, and both of those are silent BREAKING changes: keys written
    /// by an older worker stop matching and an idempotent task runs twice.
    #[test]
    fn idemp_digest_matches_reference_sha256() {
        assert_eq!(
            idemp_digest_hex("order-42"),
            "3bf8b157c4238eefe5ae4a66eca81c6b"
        );
        assert_eq!(
            idemp_digest_hex("order-43"),
            "7cb94acade5a102a33b58fe6f51ea4c4"
        );
        assert_eq!(idemp_digest_hex(""), "e3b0c44298fc1c149afbf4c8996fb924");
        assert_eq!(
            idemp_key("order-42"),
            "cauli:idemp:3bf8b157c4238eefe5ae4a66eca81c6b"
        );
    }

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
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 1, 1);
        let sid: String = redis::cmd("XADD")
            .arg(q_key(queue))
            .arg("*")
            .arg("e")
            .arg(r#"{"id":"d1","task":"t"}"#)
            .query_async(&mut conn)
            .await
            .expect("seed entry");
        // Deliver it: XACK only removes entries that are actually pending.
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

        finish_dlq(
            &bufs,
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
        // The EXPIRE must not have displaced the ack half of the flush.
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
            &bufs,
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

    /// The ordering rules a flush pipeline lives by: every request's own
    /// writes (result SET, retry ZADD, DLQ XADD) are queued BEFORE the
    /// single batch XACK, the single batch XDEL comes AFTER the XACK, and
    /// both name every entry in the batch. Torn before the XACK: writes
    /// without the ack, redelivered and resolved as a duplicate. Torn
    /// between XACK and XDEL: acked-but-undeleted, reclaimed by the trim.
    /// The reverse order would delete entries still in the PEL, which §4.4
    /// can then never peek, claim or ack — an entry pinned forever.
    #[test]
    fn extras_precede_ack_precedes_del_on_the_wire() {
        let req = |sid: &str, extra: Vec<redis::Cmd>| {
            let (done, _rx) = oneshot::channel();
            (
                AckReq {
                    stream_id: sid.to_string(),
                    extra,
                    done,
                },
                _rx,
            )
        };
        let mut set = redis::cmd("SET");
        set.arg(result_key("t1")).arg("{}").arg("EX").arg(60);
        let mut zadd = redis::cmd("ZADD");
        zadd.arg(delayed_key("q")).arg(123).arg("{}");
        let (r1, _k1) = req("1-1", vec![set]);
        let (r2, _k2) = req("2-2", vec![zadd]);
        let (r3, _k3) = req("3-3", vec![]);
        let (pipe, dones) = build_flush("q", vec![r1, r2, r3]);
        assert_eq!(dones.len(), 3, "one answer channel per request");
        let wire = String::from_utf8(pipe.get_packed_pipeline()).expect("utf8 wire");
        let at = |needle: &str| {
            wire.find(needle)
                .unwrap_or_else(|| panic!("{needle} missing from wire: {wire:?}"))
        };
        assert!(
            at("SET") < at("XACK") && at("ZADD") < at("XACK"),
            "every extra must precede the ack: {wire:?}"
        );
        assert!(
            at("XACK") < at("XDEL"),
            "the ack must land before the delete, or a tear strands a PEL              entry whose payload is gone: {wire:?}"
        );
        assert_eq!(
            wire.matches("XACK").count(),
            1,
            "one batch XACK, not one per entry: {wire:?}"
        );
        assert_eq!(
            wire.matches("XDEL").count(),
            1,
            "one batch XDEL, not one per entry: {wire:?}"
        );
        let ack_tail = &wire[at("XACK")..at("XDEL")];
        let del_tail = &wire[at("XDEL")..];
        for sid in ["1-1", "2-2", "3-3"] {
            assert!(
                ack_tail.contains(sid),
                "the batch XACK must name {sid}: {ack_tail:?}"
            );
            assert!(
                del_tail.contains(sid),
                "the batch XDEL must name {sid}: {del_tail:?}"
            );
        }
        assert!(
            !wire.contains("MULTI"),
            "no transaction on the completion path: {wire:?}"
        );
    }

    /// End to end proof of one success completion under the buffered path:
    /// the result key lands with its TTL, the entry leaves the PEL, and the
    /// flush's own batched XDEL removes it from the stream immediately —
    /// stream residence must never depend on the trim (whose boundary a
    /// long-running task can pin) for ordinary completions.
    #[tokio::test]
    async fn finish_success_acks_writes_the_result_and_deletes() {
        let redis = ThrowawayRedis::start(6425, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "done";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 64, 1);
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

        finish_success(&bufs, queue, &sid, "s1", Some(r#"{"ok":true}"#), 60)
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
        assert_eq!(
            len, 0,
            "the flush's batched XDEL must remove the entry immediately,              not leave it for the trim"
        );
    }

    /// THE regression this file's trim exists to avoid reintroducing: one
    /// slow task pins the group's oldest-pending id, and stream residence of
    /// COMPLETED entries must not depend on it. One entry is delivered and
    /// held (never acked) while 500 later entries are delivered and
    /// completed through the buffered path; XLEN must come back to the one
    /// held entry, not grow with the completed count. Under trim-only
    /// removal this reads 501.
    #[tokio::test]
    async fn a_long_pending_task_does_not_retain_completed_entries() {
        let redis = ThrowawayRedis::start(6433, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "slowmix";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 64, 1);

        // The slow task: delivered, in the PEL, never acked in this test.
        let _: String = redis::cmd("XADD")
            .arg(q_key(queue))
            .arg("*")
            .arg("e")
            .arg(r#"{"id":"slow","task":"t"}"#)
            .query_async(&mut conn)
            .await
            .expect("seed slow");
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
            .expect("deliver slow");

        // The fast churn behind it.
        for i in 0..500 {
            let sid: String = redis::cmd("XADD")
                .arg(q_key(queue))
                .arg("*")
                .arg("e")
                .arg(format!(r#"{{"id":"fast{i}","task":"t"}}"#))
                .query_async(&mut conn)
                .await
                .expect("seed fast");
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
                .expect("deliver fast");
            finish_success(&bufs, queue, &sid, "f", None, 60)
                .await
                .expect("ack fast");
        }

        let len: u64 = redis::cmd("XLEN")
            .arg(q_key(queue))
            .query_async(&mut conn)
            .await
            .expect("xlen");
        assert_eq!(
            len, 1,
            "only the still-pending slow entry may remain; completed entries              must not be retained behind it (trim-only removal reads 501 here)"
        );
        let pending: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(q_key(queue))
            .arg("cauli")
            .arg("-")
            .arg("+")
            .arg(10)
            .query_async(&mut conn)
            .await
            .expect("xpending");
        assert_eq!(pending.len(), 1, "the slow task is still pending");
    }

    /// Ack WITHOUT delete: the orphan shape a flush torn between its XACK
    /// and its XDEL leaves behind, and exactly what the trim backstop
    /// exists to reclaim.
    async fn raw_ack(conn: &mut ConnectionManager, queue: &str, sid: &str) {
        let _: u64 = redis::cmd("XACK")
            .arg(q_key(queue))
            .arg("cauli")
            .arg(sid)
            .query_async(conn)
            .await
            .expect("raw xack");
    }

    /// The correctness bar of the trim backstop, live against redis: the
    /// boundary must never name an id at or below a pending or undelivered
    /// entry, whatever mix of acked-undeleted orphans, pending and
    /// undelivered the stream holds, so `XTRIM MINID` can never destroy
    /// work. Orphans are manufactured with `raw_ack` (ack, no delete — the
    /// torn-flush shape); a normal completion's own XDEL never reaches the
    /// trim at all. Walks the exact states: orphan-behind-a-pending-entry
    /// (retained until the boundary passes, harmless), pending (kept),
    /// undelivered backlog (kept), then full drain.
    #[tokio::test]
    async fn trim_never_removes_a_pending_or_undelivered_entry() {
        let redis = ThrowawayRedis::start(6406, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "trimsafe";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let mut sids: Vec<String> = Vec::new();
        for i in 0..5 {
            let sid: String = redis::cmd("XADD")
                .arg(q_key(queue))
                .arg("*")
                .arg("e")
                .arg(format!(r#"{{"id":"t{i}","task":"t"}}"#))
                .query_async(&mut conn)
                .await
                .expect("seed");
            sids.push(sid);
        }
        // Deliver the first three; the last two stay undelivered backlog.
        let _: Option<redis::streams::StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("cauli")
            .arg("c1")
            .arg("COUNT")
            .arg(3)
            .arg("STREAMS")
            .arg(q_key(queue))
            .arg(">")
            .query_async(&mut conn)
            .await
            .expect("deliver 3");

        async fn exists(conn: &mut ConnectionManager, sid: &str) -> bool {
            let r: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
                .arg(q_key("trimsafe"))
                .arg(sid)
                .arg(sid)
                .query_async(conn)
                .await
                .expect("xrange");
            !r.ids.is_empty()
        }

        // Ack #0 and #2. #1 stays pending, so the boundary is #1: the trim
        // removes #0 and MUST retain #2 (acked, but above a pending id).
        raw_ack(&mut conn, queue, &sids[0]).await;
        raw_ack(&mut conn, queue, &sids[2]).await;
        let boundary = acked_below(&mut conn, queue)
            .await
            .expect("boundary")
            .expect("a pending entry bounds the trim");
        assert_eq!(boundary, sids[1], "the oldest pending id is the boundary");
        trim_acked(&mut conn, queue, &boundary).await.expect("trim");
        assert!(!exists(&mut conn, &sids[0]).await, "acked below: gone");
        assert!(exists(&mut conn, &sids[1]).await, "pending: kept");
        assert!(
            exists(&mut conn, &sids[2]).await,
            "acked above pending: kept"
        );
        assert!(exists(&mut conn, &sids[3]).await, "undelivered: kept");
        assert!(exists(&mut conn, &sids[4]).await, "undelivered: kept");

        // Ack #1: PEL is now empty, so the boundary moves to one past
        // last-delivered (#2), and the undelivered backlog must survive.
        raw_ack(&mut conn, queue, &sids[1]).await;
        let boundary = acked_below(&mut conn, queue)
            .await
            .expect("boundary")
            .expect("empty PEL with deliveries still yields a boundary");
        assert_eq!(
            boundary,
            stream_id_after(&sids[2]).expect("valid id"),
            "empty PEL: one past last-delivered-id"
        );
        trim_acked(&mut conn, queue, &boundary).await.expect("trim");
        assert!(!exists(&mut conn, &sids[1]).await, "acked: gone");
        assert!(!exists(&mut conn, &sids[2]).await, "acked: gone");
        assert!(exists(&mut conn, &sids[3]).await, "undelivered: kept");

        // Drain the backlog: deliver, ack, trim -> empty stream.
        let _: Option<redis::streams::StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("cauli")
            .arg("c1")
            .arg("COUNT")
            .arg(2)
            .arg("STREAMS")
            .arg(q_key(queue))
            .arg(">")
            .query_async(&mut conn)
            .await
            .expect("deliver rest");
        raw_ack(&mut conn, queue, &sids[3]).await;
        raw_ack(&mut conn, queue, &sids[4]).await;
        let boundary = acked_below(&mut conn, queue)
            .await
            .expect("boundary")
            .expect("boundary after full drain");
        trim_acked(&mut conn, queue, &boundary).await.expect("trim");
        let len: u64 = redis::cmd("XLEN")
            .arg(q_key(queue))
            .query_async(&mut conn)
            .await
            .expect("xlen");
        assert_eq!(len, 0, "fully acked stream trims to empty");
    }

    /// The boundary's own edge states: no group at all (None, the fetch
    /// loop's NOGROUP path owns that), and a group that has never delivered
    /// (None: nothing is provably acked, so nothing may be trimmed).
    #[tokio::test]
    async fn trim_boundary_is_none_without_a_group_or_a_delivery() {
        let redis = ThrowawayRedis::start(6413, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "trimedge";
        // Stream exists, group does not.
        let _: String = redis::cmd("XADD")
            .arg(q_key(queue))
            .arg("*")
            .arg("e")
            .arg("{}")
            .query_async(&mut conn)
            .await
            .expect("seed");
        assert_eq!(
            acked_below(&mut conn, queue).await.expect("no group"),
            None,
            "no consumer group: nothing may be trimmed"
        );
        // Group exists, nothing ever delivered: the seeded entry is pure
        // backlog and must not be trimmable.
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        assert_eq!(
            acked_below(&mut conn, queue).await.expect("no delivery"),
            None,
            "nothing delivered: nothing is provably acked"
        );
    }

    /// Concurrent trimming against live acking, the way two worker
    /// processes overlap in production: while entries are being delivered
    /// and acked one at a time, a second connection trims in a loop. Before
    /// each ack, the still-pending entry must exist; at the end, everything
    /// acked must be trimmable to an empty stream. Any trim of a pending
    /// entry fails the mid-loop existence check.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_trim_never_eats_the_entry_being_worked() {
        let redis = ThrowawayRedis::start(6418, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "trimrace";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 8, 1);

        // The rival: trims as fast as it can for the whole run.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let rival_stop = stop.clone();
        let mut rival_conn = conn.clone();
        let rival = tokio::spawn(async move {
            while !rival_stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok(Some(boundary)) = acked_below(&mut rival_conn, "trimrace").await {
                    let _ = trim_acked(&mut rival_conn, "trimrace", &boundary).await;
                }
            }
        });

        for i in 0..100 {
            let sid: String = redis::cmd("XADD")
                .arg(q_key(queue))
                .arg("*")
                .arg("e")
                .arg(format!(r#"{{"id":"r{i}","task":"t"}}"#))
                .query_async(&mut conn)
                .await
                .expect("seed");
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
            // Pending: whatever the rival has trimmed, THIS entry must
            // still be here, or the trim destroyed in-flight work.
            let r: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
                .arg(q_key(queue))
                .arg(&sid)
                .arg(&sid)
                .query_async(&mut conn)
                .await
                .expect("xrange");
            assert!(
                !r.ids.is_empty(),
                "entry {sid} was pending and a concurrent trim removed it"
            );
            finish_success(&bufs, queue, &sid, "r", None, 60)
                .await
                .expect("ack");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        rival.await.expect("rival task");

        // Everything is acked: one final trim empties the stream.
        if let Some(boundary) = acked_below(&mut conn, queue).await.expect("boundary") {
            trim_acked(&mut conn, queue, &boundary).await.expect("trim");
        }
        let len: u64 = redis::cmd("XLEN")
            .arg(q_key(queue))
            .query_async(&mut conn)
            .await
            .expect("xlen");
        assert_eq!(len, 0, "all 100 acked entries trim away, none early");
    }

    /// The two flush triggers, each proven to fire without the other: a
    /// full batch flushes immediately under a window that would otherwise
    /// park it for ten seconds, and a lone completion is answered within
    /// its window rather than waiting for a batch that never fills. The
    /// second half is what makes §4.7's drain bounded: nothing buffered is
    /// ever held past `--ack-flush-ms`.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_fires_on_a_full_batch_and_on_the_window() {
        let redis = ThrowawayRedis::start(6444, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "flushq";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let mut sids = Vec::new();
        for i in 0..5 {
            let sid: String = redis::cmd("XADD")
                .arg(q_key(queue))
                .arg("*")
                .arg("e")
                .arg(format!(r#"{{"id":"f{i}","task":"t"}}"#))
                .query_async(&mut conn)
                .await
                .expect("seed");
            sids.push(sid);
        }
        let _: Option<redis::streams::StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("cauli")
            .arg("c1")
            .arg("COUNT")
            .arg(5)
            .arg("STREAMS")
            .arg(q_key(queue))
            .arg(">")
            .query_async(&mut conn)
            .await
            .expect("deliver");

        // Batch of 4 under a 10s window: four submits must complete on the
        // count trigger, far inside the window.
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 4, 10_000);
        let t0 = std::time::Instant::now();
        let mut waits = Vec::new();
        for sid in &sids[..4] {
            let bufs = bufs.clone();
            let sid = sid.clone();
            waits.push(tokio::spawn(async move {
                finish_success(&bufs, "flushq", &sid, "f", None, 60).await
            }));
        }
        for w in waits {
            w.await.expect("join").expect("ack");
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(5),
            "a full batch must flush on count, not wait out the window"
        );

        // The fifth, alone in a buffer whose batch (1000) will never fill,
        // must be answered by its 200ms window, not parked forever.
        let lone = AckBufs::start(&conn, &[queue.to_string()], 1000, 200);
        let t0 = std::time::Instant::now();
        finish_success(&lone, queue, &sids[4], "f4", None, 60)
            .await
            .expect("ack");
        let waited = t0.elapsed();
        assert!(
            waited < std::time::Duration::from_secs(5),
            "a lone completion waited {waited:?}; the window must bound it"
        );
        let pending: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(q_key(queue))
            .arg("cauli")
            .arg("-")
            .arg("+")
            .arg(10)
            .query_async(&mut conn)
            .await
            .expect("xpending");
        assert!(pending.is_empty(), "all five acked: {pending:?}");
    }
}
