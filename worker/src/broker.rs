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
/// Deterministic FNV-1a 64-bit hash, hex-encoded. Folds an app-supplied
/// idempotency_key (arbitrary length/charset — attacker/app controlled per
/// audit M1) into a bounded, redis-key-safe token: neutralizes cluster
/// hash-tag injection (`{...}`) and unbounded key-size DoS. Not cryptographic;
/// callers only need stability across workers, not adversarial resistance.
fn fnv1a_hex(s: &str) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

pub fn idemp_key(key: &str) -> String {
    format!("cauli:idemp:{}", fnv1a_hex(key))
}

/// PROTOCOL §4.3 delayed mover script (verbatim).
pub const MOVER_LUA: &str = r#"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i, e in ipairs(due) do
  redis.call('ZREM', KEYS[1], e)
  redis.call('XADD', KEYS[2], '*', 'e', e)
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
) -> Result<i64> {
    let n: i64 = script
        .key(delayed_key(queue))
        .key(q_key(queue))
        .arg(now_ms)
        .arg(128)
        .invoke_async(conn)
        .await?;
    Ok(n)
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
    Duplicate,
}

/// §4.5 idempotency guard. Atomic via a single Lua script: `SET NX`, and on
/// failure `GET` the existing value to distinguish "my own claim" (proceed)
/// from "someone else's claim" (duplicate) — see `IdempClaim`.
const IDEMP_CLAIM_LUA: &str = r#"
local ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if ok then
  return 1
end
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
  return 2
end
return 0
"#;

/// Built once, not per task. `redis::Script::new` computes the script's SHA1
/// on construction, so building it inside `idemp_claim` re-hashed the source
/// on every single idempotent task before the EVALSHA could even be sent.
static IDEMP_CLAIM_SCRIPT: std::sync::LazyLock<redis::Script> =
    std::sync::LazyLock::new(|| redis::Script::new(IDEMP_CLAIM_LUA));

pub async fn idemp_claim(
    conn: &mut ConnectionManager,
    key: &str,
    task_id: &str,
    ttl_s: u64,
) -> Result<IdempClaim> {
    let script = &*IDEMP_CLAIM_SCRIPT;
    let code: i64 = script
        .key(idemp_key(key))
        .arg(task_id)
        .arg(ttl_s)
        .invoke_async(conn)
        .await?;
    Ok(match code {
        1 => IdempClaim::Fresh,
        2 => IdempClaim::MineAgain,
        _ => IdempClaim::Duplicate,
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
    let mut pipe = redis::pipe();
    pipe.cmd("XADD")
        .arg(dlq_key(queue))
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

fn add_ack_del(pipe: &mut redis::Pipeline, queue: &str, stream_id: &str) {
    // One allocation, not two: this runs on every single completion (success,
    // duplicate, retry, and DLQ all funnel through here).
    let qk = q_key(queue);
    pipe.cmd("XACK")
        .arg(&qk)
        .arg("cauli")
        .arg(stream_id)
        .ignore();
    pipe.cmd("XDEL").arg(&qk).arg(stream_id).ignore();
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
                "cauli:idemp:".len() + 16,
                "hash must be a fixed 16 hex chars"
            );
            assert!(
                !k.contains('{') && !k.contains('}'),
                "no hash-tag characters may survive"
            );
        }
    }
}
