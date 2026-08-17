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
                        let add = std::process::Command::new("redis-cli")
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
                        assert!(
                            add.status.success(),
                            "cluster addslotsrange failed: {add:?}"
                        );
                        // cluster_state flips to "ok" on the next cluster
                        // cron tick, not synchronously with this reply.
                        let mut became_ok = false;
                        for _ in 0..50 {
                            let info = std::process::Command::new("redis-cli")
                                .args(["-p", &port.to_string(), "cluster", "info"])
                                .output()
                                .expect("cluster info");
                            if String::from_utf8_lossy(&info.stdout).contains("cluster_state:ok") {
                                became_ok = true;
                                break;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                        assert!(became_ok, "cluster never reached cluster_state:ok");
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
        let err = run_mover(&mut conn, &script, queue, due_at + 1)
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
        let err = run_mover(&mut conn, &script, "myqueue", 1_000)
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
}
