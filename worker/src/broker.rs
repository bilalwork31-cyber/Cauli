//! Redis broker primitives: key naming, consumer group setup, the delayed
//! mover Lua script, idempotency guard, and the pipelined completion writes
//! (PROTOCOL §1, §4.1-§4.3, §4.5).

use crate::envelope::ErrorJson;
use anyhow::Result;
use redis::aio::ConnectionManager;

pub fn q_key(queue: &str) -> String {
    format!("rupy:q:{queue}")
}
pub fn delayed_key(queue: &str) -> String {
    format!("rupy:delayed:{queue}")
}
pub fn dlq_key(queue: &str) -> String {
    format!("rupy:dlq:{queue}")
}
pub fn result_key(id: &str) -> String {
    format!("rupy:result:{id}")
}
pub fn idemp_key(key: &str) -> String {
    format!("rupy:idemp:{key}")
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
            .arg("rupy")
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

/// §4.5 idempotency guard. Returns true if this task claimed the key (execute),
/// false if the key already exists (duplicate).
pub async fn idemp_claim(
    conn: &mut ConnectionManager,
    key: &str,
    task_id: &str,
    ttl_s: u64,
) -> Result<bool> {
    let r: Option<String> = redis::cmd("SET")
        .arg(idemp_key(key))
        .arg(task_id)
        .arg("NX")
        .arg("EX")
        .arg(ttl_s)
        .query_async(conn)
        .await?;
    Ok(r.is_some())
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
    pipe.cmd("XACK")
        .arg(q_key(queue))
        .arg("rupy")
        .arg(stream_id)
        .ignore();
    pipe.cmd("XDEL").arg(q_key(queue)).arg(stream_id).ignore();
}

/// §4.4 extended XPENDING: (entry_id, consumer, idle_ms, delivery_count).
pub async fn xpending_idle(
    conn: &mut ConnectionManager,
    queue: &str,
    min_idle_ms: u64,
    count: usize,
) -> Result<Vec<(String, String, u64, u64)>> {
    let r: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
        .arg(q_key(queue))
        .arg("rupy")
        .arg("IDLE")
        .arg(min_idle_ms)
        .arg("-")
        .arg("+")
        .arg(count)
        .query_async(conn)
        .await?;
    Ok(r)
}

/// §4.4 XCLAIM one entry; returns the envelope field `e` if the claim
/// succeeded and the entry still exists (None means someone else won or the
/// entry vanished). The raw payload is returned even if it is not valid JSON.
pub async fn xclaim_entry(
    conn: &mut ConnectionManager,
    queue: &str,
    consumer: &str,
    min_idle_ms: u64,
    entry_id: &str,
) -> Result<Option<Option<String>>> {
    use redis::streams::StreamClaimReply;
    let reply: StreamClaimReply = redis::cmd("XCLAIM")
        .arg(q_key(queue))
        .arg("rupy")
        .arg(consumer)
        .arg(min_idle_ms)
        .arg(entry_id)
        .query_async(conn)
        .await?;
    for sid in reply.ids {
        if sid.id == entry_id {
            let raw = sid
                .map
                .get("e")
                .and_then(|v| redis::from_redis_value::<String>(v).ok());
            return Ok(Some(raw));
        }
    }
    Ok(None)
}
