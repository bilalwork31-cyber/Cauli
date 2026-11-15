//! Redis broker primitives: key naming, consumer group setup, the delayed
//! mover Lua script, idempotency guard, and the pipelined completion writes
//! (PROTOCOL §1, §4.1-§4.3, §4.5).

use crate::envelope::ErrorJson;
use crate::transport::Transport;
use anyhow::{anyhow, Result};
use redis::aio::{ConnectionLike, ConnectionManager};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

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
/// SHA-256 through ring, the implementation rustls already links.
fn sha256(data: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, data)
        .as_ref()
        .try_into()
        .expect("SHA-256 is 32 bytes")
}

/// An untrusted idempotency_key as a fixed 32 hex char token: no hash tag
/// injection, no key size DoS. Must stay cryptographic (SHA-256, 128 bits): a
/// collision silently resolves another task as a duplicate, and an
/// invertible hash lets anyone suppress a chosen task.
fn idemp_digest_hex(s: &str) -> String {
    // A nibble table, because core::fmt costs more than the SHA-256 itself.
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

/// §14 mover lease, for a ready queue outside Redis. Returns the due
/// envelopes and pushes their score to ARGV[3], so another worker's sweep
/// skips them while this one publishes. An envelope whose publish is never
/// confirmed is due again at ARGV[3]: at least once, like MOVER_LUA.
pub const MOVER_CLAIM_LUA: &str = r#"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i, e in ipairs(due) do
  redis.call('ZADD', KEYS[1], ARGV[3], e)
end
return due
"#;

pub async fn claim_due(
    conn: &mut ConnectionManager,
    script: &redis::Script,
    queue: &str,
    now_ms: u64,
    limit: usize,
    leased_until_ms: u64,
) -> Result<Vec<String>> {
    let due: Vec<String> = script
        .key(delayed_key(queue))
        .arg(now_ms)
        .arg(limit)
        .arg(leased_until_ms)
        .invoke_async(conn)
        .await?;
    Ok(due)
}

/// How long a redelivery count outlives its last increment: the DLQ's own
/// retention, since both exist to explain a task that never finished.
const REDELIVERY_COUNT_TTL_S: u64 = DLQ_TTL_S;

/// §14: counts one more start of a redelivered attempt, and returns the
/// count. Keyed by attempt, so every retry begins at zero like a fresh
/// stream entry's `delivery_count`.
pub async fn count_redelivery(
    conn: &mut ConnectionManager,
    task_id: &str,
    retries: u32,
) -> Result<u64> {
    let key = format!("cauli:redelivered:{task_id}:{retries}");
    let (started,): (u64,) = redis::pipe()
        .cmd("INCR")
        .arg(&key)
        .cmd("EXPIRE")
        .arg(&key)
        .arg(REDELIVERY_COUNT_TTL_S)
        .ignore()
        .query_async(conn)
        .await?;
    Ok(started)
}

/// Drops envelopes the broker has confirmed from the delayed set.
pub async fn forget_moved(
    conn: &mut ConnectionManager,
    queue: &str,
    envelopes: &[&str],
) -> Result<()> {
    if envelopes.is_empty() {
        return Ok(());
    }
    let _: u64 = redis::cmd("ZREM")
        .arg(delayed_key(queue))
        .arg(envelopes)
        .query_async(conn)
        .await?;
    Ok(())
}

/// Redis Cluster CROSSSLOT: permanent for this script's keys, so not to be
/// retried like a transient error.
pub fn is_crossslot(e: &anyhow::Error) -> bool {
    e.downcast_ref::<redis::RedisError>()
        .is_some_and(|re| re.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::CrossSlot))
}

/// NOGROUP, matched on the error code: the dataset was reset under a live
/// connection, the one XREADGROUP failure waiting never clears
/// (`loops::recreate_groups`).
pub fn is_nogroup(e: &redis::RedisError) -> bool {
    e.code() == Some("NOGROUP")
}

/// §4.5 idempotency guard outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum IdempClaim {
    /// Nobody held the key.
    Fresh,
    /// Held by this task's own id (a retry or a §4.4 redelivery): execute.
    /// Otherwise retry plus idempotency_key would resolve as a duplicate.
    MineAgain,
    /// Held by another id. `claimant` is empty only if the key expired
    /// between the failed SET and the GET.
    Duplicate { claimant: String },
}

/// §4.5 guard: `SET NX`, else `GET` to tell this task's own claim from another's.
/// The PEXPIRE extends the lease on a retry or redelivery, or a retry chain
/// outlives its key. Returns `{code, holder}`, holder empty unless duplicate.
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

/// Never plain `idemp_ttl`: one shorter than the task's timeout expires the
/// key mid execution and the next attempt runs concurrently.
fn claim_ttl_s(idemp_ttl_s: u64, timeout_ms: u64) -> u64 {
    let execution_s = timeout_ms
        .saturating_add(crate::exec::BACKSTOP_GRACE_MS)
        .div_ceil(1000);
    idemp_ttl_s.max(execution_s)
}

/// Built once: `Script::new` hashes the source.
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

/// One buffered completion: its own writes, the entry to ack, and the reply
/// channel.
struct AckReq {
    stream_id: String,
    extra: Vec<redis::Cmd>,
    done: oneshot::Sender<FlushOutcome>,
}

/// Cloneable: one flush answers every waiter in the batch.
type FlushOutcome = std::result::Result<(), Arc<anyhow::Error>>;

/// RabbitMQ or SQS as the ready queue (PROTOCOL §14): where a flush sends
/// its acks, and the shutdown signal that ends a completion's wait for Redis.
#[derive(Clone)]
pub struct Foreign {
    pub transport: Arc<Transport>,
    pub shutdown: watch::Receiver<bool>,
}

/// Backoff of a completion whose writes Redis refused, under `Foreign`.
const STALLED_RETRY_MIN: Duration = Duration::from_millis(250);
const STALLED_RETRY_MAX: Duration = Duration::from_secs(5);

/// §4.1 completion buffers, one flusher per queue. A flush is one batch
/// XACK plus one batch XDEL, sent at `--ack-batch` completions or
/// `--ack-flush-ms` after the oldest; a crash loses at most one buffer of
/// acks per queue (redelivered, §4.4). `finish_*` return only once flushed.
///
/// Order is correctness: writes are answered before the XACK, and only
/// entries whose writes landed are acked (an ack without its retry or dead
/// letter loses the task). XDEL comes after XACK: the reverse could delete
/// a pending entry, which pins XPENDING forever. The XDEL must stay: trim
/// alone lets one slow task retain every completed entry behind it.
#[derive(Clone)]
pub struct AckBufs {
    senders: Arc<HashMap<String, mpsc::Sender<AckReq>>>,
    foreign: Option<Foreign>,
    /// Completions waiting in `submit` for Redis to take their writes.
    stalled: Arc<AtomicUsize>,
}

impl AckBufs {
    /// Must be called on a tokio runtime. `foreign` is None when Redis
    /// Streams are the ready queue.
    pub fn start(
        conn: &ConnectionManager,
        queues: &[String],
        batch: usize,
        flush_ms: u64,
        foreign: Option<Foreign>,
    ) -> Self {
        let batch = batch.max(1);
        let mut senders = HashMap::new();
        for q in queues {
            // Bounds memory if redis stalls; senders then wait in `submit`.
            let (tx, rx) = mpsc::channel(batch.saturating_mul(4).max(256));
            let transport = foreign.as_ref().map(|f| f.transport.clone());
            tokio::spawn(flusher(
                conn.clone(),
                q.clone(),
                rx,
                batch,
                flush_ms,
                transport,
            ));
            senders.insert(q.clone(), tx);
        }
        Self {
            senders: Arc::new(senders),
            foreign,
            stalled: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// True while a completion cannot reach Redis (only under `Foreign`).
    pub fn stalled(&self) -> bool {
        self.stalled.load(Ordering::Relaxed) > 0
    }

    /// Waits for the flush: §4.7 drain counts a task until its ack lands.
    ///
    /// A failed flush under Redis Streams leaves the entry pending, and §4.4
    /// recovery runs it again. RabbitMQ never redelivers on its own and SQS
    /// would rerun a task that already finished, so there the outcome waits
    /// here and is written again until Redis takes it or the worker stops.
    async fn submit(&self, queue: &str, stream_id: &str, extra: Vec<redis::Cmd>) -> Result<()> {
        let Some(foreign) = &self.foreign else {
            return self.submit_once(queue, stream_id, extra).await;
        };
        let mut delay = STALLED_RETRY_MIN;
        let mut counted = None;
        loop {
            let err = match self.submit_once(queue, stream_id, extra.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            if *foreign.shutdown.borrow() {
                return Err(err);
            }
            if counted.is_none() {
                tracing::warn!(
                    queue,
                    "completion write failed ({err}); holding the delivery unacked and \
                     retrying until redis accepts it"
                );
                self.stalled.fetch_add(1, Ordering::Relaxed);
                counted = Some(StalledGuard(&self.stalled));
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(STALLED_RETRY_MAX);
        }
    }

    async fn submit_once(
        &self,
        queue: &str,
        stream_id: &str,
        extra: Vec<redis::Cmd>,
    ) -> Result<()> {
        let Some(tx) = self.senders.get(queue) else {
            // Unreachable (queues are fixed), but never drop silently.
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

/// Leaves the stalled count on every exit from `submit`, a cancelled one
/// included.
struct StalledGuard<'a>(&'a AtomicUsize);

impl Drop for StalledGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Exits when every sender is gone; `submit` keeps its sender alive until
/// answered, so nothing is dropped.
async fn flusher(
    mut conn: ConnectionManager,
    queue: String,
    mut rx: mpsc::Receiver<AckReq>,
    max_batch: usize,
    flush_ms: u64,
    transport: Option<Arc<Transport>>,
) {
    loop {
        let Some(first) = rx.recv().await else { return };
        let mut batch = vec![first];
        // Anchored on the oldest, so no ack waits more than one window.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(flush_ms);
        while batch.len() < max_batch {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(req)) => batch.push(req),
                Ok(None) | Err(_) => break, // senders gone, or window elapsed
            }
        }
        flush_batch(&mut conn, &queue, batch, transport.as_deref()).await;
    }
}

/// Two round trips, never one pipeline: OOM under `noeviction` refuses ZADD,
/// XADD and SET but still runs a later XACK and XDEL, which would ack work
/// never written. A refused entry stays pending for §4.4.
async fn flush_batch(
    conn: &mut ConnectionManager,
    queue: &str,
    batch: Vec<AckReq>,
    transport: Option<&Transport>,
) {
    let mut writes = redis::pipe();
    let mut owner = Vec::new();
    let mut reqs: Vec<(String, oneshot::Sender<FlushOutcome>, FlushOutcome)> = Vec::new();
    for (i, req) in batch.into_iter().enumerate() {
        for cmd in req.extra {
            writes.add_command(cmd);
            owner.push(i);
        }
        reqs.push((req.stream_id, req.done, Ok(())));
    }
    if !owner.is_empty() {
        match conn.req_packed_commands(&writes, 0, owner.len()).await {
            Ok(replies) => {
                for (i, reply) in owner.into_iter().zip(replies) {
                    if let (Err(e), Ok(())) = (reply.extract_error(), &reqs[i].2) {
                        reqs[i].2 = Err(Arc::new(e.into()));
                    }
                }
            }
            Err(e) => {
                let e = Arc::new(anyhow::Error::from(e));
                reqs.iter_mut().for_each(|r| r.2 = Err(e.clone()));
            }
        }
    }
    let (written, refused): (Vec<_>, Vec<_>) = reqs.into_iter().partition(|r| r.2.is_ok());
    for (_, done, outcome) in refused {
        let _ = done.send(outcome);
    }
    if written.is_empty() {
        return;
    }
    let ids = written.iter().map(|r| r.0.as_str());
    let outcome: FlushOutcome = match transport {
        None => ack_pipeline(queue, ids)
            .query_async::<()>(conn)
            .await
            .map_err(|e| Arc::new(e.into())),
        Some(transport) => transport
            .ack(queue, &ids.collect::<Vec<_>>())
            .await
            .map_err(Arc::new),
    };
    for (_, done, _) in written {
        let _ = done.send(outcome.clone());
    }
}

/// XACK then XDEL, never the reverse (see `AckBufs`).
fn ack_pipeline<'a>(queue: &str, stream_ids: impl Iterator<Item = &'a str>) -> redis::Pipeline {
    let qk = q_key(queue);
    let mut xack = redis::cmd("XACK");
    xack.arg(&qk).arg("cauli");
    let mut xdel = redis::cmd("XDEL");
    xdel.arg(&qk);
    for sid in stream_ids {
        xack.arg(sid);
        xdel.arg(sid);
    }
    let mut pipe = redis::pipe();
    pipe.add_command(xack).ignore();
    pipe.add_command(xdel).ignore();
    pipe
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

/// §4.2 retry: ZADD delayed + batched XACK + XDEL.
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

/// DLQ length cap (approximate MAXLEN): unbounded, a trickle of failures
/// eventually OOMs Redis for every queue. Worst case about 1 GB per queue at
/// 1 MiB envelopes. See PROTOCOL §1.
const DLQ_MAXLEN: u64 = 1000;

/// DLQ retention, refreshed by every dead letter. Count bounds a queue that
/// fails constantly, age one that failed once; both are needed. 7 days so a
/// dead letter outlives a weekend. See PROTOCOL §1.
const DLQ_TTL_S: u64 = 7 * 24 * 60 * 60;

/// The writes of one dead letter: XADD dlq, its retention, [SET result]?.
fn dlq_writes(
    queue: &str,
    envelope_json: &str,
    reason: &str,
    error: Option<&ErrorJson>,
    result: Option<(&str, &str, u64)>, // (task_id, result_json, ttl_s)
) -> Vec<redis::Cmd> {
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
    let mut expire = redis::cmd("EXPIRE");
    expire.arg(&dk).arg(DLQ_TTL_S);
    extra.push(expire);
    if let Some((task_id, rj, ttl)) = result {
        let mut set = redis::cmd("SET");
        set.arg(result_key(task_id)).arg(rj).arg("EX").arg(ttl);
        extra.push(set);
    }
    extra
}

/// DLQ write (§4.2, §4, §4.4): the dead letter + batched XACK + XDEL.
pub async fn finish_dlq(
    bufs: &AckBufs,
    queue: &str,
    stream_id: &str,
    envelope_json: &str,
    reason: &str,
    error: Option<&ErrorJson>,
    result: Option<(&str, &str, u64)>, // (task_id, result_json, ttl_s)
) -> Result<()> {
    let writes = dlq_writes(queue, envelope_json, reason, error, result);
    bufs.submit(queue, stream_id, writes).await
}

/// A dead letter with no delivery to ack: the §14 mover's, for an envelope
/// the broker will never accept. Every reply is read, so a refused write
/// fails the call and the envelope stays in the delayed set.
pub async fn dead_letter(
    conn: &mut ConnectionManager,
    queue: &str,
    envelope_json: &str,
    reason: &str,
    error: Option<&ErrorJson>,
    result: Option<(&str, &str, u64)>,
) -> Result<()> {
    let writes = dlq_writes(queue, envelope_json, reason, error, result);
    let mut pipe = redis::pipe();
    for cmd in writes {
        pipe.add_command(cmd).ignore();
    }
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

/// §4.1 trim boundary: every entry below it is acked. The oldest pending id,
/// or with an empty PEL one past last-delivered-id; None with no group or no
/// delivery. Safe against concurrent delivery and a second trimmer because
/// XREADGROUP `>` delivers ascending ids.
pub async fn acked_below(conn: &mut ConnectionManager, queue: &str) -> Result<Option<String>> {
    let key = q_key(queue);
    // XINFO before XPENDING. Swapped, a delivery between the two reads sits
    // below the boundary while pending and the trim destroys it.
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

/// `"5-3"` -> `"5-4"`, so MINID also removes `id`. At `seq == u64::MAX` the
/// id is returned unchanged: one entry retained, never one destroyed.
fn stream_id_after(id: &str) -> Option<String> {
    let (ms, seq) = crate::ctx::parse_stream_id(id)?;
    Some(match seq.checked_add(1) {
        Some(next) => format!("{ms}-{next}"),
        None => id.to_string(),
    })
}

/// §4.1 backstop for acked but undeleted entries (a flush torn between XACK
/// and XDEL). MINID removes ids strictly below `boundary`. Returns the count.
pub async fn trim_acked(conn: &mut ConnectionManager, queue: &str, boundary: &str) -> Result<u64> {
    let n: u64 = redis::cmd("XTRIM")
        .arg(q_key(queue))
        .arg("MINID")
        .arg(boundary)
        .query_async(conn)
        .await?;
    Ok(n)
}

/// Oldest delivered, unacked entry id, or None with an empty PEL.
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

/// Oldest undelivered entry id. Anchored on last-delivered-id, not the
/// stream head, so an acked but undeleted orphan never reads as backlog.
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

/// §4.4 XPENDING page: (entry_id, consumer, idle_ms, delivery_count).
/// `start` is `"-"`, then `"(<last>"`: restarting at `"-"` within a tick
/// would spin forever on entries skipped as still running.
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
        .and_then(|v| redis::from_redis_value_ref::<String>(v).ok())
}

/// §4.4 peek: XRANGE leaves the PEL untouched, unlike XCLAIM, so each
/// entry's own `timeout_ms` is read before deciding it is stuck. Per entry:
/// None if gone, Some(None) if it has no `e` field.
pub async fn peek_entries(
    conn: &mut ConnectionManager,
    queue: &str,
    entry_ids: &[String],
) -> Result<Vec<Option<Option<String>>>> {
    use redis::streams::StreamRangeReply;
    // redis-rs refuses an empty pipeline ("empty command").
    if entry_ids.is_empty() {
        return Ok(Vec::new());
    }
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

/// §4.4 XCLAIM, one round trip. Per entry: `e` if claimed, None if another
/// worker won or the entry vanished.
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

    /// Wire values from `hashlib.sha256(k).hexdigest()[:32]`. Any change is
    /// breaking: older keys stop matching and an idempotent task runs twice.
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
        assert_eq!(idemp_key("order-42"), idemp_key("order-42"));
        assert_ne!(idemp_key("order-42"), idemp_key("order-43"));

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

    /// FIPS 180-4 vectors, multi block input and both padding branches.
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
        // 55/56: length field fits or not. 63/64/65: block edges.
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

        assert_eq!(
            idemp_key("order-42"),
            format!("cauli:idemp:{}", &hex(sha256(b"order-42"))[..32])
        );
        assert_ne!(idemp_key("order-42"), idemp_key("order-43"));
    }

    #[test]
    fn claim_ttl_never_expires_before_the_execution_it_guards() {
        let grace = crate::exec::BACKSTOP_GRACE_MS;
        assert_eq!(claim_ttl_s(86_400, 300_000), 86_400);
        assert_eq!(
            claim_ttl_s(60, 300_000),
            (300_000 + grace).div_ceil(1000),
            "a 300s task under a 60s idemp_ttl must claim for its execution"
        );
        // Rounds up, never down.
        assert_eq!(claim_ttl_s(0, 1_500), (1_500 + grace).div_ceil(1000));
        assert_eq!(claim_ttl_s(u64::MAX, u64::MAX), u64::MAX);
    }

    /// Ports owned by these tests only: never 6392 (worker/tests/common),
    /// 6391 (py/itest), 6390 (redis_response_timeout.rs) or 6379.
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
                // A stale nodes.conf claims every slot and ADDSLOTSRANGE
                // refuses assigned slots.
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
                        // Retry ADDSLOTSRANGE until CLUSTER INFO shows every
                        // slot: redis-cli exits 0 on a server error, and a node
                        // that just answered PING can still refuse it.
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

    /// A failing XADD (WRONGTYPE) must leave the entry in the delayed set.
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

    /// §14 mover: two workers sweeping at once must not both publish an
    /// envelope, and one that was never confirmed must come due again.
    #[tokio::test]
    async fn a_claimed_envelope_is_leased_not_removed() {
        let redis = ThrowawayRedis::start(6445, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");
        let queue = "leased";
        for (member, due_at) in [("due-1", 900), ("due-2", 1_000), ("later", 5_000)] {
            let _: () = redis::cmd("ZADD")
                .arg(delayed_key(queue))
                .arg(due_at)
                .arg(member)
                .query_async(&mut conn)
                .await
                .expect("seed");
        }
        let script = redis::Script::new(MOVER_CLAIM_LUA);

        let first = claim_due(&mut conn, &script, queue, 1_000, 128, 31_000)
            .await
            .expect("claim");
        assert_eq!(first, ["due-1", "due-2"]);
        let rival = claim_due(&mut conn, &script, queue, 1_000, 128, 31_000)
            .await
            .expect("rival claim");
        assert!(rival.is_empty(), "a leased envelope was claimed twice");

        forget_moved(&mut conn, queue, &["due-1"])
            .await
            .expect("forget");
        let after_lease = claim_due(&mut conn, &script, queue, 31_000, 128, 61_000)
            .await
            .expect("claim after the lease");
        assert_eq!(
            after_lease,
            ["later", "due-2"],
            "the unconfirmed envelope is due again, the confirmed one is gone"
        );
    }

    #[tokio::test]
    async fn a_redelivery_count_is_per_attempt_and_expires() {
        let redis = ThrowawayRedis::start(6446, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");
        let id = "a".repeat(32);

        assert_eq!(count_redelivery(&mut conn, &id, 0).await.expect("1"), 1);
        assert_eq!(count_redelivery(&mut conn, &id, 0).await.expect("2"), 2);
        assert_eq!(
            count_redelivery(&mut conn, &id, 1).await.expect("retry"),
            1,
            "a retry is a new attempt and starts its own count"
        );
        let ttl: i64 = redis::cmd("TTL")
            .arg(format!("cauli:redelivered:{id}:0"))
            .query_async(&mut conn)
            .await
            .expect("ttl");
        assert!(ttl > 0 && ttl <= REDELIVERY_COUNT_TTL_S as i64, "ttl {ttl}");
    }

    /// cauli:q:myqueue and cauli:delayed:myqueue hash to slots 416 and 439.
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

    #[tokio::test]
    async fn claim_outlives_a_long_execution_and_refreshes_on_mine_again() {
        let redis = ThrowawayRedis::start(6420, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let key = "order-77";
        let task_id = "a".repeat(32);
        let timeout_ms: u64 = 600_000;
        let idemp_ttl_s: u64 = 60;

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
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 1, 1, None);
        let sid: String = redis::cmd("XADD")
            .arg(q_key(queue))
            .arg("*")
            .arg("e")
            .arg(r#"{"id":"d1","task":"t"}"#)
            .query_async(&mut conn)
            .await
            .expect("seed entry");
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

    #[test]
    fn ack_precedes_del_on_the_wire() {
        let pipe = ack_pipeline("q", ["1-1", "2-2", "3-3"].into_iter());
        let wire = String::from_utf8(pipe.get_packed_pipeline()).expect("utf8 wire");
        let at = |needle: &str| {
            wire.find(needle)
                .unwrap_or_else(|| panic!("{needle} missing from wire: {wire:?}"))
        };
        assert!(
            at("XACK") < at("XDEL"),
            "the ack must land before the delete, or a tear strands a PEL entry whose payload is gone: {wire:?}"
        );
        assert_eq!(wire.matches("XACK").count(), 1, "one batch XACK: {wire:?}");
        assert_eq!(wire.matches("XDEL").count(), 1, "one batch XDEL: {wire:?}");
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
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 64, 1, None);
        let _: String = redis::cmd("XADD")
            .arg(q_key(queue))
            .arg("*")
            .arg("e")
            .arg(r#"{"id":"s1","task":"t"}"#)
            .query_async(&mut conn)
            .await
            .expect("seed entry");
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

    /// One slow pending task must not retain the 500 completed entries
    /// behind it; trim only removal reads 501.
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
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 64, 1, None);

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

    /// The orphan a flush torn between XACK and XDEL leaves.
    async fn raw_ack(conn: &mut ConnectionManager, queue: &str, sid: &str) {
        let _: u64 = redis::cmd("XACK")
            .arg(q_key(queue))
            .arg("cauli")
            .arg(sid)
            .query_async(conn)
            .await
            .expect("raw xack");
    }

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

    #[tokio::test]
    async fn trim_boundary_is_none_without_a_group_or_a_delivery() {
        let redis = ThrowawayRedis::start(6413, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "trimedge";
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
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        assert_eq!(
            acked_below(&mut conn, queue).await.expect("no delivery"),
            None,
            "nothing delivered: nothing is provably acked"
        );
    }

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
        let bufs = AckBufs::start(&conn, &[queue.to_string()], 8, 1, None);

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

        let bufs = AckBufs::start(&conn, &[queue.to_string()], 4, 10_000, None);
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

        let lone = AckBufs::start(&conn, &[queue.to_string()], 1000, 200, None);
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

    /// Recovery peeks every scan, often with nothing idle enough to reclaim.
    #[tokio::test(flavor = "multi_thread")]
    async fn peeking_no_entries_is_not_an_error() {
        let redis = ThrowawayRedis::start(6402, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");
        let peeked = peek_entries(&mut conn, "peekq", &[]).await.expect("peek");
        assert!(peeked.is_empty());
    }

    /// OOM under `noeviction` refuses the ZADD but would accept the XACK.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_write_never_lets_its_ack_land() {
        let redis = ThrowawayRedis::start(6401, false);
        let client = redis::Client::open(redis.url()).expect("client");
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("connection manager");

        let queue = "oomq";
        ensure_groups(&mut conn, &[queue.to_string()])
            .await
            .expect("groups");
        let mut sids = Vec::new();
        for i in 0..2 {
            let sid: String = redis::cmd("XADD")
                .arg(q_key(queue))
                .arg("*")
                .arg("e")
                .arg(format!(r#"{{"id":"o{i}","task":"t"}}"#))
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
            .arg(2)
            .arg("STREAMS")
            .arg(q_key(queue))
            .arg(">")
            .query_async(&mut conn)
            .await
            .expect("deliver");
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg(1)
            .query_async(&mut conn)
            .await
            .expect("force OOM");

        let bufs = AckBufs::start(&conn, &[queue.to_string()], 2, 10_000, None);
        let (retry, plain) = tokio::join!(
            finish_retry(&bufs, queue, &sids[0], r#"{"id":"o0"}"#, 1),
            finish_success(&bufs, queue, &sids[1], "o1", None, 60),
        );
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg(0)
            .query_async(&mut conn)
            .await
            .expect("lift OOM");

        let err = retry.expect_err("a refused ZADD must fail the retry write");
        assert!(err.to_string().contains("OOM"), "unexpected error: {err}");
        plain.expect("a completion with no writes of its own is unaffected");
        let pending: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(q_key(queue))
            .arg("cauli")
            .arg("-")
            .arg("+")
            .arg(10)
            .query_async(&mut conn)
            .await
            .expect("xpending");
        let pending: Vec<&str> = pending.iter().map(|p| p.0.as_str()).collect();
        assert_eq!(
            pending,
            vec![sids[0].as_str()],
            "the entry whose retry was refused must stay pending for redelivery"
        );
        let len: u64 = redis::cmd("XLEN")
            .arg(q_key(queue))
            .query_async(&mut conn)
            .await
            .expect("xlen");
        assert_eq!(len, 1, "the refused entry must not be deleted");
    }
}
