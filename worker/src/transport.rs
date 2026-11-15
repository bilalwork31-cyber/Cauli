//! RabbitMQ and SQS as the ready queue (PROTOCOL §14). Redis stays the store
//! for results, the delayed set, idempotency claims and dead letters, so
//! everything here is delivery only: receive, ack, and publish for the mover.

use crate::amqp::Amqp;
use crate::ctx::Ctx;
use crate::dispatch::{dlq_terminal, spawn_dispatch, valid_task_id};
use crate::envelope::{redelivery_limit, Envelope};
use crate::sqs::Sqs;
use anyhow::{bail, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub enum Transport {
    Amqp(Amqp),
    Sqs(Sqs),
}

/// What the mover learns about one envelope it handed to the broker.
#[derive(Debug, PartialEq, Eq)]
pub enum Published {
    Done,
    /// Transient: the envelope stays in the delayed set for a later sweep.
    Failed(String),
    /// The broker will never accept this body, so it is dead lettered.
    Rejected(String),
}

pub struct Settings {
    pub queues: Vec<String>,
    /// Seconds a delivery stays invisible to other workers without a renewal.
    pub visibility_timeout_s: u64,
    /// Unacked deliveries the broker may push per queue (RabbitMQ only).
    pub prefetch: u16,
    /// Connect and per call deadline; the worker's `--redis-timeout`.
    pub timeout: Duration,
}

fn scheme(url: &str) -> &str {
    url.split_once("://").map_or("", |(scheme, _)| scheme)
}

impl Transport {
    pub async fn connect(url: &str, settings: Settings) -> Result<Self> {
        match scheme(url) {
            "amqp" | "amqps" => Ok(Self::Amqp(Amqp::connect(url, settings).await?)),
            "sqs" => Ok(Self::Sqs(Sqs::connect(url, settings).await?)),
            other => bail!(
                "broker url scheme {other:?} is not supported: use amqp://, amqps:// or \
                 sqs://. Redis is the broker when no broker url is set"
            ),
        }
    }

    /// Receives and dispatches until shutdown.
    pub async fn fetch_loop(&self, ctx: &Arc<Ctx>) {
        match self {
            Self::Amqp(amqp) => amqp.fetch_loop(ctx).await,
            Self::Sqs(sqs) => sqs.fetch_loop(ctx).await,
        }
    }

    /// Acks deliveries whose completion writes have landed in Redis.
    pub async fn ack(&self, queue: &str, handles: &[&str]) -> Result<()> {
        match self {
            Self::Amqp(amqp) => amqp.ack(handles).await,
            Self::Sqs(sqs) => sqs.ack(queue, handles).await,
        }
    }

    /// Publishes due envelopes for the mover; one outcome per envelope.
    pub async fn publish(&self, queue: &str, envelopes: &[String]) -> Vec<Published> {
        match self {
            Self::Amqp(amqp) => amqp.publish(queue, envelopes).await,
            Self::Sqs(sqs) => sqs.publish(queue, envelopes).await,
        }
    }

    /// Keeps held deliveries invisible to other workers. Returns at once for
    /// RabbitMQ, which holds them for as long as the connection lives.
    pub async fn renew_loop(&self) {
        if let Self::Sqs(sqs) = self {
            sqs.renew_loop().await;
        }
    }
}

/// How long a fetch loop waits before asking again while completions cannot
/// reach Redis.
const STALLED_POLL: Duration = Duration::from_millis(250);

/// Free io slots once a delivery may start, `None` on shutdown. Closed while
/// completions are failing to reach Redis: running more tasks then only
/// grows the pile of outcomes that cannot be recorded.
pub async fn capacity(ctx: &Arc<Ctx>) -> Option<usize> {
    loop {
        let free = crate::loops::admission_open(ctx).await?;
        if !ctx.acks.stalled() {
            return Some(free);
        }
        tokio::time::sleep(STALLED_POLL).await;
    }
}

/// Dispatches one delivery, or dead letters it past the §4.4 redelivery
/// limit. `deliveries` is the broker's count including this delivery, so 1
/// is a first delivery and costs nothing extra.
pub async fn deliver(
    ctx: &Arc<Ctx>,
    queue: &str,
    handle: String,
    raw: Option<String>,
    deliveries: u64,
) {
    if deliveries > 1 && past_redelivery_limit(ctx, raw.as_deref(), deliveries).await {
        warn!(queue, deliveries, "redelivery limit exceeded -> DLQ");
        let raw = raw.as_deref().unwrap_or("");
        return dlq_terminal(ctx, queue, &handle, raw, "redelivery_limit", None).await;
    }
    spawn_dispatch(ctx.clone(), queue.to_string(), handle, raw);
}

/// Whether a redelivered envelope has been started more often than §4.4
/// allows. Counted here, in Redis, per attempt, not taken from the broker:
/// the broker's count also rises for deliveries a dying worker took and
/// never started (its prefetch buffer on RabbitMQ, a long poll it left open
/// on SQS), so a crash looping worker would dead letter its neighbours'
/// tasks unrun. The broker's count decides only when the envelope has no
/// usable id to count under.
async fn past_redelivery_limit(ctx: &Arc<Ctx>, raw: Option<&str>, deliveries: u64) -> bool {
    let parsed = raw.and_then(|r| serde_json::from_str::<Envelope>(r).ok());
    let limit = redelivery_limit(parsed.as_ref());
    let Some(env) = parsed.filter(|env| valid_task_id(&env.id)) else {
        return deliveries > limit + 1;
    };
    let mut conn = ctx.redis.clone();
    match crate::broker::count_redelivery(&mut conn, &env.id, env.retries).await {
        // The first delivery is not counted, so this is start `started + 1`.
        Ok(started) => started > limit,
        Err(e) => {
            // Toward running it: a lost count must not cost a task.
            warn!(id = %env.id, "redelivery count failed ({e}); dispatching");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_is_what_precedes_the_authority() {
        assert_eq!(scheme("amqp://guest:guest@host:5672/%2f"), "amqp");
        assert_eq!(scheme("amqps://host"), "amqps");
        assert_eq!(scheme("sqs://us-east-1"), "sqs");
        assert_eq!(scheme("sqs://"), "sqs");
        assert_eq!(scheme("not a url"), "");
    }
}
