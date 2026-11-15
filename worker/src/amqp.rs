//! RabbitMQ transport (PROTOCOL §14.1): durable quorum queues, manual acks,
//! confirmed publishes. A lost connection is replaced; the broker requeues
//! what that connection had not acked, so those tasks run again.

use crate::ctx::{parse_stream_id, Ctx};
use crate::transport::{capacity, deliver, Published, Settings};
use anyhow::{anyhow, bail, Context, Result};
use futures_util::stream::{select_all, StreamExt};
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

pub fn queue_name(queue: &str) -> String {
    format!("cauli.q.{queue}")
}

/// Every declarer (workers and the Python client) must send the same
/// arguments, or RabbitMQ refuses the declare with PRECONDITION_FAILED.
fn queue_arguments() -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("quorum".into()),
    );
    arguments
}

/// `url` with the default vhost spelled out. The AMQP URI spec reads
/// `amqp://host/` as the vhost with the empty name, which no RabbitMQ has;
/// the Python client reads it as `/`, and both sides must reach one vhost.
fn with_default_vhost(url: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let authority_start = base.find("://").map_or(0, |i| i + 3);
    let path = base[authority_start..]
        .find('/')
        .map(|i| &base[authority_start + i..]);
    let mut out = match path {
        None => format!("{base}/%2f"),
        Some("/") => format!("{base}%2f"),
        Some(_) => base.to_string(),
    };
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    out
}

/// Persistent, so a classic queue would keep it too.
const DELIVERY_MODE_PERSISTENT: u8 = 2;

const RECONNECT_MIN: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

/// One connection's channels. Delivery tags are scoped to `consume`, so a
/// handle carries `generation` to tell an ack for a replaced connection.
struct Session {
    generation: u64,
    connection: Connection,
    consume: Channel,
    publish: Channel,
}

pub struct Amqp {
    url: String,
    settings: Settings,
    session: Mutex<Option<Arc<Session>>>,
    generations: AtomicU64,
}

impl Amqp {
    /// Connects once up front, so a bad URL or refused login fails startup.
    pub async fn connect(url: &str, settings: Settings) -> Result<Self> {
        let amqp = Amqp {
            url: with_default_vhost(url),
            settings,
            session: Mutex::new(None),
            generations: AtomicU64::new(0),
        };
        amqp.open_session().await?;
        Ok(amqp)
    }

    fn session(&self) -> Option<Arc<Session>> {
        self.session.lock().expect("amqp session poisoned").clone()
    }

    async fn open_session(&self) -> Result<Arc<Session>> {
        let connect = Connection::connect(&self.url, ConnectionProperties::default());
        let connection = tokio::time::timeout(self.settings.timeout, connect)
            .await
            .map_err(|_| anyhow!("no rabbitmq connection within {:?}", self.settings.timeout))?
            .context("rabbitmq connect")?;
        let consume = connection.create_channel().await?;
        let publish = connection.create_channel().await?;
        publish
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        consume
            .basic_qos(self.settings.prefetch, BasicQosOptions::default())
            .await?;
        for queue in &self.settings.queues {
            consume
                .queue_declare(
                    queue_name(queue).into(),
                    QueueDeclareOptions {
                        durable: true,
                        ..QueueDeclareOptions::default()
                    },
                    queue_arguments(),
                )
                .await
                .with_context(|| format!("declare rabbitmq queue {}", queue_name(queue)))?;
        }
        let session = Arc::new(Session {
            generation: self.generations.fetch_add(1, Ordering::Relaxed) + 1,
            connection,
            consume,
            publish,
        });
        *self.session.lock().expect("amqp session poisoned") = Some(session.clone());
        Ok(session)
    }

    async fn live_session(&self) -> Result<Arc<Session>> {
        match self.session() {
            Some(session) if session.connection.status().connected() => Ok(session),
            _ => self.open_session().await,
        }
    }

    pub async fn fetch_loop(&self, ctx: &Arc<Ctx>) {
        let mut backoff = RECONNECT_MIN;
        while !ctx.shutting_down() {
            let lost = match self.live_session().await {
                Ok(session) => {
                    backoff = RECONNECT_MIN;
                    self.consume(ctx, &session).await.err()
                }
                Err(e) => Some(e),
            };
            let Some(e) = lost else { continue };
            warn!(
                "rabbitmq connection lost ({e:#}): deliveries it had not acked return to \
                 their queues and run again; reconnecting in {backoff:?}"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }

    /// Dispatches deliveries until shutdown (`Ok`) or a connection error.
    async fn consume(&self, ctx: &Arc<Ctx>, session: &Session) -> Result<()> {
        let mut consumers = Vec::with_capacity(self.settings.queues.len());
        for (index, queue) in self.settings.queues.iter().enumerate() {
            let consumer = session
                .consume
                .basic_consume(
                    queue_name(queue).into(),
                    format!("cauli-{}-{index}", ctx.consumer).into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?;
            consumers.push(consumer.map(move |delivery| (index, delivery)));
        }
        info!(generation = session.generation, "consuming from rabbitmq");
        let mut deliveries = select_all(consumers);
        let mut shutdown = ctx.shutdown.clone();
        loop {
            let next = tokio::select! {
                biased;
                _ = shutdown.wait_for(|stop| *stop) => return Ok(()),
                next = deliveries.next() => next,
            };
            let Some((index, delivery)) = next else {
                bail!("every consumer was cancelled");
            };
            let delivery = delivery?;
            if capacity(ctx).await.is_none() {
                return Ok(());
            }
            let handle = format!("{}-{}", session.generation, delivery.delivery_tag);
            let deliveries_so_far = delivery_ordinal(&delivery.properties);
            let raw = String::from_utf8(delivery.data).ok();
            let queue = &self.settings.queues[index];
            deliver(ctx, queue, handle, raw, deliveries_so_far).await;
        }
    }

    /// Never fails: an ack that cannot be sent belongs to a connection that
    /// is gone, and the broker has already requeued that delivery.
    pub async fn ack(&self, handles: &[&str]) -> Result<()> {
        let session = self.session();
        let mut dropped = 0;
        for handle in handles {
            let acked = match (&session, parse_stream_id(handle)) {
                (Some(live), Some((generation, tag))) if generation == live.generation => live
                    .consume
                    .basic_ack(tag, BasicAckOptions::default())
                    .await
                    .is_ok(),
                _ => false,
            };
            if !acked {
                dropped += 1;
            }
        }
        if dropped > 0 {
            warn!(
                dropped,
                "finished tasks could not be acked: their rabbitmq connection was replaced, \
                 so the broker has requeued them and they run again"
            );
        }
        Ok(())
    }

    pub async fn publish(&self, queue: &str, envelopes: &[String]) -> Vec<Published> {
        let Some(session) = self.session() else {
            return envelopes
                .iter()
                .map(|_| Published::Failed("no rabbitmq connection".into()))
                .collect();
        };
        let name = queue_name(queue);
        let properties = BasicProperties::default()
            .with_delivery_mode(DELIVERY_MODE_PERSISTENT)
            .with_content_type("application/json".into());
        // Written first and confirmed after, so a page costs one round of
        // fsyncs on the broker instead of one per envelope.
        let mut pending = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            pending.push(
                session
                    .publish
                    .basic_publish(
                        "".into(),
                        name.as_str().into(),
                        BasicPublishOptions {
                            mandatory: true,
                            ..BasicPublishOptions::default()
                        },
                        envelope.as_bytes(),
                        properties.clone(),
                    )
                    .await,
            );
        }
        let mut outcomes = Vec::with_capacity(pending.len());
        for confirm in pending {
            outcomes.push(match confirm {
                Ok(confirm) => self.confirmed(confirm).await,
                Err(e) => Published::Failed(e.to_string()),
            });
        }
        outcomes
    }

    async fn confirmed(
        &self,
        confirm: impl std::future::Future<Output = lapin::Result<Confirmation>>,
    ) -> Published {
        match tokio::time::timeout(self.settings.timeout, confirm).await {
            Ok(Ok(Confirmation::Ack(None))) => Published::Done,
            Ok(Ok(Confirmation::Ack(Some(returned)))) => {
                Published::Failed(format!("returned as unroutable: {}", returned.reply_text))
            }
            Ok(Ok(Confirmation::Nack(_))) => Published::Failed("nacked by the broker".into()),
            Ok(Ok(Confirmation::NotRequested)) => {
                Published::Failed("the broker sent no confirm".into())
            }
            Ok(Err(e)) => Published::Failed(e.to_string()),
            Err(_) => Published::Failed(format!("no confirm within {:?}", self.settings.timeout)),
        }
    }
}

/// 1 for a first delivery. Quorum queues count redeliveries in the
/// `x-delivery-count` header and omit it the first time.
fn delivery_ordinal(properties: &BasicProperties) -> u64 {
    let redeliveries = properties
        .headers()
        .as_ref()
        .and_then(|headers| headers.inner().get("x-delivery-count"))
        .and_then(|count| match count {
            AMQPValue::LongLongInt(n) => u64::try_from(*n).ok(),
            AMQPValue::LongInt(n) => u64::try_from(*n).ok(),
            AMQPValue::LongUInt(n) => Some(u64::from(*n)),
            AMQPValue::ShortInt(n) => u64::try_from(*n).ok(),
            AMQPValue::ShortUInt(n) => Some(u64::from(*n)),
            AMQPValue::ShortShortInt(n) => u64::try_from(*n).ok(),
            AMQPValue::ShortShortUInt(n) => Some(u64::from(*n)),
            _ => None,
        })
        .unwrap_or(0);
    redeliveries.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_count(value: AMQPValue) -> BasicProperties {
        let mut headers = FieldTable::default();
        headers.insert("x-delivery-count".into(), value);
        BasicProperties::default().with_headers(headers)
    }

    #[test]
    fn a_first_delivery_has_no_count_header() {
        assert_eq!(delivery_ordinal(&BasicProperties::default()), 1);
    }

    #[test]
    fn a_redelivery_counts_from_the_header_whatever_its_integer_width() {
        assert_eq!(delivery_ordinal(&with_count(AMQPValue::LongLongInt(1))), 2);
        assert_eq!(delivery_ordinal(&with_count(AMQPValue::LongInt(3))), 4);
        assert_eq!(
            delivery_ordinal(&with_count(AMQPValue::ShortShortUInt(7))),
            8
        );
        assert_eq!(delivery_ordinal(&with_count(AMQPValue::LongLongInt(-1))), 1);
        assert_eq!(
            delivery_ordinal(&with_count(AMQPValue::LongString("x".into()))),
            1
        );
    }

    #[test]
    fn an_empty_vhost_means_the_default_one() {
        for (given, sent) in [
            ("amqp://u:p@host:5672", "amqp://u:p@host:5672/%2f"),
            ("amqp://u:p@host:5672/", "amqp://u:p@host:5672/%2f"),
            ("amqp://u:p@host/prod", "amqp://u:p@host/prod"),
            ("amqp://u:p@host/%2f", "amqp://u:p@host/%2f"),
            (
                "amqps://host/?heartbeat=10",
                "amqps://host/%2f?heartbeat=10",
            ),
            ("amqps://host?heartbeat=10", "amqps://host/%2f?heartbeat=10"),
        ] {
            assert_eq!(with_default_vhost(given), sent, "{given}");
        }
    }

    #[test]
    fn queue_names_share_the_redis_key_shape() {
        assert_eq!(queue_name("default"), "cauli.q.default");
    }
}
