//! Amazon SQS transport (PROTOCOL §14.2): standard queues, long polling, a
//! visibility timeout this worker keeps renewing while it holds a message.
//! A worker that dies stops renewing, and SQS redelivers.

use crate::ctx::Ctx;
use crate::transport::{capacity, deliver, Published, Settings};
use anyhow::{anyhow, bail, Context, Result};
use aws_sdk_sqs::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry,
    MessageSystemAttributeName, SendMessageBatchRequestEntry,
};
use aws_sdk_sqs::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info, warn};

/// SQS names allow letters, digits, `-` and `_`, up to 80 characters.
const QUEUE_NAME_MAX: usize = 80;

pub fn queue_name(queue: &str) -> Result<String> {
    let name = format!("cauli-{queue}");
    if queue.contains('.') || name.len() > QUEUE_NAME_MAX {
        bail!(
            "queue {queue:?} cannot be an SQS queue: SQS names allow letters, digits, '-' \
             and '_' only, and {name:?} must fit in {QUEUE_NAME_MAX} characters"
        );
    }
    Ok(name)
}

/// Entries per batch call, an SQS limit shared by every batch operation.
const BATCH_ENTRIES: usize = 10;
/// Bytes per message, and over a whole SendMessageBatch call.
const MESSAGE_BYTES: usize = 1_048_576;
/// The longest long poll SQS allows; an empty queue costs one request per
/// 20 seconds per queue instead of a busy loop.
const LONG_POLL_S: i32 = 20;
/// SQS refuses a visibility timeout above 12 hours.
const VISIBILITY_MAX_S: u64 = 43_200;
const RECEIVE_RETRY: Duration = Duration::from_secs(1);

pub struct Sqs {
    client: Client,
    settings: Settings,
    /// cauli queue name to SQS queue URL.
    urls: HashMap<String, String>,
    /// Receipt handle to queue URL for every message this worker holds.
    /// The renew loop keeps exactly these invisible to other workers.
    held: Mutex<HashMap<String, String>>,
}

fn region_of(url: &str) -> Option<String> {
    let region = url.strip_prefix("sqs://")?.trim_end_matches('/');
    (!region.is_empty()).then(|| region.to_string())
}

impl Sqs {
    /// Resolves every queue URL up front, creating a missing queue, so bad
    /// credentials or a missing permission fail startup.
    pub async fn connect(url: &str, settings: Settings) -> Result<Self> {
        if url.contains('@') {
            bail!(
                "an sqs:// broker url is sqs:// or sqs://<region>. Credentials come from \
                 the AWS credential chain, never from the url"
            );
        }
        // The same TLS backend as the Redis connection, with the system roots.
        let http = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
            ))
            .build_https();
        let timeouts = aws_config::timeout::TimeoutConfig::builder()
            .connect_timeout(settings.timeout)
            .operation_attempt_timeout(Duration::from_secs(LONG_POLL_S as u64) + settings.timeout)
            .build();
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::v2026_11_10())
            .http_client(http)
            .timeout_config(timeouts);
        if let Some(region) = region_of(url) {
            loader = loader.region(aws_config::Region::new(region));
        }
        let client = Client::new(&loader.load().await);
        let mut urls = HashMap::new();
        for queue in &settings.queues {
            urls.insert(queue.clone(), queue_url(&client, queue).await?);
        }
        Ok(Sqs {
            client,
            settings,
            urls,
            held: Mutex::new(HashMap::new()),
        })
    }

    fn visibility_s(&self) -> i32 {
        self.settings
            .visibility_timeout_s
            .clamp(1, VISIBILITY_MAX_S) as i32
    }

    pub async fn fetch_loop(&self, ctx: &Arc<Ctx>) {
        info!(queues = ?self.settings.queues, "consuming from sqs");
        futures_util::future::join_all(
            self.settings
                .queues
                .iter()
                .map(|queue| self.poll(ctx, queue)),
        )
        .await;
    }

    async fn poll(&self, ctx: &Arc<Ctx>, queue: &str) {
        let url = &self.urls[queue];
        let mut shutdown = ctx.shutdown.clone();
        loop {
            let Some(free) = capacity(ctx).await else {
                return;
            };
            let max = free.min(ctx.args.batch).min(BATCH_ENTRIES) as i32;
            let receive = self
                .client
                .receive_message()
                .queue_url(url)
                .max_number_of_messages(max)
                .wait_time_seconds(LONG_POLL_S)
                .visibility_timeout(self.visibility_s())
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .send();
            let reply = tokio::select! {
                biased;
                _ = shutdown.wait_for(|stop| *stop) => return,
                reply = receive => reply,
            };
            let messages = match reply {
                Ok(reply) => reply.messages.unwrap_or_default(),
                Err(e) => {
                    warn!(queue, "sqs receive failed: {}", DisplayErrorContext(&e));
                    tokio::time::sleep(RECEIVE_RETRY).await;
                    continue;
                }
            };
            for message in messages {
                let Some(handle) = message.receipt_handle else {
                    continue;
                };
                self.held
                    .lock()
                    .expect("sqs held set poisoned")
                    .insert(handle.clone(), url.clone());
                let deliveries = message
                    .attributes
                    .as_ref()
                    .and_then(|a| a.get(&MessageSystemAttributeName::ApproximateReceiveCount))
                    .and_then(|count| count.parse().ok())
                    .unwrap_or(1);
                deliver(ctx, queue, handle, message.body, deliveries).await;
            }
        }
    }

    /// Renews the visibility timeout of every held message, forever. Runs
    /// through the drain too: tasks still finishing must stay invisible.
    pub async fn renew_loop(&self) {
        let period = Duration::from_secs((self.visibility_s() as u64 / 3).max(1));
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let mut by_url: HashMap<String, Vec<String>> = HashMap::new();
            for (handle, url) in self.held.lock().expect("sqs held set poisoned").iter() {
                by_url.entry(url.clone()).or_default().push(handle.clone());
            }
            for (url, handles) in by_url {
                for chunk in handles.chunks(BATCH_ENTRIES) {
                    self.renew(&url, chunk).await;
                }
            }
        }
    }

    async fn renew(&self, url: &str, handles: &[String]) {
        let mut request = self.client.change_message_visibility_batch().queue_url(url);
        for (index, handle) in handles.iter().enumerate() {
            let entry = ChangeMessageVisibilityBatchRequestEntry::builder()
                .id(index.to_string())
                .receipt_handle(handle)
                .visibility_timeout(self.visibility_s())
                .build()
                .expect("id and receipt handle are set");
            request = request.entries(entry);
        }
        match request.send().await {
            // A handle acked since the snapshot, or past the 12 hour ceiling.
            Ok(reply) if !reply.failed.is_empty() => debug!(
                failed = reply.failed.len(),
                "sqs did not renew some visibility timeouts: {:?}",
                reply.failed.first().map(|f| f.code())
            ),
            Ok(_) => {}
            Err(e) => warn!(
                "sqs visibility renewal failed, held messages may be redelivered: {}",
                DisplayErrorContext(&e)
            ),
        }
    }

    pub async fn ack(&self, queue: &str, handles: &[&str]) -> Result<()> {
        let url = self
            .urls
            .get(queue)
            .ok_or_else(|| anyhow!("no sqs queue url for queue {queue}"))?;
        for chunk in handles.chunks(BATCH_ENTRIES) {
            let mut request = self.client.delete_message_batch().queue_url(url);
            for (index, handle) in chunk.iter().enumerate() {
                let entry = DeleteMessageBatchRequestEntry::builder()
                    .id(index.to_string())
                    .receipt_handle(*handle)
                    .build()
                    .expect("id and receipt handle are set");
                request = request.entries(entry);
            }
            let reply = request
                .send()
                .await
                .map_err(|e| anyhow!("sqs delete failed: {}", DisplayErrorContext(&e)))?;
            if !reply.failed.is_empty() {
                // An expired receipt handle: the message is already visible
                // again and will run a second time.
                warn!(
                    queue,
                    failed = reply.failed.len(),
                    "sqs refused to delete finished messages ({:?}); they run again",
                    reply.failed.first().map(|f| f.code())
                );
            }
            let mut held = self.held.lock().expect("sqs held set poisoned");
            for handle in chunk {
                held.remove(*handle);
            }
        }
        Ok(())
    }

    pub async fn publish(&self, queue: &str, envelopes: &[String]) -> Vec<Published> {
        let Some(url) = self.urls.get(queue) else {
            return envelopes
                .iter()
                .map(|_| Published::Failed(format!("no sqs queue url for queue {queue}")))
                .collect();
        };
        let mut outcomes: Vec<Option<Published>> = envelopes
            .iter()
            .map(|envelope| {
                (envelope.len() > MESSAGE_BYTES).then(|| {
                    Published::Rejected(format!(
                        "{} bytes is over the SQS message limit of {MESSAGE_BYTES}",
                        envelope.len()
                    ))
                })
            })
            .collect();
        let mut batch: Vec<usize> = Vec::new();
        let mut batch_bytes = 0;
        for index in 0..envelopes.len() {
            if outcomes[index].is_some() {
                continue;
            }
            let size = envelopes[index].len();
            if batch.len() == BATCH_ENTRIES || batch_bytes + size > MESSAGE_BYTES {
                self.send_batch(url, envelopes, &batch, &mut outcomes).await;
                batch.clear();
                batch_bytes = 0;
            }
            batch.push(index);
            batch_bytes += size;
        }
        if !batch.is_empty() {
            self.send_batch(url, envelopes, &batch, &mut outcomes).await;
        }
        outcomes
            .into_iter()
            .map(|outcome| outcome.unwrap_or(Published::Failed("sqs gave no verdict".into())))
            .collect()
    }

    /// Sends one batch. A call SQS refuses over a message body is sent
    /// again one message at a time, so the refusal lands on its culprit and
    /// the rest of the batch is still published.
    async fn send_batch(
        &self,
        url: &str,
        envelopes: &[String],
        batch: &[usize],
        outcomes: &mut [Option<Published>],
    ) {
        let Some(refused) = self.try_send(url, envelopes, batch, outcomes).await else {
            return;
        };
        if let [only] = batch {
            outcomes[*only] = Some(Published::Rejected(refused));
            return;
        }
        for index in batch {
            if let Some(refused) = self.try_send(url, envelopes, &[*index], outcomes).await {
                outcomes[*index] = Some(Published::Rejected(refused));
            }
        }
    }

    /// One SendMessageBatch; entry ids are indexes into `envelopes`. Returns
    /// the reason when SQS refused the whole call over a message body; every
    /// other result is written to `outcomes`.
    async fn try_send(
        &self,
        url: &str,
        envelopes: &[String],
        batch: &[usize],
        outcomes: &mut [Option<Published>],
    ) -> Option<String> {
        let mut request = self.client.send_message_batch().queue_url(url);
        for index in batch {
            let entry = SendMessageBatchRequestEntry::builder()
                .id(index.to_string())
                .message_body(&envelopes[*index])
                .build()
                .expect("id and body are set");
            request = request.entries(entry);
        }
        match request.send().await {
            Ok(reply) => {
                for sent in reply.successful {
                    if let Ok(index) = sent.id.parse::<usize>() {
                        outcomes[index] = Some(Published::Done);
                    }
                }
                for failed in reply.failed {
                    let Ok(index) = failed.id.parse::<usize>() else {
                        continue;
                    };
                    let why = format!("{}: {}", failed.code, failed.message.unwrap_or_default());
                    outcomes[index] = Some(if failed.sender_fault {
                        Published::Rejected(why)
                    } else {
                        Published::Failed(why)
                    });
                }
            }
            Err(e) => {
                let why = DisplayErrorContext(&e).to_string();
                if e.code().is_some_and(is_body_refusal) {
                    return Some(why);
                }
                for index in batch {
                    outcomes[*index] = Some(Published::Failed(why.clone()));
                }
            }
        }
        None
    }
}

/// Error codes that condemn a message body rather than the request: too
/// long, or characters SQS does not accept. Deliberately a short list. A
/// code outside it (throttling, expired credentials, a missing queue) is
/// retried, because dead lettering on those would discard good tasks.
fn is_body_refusal(code: &str) -> bool {
    matches!(
        code,
        "InvalidMessageContents"
            | "MessageTooLong"
            | "InvalidParameterValue"
            | "InvalidAttributeValue"
            | "BatchRequestTooLong"
            | "AWS.SimpleQueueService.BatchRequestTooLong"
    )
}

/// The error code of a queue that does not exist: the first is what SQS and
/// LocalStack send, the second what ElasticMQ sends.
fn is_missing_queue(code: &str) -> bool {
    matches!(
        code,
        "AWS.SimpleQueueService.NonExistentQueue" | "QueueDoesNotExist"
    )
}

async fn queue_url(client: &Client, queue: &str) -> Result<String> {
    let name = queue_name(queue)?;
    let found = client.get_queue_url().queue_name(&name).send().await;
    let url = match found {
        Ok(reply) => reply.queue_url,
        Err(e) if e.code().is_some_and(is_missing_queue) => {
            info!(queue = %name, "sqs queue does not exist, creating it");
            client
                .create_queue()
                .queue_name(&name)
                .send()
                .await
                .map_err(|e| anyhow!("{}", DisplayErrorContext(&e)))
                .with_context(|| {
                    format!(
                        "create sqs queue {name} (create it yourself, or grant sqs:CreateQueue)"
                    )
                })?
                .queue_url
        }
        Err(e) => {
            return Err(anyhow!("{}", DisplayErrorContext(&e)))
                .with_context(|| format!("look up sqs queue {name}"));
        }
    };
    url.ok_or_else(|| anyhow!("sqs returned no url for queue {name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_region_is_the_url_host() {
        assert_eq!(region_of("sqs://us-east-1"), Some("us-east-1".into()));
        assert_eq!(region_of("sqs://eu-west-2/"), Some("eu-west-2".into()));
        assert_eq!(region_of("sqs://"), None);
    }

    #[test]
    fn only_a_refused_body_is_permanent() {
        for code in [
            "InvalidMessageContents",
            "MessageTooLong",
            "BatchRequestTooLong",
        ] {
            assert!(is_body_refusal(code), "{code}");
        }
        for code in [
            "ThrottlingException",
            "RequestThrottled",
            "ExpiredToken",
            "AccessDenied",
            "AWS.SimpleQueueService.NonExistentQueue",
            "InternalError",
        ] {
            assert!(
                !is_body_refusal(code),
                "{code} must be retried, not dead lettered"
            );
        }
    }

    #[test]
    fn queue_names_are_checked_against_the_sqs_alphabet() {
        assert_eq!(queue_name("default").unwrap(), "cauli-default");
        assert_eq!(queue_name("high_prio-1").unwrap(), "cauli-high_prio-1");
        assert!(queue_name("a.b").is_err());
        assert!(queue_name(&"q".repeat(74)).is_ok());
        assert!(queue_name(&"q".repeat(75)).is_err());
    }
}
