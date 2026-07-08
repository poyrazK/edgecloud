//! NATS client for task subscription and heartbeat publishing.
//!
//! Workers subscribe to the `edgecloud.tasks.<region>` JetStream stream as
//! members of a queue group. NATS delivers each `TaskMessage` to exactly
//! one worker in the group, preventing duplicate app starts across workers
//! in the same region (issue #86). The control plane's publisher is
//! unchanged — it still publishes to the subject; the queue-group is a
//! consumer-side property.

use std::time::Duration;

use anyhow::Context;
use async_nats::jetstream::consumer::push::Config as PushConsumerConfig;
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy};
use async_nats::jetstream::{self, AckKind, Message as JetStreamMessage};
use async_trait::async_trait;
use futures::{Stream, StreamExt};

use crate::messages::HeartbeatMessage;

/// NATS stream that holds task messages for the platform.
pub const TASK_STREAM: &str = "edgecloud-tasks";
/// Subject wildcard that captures all `edgecloud.tasks.<region>` traffic.
pub const TASK_SUBJECT_WILDCARD: &str = "edgecloud.tasks.>";
/// Default queue group all workers in a region join.
pub const DEFAULT_QUEUE_GROUP: &str = "edgecloud-workers";

/// Stream of JetStream task messages. Items expose `.ack()` / `.ack_with()`
/// for flow control.
pub type TaskMessageStream = Box<dyn Stream<Item = JetStreamMessage> + Send + Unpin>;

/// Convert any async-nats error (which may be `Box<dyn StdError + Send + Sync + 'static>`
/// or a generic `Error<Kind>`) into an `anyhow::Error`. The `?` operator
/// trips on the unsized `dyn StdError` variant, so we route through Display.
fn nats_err<E: std::fmt::Display>(e: E) -> anyhow::Error {
    anyhow::anyhow!("nats: {}", e)
}

/// Trait for NATS operations — allows test doubles and fakes.
#[async_trait]
pub trait NatsClient: Send + Sync + 'static {
    /// Subscribe to task updates for a region as a member of `queue_group`.
    ///
    /// `consumer_name` is the durable consumer identity. Workers should pick
    /// a stable name (typically derived from `worker_id`) so that on restart
    /// they resume from their last ack position rather than re-processing
    /// the entire stream.
    async fn subscribe_tasks(
        &self,
        region: &str,
        queue_group: &str,
        consumer_name: &str,
    ) -> anyhow::Result<TaskMessageStream>;

    /// Acknowledge successful processing of a task message.
    async fn ack(&self, msg: &JetStreamMessage) -> anyhow::Result<()>;

    /// Negative-ack a task message — server will re-deliver. If `delay`
    /// is `Some`, the server waits at least that long before redelivery.
    async fn nack(&self, msg: &JetStreamMessage, delay: Option<Duration>) -> anyhow::Result<()>;

    /// Terminate a poison-pill message — do not re-deliver.
    async fn term(&self, msg: &JetStreamMessage) -> anyhow::Result<()>;

    /// Publish a heartbeat message to the given region.
    async fn publish_heartbeat(&self, region: &str, msg: &HeartbeatMessage) -> anyhow::Result<()>;
}

/// Production NATS client wrapping async-nats with JetStream support.
pub struct NatsClientImpl {
    client: async_nats::Client,
    task_stream_replicas: usize,
}

impl NatsClientImpl {
    /// Connect to a NATS server.
    pub async fn connect(url: &str, task_stream_replicas: usize) -> anyhow::Result<Self> {
        let client = async_nats::connect(url).await?;
        Ok(Self {
            client,
            task_stream_replicas,
        })
    }

    /// Idempotently create the tasks stream if it does not exist.
    ///
    /// Matches the whitepaper §8.4 contract: interest retention (allows
    /// per-region consumers with different filter subjects), 24h max
    /// age, replication factor 3. Safe to call from both the worker and
    /// the control plane.
    pub async fn ensure_task_stream(&self, replicas: usize) -> anyhow::Result<()> {
        let js = jetstream::new(self.client.clone());
        js.get_or_create_stream(StreamConfig {
            name: TASK_STREAM.to_string(),
            subjects: vec![TASK_SUBJECT_WILDCARD.to_string()],
            retention: RetentionPolicy::Interest,
            max_age: Duration::from_secs(24 * 60 * 60),
            num_replicas: replicas,
            ..Default::default()
        })
        .await
        .map_err(nats_err)
        .context("failed to ensure task stream")?;
        Ok(())
    }
}

#[async_trait]
impl NatsClient for NatsClientImpl {
    async fn subscribe_tasks(
        &self,
        region: &str,
        queue_group: &str,
        consumer_name: &str,
    ) -> anyhow::Result<TaskMessageStream> {
        // Idempotent — works whether or not the control plane already
        // created the stream.
        self.ensure_task_stream(self.task_stream_replicas).await?;
        let js = jetstream::new(self.client.clone());
        let stream = js
            .get_stream(TASK_STREAM)
            .await
            .map_err(nats_err)
            .context("tasks stream missing after EnsureStream")?;

        // Queue-grouped durable push consumer with explicit ack. The
        // server picks the delivery subject; `deliver_group` is the
        // queue-group name — NATS load-balances messages across consumers
        // in the same group, preventing duplicate app starts across
        // workers in the same region (issue #86).
        let deliver_subject = format!("_INBOX.task.{}", consumer_name);
        let config = PushConsumerConfig {
            name: Some(consumer_name.to_string()),
            durable_name: Some(consumer_name.to_string()),
            deliver_subject,
            deliver_group: Some(queue_group.to_string()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
            filter_subject: format!("edgecloud.tasks.{}", region),
            max_deliver: 20,
            ..Default::default()
        };
        let consumer: jetstream::consumer::PushConsumer = stream
            .get_or_create_consumer(consumer_name, config)
            .await
            .map_err(nats_err)
            .context("failed to create durable consumer")?;
        let messages = consumer
            .messages()
            .await
            .map_err(nats_err)
            .context("consumer.messages()")?;
        // The push consumer stream yields `Result<Message, Error>`. Surface
        // per-message errors via tracing and forward only the successes.
        // Errors that should terminate the loop (e.g., the consumer being
        // deleted) will return None from the underlying stream once the
        // server closes it.
        let messages = messages.filter_map(|result| {
            if let Err(e) = &result {
                tracing::error!(err = %e, "jetstream message error");
            }
            std::future::ready(result.ok())
        });
        Ok(Box::new(messages))
    }

    async fn ack(&self, msg: &JetStreamMessage) -> anyhow::Result<()> {
        msg.ack().await.map_err(nats_err)?;
        Ok(())
    }

    async fn nack(&self, msg: &JetStreamMessage, delay: Option<Duration>) -> anyhow::Result<()> {
        msg.ack_with(AckKind::Nak(delay)).await.map_err(nats_err)?;
        Ok(())
    }

    async fn term(&self, msg: &JetStreamMessage) -> anyhow::Result<()> {
        msg.ack_with(AckKind::Term).await.map_err(nats_err)?;
        Ok(())
    }

    async fn publish_heartbeat(&self, region: &str, msg: &HeartbeatMessage) -> anyhow::Result<()> {
        let subject = format!("edgecloud.heartbeats.{}", region);
        let payload = serde_json::to_vec(msg)?;
        // Heartbeats are fire-and-forget; we don't need ack/durability for
        // them. If a heartbeat is lost, the next tick (30s) republishes.
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(nats_err)?;
        Ok(())
    }
}
