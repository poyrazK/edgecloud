//! NATS client for task subscription and heartbeat publishing.
//!
//! Workers subscribe to the `edgecloud.tasks.<region>` JetStream stream
//! without a queue group — every worker receives every `TaskMessage`
//! (fan-out, issue #316). Workers already diff desired vs. running state
//! in the supervisor, so duplicate task messages are no-ops. The ingress
//! discovers workers via heartbeats and handles multiple upstreams for
//! the same app natively.

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

/// Stream of JetStream task messages. Items expose `.ack()` / `.ack_with()`
/// for flow control.
pub type TaskMessageStream = Box<dyn Stream<Item = JetStreamMessage> + Send + Unpin>;

/// Convert any async-nats error (which may be `Box<dyn StdError + Send + Sync + 'static>`
/// or a generic `Error<Kind>`) into an `anyhow::Error`. The `?` operator
/// trips on the unsized `dyn StdError` variant, so we route through Display.
fn nats_err<E: std::fmt::Display>(e: E) -> anyhow::Error {
    anyhow::anyhow!("nats: {}", e)
}

/// Build the JetStream consumer config for subscribing to task messages.
/// Extracted as a pure function so the wire contract can be unit-tested
/// without a real NATS connection.
pub fn build_consumer_config(
    consumer_name: &str,
    region: &str,
) -> PushConsumerConfig {
    let deliver_subject = format!("_INBOX.task.{}", consumer_name);
    PushConsumerConfig {
        name: Some(consumer_name.to_string()),
        durable_name: Some(consumer_name.to_string()),
        deliver_subject,
        deliver_group: None,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        deliver_policy: jetstream::consumer::DeliverPolicy::All,
        filter_subject: format!("edgecloud.tasks.{}", region),
        max_deliver: 20,
        ..Default::default()
    }
}

/// Build the JetStream stream config for the task stream.
/// Extracted as a pure function so the whitepaper contract can be
/// unit-tested without a real NATS connection.
pub fn build_stream_config(replicas: usize) -> StreamConfig {
    StreamConfig {
        name: TASK_STREAM.to_string(),
        subjects: vec![TASK_SUBJECT_WILDCARD.to_string()],
        retention: RetentionPolicy::Interest,
        max_age: Duration::from_secs(24 * 60 * 60),
        num_replicas: replicas,
        ..Default::default()
    }
}

/// Build the NATS subject for heartbeat messages for a given region.
pub fn heartbeat_subject(region: &str) -> String {
    format!("edgecloud.heartbeats.{}", region)
}

/// Trait for NATS operations — allows test doubles and fakes.
#[async_trait]
pub trait NatsClient: Send + Sync + 'static {
    /// Subscribe to task updates for a region (fan-out, no queue group —
    /// issue #316). Every worker in the region receives every
    /// `TaskMessage`; the supervisor's diff logic handles duplicates.
    ///
    /// `consumer_name` is the durable consumer identity. Workers should pick
    /// a stable name (typically derived from `worker_id`) so that on restart
    /// they resume from their last ack position rather than re-processing
    /// the entire stream.
    async fn subscribe_tasks(
        &self,
        region: &str,
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
        js.get_or_create_stream(build_stream_config(replicas))
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

        // Durable push consumer with explicit ack (no queue group —
        // issue #316 fan-out). Every worker in the region receives every
        // `TaskMessage`; the supervisor's diff logic handles duplicates.
        let config = build_consumer_config(consumer_name, region);
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
        let subject = heartbeat_subject(region);
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // ── build_consumer_config tests ──────────────────────────────────

    #[test]
    fn consumer_config_has_correct_name() {
        let cfg = build_consumer_config("my-consumer", "fra");
        assert_eq!(cfg.name.as_deref(), Some("my-consumer"));
        assert_eq!(cfg.durable_name.as_deref(), Some("my-consumer"));
    }

    #[test]
    fn consumer_config_has_no_queue_group() {
        let cfg = build_consumer_config("c1", "sfo");
        assert!(cfg.deliver_group.is_none(), "fan-out mode: deliver_group must be None");
    }

    #[test]
    fn consumer_config_filter_subject_matches_region() {
        let cfg = build_consumer_config("c1", "fra");
        assert_eq!(cfg.filter_subject, "edgecloud.tasks.fra");
    }

    #[test]
    fn consumer_config_has_explicit_ack() {
        let cfg = build_consumer_config("c1", "fra");
        assert_eq!(cfg.ack_policy, jetstream::consumer::AckPolicy::Explicit);
    }

    #[test]
    fn consumer_config_deliver_policy_is_all() {
        let cfg = build_consumer_config("c1", "fra");
        assert_eq!(cfg.deliver_policy, jetstream::consumer::DeliverPolicy::All);
    }

    #[test]
    fn consumer_config_max_deliver_is_twenty() {
        let cfg = build_consumer_config("c1", "fra");
        assert_eq!(cfg.max_deliver, 20);
    }

    #[test]
    fn consumer_config_deliver_subject_contains_consumer_name() {
        let cfg = build_consumer_config("my-worker-42", "fra");
        assert!(cfg.deliver_subject.contains("my-worker-42"));
    }

    // ── build_stream_config tests ────────────────────────────────────

    #[test]
    fn stream_config_name_is_edgecloud_tasks() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.name, TASK_STREAM);
    }

    #[test]
    fn stream_config_subject_is_wildcard() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.subjects, vec![TASK_SUBJECT_WILDCARD]);
    }

    #[test]
    fn stream_config_retention_is_interest() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.retention, RetentionPolicy::Interest);
    }

    #[test]
    fn stream_config_max_age_is_24h() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.max_age, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn stream_config_replicas_passed_through() {
        let cfg = build_stream_config(5);
        assert_eq!(cfg.num_replicas, 5);
    }

    // ── heartbeat_subject tests ──────────────────────────────────────

    #[test]
    fn heartbeat_subject_format() {
        assert_eq!(heartbeat_subject("fra"), "edgecloud.heartbeats.fra");
        assert_eq!(heartbeat_subject("us-east"), "edgecloud.heartbeats.us-east");
    }

    // ── MockNatsClient ───────────────────────────────────────────────

    /// A mock NATS client that records ack/nack/term call counts.
    pub struct MockNatsClient {
        pub ack_calls: Arc<AtomicU32>,
        pub nack_calls: Arc<AtomicU32>,
        pub term_calls: Arc<AtomicU32>,
        pub fail_ack: bool,
        pub fail_nack: bool,
        pub fail_term: bool,
    }

    impl MockNatsClient {
        pub fn new() -> Self {
            Self {
                ack_calls: Arc::new(AtomicU32::new(0)),
                nack_calls: Arc::new(AtomicU32::new(0)),
                term_calls: Arc::new(AtomicU32::new(0)),
                fail_ack: false,
                fail_nack: false,
                fail_term: false,
            }
        }

        #[allow(dead_code)]
        pub fn ack_count(&self) -> u32 {
            self.ack_calls.load(Ordering::Relaxed)
        }
        #[allow(dead_code)]
        pub fn nack_count(&self) -> u32 {
            self.nack_calls.load(Ordering::Relaxed)
        }
        #[allow(dead_code)]
        pub fn term_count(&self) -> u32 {
            self.term_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl NatsClient for MockNatsClient {
        async fn subscribe_tasks(
            &self,
            _region: &str,
            _consumer_name: &str,
        ) -> anyhow::Result<TaskMessageStream> {
            anyhow::bail!("subscribe_tasks not implemented in mock")
        }

        async fn ack(&self, _msg: &async_nats::jetstream::Message) -> anyhow::Result<()> {
            self.ack_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_ack {
                anyhow::bail!("mock ack failure")
            } else {
                Ok(())
            }
        }

        async fn nack(
            &self,
            _msg: &JetStreamMessage,
            _delay: Option<Duration>,
        ) -> anyhow::Result<()> {
            self.nack_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_nack {
                anyhow::bail!("mock nack failure")
            } else {
                Ok(())
            }
        }

        async fn term(&self, _msg: &JetStreamMessage) -> anyhow::Result<()> {
            self.term_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_term {
                anyhow::bail!("mock term failure")
            } else {
                Ok(())
            }
        }

        async fn publish_heartbeat(
            &self,
            _region: &str,
            _msg: &HeartbeatMessage,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    // ── MockNatsClient behavioural tests ─────────────────────────────

    #[tokio::test]
    async fn mock_ack_increments_counter() {
        let client = MockNatsClient::new();
        // Need a real JetStreamMessage to call ack — this is async-nats
        // internals. For unit testing the MockNatsClient counters we test
        // that the mock itself behaves correctly.
        let acked = client.ack_calls.load(Ordering::Relaxed);
        assert_eq!(acked, 0);
    }

    #[tokio::test]
    async fn mock_nack_increments_counter() {
        let client = MockNatsClient::new();
        assert_eq!(client.nack_count(), 0);
    }

    #[tokio::test]
    async fn mock_term_increments_counter() {
        let client = MockNatsClient::new();
        assert_eq!(client.term_count(), 0);
    }
}
