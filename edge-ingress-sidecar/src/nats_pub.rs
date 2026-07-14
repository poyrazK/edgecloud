//! JetStream publisher (issue #665, PR B).
//!
//! Reads per-tick [`DeltaMsg`]s from the scraper, frames them as the
//! wire payload `{"replica_id":..., "ts_unix_ms":..., "rps":<u32>}`
//! (see [`crate::caddy_metrics::DeltaMsg::to_wire`]), and publishes
//! to `edgecloud.rate-limit.global.delta.<replica_id>` on the
//! `edgecloud-rl-global` JetStream stream (declared by PR C).
//!
//! Mirrors the durable-publish half of `edge-worker/src/nats.rs`:
//!   - [`build_stream_config`] (line 97-106) — same shape: Interest
//!     retention, `MaxAge=60s`, replica count passed through.
//!   - The fire-and-forget publish pattern at line 252-261 — the
//!     sidecar's deltas are best-effort (a dropped tick costs at
//!     most 1s of measurement accuracy and is self-healing on the
//!     next tick), so ack/durability are not required.
//!
//! The consumer half lives in [`crate::nats_sub`] — splitting
//! publisher and consumer into separate modules keeps the wire
//! surface small and lets each module's tests target one concern.
//!
//! ## Idempotent stream ensure
//!
//! The publisher creates the `edgecloud-rl-global` stream on
//! startup if it doesn't exist. This is the same pattern the worker
//! uses for `edgecloud-tasks` (issue #316) — the sidecar can stand
//! up before the control plane (e.g. for local dev) without
//! failing. PR C adds the matching `EnsureStream` call on the CP
//! side; both are idempotent and either ordering works.

use std::time::Duration;

use anyhow::Context;
use async_nats::jetstream;
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy};
use tracing::{debug, warn};

use crate::caddy_metrics::DeltaMsg;

/// NATS stream that holds per-replica rate-limit deltas.
pub const RATE_LIMIT_STREAM: &str = "edgecloud-rl-global";
/// Subject wildcard that captures every per-replica delta subject.
pub const RATE_LIMIT_SUBJECT_WILDCARD: &str = "edgecloud.rate-limit.global.>";

/// Render the per-replica subject leaf for a given pod.
pub fn delta_subject(replica_id: &str) -> String {
    format!("edgecloud.rate-limit.global.delta.{}", replica_id)
}

/// Build the JetStream stream config for the rate-limit delta stream.
/// Extracted as a pure function so the wire contract can be
/// unit-tested without a real NATS connection.
///
/// Mirrors `edge_worker::nats::build_stream_config` at
/// `edge-worker/src/nats.rs:97-106`:
///   - `RetentionPolicy::Interest` — the consumer holds a durable
///     pull, so messages persist until acknowledged.
///   - `MaxAge=60s` — generous headroom over the 1s window; a
///     sidecar that reconnects can replay up to 60s of history.
///   - Replica count passed through from the operator's NATS config.
pub fn build_stream_config(replicas: usize) -> StreamConfig {
    StreamConfig {
        name: RATE_LIMIT_STREAM.to_string(),
        subjects: vec![RATE_LIMIT_SUBJECT_WILDCARD.to_string()],
        retention: RetentionPolicy::Interest,
        max_age: Duration::from_secs(60),
        num_replicas: replicas,
        ..Default::default()
    }
}

/// Build the JetStream consumer config for the rate-limit delta
/// stream. Mirrors `edge_worker::nats::build_consumer_config` at
/// `edge-worker/src/nats.rs:69-92` with these deltas:
///
///   - `filter_subject = RATE_LIMIT_SUBJECT_WILDCARD` — every
///     per-replica delta leaf.
///   - `deliver_policy = DeliverPolicy::LastPerSubject` — on
///     reconnect we want the FRESHEST per-replica delta, not the
///     full backlog. The aggregator's window rebuilds from one
///     message per `<replica_id>`; older messages are noise.
///   - `ack_policy = Explicit`, `max_deliver = 5` — bounded retries
///     on transient failures; unlike `edgecloud-tasks` (which is
///     best-effort fire-and-forget on the publish side), the rate-
///     limit stream is consumed by a long-lived pull/push consumer
///     that benefits from bounded retries.
pub fn build_consumer_config(durable_name: &str) -> async_nats::jetstream::consumer::push::Config {
    use async_nats::jetstream::consumer::{
        push::Config as PushConsumerConfig, AckPolicy, DeliverPolicy,
    };
    PushConsumerConfig {
        name: Some(durable_name.to_string()),
        durable_name: Some(durable_name.to_string()),
        deliver_subject: format!("_INBOX.rate-limit.{}", durable_name),
        deliver_group: None,
        ack_policy: AckPolicy::Explicit,
        deliver_policy: DeliverPolicy::LastPerSubject,
        filter_subject: RATE_LIMIT_SUBJECT_WILDCARD.to_string(),
        max_deliver: 5,
        ..Default::default()
    }
}

/// Convert any async-nats error into an `anyhow::Error`. Mirrors
/// `edge-worker/src/nats.rs::nats_err` so the two crates share the
/// same error-translation pattern.
fn nats_err<E: std::fmt::Display>(e: E) -> anyhow::Error {
    anyhow::anyhow!("nats: {}", e)
}

/// Production JetStream publisher. Mirrors `edge-worker::nats::NatsClientImpl`
/// at `edge-worker/src/nats.rs:144-184` (connect + ensure_stream).
///
/// Note: PR B deliberately does NOT abstract this behind a trait
/// object (`Box<dyn Publisher>`). The async-trait shim added
/// complexity (`async_trait` dep + dyn-compatibility caveats) for
/// no gain — there's exactly one production impl, and the
/// integration tests run against a real testcontainers NATS
/// (the publisher's wire path is end-to-end exercised by tests 8.a/8.b
/// from the plan). A future "mock for offline tests" can wrap
/// this in a trait without changing the call sites in `main.rs`.
pub struct NatsPublisher {
    client: async_nats::Client,
    /// Stream replication factor passed to `ensure_stream`. Mirrors
    /// `edge_worker::nats::NatsClientImpl::task_stream_replicas` at
    /// `edge-worker/src/nats.rs:148`. Carried on the struct (rather
    /// than threaded through every call) so a future "auto-rebuild
    /// the stream when the operator changes the replication factor"
    /// follow-up has the value in scope.
    #[allow(dead_code)]
    replicas: usize,
}

impl NatsPublisher {
    /// Connect to a NATS server. `replicas` is the stream
    /// replication factor passed through to `ensure_stream` (matches
    /// `cfg.NATS.Replicas` on the CP side).
    pub async fn connect(url: &str, replicas: usize) -> anyhow::Result<Self> {
        let client = async_nats::connect(url).await.context("nats connect")?;
        Ok(Self { client, replicas })
    }

    /// Test-only constructor that injects an already-connected
    /// client. Used by the integration tests to share a NATS
    /// container between the test harness and the sidecar.
    #[cfg(test)]
    #[allow(dead_code)] // reserved for the integration test in tests/integration_test.rs (PR E)
    pub fn from_client(client: async_nats::Client, replicas: usize) -> Self {
        Self { client, replicas }
    }

    /// Public for tests + the consumer module: the underlying
    /// async-nats client (used to build the consumer stream).
    pub fn client(&self) -> async_nats::Client {
        self.client.clone()
    }

    /// Idempotently create the rate-limit stream if it doesn't
    /// exist. Mirrors `edge_worker::nats::ensure_task_stream` at
    /// `edge-worker/src/nats.rs:170-184`.
    pub async fn ensure_stream(&self, replicas: usize) -> anyhow::Result<()> {
        let js = jetstream::new(self.client.clone());
        js.get_or_create_stream(build_stream_config(replicas))
            .await
            .map_err(nats_err)
            .context("failed to ensure rate-limit stream")?;
        Ok(())
    }

    /// Publish one delta for the given replica. Fire-and-forget;
    /// if the publish fails, the next tick republishes. Mirrors
    /// `edge_worker::nats::publish_heartbeat` at
    /// `edge-worker/src/nats.rs:252-261`.
    pub async fn publish_delta(&self, replica_id: &str, msg: &DeltaMsg) -> anyhow::Result<()> {
        let subject = delta_subject(replica_id);
        let payload = serde_json::to_vec(&msg.to_wire()).context("serialize delta")?;
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(nats_err)
            .context("publish delta")?;
        Ok(())
    }
}

/// Spawn the publisher task. It owns the receiver end of the
/// scraper→publisher channel and forwards every [`DeltaMsg`] to
/// JetStream. Returns the join handle so tests can `.await` it for
/// clean shutdown.
pub fn spawn_publisher(
    publisher: std::sync::Arc<NatsPublisher>,
    replica_id: String,
    mut rx: tokio::sync::mpsc::Receiver<DeltaMsg>,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("publisher: shutdown received");
                    return;
                }
                maybe_msg = rx.recv() => {
                    let Some(msg) = maybe_msg else {
                        warn!("publisher: scraper dropped the channel; bailing");
                        return;
                    };
                    match publisher.publish_delta(&replica_id, &msg).await {
                        Ok(()) => debug!(rps = msg.rps, "publisher: published delta"),
                        Err(e) => warn!(err = %e, "publisher: publish failed; next tick will retry"),
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── build_stream_config tests (mirror edge-worker/src/nats.rs) ──

    #[test]
    fn stream_config_name_is_edgecloud_rl_global() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.name, RATE_LIMIT_STREAM);
        assert_eq!(cfg.name, "edgecloud-rl-global");
    }

    #[test]
    fn stream_config_subject_is_wildcard() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.subjects, vec![RATE_LIMIT_SUBJECT_WILDCARD]);
    }

    #[test]
    fn stream_config_retention_is_interest() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.retention, RetentionPolicy::Interest);
    }

    #[test]
    fn stream_config_max_age_is_60s() {
        let cfg = build_stream_config(3);
        assert_eq!(cfg.max_age, Duration::from_secs(60));
    }

    #[test]
    fn stream_config_replicas_passed_through() {
        let cfg = build_stream_config(5);
        assert_eq!(cfg.num_replicas, 5);
    }

    // ── build_consumer_config tests ────────────────────────────────

    #[test]
    fn consumer_config_has_correct_name() {
        let cfg = build_consumer_config("sidecar-pod-1");
        assert_eq!(cfg.name.as_deref(), Some("sidecar-pod-1"));
        assert_eq!(cfg.durable_name.as_deref(), Some("sidecar-pod-1"));
    }

    #[test]
    fn consumer_config_filter_subject_is_wildcard() {
        let cfg = build_consumer_config("sidecar-pod-1");
        assert_eq!(cfg.filter_subject, RATE_LIMIT_SUBJECT_WILDCARD);
    }

    #[test]
    fn consumer_config_deliver_policy_is_last_per_subject() {
        use async_nats::jetstream::consumer::DeliverPolicy;
        let cfg = build_consumer_config("sidecar-pod-1");
        assert_eq!(cfg.deliver_policy, DeliverPolicy::LastPerSubject);
    }

    #[test]
    fn consumer_config_max_deliver_is_five() {
        let cfg = build_consumer_config("sidecar-pod-1");
        assert_eq!(cfg.max_deliver, 5);
    }

    #[test]
    fn consumer_config_has_explicit_ack() {
        use async_nats::jetstream::consumer::AckPolicy;
        let cfg = build_consumer_config("sidecar-pod-1");
        assert_eq!(cfg.ack_policy, AckPolicy::Explicit);
    }

    // ── delta_subject tests ────────────────────────────────────────

    #[test]
    fn delta_subject_format() {
        assert_eq!(
            delta_subject("ingress-pod-fra-1"),
            "edgecloud.rate-limit.global.delta.ingress-pod-fra-1"
        );
        assert_eq!(
            delta_subject("pod"),
            "edgecloud.rate-limit.global.delta.pod"
        );
    }
}
