//! JetStream push consumer (issue #665, PR B).
//!
//! Subscribes to `edgecloud.rate-limit.global.>` via a push consumer
//! with [`DeliverPolicy::LastPerSubject`], parses each message into
//! a [`DeltaMsg`], and pushes it into the aggregator's channel.
//!
//! Mirrors `edge-worker/src/nats.rs:69-92` `build_consumer_config`
//! with these deltas (the helpers live in [`crate::nats_pub`]):
//!
//!   - `filter_subject = RATE_LIMIT_SUBJECT_WILDCARD` — every
//!     per-replica delta leaf under
//!     `edgecloud.rate-limit.global.delta.<replica_id>`.
//!   - `deliver_policy = DeliverPolicy::LastPerSubject` — on
//!     reconnect we want the FRESHEST per-replica delta, not the
//!     full backlog. The aggregator's sliding window rebuilds from
//!     one message per `<replica_id>`; older messages are noise.
//!   - `ack_policy = Explicit`, `max_deliver = 5` — bounded retries
//!     on transient failures.
//!
//! ## Resilient loop
//!
//! `spawn_consumer` is a long-running task that survives transient
//! JetStream failures (network blips, consumer-rebuild races). On
//! any error we log, sleep 1s, and rebuild the stream + consumer.
//! The aggregator keeps the last-known window the whole time, so
//! the sidecar degrades to "rendering the last value" rather than
//! panicking — matching the failure-mode table in the issue #665
//! plan.

use std::time::Duration;

use anyhow::Context;
use async_nats::jetstream::{self};
use futures::StreamExt;
use tracing::{debug, warn};

use crate::caddy_metrics::DeltaMsg;
use crate::nats_pub::{build_consumer_config, RATE_LIMIT_STREAM};

/// Spawn the consumer task. See module docs for the wire contract.
///
/// `consumer_name` MUST be stable across restarts so the durable
/// consumer is reused (issue #86 sibling semantics on the worker
/// side — a fresh name would create a new durable and miss the
/// `LastPerSubject` replay of the prior tick). The sidecar uses
/// `<replica_id>-rl-consumer` by default.
pub fn spawn_consumer(
    client: async_nats::Client,
    consumer_name: String,
    tx: tokio::sync::mpsc::Sender<DeltaMsg>,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("consumer: shutdown received");
                    return;
                }
                else => {}
            }
            // Build the consumer config + create / fetch. If the
            // stream or consumer creation fails (e.g. transient
            // connection issue), back off and retry — the
            // aggregator keeps the last-known cache.
            let config = build_consumer_config(&consumer_name);
            let stream = match jetstream::new(client.clone())
                .get_stream(RATE_LIMIT_STREAM)
                .await
                .context("get_stream")
            {
                Ok(s) => s,
                Err(e) => {
                    warn!(err = %e, "consumer: get_stream failed; retrying in 1s");
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                }
            };
            let consumer: async_nats::jetstream::consumer::PushConsumer = match stream
                .get_or_create_consumer(&consumer_name, config)
                .await
                .context("get_or_create_consumer")
            {
                Ok(c) => c,
                Err(e) => {
                    warn!(err = %e, "consumer: get_or_create_consumer failed; retrying in 1s");
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                }
            };
            let messages = match consumer.messages().await.context("messages()") {
                Ok(m) => m,
                Err(e) => {
                    warn!(err = %e, "consumer: messages() failed; retrying in 1s");
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    }
                }
            };
            // Pin on_ack: every message gets Explicit-ack'd so the
            // server knows we received it. We don't act on the ack
            // result beyond logging — `LastPerSubject` + `MaxAge=60s`
            // means a missed ack only costs the next 1s of freshness.
            futures::pin_mut!(messages);
            let mut stream_ended = false;
            while !stream_ended {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    next = messages.next() => {
                        match next {
                            Some(Ok(msg)) => {
                                let subject = msg.subject.to_string();
                                let replica_id = subject
                                    .strip_prefix("edgecloud.rate-limit.global.delta.")
                                    .unwrap_or(&subject)
                                    .to_string();
                                match serde_json::from_slice::<DeltaMsgWire>(&msg.payload) {
                                    Ok(wire) => {
                                        let delta = DeltaMsg {
                                            replica_id: wire.replica_id.unwrap_or(replica_id),
                                            rps: wire.rps,
                                        };
                                        debug!(replica = %delta.replica_id, rps = delta.rps, "consumer: received delta");
                                        if tx.send(delta).await.is_err() {
                                            warn!("consumer: aggregator dropped the channel; bailing");
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        warn!(err = %e, subject = %subject, "consumer: failed to parse delta");
                                    }
                                }
                                if let Err(e) = msg.ack().await {
                                    warn!(err = %e, "consumer: ack failed");
                                }
                            }
                            Some(Err(e)) => {
                                warn!(err = %e, "consumer: jetstream message error");
                            }
                            None => {
                                // Stream ended (server closed the
                                // consumer or fatal error). Rebuild
                                // and continue.
                                warn!("consumer: stream ended; rebuilding");
                                stream_ended = true;
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Wire shape produced by [`crate::caddy_metrics::DeltaMsg::to_wire`]
/// and consumed here. `replica_id` may be absent on a malformed
/// message — fall back to the subject leaf so a buggy publisher
/// can't poison the aggregator.
#[derive(Debug, serde::Deserialize)]
struct DeltaMsgWire {
    #[serde(default)]
    replica_id: Option<String>,
    rps: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── DeltaMsgWire shape tests (parity with DeltaMsg::to_wire) ──

    #[test]
    fn wire_parses_replica_id_and_rps() {
        let body = serde_json::json!({
            "replica_id": "pod-A",
            "ts_unix_ms": 1700000000000_u64,
            "rps": 5_000,
        });
        let wire: DeltaMsgWire = serde_json::from_value(body).unwrap();
        assert_eq!(wire.replica_id.as_deref(), Some("pod-A"));
        assert_eq!(wire.rps, 5_000);
    }

    #[test]
    fn wire_tolerates_missing_replica_id() {
        // Subject-leaf fallback is the consumer's job — the wire
        // parser should accept the field as absent.
        let body = serde_json::json!({
            "ts_unix_ms": 1700000000000_u64,
            "rps": 5_000,
        });
        let wire: DeltaMsgWire = serde_json::from_value(body).unwrap();
        assert!(wire.replica_id.is_none());
        assert_eq!(wire.rps, 5_000);
    }

    #[test]
    fn wire_rejects_missing_rps() {
        let body = serde_json::json!({"replica_id": "pod-A"});
        let err = serde_json::from_value::<DeltaMsgWire>(body).unwrap_err();
        assert!(err.to_string().contains("rps"));
    }
}
