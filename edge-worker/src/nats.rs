//! NATS JetStream client for task subscription and heartbeat publishing.

use futures::StreamExt;

use crate::messages::HeartbeatMessage;

/// NATS client wrapping async-nats with JetStream support.
pub struct NatsClient {
    client: async_nats::Client,
}

impl NatsClient {
    /// Connect to a NATS server.
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let client = async_nats::connect(url).await?;
        Ok(Self { client })
    }

    /// Subscribe to task updates for a region.
    ///
    /// Returns a `Stream` of raw `async_nats::Message`. The caller is responsible
    /// for deserializing the payload.
    pub async fn subscribe(
        &self,
        region: &str,
    ) -> anyhow::Result<impl StreamExt<Item = async_nats::Message>> {
        let subject = format!("edgecloud.tasks.{}", region);

        // Use a simple NATS subscription (non-JetStream for MVP simplicity).
        // JetStream pull consumers can be added when at-least-once delivery is needed.
        let subscription = self.client.subscribe(subject).await?;
        Ok(subscription)
    }

    /// Publish a heartbeat message to the given region.
    pub async fn publish_heartbeat(
        &self,
        region: &str,
        msg: &HeartbeatMessage,
    ) -> anyhow::Result<()> {
        let subject = format!("edgecloud.heartbeats.{}", region);
        let payload = serde_json::to_vec(msg)?;
        self.client.publish(subject, payload.into()).await?;
        Ok(())
    }
}
