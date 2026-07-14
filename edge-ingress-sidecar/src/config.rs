//! Sidecar configuration. Filled in by PR B (issue #665).

pub struct Config {
    pub nats_url: String,
    pub replica_id: String,
    pub caddy_admin_url: String,
    /// Local HTTP listener for the Prometheus `/metrics` endpoint.
    /// Operators grep this endpoint to confirm a sidecar pod is healthy.
    metrics_listen: String,
}

impl Config {
    /// Skeleton — reads nothing yet, returns a hardcoded default. PR B
    /// will replace this with the full env-var parse mirroring
    /// `edge-ingress/src/config.rs:375-432`.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            nats_url: String::from("nats://127.0.0.1:4222"),
            replica_id: String::from("ingress-skeleton"),
            caddy_admin_url: String::from("http://127.0.0.1:2019"),
            metrics_listen: String::from("0.0.0.0:9091"),
        })
    }

    pub fn metrics_listen(&self) -> &str {
        &self.metrics_listen
    }
}
