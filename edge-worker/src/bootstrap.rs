//! Bootstrap handshake client for the JWT secret provisioning flow
//! (issue #104).
//!
//! When the worker starts without `WORKER_JWT_SECRET` but with
//! `WORKER_BOOTSTRAP_SECRET`, it performs a two-phase handshake:
//!
//! 1. POST `/api/internal/bootstrap` — sends an HMAC-SHA256-signed
//!    payload to prove knowledge of the shared bootstrap secret.
//!    Receives a short-lived (5 min) bootstrap JWT.
//!
//! 2. GET `/api/internal/worker-secret` — presents the bootstrap JWT
//!    as a Bearer token. Receives the real `JWT_SECRET`.
//!
//! The fetched secret is cached in memory and used to initialize the
//! `WorkerJwtSigner` for all subsequent outbound calls.

use anyhow::Context;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

/// HMAC-SHA256 type alias.
type HmacSha256 = Hmac<Sha256>;

/// BootstrapClient handles the two-phase bootstrap handshake with the
/// control plane.
pub struct BootstrapClient {
    cp_url: String,
    bootstrap_secret: Vec<u8>,
    client: reqwest::Client,
    worker_id: String,
    region: String,
    tenant_id: String,
}

impl BootstrapClient {
    /// Create a new BootstrapClient.
    pub fn new(
        cp_url: String,
        bootstrap_secret: Vec<u8>,
        worker_id: String,
        region: String,
        tenant_id: String,
    ) -> Self {
        Self {
            cp_url,
            bootstrap_secret,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("reqwest Client::new should not fail"),
            worker_id,
            region,
            tenant_id,
        }
    }

    /// Run the full bootstrap handshake.
    ///
    /// Returns the JWT secret on success. The caller should use this
    /// secret to initialize (or hot-reload) the `WorkerJwtSigner`.
    ///
    /// On failure returns an error with details about which phase failed.
    pub async fn run(&self) -> anyhow::Result<String> {
        // Phase 1: POST /api/internal/bootstrap
        let bootstrap_jwt = self.bootstrap().await?;

        // Phase 2: GET /api/internal/worker-secret
        let jwt_secret = self.fetch_secret(&bootstrap_jwt).await?;

        tracing::info!(
            worker_id = %self.worker_id,
            "bootstrap handshake completed successfully"
        );

        Ok(jwt_secret)
    }

    /// Phase 1: Authenticate with the bootstrap secret.
    ///
    /// Sends a signed payload to `POST /api/internal/bootstrap` and
    /// receives a short-lived bootstrap JWT.
    async fn bootstrap(&self) -> anyhow::Result<String> {
        let timestamp = Self::rfc3339_now();
        let nonce = Uuid::new_v4().to_string();

        // Build the payload to sign: worker_id:region:tenant_id:timestamp:nonce
        let payload = format!(
            "{}:{}:{}:{}:{}",
            self.worker_id, self.region, self.tenant_id, timestamp, nonce
        );

        // Compute HMAC-SHA256 signature.
        let mut mac = HmacSha256::new_from_slice(&self.bootstrap_secret)
            .context("failed to create HMAC-SHA256 from bootstrap secret")?;
        mac.update(payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let body = serde_json::json!({
            "worker_id": self.worker_id,
            "region": self.region,
            "tenant_id": self.tenant_id,
            "timestamp": timestamp,
            "nonce": nonce,
            "signature": signature,
        });

        let url = format!("{}/api/internal/bootstrap", self.cp_url);
        tracing::info!(%url, "bootstrap phase 1: POST /api/internal/bootstrap");

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("bootstrap phase 1 POST to {url} failed"))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("bootstrap phase 1 failed (HTTP {status}): {body_text}");
        }

        let data: serde_json::Value = resp
            .json()
            .await
            .context("bootstrap phase 1: failed to parse response JSON")?;

        let token = data["token"]
            .as_str()
            .context("bootstrap phase 1: response missing 'token' field")?
            .to_string();

        tracing::info!(
            worker_id = %self.worker_id,
            "bootstrap phase 1: received bootstrap JWT"
        );

        Ok(token)
    }

    /// Phase 2: Exchange the bootstrap JWT for the real JWT secret.
    ///
    /// Presents the short-lived bootstrap JWT as a Bearer token and
    /// receives the long-lived JWT secret.
    async fn fetch_secret(&self, bootstrap_jwt: &str) -> anyhow::Result<String> {
        let url = format!("{}/api/internal/worker-secret", self.cp_url);
        tracing::info!(%url, "bootstrap phase 2: GET /api/internal/worker-secret");

        let resp = self
            .client
            .get(&url)
            .bearer_auth(bootstrap_jwt)
            .send()
            .await
            .with_context(|| format!("bootstrap phase 2 GET to {url} failed"))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("bootstrap phase 2 failed (HTTP {status}): {body_text}");
        }

        let data: serde_json::Value = resp
            .json()
            .await
            .context("bootstrap phase 2: failed to parse response JSON")?;

        let secret = data["secret"]
            .as_str()
            .context("bootstrap phase 2: response missing 'secret' field")?
            .to_string();

        if secret.is_empty() {
            anyhow::bail!("bootstrap phase 2: received empty secret");
        }

        tracing::info!(
            worker_id = %self.worker_id,
            secret_len = secret.len(),
            "bootstrap phase 2: received JWT secret"
        );

        Ok(secret)
    }

    /// Return the current time as an RFC 3339 string.
    fn rfc3339_now() -> String {
        // Use chrono if available, otherwise use manual formatting.
        // The worker already has chrono in its dependency tree.
        chrono::Utc::now().to_rfc3339()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn bootstrap_full_handshake_succeeds() {
        let server = MockServer::start().await;

        // Phase 1 mock
        Mock::given(method("POST"))
            .and(path("/api/internal/bootstrap"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token": "test-bootstrap-jwt"
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Phase 2 mock
        Mock::given(method("GET"))
            .and(path("/api/internal/worker-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "secret": "the-real-jwt-secret-that-is-at-least-32-bytes-long!"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = BootstrapClient::new(
            server.uri(),
            b"test-bootstrap-secret".to_vec(),
            "w_test_abc".to_string(),
            "fra".to_string(),
            "t_test".to_string(),
        );

        let secret = client
            .run()
            .await
            .expect("bootstrap handshake should succeed");
        assert_eq!(
            secret,
            "the-real-jwt-secret-that-is-at-least-32-bytes-long!"
        );
    }

    #[tokio::test]
    async fn bootstrap_phase1_fails_on_401() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/internal/bootstrap"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid signature"))
            .expect(1)
            .mount(&server)
            .await;

        let client = BootstrapClient::new(
            server.uri(),
            b"wrong-secret".to_vec(),
            "w_test_abc".to_string(),
            "fra".to_string(),
            "t_test".to_string(),
        );

        let err = client
            .run()
            .await
            .expect_err("bootstrap should fail on 401");
        assert!(
            err.to_string().contains("401"),
            "error should mention 401, got: {err}"
        );
    }

    #[tokio::test]
    async fn bootstrap_phase1_missing_token_field() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/internal/bootstrap"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "not_token": "some-value"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = BootstrapClient::new(
            server.uri(),
            b"test-secret".to_vec(),
            "w_test_abc".to_string(),
            "fra".to_string(),
            "t_test".to_string(),
        );

        let err = client
            .run()
            .await
            .expect_err("bootstrap should fail on missing token");
        assert!(
            err.to_string().contains("token"),
            "error should mention token, got: {err}"
        );
    }

    #[tokio::test]
    async fn bootstrap_phase2_fails_on_401() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/internal/bootstrap"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token": "test-bootstrap-jwt"
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/internal/worker-secret"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid bootstrap token"))
            .expect(1)
            .mount(&server)
            .await;

        let client = BootstrapClient::new(
            server.uri(),
            b"test-secret".to_vec(),
            "w_test_abc".to_string(),
            "fra".to_string(),
            "t_test".to_string(),
        );

        let err = client
            .run()
            .await
            .expect_err("bootstrap phase 2 should fail on 401");
        assert!(
            err.to_string().contains("401"),
            "error should mention 401, got: {err}"
        );
    }
}
