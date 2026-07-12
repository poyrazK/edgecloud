//! Tenant webhook subscriptions (issue #565).
//!
//! Four routes on the control plane back this client. The handler
//! contract is documented in
//! `edge-control-plane/internal/handler/webhook.go`; the wire shape
//! mirrors the Go `domain.Webhook` struct field-for-field so a serde
//! `Deserialize` on this end cannot drift from the JSON the server
//! emits.
//!
//! `secret` is `json:"-"` on the Go side (see
//! `edge-control-plane/internal/domain/webhook.go:14`) and so is
//! intentionally absent from the `Webhook` struct here — the server
//! never echoes it back, so there is no field to deserialize. The
//! `WebhookDelivery` struct mirrors the `webhook_deliveries` table
//! shape for forward compatibility; the `webhooks deliveries`
//! subcommand that consumes it is gated on a follow-up CP route
//! (filed in the same PR chain).
//!
//! `WebhookClient` borrows the parent `ApiClient` so the API key
//! and base URL are shared across all subcommands without cloning
//! the underlying HTTP client (which is already internally
//! `Arc`-shared by reqwest). The methods are the only path that
//! hits `/api/v1/webhooks*`; tests stub wiremock at those paths.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::client::check_response;

/// One row of the `webhooks` table as seen by the tenant. Mirrors
/// the Go `domain.Webhook` struct field-for-field minus `Secret`
/// (which the server omits via `json:"-"`).
///
/// `events` arrives as a JSON array on the wire; the CLI sends a
/// `Vec<String>` on create/update and decodes the same shape on
/// read. `enabled` is server-driven on create (`true`) but
/// tenant-mutable on update; the `Update` method accepts an
/// `Option<bool>` so callers can distinguish "leave alone" from
/// "set false".
#[derive(Debug, Deserialize, Serialize)]
pub struct Webhook {
    pub id: String,
    pub tenant_id: String,
    pub url: String,
    pub events: Vec<String>,
    pub description: String,
    pub enabled: bool,
    pub created_at: String,
}

/// Wire shape for `GET /api/v1/webhooks`. The handler wraps the
/// array in an object so the OpenAPI spec can document a named
/// property — `WebhookListResponse` mirrors that exactly so future
/// fields (pagination, totals) can be added without breaking the
/// CLI. Without this wrapper, decoding the response as
/// `Vec<Webhook>` would silently fail with "missing field
/// `webhooks`" on every call.
#[derive(Debug, Deserialize)]
struct WebhookListResponse {
    webhooks: Vec<Webhook>,
}

/// One row of the `webhook_deliveries` table. Mirrors the Go
/// `domain.WebhookDelivery` struct field-for-field; the
/// `request_body` and `response_body` columns are server-redacted
/// (both `json:"-"`) so they are absent here too. Not yet
/// consumed by any HTTP method — included so the follow-up
/// `webhooks deliveries` subcommand is a 30-line addition, not a
/// full module introduction.
#[derive(Debug, Deserialize)]
pub struct WebhookDelivery {
    pub id: i64,
    pub webhook_id: String,
    pub event_type: String,
    pub status: String,
    #[serde(default)]
    pub status_code: Option<i32>,
    #[serde(default)]
    pub error_msg: Option<String>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub created_at: String,
    #[serde(default)]
    pub completed_at: Option<String>,
}

/// Borrowed accessor for the webhook endpoints. Constructed via
/// `ApiClient::webhooks()`.
pub struct WebhookClient<'a> {
    pub(crate) client: &'a super::client::ApiClient,
}

impl<'a> WebhookClient<'a> {
    /// Register a new webhook subscription. The `secret` is sent in
    /// the request body but **never** returned in the response —
    /// the tenant must store it on their end before the CLI
    /// exits. Returns the server-minted row (without the secret).
    pub fn add(
        &self,
        url: &str,
        events: &[String],
        description: &str,
        secret: &str,
    ) -> Result<Webhook> {
        let endpoint = format!("{}/api/v1/webhooks", self.client.base_url());
        let resp = self
            .client
            .http()
            .post(&endpoint)
            .header("Authorization", self.client.auth_header())
            .json(&json!({
                "url": url,
                "secret": secret,
                "events": events,
                "description": description,
            }))
            .send()
            .context("POST /api/v1/webhooks")?;
        let resp = check_response(resp).context("add webhook request failed")?;
        resp.json().context("decoding add-webhook response")
    }

    /// List all webhook subscriptions for the current tenant.
    pub fn list(&self) -> Result<Vec<Webhook>> {
        let endpoint = format!("{}/api/v1/webhooks", self.client.base_url());
        let resp = self
            .client
            .http()
            .get(&endpoint)
            .header("Authorization", self.client.auth_header())
            .send()
            .context("GET /api/v1/webhooks")?;
        let resp = check_response(resp).context("list webhooks request failed")?;
        let parsed: WebhookListResponse = resp
            .json()
            .context("decoding list-webhooks response (missing 'webhooks' field?)")?;
        Ok(parsed.webhooks)
    }

    /// Update an existing webhook by id. `Option` fields use
    /// `None` to mean "leave alone" — the server treats absent
    /// fields in the JSON body as no-ops (see
    /// `internal/handler/webhook.go:85-115` for the pointer
    /// handling). Pass `events = Some(vec)` to replace the event
    /// list; pass `None` to keep it. `Some(vec![])` would clear
    /// it server-side — rejected by the server's
    /// `validateWebhookRequest` so it's a 400 the CLI surfaces
    /// directly.
    pub fn update(
        &self,
        id: &str,
        url: Option<&str>,
        events: Option<&[String]>,
        description: Option<&str>,
        enabled: Option<bool>,
        secret: Option<&str>,
    ) -> Result<Webhook> {
        let endpoint = format!("{}/api/v1/webhooks/{}", self.client.base_url(), id);
        let mut body = serde_json::Map::new();
        if let Some(u) = url {
            body.insert("url".into(), json!(u));
        }
        if let Some(e) = events {
            body.insert("events".into(), json!(e));
        }
        if let Some(d) = description {
            body.insert("description".into(), json!(d));
        }
        if let Some(en) = enabled {
            body.insert("enabled".into(), json!(en));
        }
        if let Some(s) = secret {
            body.insert("secret".into(), json!(s));
        }
        let resp = self
            .client
            .http()
            .put(&endpoint)
            .header("Authorization", self.client.auth_header())
            .json(&body)
            .send()
            .context("PUT /api/v1/webhooks/{id}")?;
        let resp = check_response(resp).context("update webhook request failed")?;
        resp.json().context("decoding update-webhook response")
    }

    /// Delete a webhook by id. The server returns 204 on success
    /// and 404 if the row doesn't exist (or belongs to a
    /// different tenant). The caller doesn't need to inspect the
    /// status — `check_response` returns an `ApiError::Rejected`
    /// on 404 that the CLI surfaces.
    pub fn remove(&self, id: &str) -> Result<()> {
        let endpoint = format!("{}/api/v1/webhooks/{}", self.client.base_url(), id);
        let resp = self
            .client
            .http()
            .delete(&endpoint)
            .header("Authorization", self.client.auth_header())
            .send()
            .context("DELETE /api/v1/webhooks/{id}")?;
        check_response(resp).context("remove webhook request failed")?;
        Ok(())
    }
}
