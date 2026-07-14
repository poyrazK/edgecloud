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
//! never echoes it back, so there is no field to deserialize.
//!
//! **Note on `WebhookDelivery`:** the `webhook_deliveries` table is
//! readable via `GET /api/v1/webhooks/{id}/deliveries` (issue #659,
//! closed by the same PR that introduces the DTOs in this file).
//! `request_body` / `response_body` are deliberately omitted from the
//! DTO — the Go side tags them `json:"-"` (issue #565 follow-up) so
//! the wire never carries them, and including them here as dead
//! fields would let a future refactor drift the wire shape without
//! any test catching it.
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
/// "set false". `#[serde(default)]` is set defensively so a future
/// server refactor that omits `enabled` (e.g. via `omitempty`)
/// decodes as `false` (fail-safe: a webhook you can't tell is
/// enabled is treated as not delivering) instead of failing with
/// "missing field `enabled`".
#[derive(Debug, Deserialize, Serialize)]
pub struct Webhook {
    pub id: String,
    pub tenant_id: String,
    pub url: String,
    pub events: Vec<String>,
    pub description: String,
    #[serde(default)]
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

/// One row of the `webhook_deliveries` table as seen by the tenant.
///
/// Mirrors `domain.WebhookDelivery` in
/// `edge-control-plane/internal/domain/webhook.go` field-for-field
/// minus `RequestBody` / `ResponseBody` (which the server omits via
/// `json:"-"` — the CLI never sees them, and a future server refactor
/// that leaks them would be visible only at this serde boundary).
///
/// `status_code` is `Option<i32>` because the Go column is nullable
/// (an in-flight delivery hasn't received a status code yet). Pinning
/// the option-ness here is the single source of truth for the wire
/// shape; a future change to non-nullable status_code must change
/// both sides.
#[derive(Debug, Deserialize, Serialize)]
pub struct WebhookDelivery {
    pub id: i64,
    pub webhook_id: String,
    pub event_type: String,
    pub status: String,
    pub status_code: Option<i32>,
    pub error_msg: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub created_at: String,
    pub completed_at: Option<String>,
}

/// Wire shape for `GET /api/v1/webhooks/{webhookID}/deliveries`
/// (issue #659). The handler returns a JSON object so the OpenAPI
/// spec can document named properties — the `deliveries` array, the
/// effective `limit`, and an opaque `next_cursor`.
///
/// **Cursor codec:** the server encodes `next_cursor` as URL-safe
/// base64 (no padding) of `{ "v": 1, "ts": "<RFC3339Nano UTC>",
/// "id": <int64> }`. The CLI treats the value as opaque — a future
/// server version bump that changes the payload shape is
/// surfaced as a typed error and the CLI prints the
/// `ErrUnsupportedCursorVersion` message verbatim, no decoding on
/// the client side. The shape is owned by the server's
/// `webhook_delivery_cursor.go`; we only echo it back via `--cursor`.
#[derive(Debug, Deserialize)]
pub struct WebhookDeliveriesResponse {
    pub deliveries: Vec<WebhookDelivery>,
    pub limit: u32,
    pub next_cursor: Option<String>,
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

    /// List delivery attempts for a webhook. Cursor pagination only
    /// (issue #659); `--offset` is not exposed because the server
    /// never advertises it. `limit = None` lets the server pick the
    /// default (50); `cursor = None` requests the first page. The
    /// server clamps `limit` to `[1, 200]`.
    pub fn deliveries(
        &self,
        id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<WebhookDeliveriesResponse> {
        let mut endpoint = format!(
            "{}/api/v1/webhooks/{}/deliveries",
            self.client.base_url(),
            id
        );
        let mut query_parts: Vec<String> = Vec::new();
        if let Some(l) = limit {
            query_parts.push(format!("limit={l}"));
        }
        if let Some(c) = cursor {
            // The cursor is an opaque token; it MAY contain URL-unsafe
            // base64 chars (`-`, `_`) but never `&` / `=` — and the
            // server's codec uses RawURLEncoding (no padding) so we
            // don't need to percent-encode anything. We still append
            // it verbatim to keep the wire shape trivially roundtrip-able
            // when the user copy-pastes the next-page hint.
            query_parts.push(format!("cursor={c}"));
        }
        if !query_parts.is_empty() {
            endpoint.push('?');
            endpoint.push_str(&query_parts.join("&"));
        }
        let resp = self
            .client
            .http()
            .get(&endpoint)
            .header("Authorization", self.client.auth_header())
            .send()
            .context("GET /api/v1/webhooks/{id}/deliveries")?;
        let resp = check_response(resp).context("list webhook deliveries request failed")?;
        resp.json().context(
            "decoding list-deliveries response (missing 'deliveries'/'limit'/'next_cursor'?)",
        )
    }
}
