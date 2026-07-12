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
//! **`WebhookDelivery` (issue #565 follow-up):** mirrors the Go
//! `domain.WebhookDelivery` struct field-for-field
//! (`edge-control-plane/internal/domain/webhook.go:31-44`).
//! `request_body` and `response_body` are `json:"-"` on the server
//! side (the wire shape is intentionally redacted — the body may
//! contain tenant secrets in headers) and so are absent from this
//! DTO. The CLI only ever reads these rows via the `deliveries`
//! subcommand, so the field set is intentionally read-only.
//!
//! The `deliveries` route is `GET /api/v1/webhooks/{id}/deliveries`,
//! proposed in issue #659. Until that lands on the control plane,
//! wiremock tests cover the wire shape; a real-server smoke is
//! blocked on the CP-side PR.
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

/// One row of the `webhook_deliveries` table as seen by the tenant
/// via `GET /api/v1/webhooks/{id}/deliveries` (issue #565
/// follow-up, server side: issue #659). Mirrors the Go
/// `domain.WebhookDelivery` struct field-for-field
/// (`edge-control-plane/internal/domain/webhook.go:31-44`) minus
/// `RequestBody` and `ResponseBody` (both `json:"-"` server-side).
///
/// Field semantics:
/// - `id` is the delivery row's primary key (monotonically
///   increasing). The server's cursor is opaque — clients pass it
///   back verbatim — so the CLI never inspects the int value.
/// - `webhook_id` is the subscription this delivery belongs to.
/// - `event_type` is the event that triggered the delivery
///   (`deploy`, `activate`, `rollback`, `auto_rollback`).
/// - `status` is server-defined (`pending`, `delivered`, `failed`).
///   `#[serde(default)]` is defensive: a future server that adds a
///   new status decodes as empty string rather than failing the
///   whole row.
/// - `status_code` is the HTTP response code from the receiver, or
///   `null` if the attempt failed before a response (DNS error,
///   timeout, TLS handshake). `#[serde(default)]` is required
///   because the Go side is `*int` with `omitempty` — absent on
///   pre-response failures.
/// - `error_msg` is server-set on failure (e.g. "connection
///   refused"), empty string on success. `#[serde(default)]` for
///   the same `omitempty` reason.
/// - `attempt` / `max_attempts` distinguish the first try from a
///   retry (server-side: 3-attempt policy per issue body).
/// - `created_at` / `completed_at` are RFC3339 strings (the Go
///   side formats via `time.Time` JSON). `completed_at` may be
///   `null` for in-flight deliveries — `#[serde(default)]` keeps
///   the deserializer happy with the `omitempty` Go pattern.
#[derive(Debug, Deserialize, Serialize)]
pub struct WebhookDelivery {
    pub id: i64,
    pub webhook_id: String,
    pub event_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub status_code: Option<i32>,
    #[serde(default)]
    pub error_msg: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub created_at: String,
    #[serde(default)]
    pub completed_at: Option<String>,
}

/// Wire shape for `GET /api/v1/webhooks/{id}/deliveries` (issue
/// #659). The handler wraps the array in an object so future
/// fields (totals, filters) can be added without breaking the
/// CLI, and adds an opaque `next_cursor` token for pagination.
///
/// `next_cursor` semantics (per issue #659):
/// - `None` (decodes as `null` on the wire, `Option<String>` here)
///   means "no more pages" — the CLI stops paging.
/// - `Some("...")` means pass it back as `?cursor=...` to fetch
///   the next page. The CLI never inspects the value; it's opaque
///   from the client's perspective.
#[derive(Debug, Deserialize)]
pub struct WebhookDeliveryListResponse {
    pub deliveries: Vec<WebhookDelivery>,
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

    /// Fetch one page of delivery attempts for a webhook
    /// subscription (issue #565 follow-up). Server-side route:
    /// `GET /api/v1/webhooks/{id}/deliveries` (issue #659).
    ///
    /// `limit` is sent as `?limit=<N>`. Server clamps to the
    /// `[1, 200]` range per issue #659; the CLI doesn't enforce
    /// here (server is source of truth). `cursor` is the opaque
    /// `next_cursor` returned by the previous page (or `None` on
    /// the first call). Returns both the page rows AND the
    /// `next_cursor` — `None` means "no more pages".
    ///
    /// The 404 path (unknown `id`, or `id` belonging to a
    /// different tenant) flows through `check_response` and is
    /// surfaced as `ApiError::Rejected { status: 404, .. }`.
    pub fn deliveries(
        &self,
        id: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<WebhookDeliveryListResponse> {
        let mut endpoint = format!(
            "{}/api/v1/webhooks/{}/deliveries?limit={}",
            self.client.base_url(),
            id,
            limit
        );
        if let Some(c) = cursor {
            // Opaque token: percent-encode defensively in case the
            // server ever changes the cursor encoding to something
            // that needs URL-escaping.
            endpoint.push_str("&cursor=");
            endpoint.push_str(&url_encode(c));
        }
        let resp = self
            .client
            .http()
            .get(&endpoint)
            .header("Authorization", self.client.auth_header())
            .send()
            .context("GET /api/v1/webhooks/{id}/deliveries")?;
        let resp = check_response(resp).context("list deliveries request failed")?;
        resp.json()
            .context("decoding deliveries response (missing 'deliveries' / 'next_cursor' field?)")
    }
}

/// Minimal percent-encoder for the opaque `next_cursor` token.
/// The server's current cursor (per issue #659) is base64
/// (`A-Z`, `a-z`, `0-9`, `+`, `/`, `=`) so a literal append would
/// "work today" — but the CLI treats the cursor as opaque on
/// purpose, so we encode anything outside `[A-Za-z0-9._~-]` to
/// stay forward-compatible with a server change. Uses a small
/// pre-allocated `String` instead of pulling a new crate dep for
/// one call site.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}
