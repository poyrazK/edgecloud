//! HTTP client for the edgeCloud control plane API.

use anyhow::Result;
use reqwest::blocking::{Client, Response};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::io::Read;

use crate::config::ApiKey;

/// Distinguishes a credential rejection (4xx — the server explicitly
/// said "this key is bad") from a transient error (5xx, network
/// failure, timeout). Used by flows that need to react differently
/// to each, e.g. `edge auth login` exits 1 on `Rejected` but tolerates
/// `Transient` so users can work offline.
#[derive(Debug)]
pub enum ApiError {
    /// 4xx — the server rejected the request as invalid (typically
    /// 401 for an invalid/missing API key, 403 for insufficient role,
    /// 400 for malformed input).
    Rejected { status: StatusCode, body: String },
    /// 5xx, timeout, DNS failure, or any other transient problem.
    Transient { source: anyhow::Error },
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Rejected { status, body } => {
                write!(f, "rejected by server: {status} {body}")
            }
            ApiError::Transient { source } => write!(f, "{source}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<anyhow::Error> for ApiError {
    fn from(source: anyhow::Error) -> Self {
        ApiError::Transient { source }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(source: reqwest::Error) -> Self {
        ApiError::Transient {
            source: anyhow::anyhow!("{source}"),
        }
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(source: serde_json::Error) -> Self {
        ApiError::Transient {
            source: anyhow::anyhow!("invalid response body: {source}"),
        }
    }
}

/// Inspect a response and split 2xx (return `Ok`) from 4xx (`Rejected`)
/// and the rest (`Transient`, after reading whatever body is
/// available). The body is read once on the non-2xx path.
///
/// `pub(crate)` so sibling accessor structs (`DomainClient`,
/// `Tenants`, `Keys`, etc.) can reuse the same 2xx/4xx/5xx split
/// instead of hand-rolling `if !status.is_success()` per method.
///
/// The error body is read with a [`std::io::Read::take`] cap at
/// [`MAX_ERR_BODY`] bytes — the underlying HTTP body itself is
/// bounded, not just the post-allocation string — so a misbehaving
/// control plane returning a multi-GB 4xx/5xx body can't OOM the
/// CLI. The accumulated bytes then flow through [`truncate_body`]
/// for the UTF-8 char-boundary walk before being plumbed into
/// either the `Rejected` arm or the `Transient` anyhow! arm.
/// Issue #109 F9 + follow-up.
pub(crate) fn check_response(resp: Response) -> Result<Response, ApiError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    // Read at most MAX_ERR_BODY + 1 bytes from the response stream
    // — one more than the cap so `truncate_body` can tell "body fit"
    // (buf.len() <= MAX_ERR_BODY) apart from "we hit the cap"
    // (buf.len() == MAX_ERR_BODY + 1). `Take::read_to_end` is a hard
    // cap: a multi-GB body is discarded by reqwest after the first
    // MAX_ERR_BODY + 1 bytes. Discarded Result matches the prior
    // `unwrap_or_default()` semantics for a severed connection
    // mid-read.
    use std::io::Read;
    let mut buf: Vec<u8> = Vec::new();
    let _ = std::io::Read::read_to_end(&mut resp.take((MAX_ERR_BODY + 1) as u64), &mut buf);
    let body = truncate_body(&String::from_utf8_lossy(&buf));
    if status.is_client_error() {
        Err(ApiError::Rejected { status, body })
    } else {
        Err(ApiError::Transient {
            source: anyhow::anyhow!("server returned {status}: {body}"),
        })
    }
}

/// Maximum number of bytes of a server response body that the CLI
/// will buffer into memory before truncating. Caps the worst-case
/// memory footprint at `MAX_ERR_BODY` per response for a CLI that
/// serializes requests on a single client. Sized for real error
/// bodies (typically <1 KiB) with headroom for stack traces on 5xx.
/// Issue #109 F9. Kept in sync with the same constant in
/// `edge-migrate/edge-migrate-bin/src/main.rs`.
const MAX_ERR_BODY: usize = 4 * 1024;

/// Maximum bytes of a SUCCESS response body the CLI will buffer
/// into memory before failing the parse. Distinct from
/// [`MAX_ERR_BODY`] because success bodies are bulk endpoints —
/// `Logs::list` returns up to 1000 records (`logs::list` doc)
/// where each `LogEntry` averages ~300 bytes and can be multi-KiB
/// with verbose messages. 4 KiB is correct for diagnostics but
/// wrong for data: 1000 × 5 KiB ≈ 5 MiB is a realistic worst
/// case. 8 MiB gives ~50% headroom while bounding the process at
/// 8 MiB per request (the CLI is single-threaded, so the bound
/// is global).
///
/// Allocated up-front as a `Vec<u8>` capacity hint on every
/// success-path call. Fine for a one-request-at-a-time CLI; a
/// future concurrent refactor should revisit. Issue #109 follow-up.
const MAX_SUCCESS_BODY: u64 = 8 * 1024 * 1024;

/// Cap a server response body at [`MAX_ERR_BODY`] bytes. Real
/// server error JSON is 100-500 bytes; RFC 7807 problem-detail
/// payloads are typically <1 KiB. A misbehaving control plane
/// returning a multi-GB 4xx/5xx body would otherwise OOM the CLI
/// before the body is printed. Walks down to a UTF-8 char boundary
/// because `String::truncate` panics on a mid-multibyte-char
/// index, and a panic in this path would kill a TTY mid-`edge
/// deploy`. Issue #109 F9.
fn truncate_body(s: &str) -> String {
    if s.len() <= MAX_ERR_BODY {
        return s.to_string();
    }
    let mut end = MAX_ERR_BODY;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + 16);
    out.push_str(&s[..end]);
    out.push_str("... [truncated]");
    out
}

/// HTTP client for all control plane API calls.
pub struct ApiClient {
    http: Client,
    base_url: String,
    api_key: ApiKey,
}

#[derive(Debug, Deserialize)]
pub struct DeployResponse {
    pub id: String,
    pub url: String,
    /// Regions the deployment is replicated to. Returned by the
    /// control plane so the CLI can persist them into state.json
    /// (and so the tenant knows what their deploy targeted when the
    /// server applies a default). May be empty if the server fills
    /// it in lazily; treated as "use default" downstream in that case.
    #[serde(default)]
    pub regions: Vec<String>,
    /// Desired replica count (issue #316). 0 means no threshold.
    #[serde(default)]
    pub desired_replicas: usize,
    /// Signed SLSA L1 envelope (issue #307 PR2). The control plane
    /// returns the full DSSE wrapper as JSON. We just persist it
    /// verbatim to `.edge/attestation.json` — a downstream verifier
    /// (or operator audit script) reads the file later. May be
    /// `None` when the server is built without the PR2 envelope
    /// constructor (pre-PR2 CP versions). Optional so a CLI built
    /// with PR2 still talks to a CP without it — the deploy
    /// succeeds, no attestation is recorded.
    #[serde(default)]
    pub build_attestation: Option<serde_json::Value>,
    /// Preview-id stamped by the control plane (issue #308). Empty
    /// for non-preview deploys; the CLI echoes it into `.edge/state.json`
    /// so `edge status` can show which preview a deployment belongs to.
    #[serde(default)]
    pub preview_id: String,
    /// GitHub PR number forwarded by the composite action. Zero for
    /// non-preview deploys and for laptop `edge deploy --preview` runs
    /// (no PR linkage). Persisted alongside `preview_id` for parity.
    #[serde(default)]
    pub preview_pr_number: u32,
    /// RFC3339 expiry timestamp for the preview. Empty for non-preview
    /// deploys. The state file persists this so a `edge status` can
    /// surface "expires in 5d 12h" without re-querying the server.
    #[serde(default)]
    pub preview_expires_at: String,
}

/// Preview options forwarded to the control plane via the three
/// `?preview-*` query params (issue #308). Construct via
/// `PreviewOpts::from_cli` so the CLI-side defaults and validation
/// stay in one place.
#[derive(Debug, Clone)]
pub struct PreviewOpts {
    pub preview_id: String,
    pub preview_pr_number: u32,
    /// Go-style duration string like `"24h"` or `"168h"`. Server
    /// resolves to an absolute timestamp; the CLI never interprets it
    /// locally. Empty means "use server default" (currently 168h).
    pub preview_ttl: String,
}

impl PreviewOpts {
    /// Build a `PreviewOpts` from CLI flags. `preview_id` is required
    /// to be a 8..16-char lowercase hex string (validated server-side;
    /// the CLI just passes it through). Empty `preview_pr_number` and
    /// `preview_ttl` map to server defaults — the CLI never errors on
    /// them; the server-side handler validates and 400s if anything's
    /// malformed.
    pub fn new(
        preview_id: impl Into<String>,
        preview_pr_number: u32,
        preview_ttl: impl Into<String>,
    ) -> Self {
        Self {
            preview_id: preview_id.into(),
            preview_pr_number,
            preview_ttl: preview_ttl.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StatusResponse {
    pub id: String,
    pub status: String,
    pub created_at: String,
    /// Public ingress hostname computed server-side from
    /// `tenant_id` + `app_name`. Always populated by the CP since
    /// commit 1 of the wire-mismatch fix.
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct EnvVar {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct DeploymentSummary {
    pub id: String,
    pub status: String,
    pub created_at: String,
    /// Public ingress hostname. Always populated.
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct TenantCreated {
    pub tenant_id: String,
    pub api_key: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateAPIKeyResponse {
    pub id: String,
    pub name: String,
    pub role: String,
    /// Raw key shown only once. The caller is responsible for
    /// persisting it — `edge auth keys create` deliberately does NOT
    /// overwrite the on-disk credential so the key that was used to
    /// authenticate this call still works.
    pub token: String,
}

/// One API key as returned by `GET /api/v1/keys`. Mirrors the Go
/// `SafeAPIKeyResponse` (handler/api_key.go → domain/api_key.go):
/// id / name / role / created_at are always present; `last_used`
/// and `expires_at` are populated by the server when non-NULL in
/// the DB and serialized via `omitempty` so they're absent for
/// never-used / never-expiring keys. `#[serde(default)]` keeps
/// the deserialize path tolerant of both old CPs (no fields) and
/// new CPs (both fields absent for an unused key).
#[derive(Debug, Deserialize, Serialize)]
pub struct APIKeySummary {
    pub id: String,
    pub name: String,
    pub role: String,
    pub created_at: String,
    #[serde(default)]
    pub last_used: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WhoamiResponse {
    pub tenant_id: String,
    pub tenant_name: String,
    pub plan: String,
    pub api_key_id: String,
    pub api_key_name: String,
    pub role: String,
    pub created_at: String,
}

/// Response from POST `/api/apps/{appName}/rollback`. The
/// `deployment_id` field is the deployment that is now active after
/// the rollback — i.e., the prior `last_good_deployment_id`.
#[derive(Debug, Deserialize)]
pub struct RollbackResponse {
    pub deployment_id: String,
}

/// One tenant log record as returned by GET
/// `/api/v1/apps/{appName}/logs` (issue #77). Mirrors the Go
/// `domain.LogEntry` field-for-field; the only divergence is
/// `labels`, which the CLI decodes into a `serde_json::Value` so
/// `edge logs` can pretty-print arbitrary label shapes without
/// needing a typed schema per record.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogEntry {
    #[serde(default)]
    pub id: i64,
    pub tenant_id: String,
    pub deployment_id: String,
    pub app_name: String,
    pub worker_id: String,
    pub region: String,
    pub level: String,
    pub message: String,
    /// `serde_json::Value` with `#[serde(default)]` so an empty
    /// JSON object on the wire deserializes to `Value::Null`
    /// rather than failing. The CLI uses this in JSON-pipe mode
    /// (`edge logs myapp | jq`) where shape preservation matters
    /// more than typed access.
    #[serde(default)]
    pub labels: serde_json::Value,
    pub ts: String,
}

/// Envelope returned by `GET /api/v1/apps/{appName}/logs`. The
/// `since` field carries the RFC3339 cutoff the server actually
/// applied; the CLI's `--follow` mode reads it once at startup to
/// prime the loop and then advances the cutoff from the newest
/// returned entry's `ts` (with client-side dedup by id to hide the
/// boundary row the server returns on every poll).
#[derive(Debug, Deserialize)]
pub struct LogListResponse {
    pub items: Vec<LogEntry>,
    pub limit: u32,
    #[serde(default)]
    pub since: String,
    #[serde(default)]
    pub next_offset: Option<u32>,
}

/// Worker-reported status of one app, returned by
/// `GET /api/v1/apps/{appName}/status`. Mirrors the Go
/// `domain.AppWorkerStatus` field-for-field.
///
/// `status` is the same string the worker publishes in NATS
/// heartbeats (`running` | `starting` | `stopping` | `crashed` |
/// `hung` | `unknown`). The CLI's `edge logs` uses
/// `status == "crashed"` to decide whether to print the
/// `edge rollback` hint (issue #77 §5).
///
/// `last_heartbeat` is `None` when no worker has reported on the
/// app. The CLI treats a heartbeat older than 5 minutes as stale
/// (the worker default is 30s) and suppresses the hint, because a
/// stale `crashed` is more likely a dead worker than an actually
/// crashed app.
#[derive(Debug, Deserialize)]
pub struct AppWorkerStatus {
    pub app_name: String,
    pub status: String,
    /// RFC3339 timestamp; `None` when no worker has reported.
    pub last_heartbeat: Option<String>,
    pub region: String,
    pub worker_id: String,
    /// Process exit code from the worker's last observation.
    /// `None` when not provided (e.g. running, hung, or unknown).
    pub exit_code: Option<i32>,
}

/// An app as returned by `GET /api/v1/apps` and
/// `GET /api/v1/apps/{appName}`. Mirrors the Go control-plane
/// `domain.App` struct field-for-field. The Go struct has no JSON
/// tags so serde must match the literal PascalCase field names.
///
/// Note: `rename_all = "PascalCase"` would map `id` → `Id` and
/// `tenant_id` → `TenantId`, but the Go struct emits `ID` and
/// `TenantID` — hence the individual renames.
#[derive(Debug, Deserialize)]
pub struct App {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "TenantID")]
    pub tenant_id: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Description", default)]
    pub description: Option<String>,
    #[serde(rename = "CreatedAt")]
    pub created_at: String,
}

/// Wrapper for the paginated list response:
/// `{"apps": [...], "limit": 50, "offset": 0}`
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AppListResponse {
    apps: Vec<App>,
    limit: u32,
    offset: u32,
}

/// Wrapper for the paginated deployments response:
/// `{"items": [...], "total": N, "limit": 20, "offset": 0}`
/// returned by `GET /api/v1/list/{appName}`.
///
/// Field types mirror the server (Go) side: `total` is signed so a
/// future "unknown" sentinel can travel as `-1`, while `limit` and
/// `offset` are unsigned because pagination offsets are always
/// non-negative.
///
/// `#[allow(dead_code)]` covers `total`/`limit`/`offset` — they are
/// accepted but not rendered by `edge deployments` today. The allow
/// is intentional: when a future `edge deployments --page N` lands,
/// the deserializer doesn't need a second round-trip. (see project
/// audit notes; CLI gap #2 deferred from PR #457)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct DeploymentListResponse {
    items: Vec<DeploymentSummary>,
    total: i64,
    limit: u32,
    offset: u32,
}

/// Quota and usage returned by `GET /api/v1/quotas`.
/// Mirrors the Go control-plane `quotaResponse` struct.
#[derive(Debug, Deserialize)]
pub struct QuotaResponse {
    pub tenant_id: String,
    pub max_deployments: i32,
    pub max_apps: i32,
    pub max_workers: i32,
    pub max_memory_mb: i32,
    pub max_outbound_mb: i32,
    pub max_requests_per_month: i32,
    pub used_outbound_bytes: i64,
    pub used_request_count: i64,
    pub quota_period_start: String,
    #[serde(default)]
    pub usage_pct: Option<f64>,
}

/// Egress allowlist returned by `GET /api/v1/egress` and sent by
/// `PUT /api/v1/egress`. Mirrors the Go control-plane
/// `egressResponse` struct.
#[derive(Debug, Deserialize, Serialize)]
pub struct EgressAllowlist {
    pub allowlist: Vec<String>,
}

/// Ingress target for a running app, returned by
/// `GET /api/v1/apps/{appName}/ingress`. The `ready` field indicates
/// whether the app is currently running on a worker. When `false`,
/// only `app_name` is populated (plus `reason` in the raw response).
#[derive(Debug, Deserialize)]
pub struct IngressResponse {
    pub ready: bool,
    pub app_name: String,
    pub tenant_id: Option<String>,
    pub worker_id: Option<String>,
    pub region: Option<String>,
    pub worker_addr: Option<String>,
    pub port: Option<i32>,
}

impl ApiClient {
    /// Create a new API client. Loads the API key from
    /// `EDGE_API_KEY` env var or `~/.config/edgecloud/config.toml`.
    pub fn new(base_url: String) -> Result<Self> {
        let mut client = Self::new_anonymous(base_url)?;
        client.api_key = ApiKey::load()?;
        Ok(client)
    }

    /// Create a new API client that loads the API key ONLY from the
    /// config file (skipping `EDGE_API_KEY`). Used by flows that just
    /// wrote a key to disk and need to validate the on-disk value
    /// without an ambient env var shadowing it.
    pub fn new_from_config_only(base_url: String) -> Result<Self> {
        let mut client = Self::new_anonymous(base_url)?;
        client.api_key = ApiKey::load_without_env()?;
        Ok(client)
    }

    /// Create a client that does not require an API key. The internal
    /// `api_key` field is set to an empty placeholder; callers must not
    /// use [`auth_header`](Self::auth_header) on a client built this way.
    /// Used by [`edge auth signup`](crate::commands::auth::signup) which
    /// has no prior key.
    pub fn new_anonymous(base_url: String) -> Result<Self> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow::anyhow!("reqwest client failed: {}", e))?;
        Ok(Self {
            http,
            base_url,
            api_key: ApiKey(String::new()),
        })
    }

    pub(crate) fn auth_header(&self) -> String {
        format!("Bearer {}", self.api_key.0)
    }

    /// The raw bearer token this client authenticates with. Exposed
    /// for CLI-side UX checks (e.g. `keys revoke`'s post-revoke
    /// warning) that need to compare against a candidate key id
    /// without going through a network round-trip. The value is
    /// the same string the server echoes back as `api_key_id` in
    /// `whoami`, so this accessor is the local equivalent of
    /// `client.auth().whoami().api_key_id`.
    pub(crate) fn bearer(&self) -> &str {
        &self.api_key.0
    }

    /// Returns the base URL this client targets. Useful for surfacing
    /// in error messages.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// GET helper: build a URL, send an authenticated GET, check the
    /// response, decode JSON. Used by every endpoint that just reads a
    /// JSON resource (`status`, `list_env`, `list_deployments`,
    /// `whoami`, `logs::list`).
    ///
    /// `format_url` is a closure that takes the base URL and returns the
    /// full path (with query params when relevant). Extracting this lets
    /// callers that need query params (`logs::list`) keep that logic
    /// local while still hitting the auth + check + decode pipeline.
    ///
    /// Returns `Result<T, ApiError>` so callers that care about the
    /// distinction (e.g. `edge auth login`) can branch on Rejected vs
    /// Transient. Callers that don't can use [`get_json_anyhow`] for a
    /// flat `Result<T>`.
    fn get_json<T, F>(&self, format_url: F) -> Result<T, ApiError>
    where
        T: serde::de::DeserializeOwned,
        F: FnOnce(&str) -> String,
    {
        let url = format_url(&self.base_url);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;
        let resp = check_response(resp)?;
        serde_json::from_reader(resp.take(MAX_SUCCESS_BODY)).map_err(ApiError::from)
    }

    /// Helper for the GET endpoints that surface as `anyhow::Error`
    /// instead of `ApiError`. Flattens `Rejected` into
    /// `anyhow!("{op} failed: {status} {body}")` and `Transient` into
    /// its source. Lets every existing call site keep returning
    /// `Result<T>` without each one re-writing the same match.
    fn get_json_anyhow<T, F>(&self, op: &str, format_url: F) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        F: FnOnce(&str) -> String,
    {
        self.get_json(format_url).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("{op} failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })
    }

    /// Group accessor for tenant-management endpoints (e.g. signup).
    pub fn tenants(&self) -> Tenants<'_> {
        Tenants { client: self }
    }

    /// Group accessor for API key management endpoints (requires the
    /// caller to be authenticated — `new_anonymous` is not enough).
    pub fn keys(&self) -> Keys<'_> {
        Keys { client: self }
    }

    /// Group accessor for auth-related endpoints (e.g. whoami).
    pub fn auth(&self) -> Auth<'_> {
        Auth { client: self }
    }

    /// Group accessor for the logs read endpoint (issue #77).
    /// Returns [`Logs`], which owns the `list` method that calls
    /// `GET /api/v1/apps/{appName}/logs`.
    pub fn logs(&self) -> Logs<'_> {
        Logs { client: self }
    }

    /// GET `/api/v1/apps/{appName}/status` — the worker's last reported
    /// status for an app. Powers the `edge logs` crashed-hint (issue
    /// #77 §5) and is a useful debugging primitive on its own.
    ///
    /// Returns `Result<AppWorkerStatus, ApiError>` so callers (e.g.
    /// `edge logs`) can choose whether to fail loudly or silently
    /// skip on a 4xx/5xx. The hint path uses the second form: a
    /// failed status fetch must NOT prevent the log fetch from
    /// proceeding, because logs are the user's actual goal.
    pub fn get_app_status(&self, app_name: &str) -> Result<AppWorkerStatus, ApiError> {
        self.get_json(|base| format!("{base}/api/v1/apps/{app_name}/status"))
    }

    /// GET `/api/v1/apps/{appName}/status` with anyhow-typed errors.
    /// The same endpoint as [`Self::get_app_status`], but routes through
    /// `get_json_anyhow` so 4xx/5xx surfaces as `runtime status failed:
    /// {status} {body}` — HTTP status and body in the top frame, not
    /// buried in a `Caused by:` chain.
    ///
    /// Used by `edge status runtime` where the user explicitly asked
    /// for the data and a 404/401/500 should be diagnostically useful.
    /// `edge logs` continues to use `get_app_status` because its hint
    /// path silently swallows failures (logs are the primary goal).
    pub fn app_status(&self, app_name: &str) -> Result<AppWorkerStatus> {
        self.get_json_anyhow("runtime status", |base| {
            format!("{base}/api/v1/apps/{app_name}/status")
        })
    }

    pub(crate) fn http(&self) -> &Client {
        &self.http
    }

    /// Upload a deployment artifact.
    ///
    /// `regions` is the list of regions the deployment should be
    /// replicated to. Empty slice means "use the control plane's default
    /// region" (the control plane applies the same default). The values
    /// are passed through the `?regions=` query parameter as a
    /// comma-separated string; the server splits, validates, and dedupes.
    ///
    /// `auto_rollback` is the tenant opt-in for issue #74 — when
    /// true, the server stores `auto_rollback_enabled = true` on both
    /// the deployment and active_deployments rows, which gates the
    /// worker-driven auto-rollback trigger and the heartbeat-driven
    /// stability-window promotion. Defaults to false on the wire (the
    /// server rejects unknown query values with 400 rather than
    /// silently coercing typos to false).
    #[allow(clippy::too_many_arguments)]
    pub fn deploy(
        &self,
        app_name: &str,
        wasm_bytes: &[u8],
        regions: &[String],
        auto_rollback: bool,
        replicas: usize,
        build_metadata: Option<&serde_json::Value>,
        preview_opts: Option<&PreviewOpts>,
    ) -> Result<DeployResponse> {
        let mut url = format!("{}/api/v1/deploy/{}", self.base_url, app_name);
        // Always parse the URL so we can append optional query params
        // (regions, auto-rollback) uniformly. Even when both are
        // absent, this is a cheap operation and avoids branching.
        let mut parsed =
            reqwest::Url::parse(&url).map_err(|e| anyhow::anyhow!("invalid base url: {e}"))?;
        if !regions.is_empty() {
            // Use reqwest::Url to percent-encode the comma list so a
            // region with a stray `+` or non-ASCII char doesn't break
            // the URL. The server splits on `,` so the encoding is
            // applied per the CSV string as a whole.
            parsed
                .query_pairs_mut()
                .append_pair("regions", &regions.join(","));
        }
        if auto_rollback {
            // Only emit the param when true. `?auto-rollback=false` is
            // redundant (the server defaults to false) and would
            // clutter the URL in logs.
            parsed
                .query_pairs_mut()
                .append_pair("auto-rollback", "true");
        }
        if replicas > 0 {
            parsed
                .query_pairs_mut()
                .append_pair("replicas", &replicas.to_string());
        }
        // issue #308: forward preview metadata via the three query
        // params the server's parsePreviewOpts handler reads. Only
        // emit `preview-id` when non-empty — the server treats its
        // presence as the "this is a preview" marker. `pr-number`
        // and `ttl` are emitted only when their values are
        // meaningful (non-zero / non-empty).
        if let Some(opts) = preview_opts {
            if !opts.preview_id.is_empty() {
                parsed
                    .query_pairs_mut()
                    .append_pair("preview-id", &opts.preview_id);
            }
            if opts.preview_pr_number > 0 {
                parsed
                    .query_pairs_mut()
                    .append_pair("preview-pr-number", &opts.preview_pr_number.to_string());
            }
            if !opts.preview_ttl.is_empty() {
                parsed
                    .query_pairs_mut()
                    .append_pair("preview-ttl", &opts.preview_ttl);
            }
        }
        url = parsed.to_string();

        // Issue #307 PR2: switch the deploy wire format from
        // raw octet-stream to multipart/form-data so we can carry
        // the SLSA L1 build_metadata alongside the wasm bytes in
        // a single atomic request. The `build_metadata` part is
        // optional — `None` still works (older CLI versions never
        // produced it), the server treats an absent part the same
        // as a JSON document with every field empty.
        let mut form = reqwest::blocking::multipart::Form::new();
        // `file` part: the wasm bytes with a sensible filename so
        // the server's `mime/multipart` parser sees
        // `filename="<app>.wasm"` (helps when the handler wants to
        // log the name).
        let cursor = std::io::Cursor::new(wasm_bytes.to_vec());
        let file_part = reqwest::blocking::multipart::Part::reader(cursor)
            .file_name(format!("{}.wasm", app_name))
            .mime_str("application/wasm")
            .map_err(|e| anyhow::anyhow!("invalid mime: {e}"))?;
        form = form.part("file", file_part);
        if let Some(meta) = build_metadata {
            // `build_metadata` is JSON — encode it once and ship as
            // a text part so the server-side handler can
            // `multipart.FormValue` it and parse.
            let raw = serde_json::to_string(meta)?;
            form = form.text("build_metadata", raw);
        }

        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .multipart(form)
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("deploy failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;

        let body: DeployResponse = serde_json::from_reader(resp.take(MAX_SUCCESS_BODY))?;
        Ok(body)
    }

    /// Get deployment status.
    pub fn status(&self, deployment_id: &str) -> Result<StatusResponse> {
        self.get_json_anyhow("status", |base| {
            format!("{base}/api/v1/status/{deployment_id}")
        })
    }

    /// List environment variables for an app.
    pub fn list_env(&self, app_name: &str) -> Result<Vec<EnvVar>> {
        self.get_json_anyhow("list env", |base| {
            format!("{base}/api/v1/apps/{app_name}/env")
        })
    }

    /// Set an environment variable.
    pub fn set_env(&self, app_name: &str, key: &str, value: &str) -> Result<()> {
        let url = format!("{}/api/v1/apps/{}/env", self.base_url, app_name);
        #[derive(Serialize)]
        struct Payload<'a> {
            key: &'a str,
            value: &'a str,
        }
        let payload = Payload { key, value };
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .json(&payload)
            .send()?;

        let _ = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("set env failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        Ok(())
    }

    /// DELETE `/api/v1/apps/{appName}/env/{key}` — delete an environment variable.
    pub fn delete_env(&self, app_name: &str, key: &str) -> Result<()> {
        let url = format!("{}/api/v1/apps/{}/env/{}", self.base_url, app_name, key);
        let resp = self
            .http
            .delete(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        let _ = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("delete env failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        Ok(())
    }

    /// Activate a deployment. If `weight` is Some(N), sends ?weight=N for canary.
    pub fn activate(&self, app_name: &str, deployment_id: &str, weight: Option<u8>) -> Result<()> {
        let url = if let Some(w) = weight {
            format!(
                "{}/api/v1/apps/{}/activate/{}?weight={}",
                self.base_url, app_name, deployment_id, w
            )
        } else {
            format!(
                "{}/api/v1/apps/{}/activate/{}",
                self.base_url, app_name, deployment_id
            )
        };
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        let _ = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("activate failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        Ok(())
    }

    /// Promote a preview deployment to production.
    /// POST /api/v1/apps/{app_name}/promote/{deployment_id}
    pub fn promote(&self, app_name: &str, deployment_id: &str) -> Result<()> {
        let url = format!(
            "{}/api/v1/apps/{}/promote/{}",
            self.base_url, app_name, deployment_id
        );
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .send()?;
        let _ = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("promote failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        Ok(())
    }

    /// Set traffic splits for an app.
    /// `splits` is a slice of (deployment_id, weight) pairs; weights must sum to 100.
    pub fn set_traffic(&self, app_name: &str, splits: &[(String, u8)]) -> Result<()> {
        #[derive(Serialize)]
        struct SplitEntry {
            deployment_id: String,
            weight: u8,
        }
        #[derive(Serialize)]
        struct TrafficRequest<'a> {
            splits: &'a [SplitEntry],
        }
        let url = format!("{}/api/v1/apps/{}/traffic", self.base_url, app_name);
        let body = serde_json::to_string(&TrafficRequest {
            splits: &splits
                .iter()
                .map(|(id, w)| SplitEntry {
                    deployment_id: id.clone(),
                    weight: *w,
                })
                .collect::<Vec<_>>(),
        })?;
        let resp = self
            .http
            .put(&url)
            .header("Authorization", self.auth_header())
            .header("Content-Type", "application/json")
            .body(body)
            .send()?;

        check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("set_traffic failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        Ok(())
    }

    /// Get current traffic splits for an app.
    pub fn get_traffic(&self, app_name: &str) -> Result<Vec<(String, u8)>> {
        #[derive(Deserialize)]
        struct SplitEntry {
            deployment_id: String,
            weight: u8,
        }
        #[derive(Deserialize)]
        struct TrafficResponse {
            splits: Vec<SplitEntry>,
        }
        let url = format!("{}/api/v1/apps/{}/traffic", self.base_url, app_name);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("get_traffic failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        let v: TrafficResponse = serde_json::from_reader(resp.take(MAX_SUCCESS_BODY))?;
        Ok(v.splits
            .into_iter()
            .map(|s| (s.deployment_id, s.weight))
            .collect())
    }

    /// Rollback the active deployment of `app_name` to the stored
    /// `last_good_deployment_id`. Returns the deployment id that is now
    /// active. If the server returns 409 ("no previous deployment to
    /// roll back to"), this surfaces as a `Rejected` `ApiError` — the
    /// caller can detect that via `body.contains("no previous")`.
    pub fn rollback(&self, app_name: &str) -> Result<RollbackResponse> {
        let url = format!("{}/api/v1/apps/{}/rollback", self.base_url, app_name);
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("rollback failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;

        serde_json::from_reader(resp.take(MAX_SUCCESS_BODY)).map_err(Into::into)
    }

    /// List all deployments for an app.
    pub fn list_deployments(&self, app_name: &str) -> Result<Vec<DeploymentSummary>> {
        let resp: DeploymentListResponse = self.get_json_anyhow("list deployments", |base| {
            format!("{base}/api/v1/list/{app_name}")
        })?;
        Ok(resp.items)
    }

    /// GET `/api/v1/apps` — list all apps for the authenticated tenant.
    pub fn list_apps(&self) -> Result<Vec<App>> {
        let resp: AppListResponse =
            self.get_json_anyhow("list apps", |base| format!("{base}/api/v1/apps"))?;
        Ok(resp.apps)
    }

    /// GET `/api/v1/apps/{appName}` — get a single app by name.
    pub fn get_app(&self, app_name: &str) -> Result<App> {
        self.get_json_anyhow("get app", |base| format!("{base}/api/v1/apps/{app_name}"))
    }

    /// POST `/api/v1/apps/{appName}` — create a new app.
    pub fn create_app(&self, app_name: &str, description: Option<&str>) -> Result<App> {
        let url = format!("{}/api/v1/apps/{}", self.base_url, app_name);
        let payload = serde_json::json!({ "description": description });
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .json(&payload)
            .send()?;
        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("create app failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        let body: App = serde_json::from_reader(resp.take(MAX_SUCCESS_BODY))?;
        Ok(body)
    }

    /// GET `/api/v1/quotas` — get tenant quota and usage.
    pub fn get_quota(&self) -> Result<QuotaResponse> {
        self.get_json_anyhow("get quota", |base| format!("{base}/api/v1/quotas"))
    }

    /// GET `/api/v1/egress` — get the current egress allowlist.
    pub fn get_egress(&self) -> Result<EgressAllowlist> {
        self.get_json_anyhow("get egress", |base| format!("{base}/api/v1/egress"))
    }

    /// PUT `/api/v1/egress` — replace the egress allowlist.
    pub fn set_egress(&self, hosts: &[String]) -> Result<EgressAllowlist> {
        let url = format!("{}/api/v1/egress", self.base_url);
        let payload = EgressAllowlist {
            allowlist: hosts.to_vec(),
        };
        let resp = self
            .http
            .put(&url)
            .header("Authorization", self.auth_header())
            .json(&payload)
            .send()?;
        let _ = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("set egress failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        // Server returns the stored allowlist; re-fetch to surface it.
        self.get_egress()
    }

    /// GET `/api/v1/apps/{appName}/ingress` — get the ingress target
    /// (worker address and port) for a running app.
    pub fn get_ingress(&self, app_name: &str) -> Result<IngressResponse> {
        self.get_json_anyhow("get ingress", |base| {
            format!("{base}/api/v1/apps/{app_name}/ingress")
        })
    }

    // ---- Custom-domain (issue #83) ----

    /// Accessor for the `domains` namespace. The returned `DomainClient`
    /// borrows this `ApiClient` so the API-key + base_url are shared
    /// across all subcommands without cloning the underlying HTTP
    /// client (which is already internally `Arc`-shared by reqwest).
    pub fn domains(&self) -> crate::api::domains::DomainClient<'_> {
        crate::api::domains::DomainClient { client: self }
    }
}

/// Tenant-management endpoints. Borrows the parent [`ApiClient`] so
/// `new_anonymous` and `new` clients can both use it.
pub struct Tenants<'a> {
    client: &'a ApiClient,
}

impl<'a> Tenants<'a> {
    /// POST `/api/v1/tenants` — self-signup. No `Authorization` header sent.
    /// Returns the new tenant id and the raw API key (shown only once).
    ///
    /// `key_name` controls the human-readable label on the API key
    /// minted for the new tenant. The CLI defaults this to `"default"`
    /// (single-tenant model) but callers can override.
    pub fn create(&self, name: &str, plan: &str, key_name: &str) -> Result<TenantCreated> {
        let url = format!("{}/api/v1/tenants", self.client.base_url);
        #[derive(Serialize)]
        struct Payload<'b> {
            name: &'b str,
            plan: &'b str,
            key_name: &'b str,
        }
        let payload = Payload {
            name,
            plan,
            key_name,
        };
        let resp = self.client.http.post(&url).json(&payload).send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("signup failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        serde_json::from_reader(resp.take(MAX_SUCCESS_BODY)).map_err(Into::into)
    }
}

/// API key management endpoints. Borrows the parent [`ApiClient`] and
/// requires the client to have been built with an API key (i.e. NOT via
/// [`ApiClient::new_anonymous`]).
pub struct Keys<'a> {
    client: &'a ApiClient,
}

impl<'a> Keys<'a> {
    /// POST `/api/v1/keys` — create an additional API key for the caller's
    /// tenant. The raw `token` in the response is shown only once and is
    /// NOT persisted to the local config by the CLI — the caller is
    /// responsible for storing it.
    pub fn create(&self, name: &str, role: &str) -> Result<CreateAPIKeyResponse> {
        let url = format!("{}/api/v1/keys", self.client.base_url);
        #[derive(Serialize)]
        struct Payload<'b> {
            name: &'b str,
            role: &'b str,
        }
        let payload = Payload { name, role };
        let resp = self
            .client
            .http
            .post(&url)
            .header("Authorization", self.client.auth_header())
            .json(&payload)
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("keys create failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        serde_json::from_reader(resp.take(MAX_SUCCESS_BODY)).map_err(Into::into)
    }

    /// GET `/api/v1/keys` — list all API keys for the caller's tenant.
    /// Returns an inline array (no envelope); used by `edge auth keys list`.
    pub fn list(&self) -> Result<Vec<APIKeySummary>> {
        let url = format!("{}/api/v1/keys", self.client.base_url);
        let resp = self
            .client
            .http
            .get(&url)
            .header("Authorization", self.client.auth_header())
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("keys list failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;
        serde_json::from_reader(resp.take(MAX_SUCCESS_BODY)).map_err(Into::into)
    }

    /// DELETE `/api/v1/keys/{id}` — hard-delete the key with the given
    /// id. Returns `Ok(())` on 204 No Content, `Err(ApiError::Rejected)`
    /// on 4xx (caller can pattern-match for clean user-facing errors),
    /// and `Err(ApiError::Transient)` on 5xx or network failure.
    pub fn revoke(&self, id: &str) -> Result<(), ApiError> {
        let url = format!("{}/api/v1/keys/{}", self.client.base_url, id);
        let resp = self
            .client
            .http
            .delete(&url)
            .header("Authorization", self.client.auth_header())
            .send()?;
        let _ = check_response(resp)?;
        Ok(())
    }
}

/// Auth-related endpoints. Borrows the parent [`ApiClient`].
pub struct Auth<'a> {
    client: &'a ApiClient,
}

impl<'a> Auth<'a> {
    /// GET `/api/v1/auth/whoami` — returns the tenant + API key info
    /// associated with the caller's Bearer token.
    ///
    /// Returns `ApiError` so callers (e.g. `edge auth login`) can
    /// distinguish a 401 rejection from a 5xx/network failure and
    /// react accordingly. Use `whoami_anyhow` for the simple
    /// `Result<WhoamiResponse>` shape.
    pub fn whoami(&self) -> Result<WhoamiResponse, ApiError> {
        self.client
            .get_json(|base| format!("{base}/api/v1/auth/whoami"))
    }

    /// Convenience wrapper around [`whoami`] that flattens the
    /// `ApiError` into an `anyhow::Error`. Used by the `edge auth
    /// whoami` subcommand, which has no need to distinguish rejection
    /// from transient failure.
    pub fn whoami_anyhow(&self) -> Result<WhoamiResponse> {
        self.whoami().map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("whoami failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })
    }
}

/// Log read endpoints (issue #77). Borrows the parent [`ApiClient`].
pub struct Logs<'a> {
    client: &'a ApiClient,
}

impl<'a> Logs<'a> {
    /// GET `/api/v1/apps/{appName}/logs` — list the most recent
    /// log entries for the app, newest first.
    ///
    /// All query parameters are optional. `since_rfc3339` is an
    /// absolute RFC3339 cutoff (the caller converts a relative
    /// `--since 5m` into an absolute timestamp before calling).
    /// The server defaults to the last 5 minutes when omitted.
    /// `level` is the minimum severity (`trace|debug|info|warn|error`).
    /// `limit` is clamped to [1, 1000] server-side; the CLI sends
    /// the user-supplied value through unmodified.
    ///
    /// Errors: any non-2xx becomes a flat `anyhow::Error` carrying
    /// the status and body. 4xx rejections (e.g. invalid level)
    /// surface as the message so `edge logs` can show the
    /// server-typed reason to the user.
    pub fn list(
        &self,
        app_name: &str,
        since_rfc3339: Option<&str>,
        level: Option<&str>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<LogListResponse> {
        // Build the URL with optional query params locally, then
        // hand the formatted string to `get_json_anyhow` for the
        // auth + check + decode pipeline. The URL build is the only
        // call-site-specific piece; the rest is generic.
        let mut parsed = reqwest::Url::parse(&format!(
            "{}/api/v1/apps/{}/logs",
            self.client.base_url, app_name
        ))
        .map_err(|e| anyhow::anyhow!("invalid base url: {e}"))?;
        if let Some(since) = since_rfc3339 {
            if !since.is_empty() {
                parsed.query_pairs_mut().append_pair("since", since);
            }
        }
        if let Some(lvl) = level {
            if !lvl.is_empty() {
                parsed.query_pairs_mut().append_pair("level", lvl);
            }
        }
        if let Some(n) = limit {
            // `0` is the CLI's "use server default" signal (the
            // handler treats it the same as omitted). We only emit
            // the param when > 0 so the URL is clean.
            if n > 0 {
                parsed
                    .query_pairs_mut()
                    .append_pair("limit", &n.to_string());
            }
        }
        if let Some(n) = offset {
            if n > 0 {
                parsed
                    .query_pairs_mut()
                    .append_pair("offset", &n.to_string());
            }
        }
        let url = parsed.to_string();

        self.client.get_json_anyhow("logs", |_| url.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F12: the three `From` impls must keep mapping their source errors
    /// into `ApiError::Transient`. A future "3xx handling" refactor that
    /// dropped one of these would surface here.
    #[test]
    fn from_anyhow_yields_transient() {
        let e: ApiError = anyhow::anyhow!("boom").into();
        assert!(matches!(e, ApiError::Transient { .. }));
    }

    #[test]
    fn from_reqwest_yields_transient() {
        // Construct a real reqwest::Error via a build failure. An
        // unparseable URL is the cleanest way to get a `reqwest::Error`
        // without making a network call.
        let err = reqwest::blocking::Client::new()
            .get("http://[::1]:not-a-port")
            .build()
            .expect_err("build should fail for invalid url");
        let e: ApiError = err.into();
        assert!(matches!(e, ApiError::Transient { .. }));
    }

    #[test]
    fn from_serde_json_yields_transient() {
        let err: serde_json::Error = serde_json::from_str::<i32>("not int").unwrap_err();
        let e: ApiError = err.into();
        assert!(matches!(e, ApiError::Transient { .. }));
    }

    // F9: `truncate_body` must (a) leave short bodies unchanged,
    // (b) cap at MAX_ERR_BODY + marker for long bodies, and
    // (c) walk down to a UTF-8 char boundary instead of
    // panicking mid-multibyte-char. The marker is ASCII so it's
    // safe to write into any log pipeline.

    #[test]
    fn truncate_body_short_input_returned_unchanged() {
        let s = "invalid key".to_string();
        assert_eq!(truncate_body(&s), s);
    }

    #[test]
    fn truncate_body_exact_cap_returned_unchanged() {
        let s = "a".repeat(MAX_ERR_BODY);
        // Equal to cap → no truncation, no marker. Marker is only
        // added when the input exceeds the cap.
        assert_eq!(truncate_body(&s), s);
        assert!(!truncate_body(&s).contains("[truncated]"));
    }

    #[test]
    fn truncate_body_over_cap_gets_marker_and_larger_bytes_pre_counted() {
        // 8 KiB of 'A'. The output must (a) be at most MAX_ERR_BODY
        // bytes of prefix + a short marker, and (b) contain the
        // marker; (c) NOT contain the full 8 KiB verbatim.
        let s = "A".repeat(8 * 1024);
        let out = truncate_body(&s);
        assert!(out.starts_with(&"A".repeat(MAX_ERR_BODY)));
        assert!(out.ends_with("... [truncated]"));
        assert!(out.len() <= MAX_ERR_BODY + "... [truncated]".len());
        // The original 8 KiB body must not survive verbatim.
        assert!(!out.starts_with(&"A".repeat(MAX_ERR_BODY + 1)));
    }

    #[test]
    fn truncate_body_walks_down_to_utf8_char_boundary() {
        // Construct a string of length MAX_ERR_BODY + 1 whose byte
        // at index MAX_ERR_BODY is the start of a 3-byte UTF-8
        // sequence (e.g. U+3000 ideographic space = E3 80 80). The
        // function must not panic and must end on a valid char
        // boundary.
        let mut s: String = "a".repeat(MAX_ERR_BODY - 1);
        s.push('\u{3000}'); // 3 bytes
                            // Now s.len() == MAX_ERR_BODY - 1 + 3 == MAX_ERR_BODY + 2.
        assert!(s.len() > MAX_ERR_BODY);
        let out = truncate_body(&s);
        assert!(out.is_char_boundary(out.find("... [truncated]").unwrap()));
        assert!(!out.is_empty());
        // The marker must be present and the function must have
        // returned without panicking.
        assert!(out.ends_with("... [truncated]"));
    }
}
