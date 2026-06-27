//! HTTP client for the edgeCloud control plane API.

use anyhow::Result;
use reqwest::blocking::{Client, Response};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

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
fn check_response(resp: Response) -> Result<Response, ApiError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = resp.text().unwrap_or_default();
    if status.is_client_error() {
        Err(ApiError::Rejected { status, body })
    } else {
        Err(ApiError::Transient {
            source: anyhow::anyhow!("server returned {status}: {body}"),
        })
    }
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
}

#[derive(Debug, Deserialize)]
pub struct StatusResponse {
    pub id: String,
    pub status: String,
    pub created_at: String,
    pub url: Option<String>,
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
    pub url: Option<String>,
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

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.api_key.0)
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
        serde_json::from_str(&resp.text()?).map_err(ApiError::from)
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
    pub fn deploy(
        &self,
        app_name: &str,
        wasm_bytes: &[u8],
        regions: &[String],
        auto_rollback: bool,
    ) -> Result<DeployResponse> {
        use reqwest::blocking::multipart;

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
        url = parsed.to_string();
        let part = multipart::Part::bytes(wasm_bytes.to_vec()).file_name("payload");
        let form = multipart::Form::new().part("payload", part);

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

        let body: DeployResponse = serde_json::from_str(&resp.text()?)?;
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
        let v: TrafficResponse = serde_json::from_str(&resp.text()?)?;
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

        serde_json::from_str(&resp.text()?).map_err(Into::into)
    }

    /// List all deployments for an app.
    pub fn list_deployments(&self, app_name: &str) -> Result<Vec<DeploymentSummary>> {
        self.get_json_anyhow("list deployments", |base| {
            format!("{base}/api/v1/list/{app_name}")
        })
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
        serde_json::from_str(&resp.text()?).map_err(Into::into)
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
        serde_json::from_str(&resp.text()?).map_err(Into::into)
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
}
