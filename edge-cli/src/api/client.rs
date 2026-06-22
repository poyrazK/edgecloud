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

    /// Upload a deployment artifact.
    ///
    /// `regions` is the list of regions the deployment should be
    /// replicated to. Empty slice means "use the control plane's default
    /// region" (the control plane applies the same default). The values
    /// are passed through the `?regions=` query parameter as a
    /// comma-separated string; the server splits, validates, and dedupes.
    pub fn deploy(
        &self,
        app_name: &str,
        wasm_bytes: &[u8],
        regions: &[String],
    ) -> Result<DeployResponse> {
        use reqwest::blocking::multipart;

        let mut url = format!("{}/api/v1/deploy/{}", self.base_url, app_name);
        if !regions.is_empty() {
            // Use reqwest::Url to percent-encode the comma list so a
            // region with a stray `+` or non-ASCII char doesn't break
            // the URL. The server splits on `,` so the encoding is
            // applied per the CSV string as a whole.
            let mut parsed =
                reqwest::Url::parse(&url).map_err(|e| anyhow::anyhow!("invalid base url: {e}"))?;
            parsed
                .query_pairs_mut()
                .append_pair("regions", &regions.join(","));
            url = parsed.to_string();
        }
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
        let url = format!("{}/api/v1/status/{}", self.base_url, deployment_id);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("status failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;

        serde_json::from_str(&resp.text()?).map_err(Into::into)
    }

    /// List environment variables for an app.
    pub fn list_env(&self, app_name: &str) -> Result<Vec<EnvVar>> {
        let url = format!("{}/api/v1/apps/{}/env", self.base_url, app_name);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("list env failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;

        serde_json::from_str(&resp.text()?).map_err(Into::into)
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

    /// Activate a deployment.
    pub fn activate(&self, app_name: &str, deployment_id: &str) -> Result<()> {
        let url = format!(
            "{}/api/v1/apps/{}/activate/{}",
            self.base_url, app_name, deployment_id
        );
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

    /// List all deployments for an app.
    pub fn list_deployments(&self, app_name: &str) -> Result<Vec<DeploymentSummary>> {
        let url = format!("{}/api/v1/list/{}", self.base_url, app_name);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        let resp = check_response(resp).map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("list deployments failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })?;

        serde_json::from_str(&resp.text()?).map_err(Into::into)
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
        let url = format!("{}/api/v1/auth/whoami", self.client.base_url);
        let resp = self
            .client
            .http
            .get(&url)
            .header("Authorization", self.client.auth_header())
            .send()?;

        let resp = check_response(resp)?;
        serde_json::from_str(&resp.text()?).map_err(ApiError::from)
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
