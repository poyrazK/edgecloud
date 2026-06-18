//! HTTP client for the edgeCloud control plane API.

use anyhow::Result;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

use crate::config::ApiKey;

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

    /// Group accessor for auth-related endpoints (e.g. whoami).
    pub fn auth(&self) -> Auth<'_> {
        Auth { client: self }
    }

    /// Upload a deployment artifact.
    pub fn deploy(&self, app_name: &str, wasm_bytes: &[u8]) -> Result<DeployResponse> {
        use reqwest::blocking::multipart;

        let url = format!("{}/api/deploy/{}", self.base_url, app_name);
        let part = multipart::Part::bytes(wasm_bytes.to_vec()).file_name("payload");
        let form = multipart::Form::new().part("payload", part);

        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .multipart(form)
            .send()?;

        if !resp.status().is_success() {
            anyhow::bail!("deploy failed: {} {}", resp.status(), resp.text()?);
        }

        let body: DeployResponse = serde_json::from_str(&resp.text()?)?;
        Ok(body)
    }

    /// Get deployment status.
    pub fn status(&self, deployment_id: &str) -> Result<StatusResponse> {
        let url = format!("{}/api/status/{}", self.base_url, deployment_id);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        if !resp.status().is_success() {
            anyhow::bail!("status failed: {} {}", resp.status(), resp.text()?);
        }

        serde_json::from_str(&resp.text()?).map_err(Into::into)
    }

    /// List environment variables for an app.
    pub fn list_env(&self, app_name: &str) -> Result<Vec<EnvVar>> {
        let url = format!("{}/api/apps/{}/env", self.base_url, app_name);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        if !resp.status().is_success() {
            anyhow::bail!("list env failed: {} {}", resp.status(), resp.text()?);
        }

        serde_json::from_str(&resp.text()?).map_err(Into::into)
    }

    /// Set an environment variable.
    pub fn set_env(&self, app_name: &str, key: &str, value: &str) -> Result<()> {
        let url = format!("{}/api/apps/{}/env", self.base_url, app_name);
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

        if !resp.status().is_success() {
            anyhow::bail!("set env failed: {} {}", resp.status(), resp.text()?);
        }

        Ok(())
    }

    /// Activate a deployment.
    pub fn activate(&self, app_name: &str, deployment_id: &str) -> Result<()> {
        let url = format!(
            "{}/api/apps/{}/activate/{}",
            self.base_url, app_name, deployment_id
        );
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        if !resp.status().is_success() {
            anyhow::bail!("activate failed: {} {}", resp.status(), resp.text()?);
        }

        Ok(())
    }

    /// List all deployments for an app.
    pub fn list_deployments(&self, app_name: &str) -> Result<Vec<DeploymentSummary>> {
        let url = format!("{}/api/list/{}", self.base_url, app_name);
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .send()?;

        if !resp.status().is_success() {
            anyhow::bail!(
                "list deployments failed: {} {}",
                resp.status(),
                resp.text()?
            );
        }

        serde_json::from_str(&resp.text()?).map_err(Into::into)
    }
}

/// Tenant-management endpoints. Borrows the parent [`ApiClient`] so
/// `new_anonymous` and `new` clients can both use it.
pub struct Tenants<'a> {
    client: &'a ApiClient,
}

impl<'a> Tenants<'a> {
    /// POST `/api/tenants` — self-signup. No `Authorization` header sent.
    /// Returns the new tenant id and the raw API key (shown only once).
    pub fn create(&self, name: &str, plan: &str) -> Result<TenantCreated> {
        let url = format!("{}/api/tenants", self.client.base_url);
        #[derive(Serialize)]
        struct Payload<'b> {
            name: &'b str,
            plan: &'b str,
            key_name: &'b str,
        }
        let payload = Payload {
            name,
            plan,
            key_name: "default",
        };
        let resp = self.client.http.post(&url).json(&payload).send()?;

        if !resp.status().is_success() {
            anyhow::bail!("signup failed: {} {}", resp.status(), resp.text()?);
        }
        serde_json::from_str(&resp.text()?).map_err(Into::into)
    }
}

/// Auth-related endpoints. Borrows the parent [`ApiClient`].
pub struct Auth<'a> {
    client: &'a ApiClient,
}

impl<'a> Auth<'a> {
    /// GET `/api/auth/whoami` — returns the tenant + API key info
    /// associated with the caller's Bearer token.
    pub fn whoami(&self) -> Result<WhoamiResponse> {
        let url = format!("{}/api/auth/whoami", self.client.base_url);
        let resp = self
            .client
            .http
            .get(&url)
            .header("Authorization", self.client.auth_header())
            .send()?;

        if !resp.status().is_success() {
            anyhow::bail!("whoami failed: {} {}", resp.status(), resp.text()?);
        }
        serde_json::from_str(&resp.text()?).map_err(Into::into)
    }
}
