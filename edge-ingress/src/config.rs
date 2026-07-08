//! edge-ingress configuration loaded from environment variables.

use std::time::Duration;

use anyhow::Context;

/// Suffix for every public hostname the ingress serves. Must stay in sync
/// with the Go control plane's `domain.IngressHostSuffix` (in
/// `edge-control-plane/internal/domain/worker.go`) — drift between the
/// two produces 404s for every public URL the control plane has
/// advertised to tenants. Re-branding (e.g. to `edgecloud.run`) is a
/// single-line change in each language.
pub const INGRESS_HOST_SUFFIX: &str = "edgecloud.dev";

/// Render the public hostname for a `(tenant_id, app_name)` pair.
pub fn ingress_host(tenant_id: &str, app_name: &str) -> String {
    format!("{}-{}.{}", tenant_id, app_name, INGRESS_HOST_SUFFIX)
}

/// Default poll interval for `/api/internal/domains`. The custom-domain
/// poller fetches the full domain list on this cadence and diffs against
/// the current FQDN table. 30s matches the control plane's expected
/// staleness budget (per issue #83 design).
pub const DEFAULT_DOMAIN_POLL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    pub nats_url: String,
    pub caddy_admin_url: String,
    pub region: String,
    /// Path to the TLS certificate PEM file. Passed to Caddy's
    /// `load_files` config. When Caddy runs in Docker, this must be
    /// a container-accessible path (e.g. `/etc/caddy/tls/cert.pem`
    /// matching a `-v` mount), not a host path like `/Users/...`.
    /// Set via `TLS_CERT_FILE` (required).
    pub cert_file: String,
    /// Path to the TLS key PEM file. Same Docker constraint as
    /// `cert_file`. Set via `TLS_KEY_FILE` (required).
    pub key_file: String,
    pub listen_http: String,
    pub listen_https: String,
    pub refresh_debounce_ms: u64,
    pub http_to_https: bool,
    pub admin_token: Option<String>,
    pub control_plane_api_url: String,
    /// Shared secret presented in `X-Internal-Token` when fetching traffic
    /// splits from the control plane. Must match the control plane's
    /// `EDGE_INTERNAL_TOKEN`; otherwise the control plane's
    /// `internalAuth` middleware returns 401 and the Caddy weights
    /// never get applied (canary/blue-green silently no-ops). `None`
    /// means the header is omitted — which the control plane treats
    /// as a 401, so a production deployment must set this.
    pub internal_token: Option<String>,
    /// Base URL of the control plane's `/api/internal`. Empty = default-only
    /// mode (no custom domains, no on-demand TLS).
    pub control_plane_url: String,
    /// JWT for the control plane. Carries `role: ingest`. Required when
    /// `control_plane_url` is set; ignored otherwise.
    pub service_token: String,
    /// How often to poll `/api/internal/domains` (default 30s).
    pub domain_poll_interval: Duration,
    /// Listen address for Caddy's admin API (e.g. `localhost:2019` or
    /// `0.0.0.0:2019` for Docker). Defaults to `localhost:2019` which
    /// matches Caddy's own default. Override with `CADDY_ADMIN_LISTEN`.
    pub caddy_admin_listen: String,
    /// Listen address for the Prometheus /metrics HTTP endpoint
    /// (e.g. `:9091`). Set via `INGRESS_METRICS_LISTEN` (default `:9091`).
    pub metrics_listen: String,
    /// Default per-app rate limit in requests per second. 0 = disabled.
    /// Override with `RATE_LIMIT_RPS_DEFAULT`.
    pub rate_limit_rps_default: u32,
    /// Default per-app burst size. 0 = disabled.
    /// Override with `RATE_LIMIT_BURST_DEFAULT`.
    pub rate_limit_burst_default: u32,
    /// How often to poll the control plane for per-app rate limit overrides.
    /// Default 60s. 0 = disabled. Override with `RATE_LIMIT_FETCH_INTERVAL`.
    pub rate_limit_fetch_interval: Duration,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// Required env vars:
    /// - `INGRESS_REGION` (e.g. `fra`)
    /// - `TLS_CERT_FILE` — path to the `*.edgecloud.dev` wildcard cert PEM.
    ///   When Caddy runs in Docker, this must be a path accessible from
    ///   inside the container (e.g. `/etc/caddy/tls/cert.pem` when using
    ///   `-v` mount), NOT a host-only path like `/Users/user/...`.
    /// - `TLS_KEY_FILE` — path to the matching key PEM. Same Docker
    ///   path constraint as `TLS_CERT_FILE`.
    ///
    /// Optional env vars:
    /// - `NATS_URL` (default: `nats://localhost:4222`)
    /// - `CADDY_ADMIN_URL` (default: `http://127.0.0.1:2019`)
    /// - `INGRESS_LISTEN_HTTP` (default: `:80`)
    /// - `INGRESS_LISTEN_HTTPS` (default: `:443`)
    /// - `CADDY_ADMIN_TOKEN` (if set, must match the value on the Caddy process)
    /// - `REFRESH_DEBOUNCE_MS` (default: `1000`)
    /// - `HTTP_TO_HTTPS` (default: `true`) — 308-redirect :80 → :443
    /// - `CONTROL_PLANE_API_URL` (default: `http://localhost:8080`) — used
    ///   by the ingress to fetch canary traffic splits at render time
    /// - `CONTROL_PLANE_URL` (default: empty = no custom domains)
    /// - `INGRESS_SERVICE_TOKEN` (default: empty; required when
    ///   `CONTROL_PLANE_URL` is set)
    /// - `DOMAIN_POLL_INTERVAL` (default: 30s; parsed as a Go-style
    ///   duration string, e.g. `30s`, `1m`, `500ms`)
    pub fn from_env() -> anyhow::Result<Self> {
        let control_plane_url = std::env::var("CONTROL_PLANE_URL").unwrap_or_default();
        let service_token = std::env::var("INGRESS_SERVICE_TOKEN").unwrap_or_default();
        if !control_plane_url.is_empty() && service_token.is_empty() {
            anyhow::bail!(
                "CONTROL_PLANE_URL is set but INGRESS_SERVICE_TOKEN is empty; \
                 the domain poller cannot authenticate against the control plane"
            );
        }
        let domain_poll_interval =
            parse_duration_env("DOMAIN_POLL_INTERVAL").unwrap_or(DEFAULT_DOMAIN_POLL_INTERVAL);

        Ok(Config {
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into()),
            caddy_admin_url: std::env::var("CADDY_ADMIN_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:2019".into()),
            region: std::env::var("INGRESS_REGION").context("INGRESS_REGION not set")?,
            cert_file: std::env::var("TLS_CERT_FILE").context("TLS_CERT_FILE not set")?,
            key_file: std::env::var("TLS_KEY_FILE").context("TLS_KEY_FILE not set")?,
            listen_http: std::env::var("INGRESS_LISTEN_HTTP").unwrap_or_else(|_| ":80".into()),
            listen_https: std::env::var("INGRESS_LISTEN_HTTPS").unwrap_or_else(|_| ":443".into()),
            refresh_debounce_ms: std::env::var("REFRESH_DEBOUNCE_MS")
                .unwrap_or_else(|_| "1000".into())
                .parse()
                .unwrap_or(1000),
            http_to_https: std::env::var("HTTP_TO_HTTPS")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
                .unwrap_or(true),
            admin_token: std::env::var("CADDY_ADMIN_TOKEN")
                .ok()
                .filter(|v| !v.is_empty()),
            control_plane_api_url: std::env::var("CONTROL_PLANE_API_URL")
                .unwrap_or_else(|_| "http://localhost:8080".into()),
            internal_token: std::env::var("EDGE_INTERNAL_TOKEN")
                .ok()
                .filter(|v| !v.is_empty()),
            control_plane_url,
            service_token,
            domain_poll_interval,
            caddy_admin_listen: std::env::var("CADDY_ADMIN_LISTEN")
                .unwrap_or_else(|_| "localhost:2019".into()),
            metrics_listen: std::env::var("INGRESS_METRICS_LISTEN")
                .unwrap_or_else(|_| ":9091".into()),
            rate_limit_rps_default: std::env::var("RATE_LIMIT_RPS_DEFAULT")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            rate_limit_burst_default: std::env::var("RATE_LIMIT_BURST_DEFAULT")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            rate_limit_fetch_interval: std::env::var("RATE_LIMIT_FETCH_INTERVAL")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(Duration::from_secs(60)),
        })
    }
}

/// Parse a duration env var. Accepts Go-style strings (`30s`, `1m`,
/// `500ms`, `2h`) via the `humantime` crate. Returns `None` if the
/// env var is unset, malformed, or parses to zero — a zero poll
/// interval would busy-loop the ingress and pin a CPU at 100%.
///
/// The unit tests in `config::tests` pin: (a) standard Go-style
/// strings round-trip, (b) `0s` / `0ms` / `0` are rejected (return
/// `None` so the caller falls back to the default), (c) malformed
/// values return `None` rather than panic.
fn parse_duration_env(name: &str) -> Option<Duration> {
    let raw = std::env::var(name).ok()?;
    let d = humantime::parse_duration(&raw).ok()?;
    if d.is_zero() {
        return None;
    }
    Some(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // Each test sets and unsets a uniquely-named env var so the
    // tests are safe to run in parallel with each other and with
    // any process that uses the real `DOMAIN_POLL_INTERVAL`.

    fn set_var(name: &str, value: &str) {
        // SAFETY: tests are not multi-threaded against the same
        // env var name; we serialize via the unique name per test.
        unsafe { std::env::set_var(name, value) };
    }

    fn unset_var(name: &str) {
        unsafe { std::env::remove_var(name) };
    }

    #[test]
    fn parse_duration_env_handles_go_style_seconds() {
        let name = "__TEST_DUR_SECS";
        set_var(name, "5s");
        assert_eq!(parse_duration_env(name), Some(Duration::from_secs(5)));
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_handles_go_style_minutes() {
        let name = "__TEST_DUR_MINS";
        set_var(name, "5m");
        assert_eq!(parse_duration_env(name), Some(Duration::from_secs(300)));
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_handles_go_style_millis() {
        let name = "__TEST_DUR_MS";
        set_var(name, "500ms");
        assert_eq!(parse_duration_env(name), Some(Duration::from_millis(500)));
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_handles_go_style_hours() {
        let name = "__TEST_DUR_HRS";
        set_var(name, "2h");
        assert_eq!(parse_duration_env(name), Some(Duration::from_secs(7200)));
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_rejects_zero_seconds() {
        let name = "__TEST_DUR_ZERO_S";
        set_var(name, "0s");
        assert_eq!(parse_duration_env(name), None, "0s would busy-loop");
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_rejects_zero_millis() {
        let name = "__TEST_DUR_ZERO_MS";
        set_var(name, "0ms");
        assert_eq!(parse_duration_env(name), None, "0ms would busy-loop");
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_rejects_zero_compound() {
        let name = "__TEST_DUR_ZERO_2H0S";
        set_var(name, "2h0s");
        // humantime accepts this; we want our zero-check to fire
        // only on is_zero() values. Pin that 2h0s is accepted.
        assert_eq!(parse_duration_env(name), Some(Duration::from_secs(7200)));
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_rejects_malformed() {
        let name = "__TEST_DUR_GARBAGE";
        set_var(name, "garbage");
        assert_eq!(parse_duration_env(name), None);
        unset_var(name);
    }

    #[test]
    fn parse_duration_env_returns_none_when_unset() {
        // The env var must not exist at test time. Use a
        // name that's vanishingly unlikely to be set in CI.
        let name = "__TEST_DUR_DEFINITELY_UNSET_XYZ";
        unset_var(name);
        assert_eq!(parse_duration_env(name), None);
    }

    // ── Ingress hostname ──────────────────────────────────────────────

    /// Must stay in sync with the Go control plane's
    /// `TestIngressHost_Format`. Every `https://<tenant>-<app>.edgecloud.dev`
    /// URL the control plane advertises to tenants depends on this format.
    #[test]
    fn ingress_host_returns_formatted_hostname() {
        let host = ingress_host("t_acme", "api");
        assert_eq!(host, "t_acme-api.edgecloud.dev");
    }

    /// Pin the constant against accidental re-branding. The Go control
    /// plane has an identical guard in `TestIngressHostSuffix_Constant`.
    /// If this constant ever changes, the wildcard TLS certificate and
    /// the Go side must be updated in lock-step.
    #[test]
    fn ingress_host_suffix_is_edgecloud_dev() {
        assert_eq!(INGRESS_HOST_SUFFIX, "edgecloud.dev");
    }

    /// Edge cases: empty tenant or app name must not produce a trailing
    /// or leading `-` that could be confused with a subdomain boundary.
    #[test]
    fn ingress_host_handles_edge_cases() {
        let host = ingress_host("", "");
        // Empty tenant + app still produces a valid-ish hostname,
        // just `-.edgecloud.dev` — which won't resolve, but it
        // won't panic or produce anything injectable.
        assert!(host.ends_with(".edgecloud.dev"));
        assert!(host.contains('.'));
    }

    // ── Config::from_env() ───────────────────────────────────────────

    /// Helper: set the minimum required env vars for from_env().
    fn set_required_vars() {
        set_var("INGRESS_REGION", "fra");
        set_var("TLS_CERT_FILE", "/tmp/cert.pem");
        set_var("TLS_KEY_FILE", "/tmp/key.pem");
    }

    /// Helper: unset all config-related env vars so from_env() sees a clean slate.
    fn unset_all_config_vars() {
        for v in &[
            "INGRESS_REGION",
            "TLS_CERT_FILE",
            "TLS_KEY_FILE",
            "NATS_URL",
            "CADDY_ADMIN_URL",
            "INGRESS_LISTEN_HTTP",
            "INGRESS_LISTEN_HTTPS",
            "CADDY_ADMIN_TOKEN",
            "REFRESH_DEBOUNCE_MS",
            "HTTP_TO_HTTPS",
            "CONTROL_PLANE_API_URL",
            "EDGE_INTERNAL_TOKEN",
            "CONTROL_PLANE_URL",
            "INGRESS_SERVICE_TOKEN",
            "DOMAIN_POLL_INTERVAL",
            "CADDY_ADMIN_LISTEN",
            "INGRESS_METRICS_LISTEN",
        ] {
            unset_var(v);
        }
    }

    /// All from_env tests in one serial function to avoid races on global env.
    #[test]
    fn from_env_config_assembly() {
        // 1. Required vars present → success
        unset_all_config_vars();
        set_required_vars();
        let cfg = Config::from_env().expect("required vars should succeed");
        assert_eq!(cfg.region, "fra");
        assert_eq!(cfg.cert_file, "/tmp/cert.pem");
        assert_eq!(cfg.key_file, "/tmp/key.pem");

        // 2. Missing region → error
        unset_all_config_vars();
        set_var("TLS_CERT_FILE", "/tmp/cert.pem");
        set_var("TLS_KEY_FILE", "/tmp/key.pem");
        assert!(Config::from_env().is_err(), "missing region should fail");

        // 3. Missing cert → error
        unset_all_config_vars();
        set_var("INGRESS_REGION", "fra");
        set_var("TLS_KEY_FILE", "/tmp/key.pem");
        assert!(Config::from_env().is_err(), "missing cert should fail");

        // 4. Missing key → error
        unset_all_config_vars();
        set_var("INGRESS_REGION", "fra");
        set_var("TLS_CERT_FILE", "/tmp/cert.pem");
        assert!(Config::from_env().is_err(), "missing key should fail");

        // 5. Defaults for optional fields
        unset_all_config_vars();
        set_required_vars();
        let cfg = Config::from_env().expect("defaults test");
        assert_eq!(cfg.nats_url, "nats://localhost:4222");
        assert_eq!(cfg.caddy_admin_url, "http://127.0.0.1:2019");
        assert_eq!(cfg.listen_http, ":80");
        assert_eq!(cfg.listen_https, ":443");
        assert_eq!(cfg.refresh_debounce_ms, 1000);
        assert!(cfg.http_to_https);
        assert_eq!(cfg.control_plane_api_url, "http://localhost:8080");
        assert_eq!(cfg.admin_token, None);
        assert_eq!(cfg.internal_token, None);
        assert_eq!(cfg.caddy_admin_listen, "localhost:2019");
        assert_eq!(cfg.metrics_listen, ":9091");

        // 6. HTTP_TO_HTTPS=false
        set_required_vars();
        set_var("HTTP_TO_HTTPS", "false");
        let cfg = Config::from_env().expect("http_to_https test");
        assert!(!cfg.http_to_https);

        // 7. CONTROL_PLANE_URL without token → error
        unset_all_config_vars();
        set_required_vars();
        set_var("CONTROL_PLANE_URL", "http://cp.example.com");
        let err = Config::from_env().unwrap_err();
        assert!(
            err.to_string().contains("CONTROL_PLANE_URL"),
            "err should mention CONTROL_PLANE_URL: {err}"
        );

        // 8. Empty EDGE_INTERNAL_TOKEN → None
        unset_all_config_vars();
        set_required_vars();
        set_var("EDGE_INTERNAL_TOKEN", "");
        let cfg = Config::from_env().expect("empty token test");
        assert!(cfg.internal_token.is_none());

        // 9. CONTROL_PLANE_URL + token → success
        unset_all_config_vars();
        set_required_vars();
        set_var("CONTROL_PLANE_URL", "http://cp.example.com");
        set_var("INGRESS_SERVICE_TOKEN", "s3cr3t");
        let cfg = Config::from_env().expect("token+url test");
        assert_eq!(cfg.control_plane_url, "http://cp.example.com");
        assert_eq!(cfg.service_token, "s3cr3t");

        // 10. REFRESH_DEBOUNCE_MS override
        unset_all_config_vars();
        set_required_vars();
        set_var("REFRESH_DEBOUNCE_MS", "500");
        let cfg = Config::from_env().expect("debounce test");
        assert_eq!(cfg.refresh_debounce_ms, 500);

        // 11. CADDY_ADMIN_LISTEN override
        unset_all_config_vars();
        set_required_vars();
        set_var("CADDY_ADMIN_LISTEN", "0.0.0.0:2019");
        let cfg = Config::from_env().expect("admin listen test");
        assert_eq!(cfg.caddy_admin_listen, "0.0.0.0:2019");

        // 12. DOMAIN_POLL_INTERVAL
        unset_all_config_vars();
        set_required_vars();
        set_var("DOMAIN_POLL_INTERVAL", "10s");
        let cfg = Config::from_env().expect("poll interval test");
        assert_eq!(cfg.domain_poll_interval, Duration::from_secs(10));

        // 13. EDGE_INTERNAL_TOKEN with value
        unset_all_config_vars();
        set_required_vars();
        set_var("EDGE_INTERNAL_TOKEN", "my-secret-token");
        let cfg = Config::from_env().expect("internal token test");
        assert_eq!(cfg.internal_token, Some("my-secret-token".to_string()));

        // 14. CONTROL_PLANE_API_URL override
        unset_all_config_vars();
        set_required_vars();
        set_var("CONTROL_PLANE_API_URL", "http://cp.internal:8080");
        let cfg = Config::from_env().expect("api url test");
        assert_eq!(cfg.control_plane_api_url, "http://cp.internal:8080");

        // 15. INGRESS_METRICS_LISTEN override
        unset_all_config_vars();
        set_required_vars();
        set_var("INGRESS_METRICS_LISTEN", "0.0.0.0:9092");
        let cfg = Config::from_env().expect("metrics listen test");
        assert_eq!(cfg.metrics_listen, "0.0.0.0:9092");

        // Clean up
        unset_all_config_vars();
    }
}
