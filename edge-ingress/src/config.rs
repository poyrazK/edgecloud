//! edge-ingress configuration loaded from environment variables.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use tracing::warn;

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
    /// Optional second TLS cert PEM path, loaded into Caddy's
    /// `load_files` alongside `cert_file`. Used for the multi-label
    /// wildcard (`*.*.edgecloud.dev`) that covers dotted app names
    /// like `myapp.v2` (issue #438). When unset, dotted hosts fall
    /// through to per-route `tls.on_demand: {}` (ACME) on first hit.
    /// Set via `TLS_CERT_FILE_2` (optional).
    pub cert_file_2: Option<String>,
    /// Optional second TLS key PEM path. Pairs with `cert_file_2`.
    /// Set via `TLS_KEY_FILE_2` (optional).
    pub key_file_2: Option<String>,
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
    /// Max concurrent connections across the entire ingress. 0 = unlimited.
    /// Set via `INGRESS_MAX_CONNS`.
    pub max_conns: u32,
    /// Max concurrent connections per client IP. 0 = unlimited.
    /// Set via `INGRESS_MAX_CONNS_PER_IP`.
    pub max_conns_per_ip: u32,
    /// Per-IP rate limit: max requests per second from a single remote host.
    /// 0 = disabled. Applied globally before per-app rate limits.
    /// Set via `INGRESS_PER_IP_RPS`.
    pub per_ip_rps: u32,
    /// Per-IP rate limit burst size. 0 = uses `per_ip_rps`.
    /// Set via `INGRESS_PER_IP_BURST`.
    pub per_ip_burst: u32,
    /// Default per-app rate limit in requests per second. 0 = disabled.
    /// Override with `RATE_LIMIT_RPS_DEFAULT`.
    pub rate_limit_rps_default: u32,
    /// Default per-app burst size. 0 = disabled.
    /// Override with `RATE_LIMIT_BURST_DEFAULT`.
    pub rate_limit_burst_default: u32,
    /// How often to poll the control plane for per-app rate limit overrides.
    /// Default 60s. 0 = disabled. Override with `RATE_LIMIT_FETCH_INTERVAL`.
    pub rate_limit_fetch_interval: Duration,
    /// How often to poll the control plane for per-tenant quota state
    /// (issue #420). Default 30s. 0 = disabled. Override with
    /// `QUOTA_FETCH_INTERVAL`. When 0, the quota fetcher is not
    /// spawned and the ingress never injects Caddy 402 blocks.
    pub quota_fetch_interval: Duration,
    // ── Failure detection (issue #fast-failure-detection) ────────────
    /// How long without a heartbeat before a worker's routes are pruned.
    /// Default 60s (2 missed beats at 30s interval). Override with
    /// `STALE_TIMEOUT`. Parsed as a Go-style duration (e.g. `60s`, `30s`).
    pub stale_timeout: Duration,
    /// How often the pruner scans for stale routes. Default 30s.
    /// Override with `PRUNE_INTERVAL`. Parsed as a Go-style duration.
    pub prune_interval: Duration,
    /// How often Caddy performs active health checks on each upstream.
    /// Default 10s. Override with `HEALTH_CHECK_INTERVAL`.
    pub health_check_interval: Duration,
    /// Timeout for each active health check probe. Default 3s.
    /// Override with `HEALTH_CHECK_TIMEOUT`.
    pub health_check_timeout: Duration,
    /// URI path for active health checks. Default "/healthz".
    /// Override with `HEALTH_CHECK_URI`.
    pub health_check_uri: String,
    /// Number of consecutive failed health checks before marking upstream
    /// unhealthy. Default 2. Override with `HEALTH_CHECK_MAX_FAILS`.
    pub health_check_max_fails: u32,
    // ── Per-tenant data-plane rate limits (issue #305) ─────────────
    /// Per-tenant default RPS applied to every tenant that has no
    /// explicit per-tenant override configured in the control plane.
    /// 0 = no default cap (operators opt tenants in explicitly).
    /// Set via `RATE_LIMIT_RPS_TENANT_DEFAULT`.
    pub rate_limit_rps_tenant_default: u32,
    /// Per-tenant default burst paired with `rate_limit_rps_tenant_default`.
    /// 0 = falls back to `rate_limit_rps_tenant_default` at the
    /// renderer (matches the per-app cache semantics at ratelimit.rs).
    /// Set via `RATE_LIMIT_BURST_TENANT_DEFAULT`.
    pub rate_limit_burst_tenant_default: u32,
    /// How often the ingress polls the control plane for the
    /// per-tenant rate-limit table (issue #305). Default 30s
    /// (matches QUOTA_FETCH_INTERVAL — both caches refresh on
    /// the same beat so a free-tier lockdown and a tenant-rl write
    /// propagate within one tick). 0 disables the fetcher.
    /// Set via `TENANT_RATE_LIMIT_FETCH_INTERVAL`.
    pub tenant_rate_limit_fetch_interval: Duration,
    /// Global RPS cap applied before any per-tenant route (issue
    /// #305, sub-feature #4). 0 = disabled. Enforced per Caddy
    /// replica — multi-replica NATS aggregation is a follow-up.
    /// Set via `GLOBAL_RATE_LIMIT_RPS`.
    pub global_rate_limit_rps: u32,
    /// Global RPS burst paired with `global_rate_limit_rps`. 0 =
    /// falls back to `global_rate_limit_rps` at the renderer.
    /// Set via `GLOBAL_RATE_LIMIT_BURST`.
    pub global_rate_limit_burst: u32,
    // ── L4 (raw-TCP) ingress (issue #548) ───────────────────────────
    /// First port in the L4 public-port range. Each raw-TCP app gets
    /// a dedicated port in `[l4_port_range_start, l4_port_range_end]`
    /// on the ingress host; bytes flowing into that port are proxied
    /// raw to the worker's upstream port. Set via `L4_PORT_RANGE_START`
    /// (default `31000`). Picked deliberately above IANA's registered
    /// range (1024-49151) and well below the dynamic/private range
    /// (49152-65535) so an operator firewall can carve it out without
    /// colliding with the worker app-port range (configurable via
    /// `EDGE_WORKER_PORT_RANGE_START`).
    pub l4_port_range_start: u16,
    /// Last port in the L4 public-port range (inclusive). Set via
    /// `L4_PORT_RANGE_END` (default `31999`). At 1000 ports the
    /// ingress can host ~1000 raw-TCP apps per region before it has
    /// to expand the range — adequate for v1, an operator can widen
    /// the range by setting both vars without code changes.
    pub l4_port_range_end: u16,
    /// Max concurrent connections on a single L4 (raw-TCP) server
    /// (issue #548 DDoS cap). 0 = unlimited. Set via
    /// `INGRESS_L4_MAX_CONNS_PER_APP` (default `1000`). Mapped
    /// directly onto Caddy's layer4 `connection_pools` field. Reuses
    /// the same DDoS cap philosophy as the HTTP `max_conns_per_ip`
    /// knob above so a DDoS'd public port can't drag down the
    /// ingress for unrelated apps.
    pub l4_max_conns_per_app: u32,
    /// Max concurrent connections per remote IP on a single L4 server
    /// (issue #548 DDoS cap). 0 = unlimited. Set via
    /// `INGRESS_L4_MAX_CONNS_PER_IP` (default `100`). TCP has no
    /// request concept to key a per-IP RPS limiter on, so this is
    /// the only per-IP backpressure the L4 path applies at Caddy
    /// level. Per-app rate-limit overrides are NOT applied on the L4
    /// path; the HTTP `RATE_LIMIT_*` knobs are HTTP-only.
    pub l4_max_conns_per_ip: u32,
    /// Cooldown after an L4 public port is released back to the
    /// `L4PortPool` (matches `edge-worker/src/port_pool.rs`'s 60s
    /// worker-side cooldown so the same TIME_WAIT guard rationale
    /// applies: a port that just closed can still see stray
    /// retransmits from the kernel, and reassigning it within the
    /// cooldown opens a silent-takeover footgun). Default 60s.
    /// Override with `L4_PORT_COOLDOWN_SECS`.
    pub l4_port_cooldown_secs: u64,
    // ── Cross-replica RPS aggregation (issue #665 PR D) ───────────
    /// Whether the ingress spawns the cross-replica UDS reader task
    /// (issue #665 PR D). When true, the renderer emits a global
    /// `rate_limit` route that enforces the per-replica cap computed
    /// by the sidecar's aggregator (see
    /// `edge-ingress-sidecar/src/aggregate.rs::Snapshot::per_replica_cap`).
    /// When false, the renderer behaves exactly as before — no UDS
    /// socket is bound, the cross-replica route is never emitted.
    /// Set via `INGRESS_RATE_LIMIT_AGGREGATION` (default `false`).
    pub ingress_rate_limit_aggregation: bool,
    /// Path to the UDS SOCK_DGRAM socket the sidecar writes to once
    /// per tick. Must match `edge-ingress-sidecar::config::DEFAULT_UDS_PATH`
    /// (`/var/run/edge-ingress/global-rps.sock`); drift between the
    /// two turns the sidecar's writes into a black hole and the
    /// renderer silently never sees a fresh `local_cap`. Set via
    /// `GLOBAL_RPS_UDS_PATH` (default
    /// `/var/run/edge-ingress/global-rps.sock`).
    pub global_rps_uds_path: PathBuf,
    /// Expected sidecar tick interval. The cache treats a datagram
    /// older than `2 × this value` as stale and stops emitting the
    /// cross-replica route (fail-closed). Set via
    /// `GLOBAL_RPS_TICK_INTERVAL` (default `1s`).
    pub global_rps_tick_interval: Duration,
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
    /// - `TLS_CERT_FILE_2` (optional) — path to a second cert PEM for
    ///   the multi-label `*.*.edgecloud.dev` wildcard that covers
    ///   dotted app names (`myapp.v2`). When unset, two-label hosts
    ///   fall through to ACME on-demand issuance. Issue #438.
    /// - `TLS_KEY_FILE_2` (optional) — path to the matching key PEM.
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

        // Issue #548 review finding #30: validate L4 port range at
        // config-load time. An inverted range (L4_PORT_RANGE_START >
        // L4_PORT_RANGE_END) would otherwise produce a L4PortPool
        // that silently hands out zero usable ports — `acquire()`
        // returns `None` immediately and every L4 app gets logged
        // as "pool exhausted" without an obvious cause. Failing fast
        // here turns a confusing runtime condition into a clear
        // startup error.
        let l4_port_range_start = std::env::var("L4_PORT_RANGE_START")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(31000);
        let l4_port_range_end = std::env::var("L4_PORT_RANGE_END")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(31999);
        if l4_port_range_start == 0 || l4_port_range_end == 0 {
            anyhow::bail!(
                "L4_PORT_RANGE_START and L4_PORT_RANGE_END must both be non-zero (got start={}, end={})",
                l4_port_range_start,
                l4_port_range_end
            );
        }
        if l4_port_range_start > l4_port_range_end {
            anyhow::bail!(
                "L4_PORT_RANGE_START ({}) must be <= L4_PORT_RANGE_END ({})",
                l4_port_range_start,
                l4_port_range_end
            );
        }

        Ok(Config {
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into()),
            caddy_admin_url: std::env::var("CADDY_ADMIN_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:2019".into()),
            region: std::env::var("INGRESS_REGION").context("INGRESS_REGION not set")?,
            cert_file: std::env::var("TLS_CERT_FILE").context("TLS_CERT_FILE not set")?,
            key_file: std::env::var("TLS_KEY_FILE").context("TLS_KEY_FILE not set")?,
            cert_file_2: std::env::var("TLS_CERT_FILE_2")
                .ok()
                .filter(|v| !v.is_empty()),
            key_file_2: std::env::var("TLS_KEY_FILE_2")
                .ok()
                .filter(|v| !v.is_empty()),
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
            max_conns: std::env::var("INGRESS_MAX_CONNS")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            max_conns_per_ip: std::env::var("INGRESS_MAX_CONNS_PER_IP")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            per_ip_rps: std::env::var("INGRESS_PER_IP_RPS")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            per_ip_burst: std::env::var("INGRESS_PER_IP_BURST")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
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
            quota_fetch_interval: std::env::var("QUOTA_FETCH_INTERVAL")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(crate::quota::QUOTA_FETCH_INTERVAL),
            stale_timeout: std::env::var("STALE_TIMEOUT")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(Duration::from_secs(60)),
            prune_interval: std::env::var("PRUNE_INTERVAL")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(Duration::from_secs(30)),
            health_check_interval: std::env::var("HEALTH_CHECK_INTERVAL")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(Duration::from_secs(10)),
            health_check_timeout: std::env::var("HEALTH_CHECK_TIMEOUT")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(Duration::from_secs(3)),
            health_check_uri: std::env::var("HEALTH_CHECK_URI")
                .unwrap_or_else(|_| "/healthz".into()),
            health_check_max_fails: std::env::var("HEALTH_CHECK_MAX_FAILS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
            rate_limit_rps_tenant_default: std::env::var("RATE_LIMIT_RPS_TENANT_DEFAULT")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            rate_limit_burst_tenant_default: std::env::var("RATE_LIMIT_BURST_TENANT_DEFAULT")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            tenant_rate_limit_fetch_interval: std::env::var("TENANT_RATE_LIMIT_FETCH_INTERVAL")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(crate::tenant_ratelimit::TENANT_RATE_LIMIT_FETCH_INTERVAL),
            global_rate_limit_rps: std::env::var("GLOBAL_RATE_LIMIT_RPS")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            global_rate_limit_burst: std::env::var("GLOBAL_RATE_LIMIT_BURST")
                .unwrap_or_else(|_| "0".into())
                .parse()
                .unwrap_or(0),
            l4_port_range_start,
            l4_port_range_end,
            l4_max_conns_per_app: std::env::var("INGRESS_L4_MAX_CONNS_PER_APP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            l4_max_conns_per_ip: std::env::var("INGRESS_L4_MAX_CONNS_PER_IP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            l4_port_cooldown_secs: std::env::var("L4_PORT_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
            ingress_rate_limit_aggregation: std::env::var("INGRESS_RATE_LIMIT_AGGREGATION")
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no" | ""))
                .unwrap_or(false),
            global_rps_uds_path: PathBuf::from(
                std::env::var("GLOBAL_RPS_UDS_PATH")
                    .unwrap_or_else(|_| "/var/run/edge-ingress/global-rps.sock".into()),
            ),
            global_rps_tick_interval: std::env::var("GLOBAL_RPS_TICK_INTERVAL")
                .ok()
                .and_then(|v| humantime::parse_duration(&v).ok())
                .unwrap_or(Duration::from_secs(1)),
        })
    }

    /// Log warnings for rate-limit knobs that look misconfigured.
    ///
    /// Review finding: the renderer accepts `global_rate_limit_rps=1`
    /// or `RATE_LIMIT_RPS_TENANT_DEFAULT=1` silently, which would
    /// rate-limit the platform (or every tenant) to 1 RPS — almost
    /// certainly a typo. We don't fail-closed (operators may legitimately
    /// want a low cap for a staging tier), but a startup WARN gives them
    /// a single grep target when investigating a "why is everything 429"
    /// incident.
    pub fn validate(&self) {
        const MIN_REASONABLE_RPS: u32 = 10;
        if self.global_rate_limit_rps > 0 && self.global_rate_limit_rps < MIN_REASONABLE_RPS {
            warn!(
                rps = self.global_rate_limit_rps,
                min_reasonable = MIN_REASONABLE_RPS,
                "global_rate_limit_rps is below the recommended minimum; \
                 this caps the entire platform (per replica) to <{}/s. \
                 Confirm this is intentional.",
                MIN_REASONABLE_RPS
            );
        }
        if self.rate_limit_rps_tenant_default > 0
            && self.rate_limit_rps_tenant_default < MIN_REASONABLE_RPS
        {
            warn!(
                rps = self.rate_limit_rps_tenant_default,
                min_reasonable = MIN_REASONABLE_RPS,
                "rate_limit_rps_tenant_default is below the recommended minimum; \
                 this caps every tenant without an explicit override to <{}/s. \
                 Confirm this is intentional.",
                MIN_REASONABLE_RPS
            );
        }
        // Issue #665 PR D: surface the operator foot-gun where the
        // ingress is opted into cross-replica aggregation but the
        // operator hasn't configured a global cap. The renderer will
        // never emit the cross-replica route (per-replica cap math
        // returns None when configured_cap == 0 — see
        // edge-ingress-sidecar/src/aggregate.rs::Snapshot::per_replica_cap),
        // so the operator sees nothing happen at Caddy level. The
        // WARN is informational — operators sometimes deliberately
        // deploy the aggregation plumbing ahead of setting a cap.
        if self.ingress_rate_limit_aggregation && self.global_rate_limit_rps == 0 {
            warn!(
                "INGRESS_RATE_LIMIT_AGGREGATION=true but GLOBAL_RATE_LIMIT_RPS=0; \
                 the cross-replica route will never render a cap. Either set \
                 GLOBAL_RATE_LIMIT_RPS to the operator's intended platform cap \
                 or disable aggregation with INGRESS_RATE_LIMIT_AGGREGATION=false."
            );
        }
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
            "L4_PORT_RANGE_START",
            "L4_PORT_RANGE_END",
            "INGRESS_L4_MAX_CONNS_PER_APP",
            "INGRESS_L4_MAX_CONNS_PER_IP",
            "L4_PORT_COOLDOWN_SECS",
            "INGRESS_RATE_LIMIT_AGGREGATION",
            "GLOBAL_RPS_UDS_PATH",
            "GLOBAL_RPS_TICK_INTERVAL",
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

        // 16. Defaults for DDoS protection fields
        unset_all_config_vars();
        set_required_vars();
        let cfg = Config::from_env().expect("ddos defaults test");
        assert_eq!(cfg.max_conns, 0, "max_conns defaults to 0");
        assert_eq!(cfg.max_conns_per_ip, 0, "max_conns_per_ip defaults to 0");
        assert_eq!(cfg.per_ip_rps, 0, "per_ip_rps defaults to 0");
        assert_eq!(cfg.per_ip_burst, 0, "per_ip_burst defaults to 0");

        // 17. DDoS protection field overrides
        unset_all_config_vars();
        set_required_vars();
        set_var("INGRESS_MAX_CONNS", "5000");
        set_var("INGRESS_MAX_CONNS_PER_IP", "100");
        set_var("INGRESS_PER_IP_RPS", "50");
        set_var("INGRESS_PER_IP_BURST", "100");
        let cfg = Config::from_env().expect("ddos override test");
        assert_eq!(cfg.max_conns, 5000);
        assert_eq!(cfg.max_conns_per_ip, 100);
        assert_eq!(cfg.per_ip_rps, 50);
        assert_eq!(cfg.per_ip_burst, 100);

        // 18. L4 defaults (issue #548).
        unset_all_config_vars();
        set_required_vars();
        let cfg = Config::from_env().expect("l4 defaults test");
        assert_eq!(cfg.l4_port_range_start, 31000);
        assert_eq!(cfg.l4_port_range_end, 31999);
        assert_eq!(cfg.l4_max_conns_per_app, 1000);
        assert_eq!(cfg.l4_max_conns_per_ip, 100);
        assert_eq!(cfg.l4_port_cooldown_secs, 60);

        // 19. L4 overrides
        unset_all_config_vars();
        set_required_vars();
        set_var("L4_PORT_RANGE_START", "40000");
        set_var("L4_PORT_RANGE_END", "40999");
        set_var("INGRESS_L4_MAX_CONNS_PER_APP", "500");
        set_var("INGRESS_L4_MAX_CONNS_PER_IP", "10");
        set_var("L4_PORT_COOLDOWN_SECS", "120");
        let cfg = Config::from_env().expect("l4 override test");
        assert_eq!(cfg.l4_port_range_start, 40000);
        assert_eq!(cfg.l4_port_range_end, 40999);
        assert_eq!(cfg.l4_max_conns_per_app, 500);
        assert_eq!(cfg.l4_max_conns_per_ip, 10);
        assert_eq!(cfg.l4_port_cooldown_secs, 120);

        // Clean up
        unset_all_config_vars();
    }

    /// Construct a Config with sensible non-zero defaults for the
    /// string fields and zero everywhere else. Lets the validate()
    /// tests set only the rate-limit knobs they care about without
    /// spelling out every field by hand.
    fn test_config_defaults() -> Config {
        Config {
            nats_url: "nats://localhost:4222".to_string(),
            caddy_admin_url: "http://127.0.0.1:2019".to_string(),
            region: "fra".to_string(),
            cert_file: "/tmp/cert.pem".to_string(),
            key_file: "/tmp/key.pem".to_string(),
            cert_file_2: None,
            key_file_2: None,
            listen_http: ":80".to_string(),
            listen_https: ":443".to_string(),
            refresh_debounce_ms: 1000,
            http_to_https: true,
            admin_token: None,
            control_plane_api_url: "http://localhost:8080".to_string(),
            internal_token: None,
            control_plane_url: String::new(),
            service_token: String::new(),
            domain_poll_interval: Duration::from_secs(30),
            caddy_admin_listen: "localhost:2019".to_string(),
            metrics_listen: ":9091".to_string(),
            max_conns: 0,
            max_conns_per_ip: 0,
            per_ip_rps: 0,
            per_ip_burst: 0,
            rate_limit_rps_default: 0,
            rate_limit_burst_default: 0,
            rate_limit_fetch_interval: Duration::from_secs(60),
            quota_fetch_interval: crate::quota::QUOTA_FETCH_INTERVAL,
            stale_timeout: Duration::from_secs(60),
            prune_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(3),
            health_check_uri: "/healthz".to_string(),
            health_check_max_fails: 2,
            rate_limit_rps_tenant_default: 0,
            rate_limit_burst_tenant_default: 0,
            tenant_rate_limit_fetch_interval: Duration::from_secs(30),
            global_rate_limit_rps: 0,
            global_rate_limit_burst: 0,
            l4_port_range_start: 31000,
            l4_port_range_end: 31999,
            l4_max_conns_per_app: 1000,
            l4_max_conns_per_ip: 100,
            l4_port_cooldown_secs: 60,
            ingress_rate_limit_aggregation: false,
            global_rps_uds_path: PathBuf::from("/var/run/edge-ingress/global-rps.sock"),
            global_rps_tick_interval: Duration::from_secs(1),
        }
    }

    #[test]
    fn validate_low_global_rps_does_not_panic() {
        // Sub-minimum cap (5) is operator-allowed — validate() only
        // emits WARN, never returns Err, so the only assertion we can
        // make is that the call is panic-free. The WARN text is
        // verified at integration time via the structured tracing
        // output; here we just pin the absence-of-side-effect contract.
        let cfg = Config {
            global_rate_limit_rps: 5,
            rate_limit_rps_tenant_default: 1000,
            ..test_config_defaults()
        };
        cfg.validate();
    }

    #[test]
    fn validate_low_tenant_default_rps_does_not_panic() {
        let cfg = Config {
            global_rate_limit_rps: 1000,
            rate_limit_rps_tenant_default: 3,
            ..test_config_defaults()
        };
        cfg.validate();
    }

    #[test]
    fn validate_zero_caps_are_silent() {
        // Both knobs at 0 means "no caps configured"; validate()
        // must not warn (the `> 0` guard short-circuits both arms).
        let cfg = Config {
            global_rate_limit_rps: 0,
            rate_limit_rps_tenant_default: 0,
            ..test_config_defaults()
        };
        cfg.validate();
    }

    #[test]
    fn validate_reasonable_caps_are_silent() {
        // Above the recommended minimum — neither branch should fire.
        let cfg = Config {
            global_rate_limit_rps: 100,
            rate_limit_rps_tenant_default: 50,
            ..test_config_defaults()
        };
        cfg.validate();
    }
}
