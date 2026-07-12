//! Worker configuration loaded from environment variables.

use anyhow::Context;
use edge_runtime::socket_egress::SocketEgressPolicy;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub worker_id: String,
    pub region: String,
    /// The address the public ingress should reverse-proxy to in order to reach
    /// apps on this worker (e.g. `203.0.113.10` or `worker-fra-1.internal:8080`).
    /// Required: the worker fails to start without it. Operators in private VPCs
    /// must set this to a routable IP or domain (Cloud NAT EIP, internal LB, etc.).
    pub worker_addr: String,
    pub nats_url: String,
    pub control_plane_url: String,
    pub cache_dir: PathBuf,
    pub heartbeat_interval_secs: u64,
    pub health_check_timeout_secs: u64,
    /// Threshold for the HTTP /sync fallback watchdog (issue #53).
    /// When the worker hasn't received any TaskMessage for this many
    /// seconds, the heartbeat task pulls the desired-state snapshot
    /// directly from the control plane. Default 60s — long enough that
    /// the periodic CP-side reconcile (5min default) usually catches
    /// up first on a healthy cluster; short enough that an isolated
    /// worker doesn't sit stale for the full NATS retention window.
    #[allow(dead_code)]
    pub worker_sync_threshold_secs: u64,
    pub port_cooldown_secs: u64,
    pub starting_port: u16,
    /// Pre-population size of the worker's port pool. Default 100
    /// (matches the canonical `PortPool::new` default — backward
    /// compatible). Tune via `EDGE_PORT_POOL_SIZE` (issue #641). The
    /// field is clamped to `u16::MAX - starting_port` at the pool
    /// constructor to keep the pre-population range inside the port
    /// space.
    pub port_pool_size: u32,
    /// Per-app memory cap in MiB, applied via wasmtime StoreLimits.
    /// Default 256 MiB. Tune via `APP_MAX_MEMORY_MB`.
    pub max_memory_mb: u64,
    /// How often (ms) the worker advances the wasmtime epoch. Default 10 ms.
    /// Tune via `EPOCH_TICK_MS`.
    pub epoch_tick_ms: u64,
    /// Number of epoch ticks an app call may consume before being interrupted.
    /// With the default tick of 10 ms and deadline of 100, each call has a
    /// ~1 s CPU budget. Tune via `EPOCH_DEADLINE_TICKS`.
    pub epoch_deadline_ticks: u64,
    /// Durable JetStream consumer name. Derived from `worker_id` by default
    /// so each worker has its own cursor and resumes from its last ack on
    /// restart. Override with `EDGE_CONSUMER_NAME`.
    pub consumer_name: String,
    /// NATS JetStream queue group for fan-out delivery within a region
    /// (issue #86). Workers in the same region joined to the same
    /// `queue_group` share a single delivery of each `TaskMessage`, so
    /// exactly one worker per group starts the app.
    /// Override with `EDGE_QUEUE_GROUP`. Empty string disables queue-group
    /// pinning (each consumer receives a copy — the historical fan-out
    /// behavior).
    pub queue_group: String,
    /// Number of JetStream replicas for the `edgecloud-tasks` stream.
    /// Must be 1 on non-clustered NATS (local dev); defaults to 3 for
    /// production. Override with `TASK_STREAM_REPLICAS`.
    pub task_stream_replicas: usize,
    /// HMAC secret the worker uses to sign outbound JWTs to the control
    /// plane's internal endpoints. Must match `JWT_SECRET` on the Go side.
    ///
    /// Optional at startup: a missing/empty value loads as `""` and main()
    /// logs a warning. With no secret, /api/internal/* calls will 401
    /// until the secret is provisioned (e.g. via a bootstrap handshake —
    /// see follow-up issue D). NATS heartbeats and the deployment
    /// supervisor keep working regardless.
    pub worker_jwt_secret: String,
    /// Optional `kid` header value for worker JWTs. Set to the matching
    /// `active_kid` in the control plane's `jwt.keys` config. When set,
    /// the JWT header includes a `kid` field so the CP can select the
    /// correct verification key during rotation. The CP also falls back
    /// to the legacy `jwt.secret` when `kid` is absent, so this is
    /// optional. Override with `WORKER_JWT_KID`.
    pub worker_jwt_kid: Option<String>,
    /// Expected `iss` claim. Must match `JWT_ISSUER` on the Go side.
    /// Defaults to `edgecloud`.
    pub worker_jwt_issuer: String,
    /// The tenant this worker is authorized for. Loaded once at startup
    /// and embedded in the worker's JWT `tenant_id` claim. All outbound
    /// calls to `/api/internal/*` (downloads, logs, sync, registration)
    /// are scoped to this tenant — the control plane only returns
    /// artifacts and data belonging to this tenant ID.
    ///
    /// The worker is **architecturally multi-tenant**: it can host apps
    /// from different tenants simultaneously via per-tenant NATS task
    /// messages. However, because the JWT is signed once at boot, every
    /// HTTP call still carries the same `tenant_id` claim. In practice
    /// this means:
    ///
    /// - A worker whose `WORKER_TENANT_ID=t_a` cannot download artifacts
    ///   or forward logs for apps that belong to `t_b` (even if the NATS
    ///   task message says `tenant_id=t_b`).
    ///
    /// - The `/sync` fallback endpoint returns this tenant's apps, not
    ///   the per-NATS-message tenant.
    ///
    /// A future change should make the JWT per-request (or per-tenant)
    /// so the worker can fully serve multiple tenants. Until then, this
    /// env var is **required** and must match the tenant whose apps this
    /// worker hosts for outbound HTTP calls.
    pub worker_tenant_id: String,
    /// Per-request CPU budget for FaaS (Handler) components, in ms.
    /// Default 1000ms (1s). The store's epoch deadline is set to
    /// `handler_request_budget_ms / epoch_tick_ms` ticks before each
    /// request is dispatched to the guest. Tune via `HANDLER_REQUEST_BUDGET_MS`.
    pub handler_request_budget_ms: u64,
    /// Per-request body-size cap for FaaS (Handler) components, in
    /// bytes. The FaaS dispatcher rejects requests whose
    /// `Content-Length` exceeds this with a 413 before invoking the
    /// guest. Default 10 MiB (matches the v0.1 `edge:http-server`
    /// `DEFAULT_MAX_BODY_SIZE`). Tune via
    /// `HANDLER_MAX_REQUEST_BODY_BYTES`. `0` means "no cap" (NOT
    /// RECOMMENDED in production — a misbehaving tenant can exhaust
    /// worker memory with one POST).
    pub handler_max_request_body_bytes: u64,
    /// Optional path to a PEM-encoded TLS certificate for the FaaS
    /// HandlerDispatch endpoint (issue #209). When set alongside
    /// `tls_key_path`, the dispatch accepts TLS connections in
    /// addition to plaintext. Both must be set for TLS to activate.
    pub tls_cert_path: Option<String>,
    /// Optional path to a PEM-encoded TLS private key for the FaaS
    /// HandlerDispatch endpoint (issue #209). Both `tls_cert_path`
    /// and `tls_key_path` must be set for TLS to activate.
    pub tls_key_path: Option<String>,
    /// Optional bootstrap secret for the bootstrap handshake (issue #104 + #430).
    /// When WORKER_JWT_SECRET is empty AND WORKER_BOOTSTRAP_SECRET is set,
    /// the worker performs the bootstrap handshake on startup:
    ///   1. POST to /api/internal/bootstrap with HMAC-SHA256 signature
    ///      (covers worker_id, region, tenant_id, timestamp, nonce, public_key)
    ///   2. Receive short-lived (5min) bootstrap JWT + enrollment challenge
    ///   3. POST to /api/internal/worker-bootstrap/enroll with an Ed25519
    ///      signature over sha256(public_key || challenge); receive the
    ///      per-worker derived HS256 secret + `wkr_<hex>` kid
    ///   4. Persist {kid, secret, public_key} to disk; restart skips steps 1-3
    pub worker_bootstrap_secret: String,

    /// Path to the worker identity keypair (issue #430).
    /// 32-byte raw Ed25519 seed, mode 0600. Generated on first boot
    /// if absent and reused on subsequent boots so the worker's
    /// `public_key` (and therefore its `kid`) stays stable across
    /// restarts. The on-disk path can be overridden by
    /// `EDGE_WORKER_KEY` (inline lowercase-hex seed) — useful for
    /// containers / immutable images where mounting a per-pod secret
    /// file is impractical. Default: `.worker-cache/identity.key`.
    pub worker_key_path: PathBuf,
    /// Path to the persisted per-worker signing secret (issue #430).
    /// Written by the bootstrap enrollment handshake on first boot
    /// and read on subsequent boots to skip the handshake. Default:
    /// `.worker-cache/identity.key`. (Same default as
    /// `worker_key_path`; keeping the two fields separate lets the
    /// operator mount each from a different volume.)
    pub worker_identity_path: PathBuf,
    /// Force re-enrollment on every boot (issue #430). When `true`,
    /// the worker ignores any persisted signing secret and runs the
    /// bootstrap handshake again. Operators rotate this with
    /// `EDGE_WORKER_REENROLL_ON_BOOT=true` for a planned rotation;
    /// the new kid becomes effective on the next restart. Default:
    /// `false`.
    pub worker_reenroll_on_boot: bool,

    /// Socket-egress mode for `wasi:sockets/{tcp,udp}` (issue #309).
    /// Read **once** at worker startup from `EDGE_EGRESS_SOCKET_MODE`
    /// (`block-all` (default, closes wasi:sockets connect-side),
    /// `allowlist` (consult `EgressPolicy::check_address`),
    /// `allow-all` (operator opt-in),
    /// `hostname-pinned` (consult `EgressPolicy::hostname_pinned_match`
    /// against the per-`Network` `HostnamePinning` cache — **dormant
    /// today**; equals `block-all` until the upstream wasmtime-wasi
    /// patch in `docs/upstream-wasmtime-resolve-check.patch` merges)).
    ///
    /// Posted into every `HandlerConfig` constructed by the supervisor,
    /// which threads it into `RuntimeState::with_env_and_meter` as a
    /// parameter — the per-request code path does **not** read the
    /// env var again (avoiding the per-request syscall the v0.2 review
    /// flagged as a perf regression). Mirrors the
    /// `handler_max_request_body_bytes` pattern above.
    pub socket_mode: SocketEgressPolicy,

    /// Per-deployment `HostnamePinned` mode toggle (issue #309
    /// follow-up). Read **once** at worker startup from
    /// `EDGE_EGRESS_HOSTNAME_PINNING` (parsed 1/0, true/false, yes/no,
    /// on/off, case-insensitive — default `false`).
    ///
    /// When `true`, the per-request `RuntimeState` swap uses
    /// `SocketEgressPolicy::HostnamePinned` instead of the worker-wide
    /// `Config::socket_mode`. Today this is dormant (the upstream
    /// resolve hook has not merged, so the `HostnamePinning` cache
    /// stays empty and `HostnamePinned` denies every connect-side
    /// call — observable parity with `BlockAll`). Once the patch
    /// merges, set this to `true` on the worker and the admit paths
    /// light up.
    pub hostname_pinning_enabled: bool,
    /// Configured size of the warm standby pool of Wasmtime engines.
    /// Default is 10. Configure via `EDGE_STANDBY_POOL_SIZE`.
    pub standby_pool_size: usize,

    /// Ed25519 artifact-signature enforcement (issue #307, PR2 + PR1
    /// follow-up multi-keyring). `true` (the default —
    /// secure-by-default) means the worker refuses to instantiate
    /// any artifact whose `AppSpec` lacks a `deployment_signature`
    /// field, AND verifies the signature against a key in the
    /// configured keyring before instantiation. `false` is the
    /// rollout escape hatch: a worker started with
    /// `EDGE_REQUIRE_SIGNATURE=false` accepts unsigned artifacts.
    pub require_signature: bool,
    /// Inline keyring payload for the artifact-signature verifier
    /// (issue #307 PR1 follow-up). Format: one
    /// `<kid> = <64-lowercase-hex>` per line, same as the file format
    /// — `Keyring::from_inline` parses both. Set via
    /// `EDGE_SIGNING_KEYRING`. When `EDGE_SIGNING_KEYRING` and
    /// `EDGE_SIGNING_KEYRING_PATH` are both set, `PATH` wins
    /// (matches the CP-side precedence: explicit file > inline).
    pub signing_keyring: Option<String>,
    /// Path to a keyring file for the artifact-signature verifier.
    /// Set via `EDGE_SIGNING_KEYRING_PATH`. See `signing_keyring`
    /// for the file format.
    pub signing_keyring_path: Option<String>,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// Required env vars:
    /// - `WORKER_ID` (e.g., `w_fra_abc123`)
    /// - `REGION` (e.g., `fra`)
    /// - `CONTROL_PLANE_URL` (e.g., `https://api.edgecloud.dev`)
    /// - `EDGE_WORKER_ADDR` (e.g., `203.0.113.10`) — the routable address of
    ///   this worker for the public ingress. Required: silent defaults have
    ///   produced every past "URL works for me but not for users" incident.
    /// - `WORKER_JWT_SECRET` (HMAC secret shared with the control plane)
    /// - `WORKER_TENANT_ID` (e.g., `t_tenant1`) — the tenant ID whose apps
    ///   this worker hosts. Required: scopes all `/api/internal/*` calls
    ///   (downloads, logs, sync) to this tenant. See struct field docs.
    ///
    /// Optional env vars:
    /// - `NATS_URL` (default: `nats://localhost:4222`)
    /// - `CACHE_DIR` (default: `.worker-cache`)
    /// - `APP_MAX_MEMORY_MB` (default: 256)
    /// - `EPOCH_TICK_MS` (default: 10)
    /// - `EPOCH_DEADLINE_TICKS` (default: 100)
    /// - `EDGE_QUEUE_GROUP` (default: `edgecloud-workers`)
    /// - `EDGE_CONSUMER_NAME` (default: derived from `WORKER_ID`)
    /// - `WORKER_JWT_ISSUER` (default: `edgecloud`)
    /// - `WORKER_JWT_SECRET` (default: empty — see warning in main.rs)
    /// - `EDGE_WORKER_LOG_LEVEL` (default: `info`) — minimum level the
    ///   worker log layer ships to the control plane via `LogForwarder`.
    ///   Independent of `RUST_LOG`, which still controls local stdout
    ///   verbosity via `EnvFilter`. See `forwarder_log_level`.
    /// - `TASK_STREAM_REPLICAS` (default: `3`) — JetStream replica count
    ///   for the `edgecloud-tasks` stream. Set to `1` for non-clustered
    ///   NATS (local dev).
    /// - `EDGE_EGRESS_SOCKET_MODE` (default: `block-all`) — see
    ///   `Config::socket_mode`.
    /// - `EDGE_REQUIRE_SIGNATURE` (default: `true`) — refuse to
    ///   instantiate an artifact without a valid Ed25519 signature
    ///   (issue #307). Set to `false` for the rollout window if the
    ///   worker is paired with a pre-PR1 control plane.
    /// - `EDGE_SIGNING_KEYRING` (default: unset) — inline keyring
    ///   payload; one `<kid> = <64-lowercase-hex>` per line. Useful for
    ///   dev / single-key setups (issue #307 PR1 follow-up multi-keyring).
    /// - `EDGE_SIGNING_KEYRING_PATH` (default: unset) — file containing
    ///   a keyring payload, same format as the inline form. When both
    ///   `EDGE_SIGNING_KEYRING_PATH` and `EDGE_SIGNING_KEYRING` are
    ///   set, `PATH` wins (matches the CP-side precedence: explicit
    ///   file > inline). Required when `EDGE_REQUIRE_SIGNATURE=true`.
    ///   Production recommendation.
    pub fn from_env() -> anyhow::Result<Self> {
        let worker_id = std::env::var("WORKER_ID").context("WORKER_ID not set")?;
        let consumer_name =
            std::env::var("EDGE_CONSUMER_NAME").unwrap_or_else(|_| format!("worker-{}", worker_id));
        // Guard against operator misconfiguration where two workers
        // share `EDGE_CONSUMER_NAME`. JetStream's `get_or_create_consumer`
        // is name-keyed; if two workers collide they end up on the same
        // durable cursor and one will silently do all the work while the
        // other sits idle — defeating issue #86's queue-group pinning.
        // The safest default is to require the consumer name to embed the
        // worker_id; an explicit override that omits it is almost always a
        // misconfiguration.
        if consumer_name != format!("worker-{}", worker_id) && !consumer_name.contains(&worker_id) {
            anyhow::bail!(
                "EDGE_CONSUMER_NAME={:?} does not contain WORKER_ID={:?}; \
                 a shared consumer name causes duplicate-app-style collisions \
                 across workers in the same region. Unset EDGE_CONSUMER_NAME \
                 to use the default (worker-{{WORKER_ID}}), or include the \
                 worker_id in the override.",
                consumer_name,
                worker_id,
            );
        }
        let cfg = Config {
            task_stream_replicas: parse_env_usize("TASK_STREAM_REPLICAS", 3)?,
            consumer_name,
            queue_group: std::env::var("EDGE_QUEUE_GROUP").unwrap_or_default(),
            worker_id,
            region: std::env::var("REGION").context("REGION not set")?,
            worker_addr: std::env::var("EDGE_WORKER_ADDR").context("EDGE_WORKER_ADDR not set")?,
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into()),
            control_plane_url: std::env::var("CONTROL_PLANE_URL")
                .context("CONTROL_PLANE_URL not set")?,
            cache_dir: std::env::var("CACHE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(".worker-cache")),
            heartbeat_interval_secs: 30,
            worker_sync_threshold_secs: std::env::var("EDGE_WORKER_SYNC_THRESHOLD_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
            health_check_timeout_secs: std::env::var("EDGE_HEALTH_CHECK_TIMEOUT_SECS")
                .unwrap_or_else(|_| "60".into())
                .parse()
                .unwrap_or(60),
            port_cooldown_secs: 60,
            starting_port: 8081,
            port_pool_size: {
                let raw = std::env::var("EDGE_PORT_POOL_SIZE").unwrap_or_else(|_| "100".into());
                let n: u32 = raw
                    .parse()
                    .with_context(|| format!("EDGE_PORT_POOL_SIZE={raw:?} is not a valid u32"))?;
                if n < 1 || n > u16::MAX as u32 {
                    anyhow::bail!(
                        "EDGE_PORT_POOL_SIZE={n} out of range [1, {}]; \
                         the pool constructor clamps to (u16::MAX - starting_port)",
                        u16::MAX
                    );
                }
                n
            },
            max_memory_mb: parse_env_u64("APP_MAX_MEMORY_MB", 256)?,
            epoch_tick_ms: parse_env_u64("EPOCH_TICK_MS", 10)?,
            epoch_deadline_ticks: parse_env_u64("EPOCH_DEADLINE_TICKS", 100)?,
            worker_jwt_secret: std::env::var("WORKER_JWT_SECRET").unwrap_or_default(),
            worker_jwt_kid: std::env::var("WORKER_JWT_KID").ok(),
            worker_jwt_issuer: std::env::var("WORKER_JWT_ISSUER")
                .unwrap_or_else(|_| "edgecloud".into()),
            worker_tenant_id: std::env::var("WORKER_TENANT_ID").unwrap_or_else(|_| "*".into()),
            handler_request_budget_ms: parse_env_u64("HANDLER_REQUEST_BUDGET_MS", 1000)?,
            handler_max_request_body_bytes: parse_env_u64(
                "HANDLER_MAX_REQUEST_BODY_BYTES",
                10 * 1024 * 1024,
            )?,
            tls_cert_path: std::env::var("EDGE_TLS_CERT_PATH").ok(),
            tls_key_path: std::env::var("EDGE_TLS_KEY_PATH").ok(),
            worker_bootstrap_secret: std::env::var("WORKER_BOOTSTRAP_SECRET").unwrap_or_default(),
            // Issue #430: per-worker identity keypair + persisted
            // signing secret. The defaults are colocated in
            // `.worker-cache/` next to the artifact cache; operators
            // can split them across volumes by setting both env
            // vars explicitly. The identity and the secret live in
            // the same file by default — they're written atomically
            // together by `auth::persist_identity` — but keeping
            // the config fields distinct lets future code (e.g. a
            // planned HMAC-secret-only rotation) reference either
            // independently.
            worker_key_path: std::env::var("EDGE_WORKER_KEY_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(".worker-cache/identity.key")),
            worker_identity_path: std::env::var("EDGE_WORKER_IDENTITY_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(".worker-cache/identity.key")),
            worker_reenroll_on_boot: parse_env_bool("EDGE_WORKER_REENROLL_ON_BOOT", false)?,
            socket_mode: SocketEgressPolicy::from_env(),
            hostname_pinning_enabled: parse_env_bool("EDGE_EGRESS_HOSTNAME_PINNING", false)?,
            standby_pool_size: parse_env_usize("EDGE_STANDBY_POOL_SIZE", 10)?,
            // Issue #307 PR2 + PR1 follow-up: signature verification
            // config. The default for `require_signature` is `true`
            // (secure-by-default) — a worker that boots with signing
            // disabled would silently accept unsigned artifacts and
            // undo the rollout's threat model. Operators who need the
            // escape hatch explicitly set `EDGE_REQUIRE_SIGNATURE=false`.
            require_signature: parse_env_bool("EDGE_REQUIRE_SIGNATURE", true)?,
            signing_keyring: std::env::var("EDGE_SIGNING_KEYRING").ok(),
            signing_keyring_path: std::env::var("EDGE_SIGNING_KEYRING_PATH").ok(),
        };

        // Validate the signature-config invariant: secure-by-default
        // means a worker with `require_signature=true` MUST have a
        // keyring configured. Without one, the verifier is None and
        // the worker would refuse to start any deployment — better to
        // fail at boot with a clear message than to surface a
        // confusing "missing pubkey" error on every task message.
        if cfg.require_signature
            && cfg.signing_keyring.is_none()
            && cfg.signing_keyring_path.is_none()
        {
            anyhow::bail!(
                "EDGE_REQUIRE_SIGNATURE=true but neither EDGE_SIGNING_KEYRING nor \
                 EDGE_SIGNING_KEYRING_PATH is set. With secure-by-default, the worker \
                 refuses to start until a keyring is configured. Set \
                 EDGE_REQUIRE_SIGNATURE=false to allow unsigned artifacts during the rollout."
            );
        }

        Ok(cfg)
    }

    /// Returns the minimum level the worker log layer ships to the
    /// control plane. Default: `info`. Override via `EDGE_WORKER_LOG_LEVEL`
    /// (one of `trace`, `debug`, `info`, `warn`, `error`; unknown values
    /// fall back to `info`). Independent of `RUST_LOG`, which still
    /// controls local stdout verbosity via the `EnvFilter`.
    ///
    /// The two knobs deliberately diverge: `RUST_LOG=info,edge_worker=debug`
    /// sets *stdout* to debug for the worker crate, while
    /// `EDGE_WORKER_LOG_LEVEL=debug` sets the *forwarder* threshold to
    /// debug. Most operators will leave both at `info`.
    pub fn forwarder_log_level(&self) -> tracing::Level {
        match std::env::var("EDGE_WORKER_LOG_LEVEL")
            .unwrap_or_else(|_| "info".into())
            .to_lowercase()
            .as_str()
        {
            "trace" => tracing::Level::TRACE,
            "debug" => tracing::Level::DEBUG,
            "info" => tracing::Level::INFO,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        }
    }
}

/// Parse an integer-valued environment variable, falling back to `default`
/// when unset. Returns an error (rather than silently using the default) when
/// the variable is set but not a valid non-negative integer — operators
/// debugging a misconfiguration prefer a startup failure over a mystery
/// default.
fn parse_env_u64(name: &str, default: u64) -> anyhow::Result<u64> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(s) => s
            .parse::<u64>()
            .with_context(|| format!("{} must be a non-negative integer (got {:?})", name, s)),
    }
}

fn parse_env_usize(name: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(s) => s
            .parse::<usize>()
            .with_context(|| format!("{} must be a non-negative integer (got {:?})", name, s)),
    }
}

/// Parse a boolean environment variable. Accepts the common truthy
/// spellings (`1`, `true`, `yes`, `on`) and falsy spellings (`0`,
/// `false`, `no`, `off`), case-insensitive. Any other value is
/// rejected with a clear error rather than silently coerced to the
/// default — operators debugging `EDGE_REQUIRE_SIGNATURE=true_or_false`
/// prefer a startup failure to a mystery default.
///
/// Mirrors the `parse_env_u64` style: missing → `default`, present
/// but unparseable → `Err` with the var name + value in the message.
fn parse_env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(s) => {
            let lower = s.to_ascii_lowercase();
            match lower.as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => anyhow::bail!(
                    "{} must be a boolean (true/false/1/0/yes/no/on/off, got {:?})",
                    name,
                    s
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serializes env-mutating tests. The Rust test runner executes tests in
    /// parallel by default; without this lock concurrent tests would stomp on
    /// each other's env-var values and produce flaky failures.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that sets an env var for the duration of a test and
    /// restores its previous value on Drop. Holds `ENV_LOCK` for the test's
    /// lifetime so env mutations don't race.
    struct EnvGuard {
        key: String,
        prev: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn set(key: &str, value: &str) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var(key).ok();
            // Safety: serialized via ENV_LOCK above.
            unsafe { std::env::set_var(key, value) };
            Self {
                key: key.to_string(),
                prev,
                _lock: lock,
            }
        }

        fn unset(key: &str) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var(key).ok();
            unsafe { std::env::remove_var(key) };
            Self {
                key: key.to_string(),
                prev,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => unsafe { std::env::set_var(&self.key, v) },
                None => unsafe { std::env::remove_var(&self.key) },
            }
        }
    }

    #[test]
    fn parse_env_u64_returns_default_when_unset() {
        let _g = EnvGuard::unset("EDGE_TEST_VAR");
        assert_eq!(parse_env_u64("EDGE_TEST_VAR", 42).unwrap(), 42);
    }

    #[test]
    fn parse_env_u64_parses_valid_value() {
        let _g = EnvGuard::set("EDGE_TEST_VAR", "1024");
        assert_eq!(parse_env_u64("EDGE_TEST_VAR", 42).unwrap(), 1024);
    }

    #[test]
    fn parse_env_u64_parses_zero() {
        let _g = EnvGuard::set("EDGE_TEST_VAR", "0");
        assert_eq!(parse_env_u64("EDGE_TEST_VAR", 42).unwrap(), 0);
    }

    #[test]
    fn parse_env_u64_errors_on_non_integer() {
        let _g = EnvGuard::set("EDGE_TEST_VAR", "hello");
        let err = parse_env_u64("EDGE_TEST_VAR", 42).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("EDGE_TEST_VAR"),
            "error should name the var: {}",
            msg
        );
        assert!(
            msg.contains("hello"),
            "error should include the bad value: {}",
            msg
        );
    }

    #[test]
    fn parse_env_u64_errors_on_negative_string() {
        let _g = EnvGuard::set("EDGE_TEST_VAR", "-1");
        let err = parse_env_u64("EDGE_TEST_VAR", 42).unwrap_err();
        // u64 can't represent -1, so we expect a parse error.
        assert!(format!("{:#}", err).contains("EDGE_TEST_VAR"));
    }

    #[test]
    fn parse_env_bool_returns_default_when_unset() {
        let _g = EnvGuard::unset("EDGE_TEST_VAR");
        assert!(parse_env_bool("EDGE_TEST_VAR", true).unwrap());
        assert!(!parse_env_bool("EDGE_TEST_VAR", false).unwrap());
    }

    #[test]
    fn parse_env_bool_accepts_truthy_tokens() {
        for tok in ["1", "true", "TRUE", "yes", "YES", "on", "On"] {
            let _g = EnvGuard::set("EDGE_TEST_VAR", tok);
            assert!(
                parse_env_bool("EDGE_TEST_VAR", false).unwrap(),
                "expected true for token {:?}",
                tok
            );
        }
    }

    #[test]
    fn parse_env_bool_accepts_falsy_tokens() {
        for tok in ["0", "false", "FALSE", "no", "NO", "off", "Off"] {
            let _g = EnvGuard::set("EDGE_TEST_VAR", tok);
            assert!(
                !parse_env_bool("EDGE_TEST_VAR", true).unwrap(),
                "expected false for token {:?}",
                tok
            );
        }
    }

    #[test]
    fn parse_env_bool_errors_on_unknown_value() {
        let _g = EnvGuard::set("EDGE_TEST_VAR", "maybe");
        let err = parse_env_bool("EDGE_TEST_VAR", false).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("EDGE_TEST_VAR"),
            "error should name the var: {}",
            msg
        );
        assert!(
            msg.contains("maybe"),
            "error should include the bad value: {}",
            msg
        );
    }

    /// `Config::from_env` requires WORKER_ID, REGION, and CONTROL_PLANE_URL
    /// to be set. Tests that exercise the full `from_env` path need to set
    /// all three; missing any of them produces a clear error.
    ///
    /// These tests set env vars directly under a single manual ENV_LOCK
    /// acquisition. The existing EnvGuard helper takes the lock internally
    /// and is non-reentrant, so creating more than one EnvGuard per test
    /// deadlocks. Direct mutation under a held lock is the only safe
    /// pattern for tests that need several env vars.
    fn lock_and_set(vars: &[(&str, Option<&str>)]) -> std::sync::MutexGuard<'static, ()> {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        for (k, v) in vars {
            match v {
                Some(s) => unsafe { std::env::set_var(k, s) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        lock
    }

    /// `Config::from_env` reads APP_MAX_MEMORY_MB and passes it to the
    /// supervisor's create_store call. Without this test, the field could
    /// regress to a hardcoded 256 (the previous behavior) and the
    /// env-var knob would become decorative.
    #[test]
    fn config_from_env_reads_max_memory_mb() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test_abc")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://localhost:8080")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("APP_MAX_MEMORY_MB", Some("64")),
            // Issue #307 PR2: secure-by-default requires a pubkey
            // unless signing is explicitly disabled for this test.
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
        ]);
        let cfg = Config::from_env().expect("from_env");
        assert_eq!(cfg.max_memory_mb, 64, "APP_MAX_MEMORY_MB should be 64");
    }

    /// EPOCH_TICK_MS and EPOCH_DEADLINE_TICKS together define the per-app
    /// CPU budget. The supervisor spawns a ticker at EPOCH_TICK_MS and
    /// sets a deadline of EPOCH_DEADLINE_TICKS — defaults of 10 ms and
    /// 100 ticks yield a ~1 s budget per call.
    #[test]
    fn config_from_env_reads_epoch_fields() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test_abc")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://localhost:8080")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EPOCH_TICK_MS", Some("5")),
            ("EPOCH_DEADLINE_TICKS", Some("50")),
            // Issue #307 PR2: same rationale as
            // config_from_env_reads_max_memory_mb.
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
        ]);
        let cfg = Config::from_env().expect("from_env");
        assert_eq!(cfg.epoch_tick_ms, 5, "EPOCH_TICK_MS should be 5");
        assert_eq!(
            cfg.epoch_deadline_ticks, 50,
            "EPOCH_DEADLINE_TICKS should be 50"
        );
    }

    /// When the env vars are unset, the defaults (256 / 10 / 100) take
    /// effect. Pinning the defaults in a test catches accidental
    /// regressions where a future refactor changes the fallback.
    #[test]
    fn config_from_env_uses_defaults_when_unset() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test_abc")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://localhost:8080")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("APP_MAX_MEMORY_MB", None),
            ("EPOCH_TICK_MS", None),
            ("EPOCH_DEADLINE_TICKS", None),
            // Issue #307 PR2: this test focuses on memory/epoch
            // defaults; disable secure-by-default to let the config
            // load (the require_signature defaults are asserted in
            // their own tests below).
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
        ]);
        let cfg = Config::from_env().expect("from_env");
        assert_eq!(cfg.max_memory_mb, 256, "default max_memory_mb is 256");
        assert_eq!(cfg.epoch_tick_ms, 10, "default epoch_tick_ms is 10");
        assert_eq!(
            cfg.epoch_deadline_ticks, 100,
            "default epoch_deadline_ticks is 100"
        );
    }

    /// `WORKER_JWT_SECRET` is intentionally optional (see main.rs warning):
    /// when unset the worker still starts, with every `/api/internal/*`
    /// call returning 401 until the secret is provisioned. This test pins
    /// the optional behavior so a future "fix" can't silently make it
    /// required again without a conscious decision.
    #[test]
    fn worker_jwt_secret_is_optional() {
        // Take the same lock as the EnvGuard-based tests above so env
        // mutations don't race with them. We acquire the lock once for
        // the whole test instead of across two `lock_and_set` calls —
        // shadowing `let _g = lock_and_set(...)` would deadlock because
        // Rust evaluates the RHS before dropping the old binding.
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("WORKER_JWT_SECRET", None),
            // Issue #307 PR2: this test predates signature
            // verification; lock the secure-by-default invariant off so
            // its absence doesn't trigger the new "no pubkey, refusing
            // to start" validator when this test runs after one that
            // unset `EDGE_REQUIRE_SIGNATURE` (nextest reorders across
            // worker threads).
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
        ]);
        let cfg = Config::from_env().expect("from_env with no JWT secret");
        assert_eq!(
            cfg.worker_jwt_secret, "",
            "missing WORKER_JWT_SECRET must load as empty string (not error)"
        );

        // Round-trip: setting the env var flows through to the config.
        // SAFETY: serialized by ENV_LOCK above (held in `_g`).
        unsafe { std::env::set_var("WORKER_JWT_SECRET", "round-trip-secret") };
        let cfg = Config::from_env().expect("from_env with JWT secret");
        assert_eq!(cfg.worker_jwt_secret, "round-trip-secret");
    }

    // ── Signature config tests (issue #307 PR2 + PR1 follow-up) ─────────
    //
    // The six tests cover the keyring resolution paths, the
    // secure-by-default fail-fast, and the env-var plumbing that
    // `main.rs` consumes to build the `Keyring`:
    //   1. require_signature defaults to true when EDGE_REQUIRE_SIGNATURE
    //      is unset.
    //   2. require_signature=false is honored when explicitly set.
    //   3. EDGE_SIGNING_KEYRING (inline) is captured.
    //   4. EDGE_SIGNING_KEYRING_PATH (file path) is captured.
    //   5. require_signature=true without any keyring source fails fast
    //      (the secure-by-default invariant).
    //   6. Both keyring env vars unset round-trip to None (the
    //      default-required-signature path is exercised in test 1;
    //      here we cover the opt-out path).

    /// `EDGE_REQUIRE_SIGNATURE` is unset → defaults to true. Pins the
    /// secure-by-default contract: a future "fix" that flips the
    /// default to false would silently undo the rollout's threat
    /// model, and this test catches it.
    #[test]
    fn config_from_env_require_signature_defaults_true() {
        // Default `true` + no keyring → secure-by-default must fail.
        // To exercise the *default value* of the bool, set an inline
        // keyring so the validator's invariant check passes.
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_REQUIRE_SIGNATURE", None),
            (
                "EDGE_SIGNING_KEYRING",
                Some("k1 = 0000000000000000000000000000000000000000000000000000000000000000"),
            ),
            ("EDGE_SIGNING_KEYRING_PATH", None),
        ]);
        let cfg = Config::from_env().expect("from_env with inline keyring");
        assert!(
            cfg.require_signature,
            "EDGE_REQUIRE_SIGNATURE must default to true when unset"
        );
        assert_eq!(
            cfg.signing_keyring.as_deref(),
            Some("k1 = 0000000000000000000000000000000000000000000000000000000000000000")
        );
    }

    /// `EDGE_REQUIRE_SIGNATURE=false` is honored verbatim. The escape
    /// hatch exists for the rollout window; this test pins the
    /// bool-parser contract for the false path.
    #[test]
    fn config_from_env_require_signature_explicit_false() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
            ("EDGE_SIGNING_KEYRING", None),
            ("EDGE_SIGNING_KEYRING_PATH", None),
        ]);
        let cfg = Config::from_env().expect("from_env with require_signature=false");
        assert!(
            !cfg.require_signature,
            "explicit EDGE_REQUIRE_SIGNATURE=false must round-trip"
        );
        assert!(
            cfg.signing_keyring.is_none(),
            "no inline keyring set → None"
        );
    }

    /// `EDGE_SIGNING_KEYRING` (inline `<kid> = <64-hex>` payload) is
    /// captured as-is in the config. We don't validate the format here
    /// — that's `Keyring::from_inline`'s job — only that the env var
    /// is plumbed through.
    #[test]
    fn config_from_env_signing_keyring_inline() {
        // Inline `<kid> = <64-hex>` payload — kid `k1`, 64 lowercase
        // hex chars of arbitrary content (the test only checks env
        // plumbing; format validation lives in `Keyring::from_inline`).
        let payload = format!("k1 = {}", "ab".repeat(32));
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_REQUIRE_SIGNATURE", Some("true")),
            ("EDGE_SIGNING_KEYRING", Some(payload.as_str())),
            ("EDGE_SIGNING_KEYRING_PATH", None),
        ]);
        let cfg = Config::from_env().expect("from_env with inline keyring");
        assert_eq!(cfg.signing_keyring.as_deref(), Some(payload.as_str()));
    }

    /// `EDGE_SIGNING_KEYRING_PATH` is captured as a path string. The
    /// file is NOT read at config-load time — `main.rs` reads it
    /// after `Config::from_env` and constructs the keyring. Here we
    /// just pin that the path is plumbed.
    #[test]
    fn config_from_env_signing_keyring_from_file() {
        let path = "/etc/edge/signing.keyring";
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_REQUIRE_SIGNATURE", Some("true")),
            ("EDGE_SIGNING_KEYRING", None),
            ("EDGE_SIGNING_KEYRING_PATH", Some(path)),
        ]);
        let cfg = Config::from_env().expect("from_env with keyring file path");
        assert_eq!(cfg.signing_keyring_path.as_deref(), Some(path));
        assert!(
            cfg.signing_keyring.is_none(),
            "no inline keyring set → None"
        );
    }

    /// Secure-by-default: `require_signature=true` + no keyring source
    /// is a fatal startup error. The error message must mention both
    /// env var names so the operator knows exactly what to set.
    #[test]
    fn config_from_env_require_signature_true_without_key_fails() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_REQUIRE_SIGNATURE", Some("true")),
            ("EDGE_SIGNING_KEYRING", None),
            ("EDGE_SIGNING_KEYRING_PATH", None),
        ]);
        let err = Config::from_env().expect_err("require_signature=true + no key must fail");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("EDGE_REQUIRE_SIGNATURE")
                && msg.contains("EDGE_SIGNING_KEYRING")
                && msg.contains("EDGE_SIGNING_KEYRING_PATH"),
            "error must name all three env vars; got: {msg}"
        );
    }

    /// Both keyring env vars unset, with the secure-by-default
    /// escape hatch also unset, round-trips to None + require=true.
    /// This is the *negative* mirror of test 1: the secure-by-default
    /// validator short-circuits before reaching the keyring
    /// plumbing, so we cover the "no keyring at all + require=true"
    /// path here explicitly.
    #[test]
    fn config_from_env_both_keyring_envs_unset_round_trip() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_REQUIRE_SIGNATURE", Some("true")),
            ("EDGE_SIGNING_KEYRING", None),
            ("EDGE_SIGNING_KEYRING_PATH", None),
        ]);
        let err = Config::from_env().expect_err(
            "no keyring + require_signature=true must fail the secure-by-default check",
        );
        // The error must surface the *kid* failure mode for ops
        // debugging after a rotation — the failure should mention
        // both keyring env vars (not the legacy EDGE_SIGNING_PUBKEY
        // names from PR2; the operator has been told to migrate).
        let msg = format!("{:#}", err);
        assert!(
            !msg.contains("EDGE_SIGNING_PUBKEY") && !msg.contains("EDGE_SIGNING_PUBKEY_PATH"),
            "error must NOT mention the legacy PR2 env vars; got: {msg}"
        );
    }

    /// Issue #641: EDGE_PORT_POOL_SIZE env-tunes `PortPool` pre-population
    /// without rebuilding. Without this test the field could regress to
    /// the hardcoded 100 (the historical default) and the env var would
    /// become decorative — defeating operators' ability to expand pool
    /// capacity for high-density regions.
    #[test]
    fn config_from_env_reads_port_pool_size() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test_pps")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://localhost:8080")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_PORT_POOL_SIZE", Some("200")),
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
        ]);
        let cfg = Config::from_env().expect("from_env");
        assert_eq!(cfg.port_pool_size, 200, "EDGE_PORT_POOL_SIZE should be 200");
        // lock_and_set is non-restoring — clean up so the var doesn't
        // leak into the next test under cargo's parallel runner.
        unsafe { std::env::remove_var("EDGE_PORT_POOL_SIZE") };
    }

    /// EDGE_PORT_POOL_SIZE unset → default 100 (backward compatible).
    /// Pins the issue #641 contract that operators can leave the knob
    /// unset and see the pre-PR behavior.
    #[test]
    fn config_from_env_port_pool_size_defaults_to_100() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test_pps")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://localhost:8080")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_PORT_POOL_SIZE", None),
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
        ]);
        let cfg = Config::from_env().expect("from_env");
        assert_eq!(
            cfg.port_pool_size, 100,
            "default port_pool_size should be 100"
        );
        unsafe { std::env::remove_var("EDGE_PORT_POOL_SIZE") };
    }

    /// EDGE_PORT_POOL_SIZE=0 must fail with a clear range error rather
    /// than silently pre-populating 0 ports (which would make every
    /// `start_app` immediately fail).
    #[test]
    fn config_from_env_port_pool_size_zero_is_rejected() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test_pps")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://localhost:8080")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("EDGE_PORT_POOL_SIZE", Some("0")),
            ("EDGE_REQUIRE_SIGNATURE", Some("false")),
        ]);
        let err = Config::from_env().expect_err("EDGE_PORT_POOL_SIZE=0 must be rejected");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("EDGE_PORT_POOL_SIZE"),
            "error must name the env var; got: {msg}"
        );
        // lock_and_set is non-restoring — explicitly clean up so the
        // var doesn't leak into the next test (the unset default is
        // what every other config test relies on).
        unsafe { std::env::remove_var("EDGE_PORT_POOL_SIZE") };
    }
}
