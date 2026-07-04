//! Worker configuration loaded from environment variables.

use anyhow::Context;
use std::path::PathBuf;

// `max_memory_mb`, `epoch_tick_ms`, and `epoch_deadline_ticks` are read from
// env vars and consumed by the supervisor (PR #64 follow-up). They plumb
// per-app wasmtime limits: StoreLimits for memory, and an epoch ticker +
// deadline for CPU budgets. The previous PR deferred the wiring; this PR
// closes the loop and removes the dead_code allow.
//
// `queue_group` and `consumer_name` (PR #96) drive the JetStream push
// consumer: every worker in a region joins `queue_group` so a TaskMessage
// is delivered to exactly one worker, and `consumer_name` is the durable
// cursor identity (derived from `worker_id` by default).

use crate::nats::DEFAULT_QUEUE_GROUP;

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
    /// NATS queue group this worker subscribes to. All workers in a region
    /// join the same group so that NATS delivers each `TaskMessage` to
    /// exactly one worker — preventing duplicate app starts across workers.
    /// Override with `EDGE_QUEUE_GROUP`.
    pub queue_group: String,
    /// Durable JetStream consumer name. Derived from `worker_id` by default
    /// so each worker has its own cursor and resumes from its last ack on
    /// restart. Override with `EDGE_CONSUMER_NAME`.
    pub consumer_name: String,
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
        Ok(Config {
            task_stream_replicas: parse_env_usize("TASK_STREAM_REPLICAS", 3)?,
            queue_group: std::env::var("EDGE_QUEUE_GROUP")
                .unwrap_or_else(|_| DEFAULT_QUEUE_GROUP.to_string()),
            consumer_name,
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
            max_memory_mb: parse_env_u64("APP_MAX_MEMORY_MB", 256)?,
            epoch_tick_ms: parse_env_u64("EPOCH_TICK_MS", 10)?,
            epoch_deadline_ticks: parse_env_u64("EPOCH_DEADLINE_TICKS", 100)?,
            worker_jwt_secret: std::env::var("WORKER_JWT_SECRET").unwrap_or_default(),
            worker_jwt_issuer: std::env::var("WORKER_JWT_ISSUER")
                .unwrap_or_else(|_| "edgecloud".into()),
            worker_tenant_id: std::env::var("WORKER_TENANT_ID").context(
                "WORKER_TENANT_ID not set — a tenant ID is required (e.g. t_abc123). \
                     This is the ID of the tenant whose apps this worker hosts. \
                     All outbound calls to /api/internal/* are scoped to this tenant.",
            )?,
            handler_request_budget_ms: parse_env_u64("HANDLER_REQUEST_BUDGET_MS", 1000)?,
            handler_max_request_body_bytes: parse_env_u64(
                "HANDLER_MAX_REQUEST_BODY_BYTES",
                10 * 1024 * 1024,
            )?,
        })
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
}
