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
    /// Filesystem directory for caching downloaded wasm artifacts.
    /// Default `/var/lib/edge-worker/cache` (absolute). Override via
    /// `CACHE_DIR`; the override can be relative for dev runs.
    pub cache_dir: PathBuf,
    pub heartbeat_interval_secs: u64,
    pub health_check_timeout_secs: u64,
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
    /// Max redeliveries the NATS server will attempt for a single message
    /// before parking it. Default 20. Tunable via `NATS_MAX_DELIVER`. A
    /// persistently-failing message that hits this cap stalls the
    /// consumer until an operator investigates or the worker `term()`s
    /// it (parse failures are already terminated on receipt).
    pub nats_max_deliver: i64,
    /// HMAC secret the worker uses to sign outbound JWTs to the control
    /// plane's internal endpoints. Must match `JWT_SECRET` on the Go side.
    ///
    /// **Deprecated.** New deployments should set `WORKER_BOOTSTRAP_PSK`
    /// and let the worker self-provision a JWT on startup (Phase 4).
    /// This field is kept as a fallback so existing deployments don't
    /// break; main.rs logs a deprecation warning when it's the only
    /// configured source.
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
    /// The tenant this worker is authorized for. Loaded once at startup;
    /// a worker is per-tenant in this design (whitepaper §9.3 calls for
    /// tenant-agnostic workers — file a follow-up to revisit).
    pub worker_tenant_id: String,
    /// Pre-shared key the worker uses to enroll with the control plane
    /// via `POST /api/internal/auth/token` (Phase 4). Must match
    /// `BOOTSTRAP_PSK` on the Go side. When set, the worker self-provisions
    /// a JWT on first `sign()` and caches it to disk at `jwt_cache_path`.
    ///
    /// `None` and empty-string are treated identically: "no bootstrap
    /// configured". Priority order at startup is `jwt_cache_path` file →
    /// `WORKER_BOOTSTRAP_PSK` → `WORKER_JWT_SECRET` (deprecated fallback).
    pub worker_bootstrap_psk: Option<String>,
    /// Path to the cached JWT file. The worker reads this on startup and
    /// uses the cached token for `sign()` until it crosses the refresh
    /// threshold (5 min before expiry). On cache miss / expiry the
    /// worker re-bootstraps via `WORKER_BOOTSTRAP_PSK`.
    ///
    /// Default `/var/lib/edge-worker/jwt-cache.json` (absolute, mode
    /// `0600`). Override via `JWT_CACHE_PATH`. The path must be writable
    /// by the worker; a read-only mount is logged but not fatal (the
    /// worker falls back to in-memory caching for the current boot).
    pub jwt_cache_path: PathBuf,
    /// Filesystem directory for the log-forwarder disk spool. Holds
    /// `emit_log` batches that the control plane refused (5xx, network
    /// timeout) so they can be retried on the next flush. Survives
    /// worker restarts.
    ///
    /// Default `/var/lib/edge-worker/spool` (absolute). Override via
    /// `SPOOL_DIR`. Operators must ensure the directory is writable
    /// and persists across restarts; a tmpfs-backed dir is fine for
    /// dev but loses the durability guarantee on reboot.
    pub spool_dir: PathBuf,
    /// Maximum on-disk size of the spool before the oldest batches are
    /// dropped (FIFO) to make room for new failures. Default 1 GiB.
    /// Override via `SPOOL_MAX_BYTES`. When the cap is exceeded during
    /// `rotate_when_over`, the oldest lines are dropped silently and
    /// a count is returned to the caller for logging.
    pub spool_max_bytes: u64,
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
    /// - `WORKER_TENANT_ID` (e.g., `t_tenant1`)
    ///
    /// Optional env vars:
    /// - `NATS_URL` (default: `nats://localhost:4222`)
    /// - `CACHE_DIR` (default: `/var/lib/edge-worker/cache`)
    /// - `APP_MAX_MEMORY_MB` (default: 256)
    /// - `EPOCH_TICK_MS` (default: 10)
    /// - `EPOCH_DEADLINE_TICKS` (default: 100)
    /// - `EDGE_QUEUE_GROUP` (default: `edgecloud-workers`)
    /// - `EDGE_CONSUMER_NAME` (default: derived from `WORKER_ID`)
    /// - `WORKER_JWT_ISSUER` (default: `edgecloud`)
    /// - `WORKER_JWT_SECRET` (default: empty — see warning in main.rs)
    /// - `WORKER_BOOTSTRAP_PSK` (default: unset) — pre-shared key for the
    ///   Phase 4 bootstrap handshake. When set, the worker self-provisions
    ///   a JWT on first `sign()` and caches it to `JWT_CACHE_PATH`. Takes
    ///   priority over `WORKER_JWT_SECRET` (the deprecated fallback).
    /// - `JWT_CACHE_PATH` (default: `/var/lib/edge-worker/jwt-cache.json`)
    ///   — path to the on-disk JWT cache.
    /// - `EDGE_WORKER_LOG_LEVEL` (default: `info`) — minimum level the
    ///   worker log layer ships to the control plane via `LogForwarder`.
    ///   Independent of `RUST_LOG`, which still controls local stdout
    ///   verbosity via `EnvFilter`. See `forwarder_log_level`.
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
            queue_group: std::env::var("EDGE_QUEUE_GROUP")
                .unwrap_or_else(|_| DEFAULT_QUEUE_GROUP.to_string()),
            consumer_name,
            nats_max_deliver: parse_env_u64("NATS_MAX_DELIVER", 20)? as i64,
            worker_id,
            region: std::env::var("REGION").context("REGION not set")?,
            worker_addr: std::env::var("EDGE_WORKER_ADDR").context("EDGE_WORKER_ADDR not set")?,
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into()),
            control_plane_url: std::env::var("CONTROL_PLANE_URL")
                .context("CONTROL_PLANE_URL not set")?,
            cache_dir: std::env::var("CACHE_DIR")
                .map(PathBuf::from)
                // Default is an absolute path. A relative default like
                // `.worker-cache` is a footgun in containers where CWD is
                // the image root — the worker silently writes to a path
                // that disappears on container restart. Operators can
                // still override with `CACHE_DIR=./local-dev` for dev
                // runs; the absolute default just removes the silent
                // failure mode for production-style deploys.
                .unwrap_or_else(|_| PathBuf::from("/var/lib/edge-worker/cache")),
            heartbeat_interval_secs: 30,
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
            worker_tenant_id: std::env::var("WORKER_TENANT_ID")
                .context("WORKER_TENANT_ID not set")?,
            worker_bootstrap_psk: parse_env_string("WORKER_BOOTSTRAP_PSK"),
            // Absolute default matches the cache_dir / spool_dir
            // policy: a relative default in a container silently
            // writes to the image root and disappears on restart,
            // defeating the cache durability guarantee.
            jwt_cache_path: std::env::var("JWT_CACHE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/var/lib/edge-worker/jwt-cache.json")),
            spool_dir: std::env::var("SPOOL_DIR")
                .map(PathBuf::from)
                // Absolute default matches the cache_dir policy: a
                // relative default in a container silently writes to
                // the image root and disappears on restart, defeating
                // the durability guarantee.
                .unwrap_or_else(|_| PathBuf::from("/var/lib/edge-worker/spool")),
            spool_max_bytes: parse_env_u64("SPOOL_MAX_BYTES", 1 << 30)?,
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

/// Parse an optional string-valued environment variable. Returns
/// `None` when unset or set to an empty string — both signal
/// "this feature is not configured" and shouldn't be distinguishable
/// at the call site (an empty `WORKER_BOOTSTRAP_PSK` would HMAC-SHA256
/// against a zero-length key, which is technically valid but
/// operationally identical to "not set").
///
/// Unlike `parse_env_u64`, this helper never errors — a malformed
/// string value is just an empty string. We can't validate against
/// a schema (length, charset) without coupling the config to a
/// specific use case; validation lives at the call site.
fn parse_env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
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

    /// The default cache directory must be absolute. A relative default
    /// silently writes to the process's CWD — fine for dev, but in a
    /// container that's the image root, which disappears on container
    /// restart (artifacts are re-downloaded on every boot, defeating the
    /// cache). Pinning the absolute default makes that failure mode
    /// impossible without an explicit operator override.
    #[test]
    fn cache_dir_default_is_absolute() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("CACHE_DIR", None),
        ]);
        let cfg = Config::from_env().expect("from_env with no CACHE_DIR");
        assert!(
            cfg.cache_dir.is_absolute(),
            "default cache_dir must be absolute; got {:?}",
            cfg.cache_dir
        );
    }

    /// `nats_max_deliver` defaults to 20. JetStream parks a message once
    /// it has been redelivered this many times; the operator must then
    /// either investigate or restart the consumer. 20 is a generous
    /// default — even a slow-failing message gets many chances before
    /// parking. Pinning the default catches regressions where a future
    /// refactor changes it.
    #[test]
    fn nats_max_deliver_default_is_20() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("NATS_MAX_DELIVER", None),
        ]);
        let cfg = Config::from_env().expect("from_env with no NATS_MAX_DELIVER");
        assert_eq!(
            cfg.nats_max_deliver, 20,
            "default nats_max_deliver must be 20; got {}",
            cfg.nats_max_deliver
        );
    }

    /// `spool_dir` defaults to an absolute path. Same rationale as
    /// `cache_dir_default_is_absolute` — a relative default in a
    /// container silently writes to the image root and the spool
    /// disappears on restart, defeating the durability guarantee. The
    /// test pins the absolute default so a future "fix" can't silently
    /// make it relative without a conscious decision.
    #[test]
    fn spool_dir_default_is_absolute() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("SPOOL_DIR", None),
        ]);
        let cfg = Config::from_env().expect("from_env with no SPOOL_DIR");
        assert!(
            cfg.spool_dir.is_absolute(),
            "default spool_dir must be absolute; got {:?}",
            cfg.spool_dir
        );
    }

    /// `spool_max_bytes` defaults to 1 GiB. At 1 GiB the spool holds
    /// ~1M small log batches before the oldest are dropped — well
    /// beyond any plausible outage window (a 5-day control-plane
    /// outage at 100 entries/s would produce ~43M entries; the cap
    /// drops the oldest). Pinning the default catches regressions
    /// where a future refactor changes the cap.
    #[test]
    fn spool_max_bytes_default_is_1_gib() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("SPOOL_MAX_BYTES", None),
        ]);
        let cfg = Config::from_env().expect("from_env with no SPOOL_MAX_BYTES");
        assert_eq!(
            cfg.spool_max_bytes,
            1u64 << 30,
            "default spool_max_bytes must be 1 GiB ({}); got {}",
            1u64 << 30,
            cfg.spool_max_bytes
        );
    }

    /// `SPOOL_MAX_BYTES` round-trips through `from_env` like every other
    /// env var. Without this test the env-var knob could regress to
    /// "always use the default" without the unit tests catching it.
    #[test]
    fn spool_max_bytes_parses_override() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("SPOOL_MAX_BYTES", Some("536870912")), // 512 MiB
        ]);
        let cfg = Config::from_env().expect("from_env with SPOOL_MAX_BYTES=512MiB");
        assert_eq!(
            cfg.spool_max_bytes,
            512 * 1024 * 1024,
            "SPOOL_MAX_BYTES override should be 512 MiB"
        );
    }

    /// `WORKER_BOOTSTRAP_PSK` defaults to `None`. The whole point of
    /// the bootstrap path is being opt-in: a worker without a PSK must
    /// still start (with the deprecation warning + 401s on outbound
    /// calls) so existing deployments upgrade without downtime.
    #[test]
    fn worker_bootstrap_psk_defaults_to_none() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("WORKER_BOOTSTRAP_PSK", None),
        ]);
        let cfg = Config::from_env().expect("from_env with no PSK");
        assert!(
            cfg.worker_bootstrap_psk.is_none(),
            "missing WORKER_BOOTSTRAP_PSK must load as None"
        );
    }

    /// `WORKER_BOOTSTRAP_PSK=hello` must flow through into the config
    /// field. The string is the PSK bytes as-is — the bootstrap layer
    /// computes HMAC-SHA256 over them.
    #[test]
    fn worker_bootstrap_psk_round_trips() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("WORKER_BOOTSTRAP_PSK", Some("my-32-byte-secret-psk-12345")),
        ]);
        let cfg = Config::from_env().expect("from_env with PSK");
        assert_eq!(
            cfg.worker_bootstrap_psk.as_deref(),
            Some("my-32-byte-secret-psk-12345")
        );
    }

    /// `WORKER_BOOTSTRAP_PSK=` (empty) is treated identically to
    /// unset. An operator who briefly clears the env var during a
    /// rotation would otherwise get an HMAC computation against a
    /// zero-length key — same outcome as "not set" but with a
    /// different code path that's harder to debug.
    #[test]
    fn worker_bootstrap_psk_empty_string_is_none() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("WORKER_BOOTSTRAP_PSK", Some("")),
        ]);
        let cfg = Config::from_env().expect("from_env with empty PSK");
        assert!(
            cfg.worker_bootstrap_psk.is_none(),
            "empty WORKER_BOOTSTRAP_PSK must load as None"
        );
    }

    /// `jwt_cache_path` defaults to an absolute path. Same rationale
    /// as `cache_dir_default_is_absolute`: a relative default silently
    /// writes to CWD and disappears on container restart. The cache
    /// holds a 24h JWT — losing it on every restart forces a
    /// re-bootstrap on every boot, defeating the durability goal.
    #[test]
    fn jwt_cache_path_default_is_absolute() {
        let _g = lock_and_set(&[
            ("WORKER_ID", Some("w_test")),
            ("REGION", Some("fra")),
            ("CONTROL_PLANE_URL", Some("http://127.0.0.1:0")),
            ("EDGE_WORKER_ADDR", Some("127.0.0.1:0")),
            ("WORKER_TENANT_ID", Some("t_test")),
            ("JWT_CACHE_PATH", None),
        ]);
        let cfg = Config::from_env().expect("from_env with no JWT_CACHE_PATH");
        assert!(
            cfg.jwt_cache_path.is_absolute(),
            "default jwt_cache_path must be absolute; got {:?}",
            cfg.jwt_cache_path
        );
    }
}
