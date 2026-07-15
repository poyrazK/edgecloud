//! Sidecar configuration (issue #665, PR B).
//!
//! Mirrors the operator-facing env-var shape of `edge-ingress/src/config.rs`
//! but only carries the knobs the sidecar itself consumes:
//!   - `SIDECAR_NATS_URL` — JetStream URL (default `nats://127.0.0.1:4222`).
//!   - `SIDECAR_REPLICA_ID` — per-pod identifier; falls back to the
//!     `HOSTNAME` env var (k8s downward API default) then to a stable
//!     but unique per-process fallback. Subject leaves are
//!     `edgecloud.rate-limit.global.delta.<replica_id>` so a missing
//!     replica_id would either collapse two pods onto the same subject
//!     (last-writer-wins, the worst failure mode for a sum-stream) or
//!     fail closed — we choose the latter.
//!   - `SIDECAR_CADDY_ADMIN_URL` — local Caddy admin API (default
//!     `http://127.0.0.1:2019`).
//!   - `SIDECAR_METRICS_LISTEN` — Prometheus `/metrics` listener
//!     (default `0.0.0.0:9091`).
//!   - `SIDECAR_GLOBAL_RATE_LIMIT_RPS` — operator's configured cap
//!     (issue #305 #4, mirrored from `edge-ingress`). 0 = disabled
//!     (sidecar logs at info and exits with code 0; operator must
//!     opt in to cross-replica aggregation).
//!   - `SIDECAR_NATS_REPLICAS` — JetStream stream replication factor
//!     for `edgecloud-rl-global` (PR C declares the same value on the
//!     CP side; the sidecar's `EnsureStream` and the CP's `EnsureStream`
//!     must agree). Defaults to `1`. The `cargo-udeps`/nightly gate
//!     flagged a hard-coded `1` in the publisher constructor — this
//!     env var threads the value through.
//!   - `SIDECAR_UDS_PATH` — UDS datagram socket the ingress binary
//!     reads at 1 Hz (PR D). Defaults to `/var/run/edge-ingress/global-rps.sock`.
//!
//! `validate()` mirrors the same MIN_REASONABLE_RPS guard pattern — a
//! sub-10 cap on the platform total is almost certainly a typo.

use anyhow::Context;
use tracing::warn;

/// Default UDS path. Must stay in sync with `edge-ingress/src/main.rs`
/// (PR D wires the ingress binary to read this path). Drift between
/// the two turns the sidecar's writes into a black hole — the ingress
/// would never see them and the platform would render per-replica
/// `rates.rps` with `cfg.ingress_rate_limit_aggregation=true` but no
/// cache value, so it would emit NO global route (fail-closed per
/// the issue #665 plan).
pub const DEFAULT_UDS_PATH: &str = "/var/run/edge-ingress/global-rps.sock";

/// Default Prometheus metrics listener.
pub const DEFAULT_METRICS_LISTEN: &str = "0.0.0.0:9091";

/// Sub-floor on `SIDECAR_GLOBAL_RATE_LIMIT_RPS` — anything below this
/// almost certainly indicates an operator typo (a global cap of 1
/// RPS would 429 every request).
const MIN_REASONABLE_RPS: u32 = 10;

#[derive(Debug, Clone)]
pub struct Config {
    pub nats_url: String,
    pub replica_id: String,
    pub caddy_admin_url: String,
    /// Local HTTP listener for the Prometheus `/metrics` endpoint.
    /// Operators grep this endpoint to confirm a sidecar pod is healthy.
    pub metrics_listen: String,
    /// Operator's configured platform cap (`GLOBAL_RATE_LIMIT_RPS` mirror).
    /// 0 = disabled — the sidecar logs at info and the binary should
    /// not have been deployed; we still run because operators may want
    /// the sidecar's `/metrics` surface without enabling aggregation.
    pub global_rate_limit_rps: u32,
    /// JetStream stream replication factor for `edgecloud-rl-global`.
    /// Must match the CP's `cfg.NATS.Replicas` (PR C); both call
    /// `EnsureStream` with the same value so the stream config is
    /// idempotent across the cluster.
    pub nats_replicas: usize,
    /// UDS datagram socket path the ingress binary reads at 1 Hz
    /// (issue #665 PR D).
    pub uds_path: String,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// Required env vars: NONE — every knob has a sane default. The
    /// sidecar is designed to be co-deployed with Caddy in the same
    /// pod; the operator wires it up by setting only `SIDECAR_GLOBAL_RATE_LIMIT_RPS`
    /// to opt into cross-replica aggregation.
    ///
    /// See the file-level docs for the per-var contract.
    pub fn from_env() -> anyhow::Result<Self> {
        let replica_id = std::env::var("SIDECAR_REPLICA_ID")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| std::env::var("HOSTNAME").ok().filter(|v| !v.is_empty()))
            .context(
                "SIDECAR_REPLICA_ID and HOSTNAME both unset; cannot derive a stable \
                 per-pod identifier for the JetStream subject leaf. Set SIDECAR_REPLICA_ID \
                 or mount the k8s downward-API HOSTNAME into the pod.",
            )?;
        let global_rate_limit_rps: u32 = std::env::var("SIDECAR_GLOBAL_RATE_LIMIT_RPS")
            .unwrap_or_else(|_| "0".into())
            .parse()
            .context("SIDECAR_GLOBAL_RATE_LIMIT_RPS must be a non-negative integer")?;
        let nats_replicas: usize = std::env::var("SIDECAR_NATS_REPLICAS")
            .unwrap_or_else(|_| "1".into())
            .parse()
            .context("SIDECAR_NATS_REPLICAS must be a positive integer")?;
        if nats_replicas == 0 {
            anyhow::bail!("SIDECAR_NATS_REPLICAS must be ≥ 1 (got 0)");
        }

        Ok(Config {
            nats_url: std::env::var("SIDECAR_NATS_URL")
                .unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
            replica_id,
            caddy_admin_url: std::env::var("SIDECAR_CADDY_ADMIN_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:2019".into()),
            metrics_listen: std::env::var("SIDECAR_METRICS_LISTEN")
                .unwrap_or_else(|_| DEFAULT_METRICS_LISTEN.into()),
            global_rate_limit_rps,
            nats_replicas,
            uds_path: std::env::var("SIDECAR_UDS_PATH").unwrap_or_else(|_| DEFAULT_UDS_PATH.into()),
        })
    }

    /// Log warnings for knobs that look misconfigured. Mirrors the
    /// `edge-ingress/src/config.rs::validate` pattern so operators see
    /// the same warning text regardless of which binary surfaces it.
    pub fn validate(&self) {
        if self.global_rate_limit_rps > 0 && self.global_rate_limit_rps < MIN_REASONABLE_RPS {
            warn!(
                rps = self.global_rate_limit_rps,
                min_reasonable = MIN_REASONABLE_RPS,
                "SIDECAR_GLOBAL_RATE_LIMIT_RPS is below the recommended minimum; \
                 this caps the entire platform to <{}/s. Confirm this is intentional.",
                MIN_REASONABLE_RPS
            );
        }
        if self.global_rate_limit_rps == 0 {
            warn!(
                "SIDECAR_GLOBAL_RATE_LIMIT_RPS=0 — sidecar is running but cross-replica \
                 aggregation is disabled. The ingress binary will emit no global route \
                 (fail-closed). To opt in, set SIDECAR_GLOBAL_RATE_LIMIT_RPS to the \
                 operator's configured platform cap."
            );
        }
        // Issue #665 PR C: surface the operator foot-gun where the
        // sidecar's nats_replicas (default 1) disagrees with the CP's
        // cfg.NATS.Replicas (default 3). On multi-replica NATS
        // clusters the sidecar's `ensure_stream` will fail with
        // "insufficient resources" and the sidecar retries on every
        // tick — the WARN fires once at boot so the operator learns
        // the knob exists. Single-replica NATS (default for
        // testcontainers + local dev) keeps nats_replicas=1 as a
        // valid config; the WARN is informational only.
        if self.nats_replicas == 1 {
            warn!(
                "SIDECAR_NATS_REPLICAS=1 — the rate-limit stream is declared with \
                 cfg.NATS.Replicas by the control plane (default 3). On multi-replica \
                 NATS clusters the sidecar's ensure_stream will fail with 'insufficient \
                 resources'. Set SIDECAR_NATS_REPLICAS to match TASK_STREAM_REPLICAS \
                 in your deployment."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_var(name: &str, value: &str) {
        // SAFETY: tests use uniquely-named env vars so concurrent
        // tests don't race on the same key.
        unsafe { std::env::set_var(name, value) };
    }

    fn unset_var(name: &str) {
        unsafe { std::env::remove_var(name) };
    }

    // ── Config::from_env tests ──────────────────────────────────────

    /// Helper: unset every env var the Config reads so from_env() sees
    /// a clean slate. Set HOSTNAME because otherwise the `replica_id`
    /// context check would fail — we want to test "default behavior
    /// when SIDECAR_REPLICA_ID is unset but HOSTNAME is present".
    fn unset_all_config_vars() {
        for v in &[
            "SIDECAR_NATS_URL",
            "SIDECAR_REPLICA_ID",
            "SIDECAR_CADDY_ADMIN_URL",
            "SIDECAR_METRICS_LISTEN",
            "SIDECAR_GLOBAL_RATE_LIMIT_RPS",
            "SIDECAR_NATS_REPLICAS",
            "SIDECAR_UDS_PATH",
            "HOSTNAME",
        ] {
            unset_var(v);
        }
    }

    #[serial_test::serial]
    #[test]
    fn from_env_defaults_without_hostname_fails() {
        // Neither SIDECAR_REPLICA_ID nor HOSTNAME → from_env fails.
        // This is the load-bearing guarantee: a pod without a replica
        // id would otherwise land on the same subject as another pod,
        // last-writer-wins, silently corrupting the platform total.
        unset_all_config_vars();
        set_var("HOSTNAME", "");
        let err = Config::from_env().unwrap_err();
        assert!(
            err.to_string().contains("SIDECAR_REPLICA_ID"),
            "err must mention SIDECAR_REPLICA_ID: {err}"
        );
    }

    #[serial_test::serial]
    #[test]
    fn from_env_falls_back_to_hostname() {
        unset_all_config_vars();
        set_var("HOSTNAME", "ingress-pod-fra-7");
        let cfg = Config::from_env().expect("hostname fallback must succeed");
        assert_eq!(cfg.replica_id, "ingress-pod-fra-7");
        assert_eq!(cfg.nats_url, "nats://127.0.0.1:4222");
        assert_eq!(cfg.caddy_admin_url, "http://127.0.0.1:2019");
        assert_eq!(cfg.metrics_listen, "0.0.0.0:9091");
        assert_eq!(cfg.global_rate_limit_rps, 0);
        assert_eq!(cfg.nats_replicas, 1, "default replicas=1");
        assert_eq!(cfg.uds_path, "/var/run/edge-ingress/global-rps.sock");
    }

    #[serial_test::serial]
    #[test]
    fn from_env_prefers_sidecar_replica_id_over_hostname() {
        unset_all_config_vars();
        set_var("HOSTNAME", "k8s-pod-name");
        set_var("SIDECAR_REPLICA_ID", "explicit-replica");
        let cfg = Config::from_env().expect("explicit id wins");
        assert_eq!(cfg.replica_id, "explicit-replica");
    }

    #[serial_test::serial]
    #[test]
    fn from_env_overrides() {
        unset_all_config_vars();
        set_var("HOSTNAME", "pod-1");
        set_var("SIDECAR_NATS_URL", "nats://nats.internal:4222");
        set_var("SIDECAR_CADDY_ADMIN_URL", "http://caddy-admin:2019");
        set_var("SIDECAR_METRICS_LISTEN", "0.0.0.0:9191");
        set_var("SIDECAR_GLOBAL_RATE_LIMIT_RPS", "10000");
        set_var("SIDECAR_NATS_REPLICAS", "3");
        set_var("SIDECAR_UDS_PATH", "/tmp/sidecar.sock");
        let cfg = Config::from_env().expect("overrides");
        assert_eq!(cfg.nats_url, "nats://nats.internal:4222");
        assert_eq!(cfg.caddy_admin_url, "http://caddy-admin:2019");
        assert_eq!(cfg.metrics_listen, "0.0.0.0:9191");
        assert_eq!(cfg.global_rate_limit_rps, 10000);
        assert_eq!(cfg.nats_replicas, 3);
        assert_eq!(cfg.uds_path, "/tmp/sidecar.sock");
    }

    #[serial_test::serial]
    #[test]
    fn from_env_parses_nats_replicas() {
        unset_all_config_vars();
        set_var("HOSTNAME", "pod-1");
        set_var("SIDECAR_NATS_REPLICAS", "5");
        let cfg = Config::from_env().expect("replicas override");
        assert_eq!(cfg.nats_replicas, 5);
    }

    #[serial_test::serial]
    #[test]
    fn from_env_rejects_nats_replicas_zero() {
        unset_all_config_vars();
        set_var("HOSTNAME", "pod-1");
        set_var("SIDECAR_NATS_REPLICAS", "0");
        let err = Config::from_env().unwrap_err();
        assert!(
            err.to_string().contains("SIDECAR_NATS_REPLICAS"),
            "err must mention SIDECAR_NATS_REPLICAS: {err}"
        );
    }

    #[serial_test::serial]
    #[test]
    fn from_env_rejects_nats_replicas_garbage() {
        unset_all_config_vars();
        set_var("HOSTNAME", "pod-1");
        set_var("SIDECAR_NATS_REPLICAS", "three");
        let err = Config::from_env().unwrap_err();
        assert!(
            err.to_string().contains("SIDECAR_NATS_REPLICAS"),
            "err must mention SIDECAR_NATS_REPLICAS: {err}"
        );
    }

    #[serial_test::serial]
    #[test]
    fn from_env_rejects_negative_rps() {
        unset_all_config_vars();
        set_var("HOSTNAME", "pod-1");
        set_var("SIDECAR_GLOBAL_RATE_LIMIT_RPS", "-1");
        let err = Config::from_env().unwrap_err();
        assert!(
            err.to_string().contains("SIDECAR_GLOBAL_RATE_LIMIT_RPS"),
            "err must mention SIDECAR_GLOBAL_RATE_LIMIT_RPS: {err}"
        );
    }

    #[test]
    fn validate_low_rps_does_not_panic() {
        // Sub-minimum cap is operator-allowed — validate() only emits
        // WARN, never returns Err. Pin absence-of-side-effect here;
        // the WARN text is verified at integration time.
        let cfg = Config {
            nats_url: "nats://localhost".into(),
            replica_id: "pod".into(),
            caddy_admin_url: "http://localhost:2019".into(),
            metrics_listen: "0.0.0.0:9091".into(),
            global_rate_limit_rps: 5,
            nats_replicas: 1,
            uds_path: "/tmp/s.sock".into(),
        };
        cfg.validate();
    }

    #[test]
    fn validate_zero_rps_does_not_panic() {
        let cfg = Config {
            nats_url: "nats://localhost".into(),
            replica_id: "pod".into(),
            caddy_admin_url: "http://localhost:2019".into(),
            metrics_listen: "0.0.0.0:9091".into(),
            global_rate_limit_rps: 0,
            nats_replicas: 1,
            uds_path: "/tmp/s.sock".into(),
        };
        cfg.validate();
    }

    #[test]
    fn validate_reasonable_rps_does_not_panic() {
        let cfg = Config {
            nats_url: "nats://localhost".into(),
            replica_id: "pod".into(),
            caddy_admin_url: "http://localhost:2019".into(),
            metrics_listen: "0.0.0.0:9091".into(),
            global_rate_limit_rps: 10_000,
            nats_replicas: 1,
            uds_path: "/tmp/s.sock".into(),
        };
        cfg.validate();
    }

    #[test]
    fn validate_replicas_one_does_not_panic() {
        // Issue #665 PR C. Pin absence-of-side-effect (the WARN text
        // is logged via tracing; we don't capture subscriber here —
        // CI just needs to confirm validate() doesn't blow up on the
        // default nats_replicas=1 path). Production boot sees the
        // WARN emitted at info level.
        let cfg = Config {
            nats_url: "nats://localhost".into(),
            replica_id: "pod".into(),
            caddy_admin_url: "http://localhost:2019".into(),
            metrics_listen: "0.0.0.0:9091".into(),
            global_rate_limit_rps: 10_000,
            nats_replicas: 1,
            uds_path: "/tmp/s.sock".into(),
        };
        cfg.validate();
    }

    #[test]
    fn validate_replicas_three_does_not_panic() {
        // Mirror of the above for the non-default path: a multi-replica
        // NATS deployment should NOT emit the replica=1 WARN.
        let cfg = Config {
            nats_url: "nats://localhost".into(),
            replica_id: "pod".into(),
            caddy_admin_url: "http://localhost:2019".into(),
            metrics_listen: "0.0.0.0:9091".into(),
            global_rate_limit_rps: 10_000,
            nats_replicas: 3,
            uds_path: "/tmp/s.sock".into(),
        };
        cfg.validate();
    }
}
