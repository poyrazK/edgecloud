//! Domain poller: pulls `GET /api/internal/domains` from the control
//! plane every `cfg.domain_poll_interval` and applies the result to
//! the shared `RoutingTable` (via `apply_poll_snapshot`).
//!
//! This is the only path that mutates the FQDN table on the ingress
//! side. The heartbeat path mutates the upstream table; this path
//! mutates the FQDN table. They are decoupled: a heartbeat can arrive
//! between two domain polls and the FQDN route is still rendered
//! (looked up from `by_app` at render time).
//!
//! Failure mode: any HTTP / decode error is logged and the loop
//! continues to the next tick. We do NOT abort the task — a 503 from
//! the control plane for one cycle is recoverable on the next 30s
//! tick, and aborting would mean losing domain state on transient
//! outages.
//!
//! The function is exposed as `pub async fn run` so the caller can
//! spawn it with its own backoff loop, mirroring `heartbeats::run`'s
//! shape. In `main.rs` we just `tokio::spawn(async move { run(...) })`
//! because domain polling is fire-and-forget — there's no reconnect
//! semantics worth re-invoking like there are with NATS.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::Notify;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::routing::RoutingTable;

/// Typed errors from the domain poller. Replaces the previous
/// `anyhow::anyhow!("control plane returned {status}")` + substring
/// match pattern: a future status code (e.g. a 418 from a proxy
/// that embeds a JSON 401 message in its body) would have silently
/// escaped the substring gate and the poller would have looped
/// forever on a real auth failure. Typed match on the variant is
/// the only correct fix.
#[derive(Debug, Error)]
pub enum PollError {
    /// HTTP non-2xx response. `status` and `body` are preserved so
    /// the operator's log line carries the actual response (not a
    /// substring match) and the run loop can match on `status` to
    /// distinguish auth errors from transient 5xx.
    #[error("control plane returned {status} for {url}: {body}")]
    HttpStatus {
        status: u16,
        url: String,
        body: String,
    },
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    /// Decode failure (malformed JSON in a 2xx response, or any
    /// other body-shape mismatch). The `reqwest::Error::json()`
    /// method collapses both transport-time JSON errors and
    /// serde-time errors into a single `reqwest::Error { kind:
    /// Decode, ... }`, so we keep the variant stringly-typed —
    /// preserving the underlying message is what the operator
    /// needs in the log.
    #[error("decode: {0}")]
    Decode(String),
}

/// Number of consecutive 401/403 responses after which the poller
/// gives up and returns Err. Three 30s ticks = ~90s of downtime
/// before the operator's alerting has had a chance to fire. The
/// `main.rs` task wrapper turns the returned Err into a process
/// exit, so the orchestrator restarts the ingress with a fresh
/// token from the (operator-copied) ingest token file.
const MAX_CONSECUTIVE_AUTH_ERRORS: u32 = 3;

/// Run the domain poller until the process exits. The renderer's
/// `Notify` is signalled on every successful apply so Caddy reloads
/// pick up the new FQDN routes. Errors are logged and the loop
/// continues; the function only returns Err if the reqwest client
/// itself fails to build (which is unrecoverable) OR the control
/// plane has rejected our token repeatedly (rotated JWT secret,
/// revoked ingest token, etc.) — see `MAX_CONSECUTIVE_AUTH_ERRORS`.
pub async fn run(
    cfg: Config,
    table: Arc<RoutingTable>,
    render_notify: Arc<Notify>,
    shutdown: CancellationToken,
) -> Result<()> {
    if cfg.control_plane_url.is_empty() {
        info!("CONTROL_PLANE_URL unset; domain poller disabled");
        return Ok(());
    }

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("building reqwest client for domain poller")?;

    let url = format!("{}/api/internal/domains", cfg.control_plane_url);
    let mut ticker = interval(cfg.domain_poll_interval);
    // Skip the first immediate tick — we want a deterministic
    // poll AFTER the NATS bring-up, not a race against it.
    ticker.tick().await;

    info!(
        %url,
        interval_secs = cfg.domain_poll_interval.as_secs(),
        "domain poller started"
    );

    let mut consecutive_auth_errors: u32 = 0;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("domain poller: shutdown signal received, stopping");
                return Ok(());
            }
            _ = ticker.tick() => {
                match fetch_and_apply(&http, &url, &cfg.service_token, &table).await {
                    Ok((added, removed)) => {
                        metrics::counter!("ingress.domain_poll.total", "status" => "success").increment(1);
                        consecutive_auth_errors = 0;
                        if !added.is_empty() || !removed.is_empty() {
                            info!(
                                added = added.len(),
                                removed = removed.len(),
                                "domain table updated"
                            );
                            debug!(?added, ?removed, "domain table diff");
                            render_notify.notify_one();
                        } else {
                            debug!("domain poll: no changes");
                        }
                    }
                    Err(e) => {
                        let status = match &e {
                            PollError::HttpStatus { status, .. } => Some(*status),
                            _ => None,
                        };
                        let is_auth = matches!(status, Some(401) | Some(403));
                        if is_auth {
                            metrics::counter!("ingress.domain_poll.total", "status" => "auth_error")
                                .increment(1);
                            consecutive_auth_errors += 1;
                            if consecutive_auth_errors >= MAX_CONSECUTIVE_AUTH_ERRORS {
                                error!(
                                    err = %e,
                                    count = consecutive_auth_errors,
                                    "domain poller got {MAX_CONSECUTIVE_AUTH_ERRORS} consecutive 401/403 — failing fast (likely rotated INGRESS_SERVICE_TOKEN); restart with the new token from the control plane's ingest token file"
                                );
                                return Err(e.into());
                            }
                            warn!(
                                err = %e,
                                count = consecutive_auth_errors,
                                max = MAX_CONSECUTIVE_AUTH_ERRORS,
                                "domain poll auth error; will retry"
                            );
                        } else {
                            metrics::counter!("ingress.domain_poll.total", "status" => "failure")
                                .increment(1);
                            consecutive_auth_errors = 0;
                            warn!(err = %e, "domain poll failed; will retry on next tick");
                        }
                    }
                }
            }
        }
    }
}

/// Fetch the current domain list and apply it to the routing table.
/// Returns the `(added, removed)` diff so the caller can log churn.
///
/// This is the only function in the module that does I/O; tests
/// exercise it directly with a wiremock `MockServer`.
pub async fn fetch_and_apply(
    http: &reqwest::Client,
    url: &str,
    token: &str,
    table: &RoutingTable,
) -> Result<(Vec<String>, Vec<String>), PollError> {
    let resp = http.get(url).bearer_auth(token).send().await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(PollError::HttpStatus {
            status: status.as_u16(),
            url: url.to_string(),
            body,
        });
    }

    let domains: Vec<crate::routing::Domain> = resp
        .json()
        .await
        .map_err(|e| PollError::Decode(e.to_string()))?;

    let (added, removed) = table.apply_poll_snapshot(domains).await;
    Ok((added, removed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::Domain;
    use wiremock::matchers::{bearer_token, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[allow(dead_code)] // available for future tests; currently unused
    fn test_domain(id: &str, tenant_id: &str, app_name: &str, fqdn: &str) -> Domain {
        Domain {
            id: id.to_string(),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
            fqdn: fqdn.to_string(),
        }
    }

    /// Happy path: wiremock serves a single domain; after
    /// `fetch_and_apply` the routing table carries that one FQDN.
    /// Without this we'd only know the poller is broken at runtime.
    #[tokio::test]
    async fn fetch_and_apply_populates_routing_table() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "id": "d_1",
                    "tenant_id": "t_a",
                    "app_name": "api",
                    "fqdn": "api.acme.com"
                }
            ])))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let url = format!("{}/api/internal/domains", server.uri());
        let table = RoutingTable::new();
        let (added, removed) = fetch_and_apply(&http, &url, "test-token", &table)
            .await
            .unwrap();

        assert_eq!(added, vec!["api.acme.com".to_string()]);
        assert!(removed.is_empty());

        let snap = table.fqdn_snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].fqdn, "api.acme.com");
        assert_eq!(snap[0].tenant_id, "t_a");
        assert_eq!(snap[0].app_name, "api");
    }

    /// 503 from the control plane must surface as an Err so the
    /// poller logs a "will retry" message instead of silently
    /// carrying an empty table. (If the token is wrong the operator
    /// needs to know — empty-by-silence is the failure mode that
    /// makes for a 6-hour debugging session.)
    #[tokio::test]
    async fn fetch_and_apply_returns_err_on_5xx() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let url = format!("{}/api/internal/domains", server.uri());
        let table = RoutingTable::new();
        let err = fetch_and_apply(&http, &url, "test-token", &table)
            .await
            .expect_err("503 must surface as Err");
        // Typed match on PollError::HttpStatus — the Display impl
        // also includes the status code, so the operator's log
        // line still names "503" without the test relying on it.
        match err {
            PollError::HttpStatus { status, .. } => {
                assert_eq!(status, 503, "status must be 503, got {status}");
            }
            other => panic!("expected PollError::HttpStatus, got {other:?}"),
        }
    }

    /// After two polls with different bodies, the second poll must
    /// produce a clean diff: only the FQDN that actually changed
    /// appears in `added` / `removed`. This is the regression test
    /// for the "diff vs full replace" choice — a full-replace impl
    /// would mark every FQDN as added on every tick, defeating the
    /// churn-logging design.
    #[tokio::test]
    async fn fetch_and_apply_second_poll_only_diff() {
        let server = MockServer::start().await;

        // First poll: two FQDNs.
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": "d_1", "tenant_id": "t_a", "app_name": "api", "fqdn": "api.acme.com"},
                {"id": "d_2", "tenant_id": "t_b", "app_name": "web", "fqdn": "web.acme.com"}
            ])))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let url = format!("{}/api/internal/domains", server.uri());
        let table = RoutingTable::new();

        fetch_and_apply(&http, &url, "test-token", &table)
            .await
            .unwrap();

        // Second poll: same two + one new.
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": "d_1", "tenant_id": "t_a", "app_name": "api", "fqdn": "api.acme.com"},
                {"id": "d_2", "tenant_id": "t_b", "app_name": "web", "fqdn": "web.acme.com"},
                {"id": "d_3", "tenant_id": "t_c", "app_name": "blog", "fqdn": "blog.acme.com"}
            ])))
            .mount(&server)
            .await;

        let (added, removed) = fetch_and_apply(&http, &url, "test-token", &table)
            .await
            .unwrap();
        assert_eq!(added, vec!["blog.acme.com".to_string()]);
        assert!(removed.is_empty());
    }

    /// The bearer-token gate: the control plane's
    /// `WorkerAuth` middleware checks the JWT before serving
    /// `/api/internal/domains`. A token-mismatch produces 401.
    /// This test pins that we DO send the configured token (not, e.g.,
    /// no Authorization header at all) and the assertion lives on
    /// the wiremock side via the `bearer_token` matcher.
    #[tokio::test]
    async fn fetch_and_apply_sends_configured_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .and(bearer_token("ingest-fra-1y"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let url = format!("{}/api/internal/domains", server.uri());
        let table = RoutingTable::new();
        fetch_and_apply(&http, &url, "ingest-fra-1y", &table)
            .await
            .unwrap();
    }

    /// Malformed JSON (e.g. a 200 with an HTML error page) must
    /// surface as a decode error, NOT silently zero out the table.
    /// Without this test, a future refactor that logs and returns
    /// Ok(()) on decode failure would erase tenant domains from the
    /// routing table on every misbehaving 200.
    #[tokio::test]
    async fn fetch_and_apply_returns_err_on_malformed_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>not json</html>"))
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let url = format!("{}/api/internal/domains", server.uri());
        let table = RoutingTable::new();
        let err = fetch_and_apply(&http, &url, "test-token", &table)
            .await
            .expect_err("malformed JSON must surface as Err");
        // Typed match: malformed JSON → serde_json::Error →
        // PollError::Decode. The previous substring match on
        // "decoding" was a smell: a future refactor that named the
        // variant differently would silently break the test.
        match err {
            PollError::Decode(msg) => {
                assert!(!msg.is_empty(), "decode error should carry a message");
            }
            other => panic!("expected PollError::Decode, got {other:?}"),
        }
    }

    /// `run` with `control_plane_url` empty returns Ok immediately
    /// (default-only mode), and `run` would normally loop forever —
    /// we don't have a test for the happy loop because it requires a
    /// controllable ticker, which `tokio::time::pause()` and a fake
    /// interval would need. The fetch_and_apply tests above cover
    /// the I/O path; this just pins the "empty URL → no-op" branch.
    #[tokio::test]
    async fn run_returns_ok_when_control_plane_url_empty() {
        let cfg = Config {
            nats_url: "nats://localhost:4222".into(),
            caddy_admin_url: "http://127.0.0.1:2019".into(),
            region: "test".into(),
            cert_file: "/tmp/c.pem".into(),
            key_file: "/tmp/k.pem".into(),
            cert_file_2: None,
            key_file_2: None,
            listen_http: ":80".into(),
            listen_https: ":443".into(),
            refresh_debounce_ms: 1000,
            http_to_https: true,
            admin_token: None,
            control_plane_url: String::new(),
            service_token: "ignored".into(),
            domain_poll_interval: Duration::from_secs(30),
            caddy_admin_listen: "localhost:2019".into(),
            metrics_listen: ":9091".into(),
            max_conns: 0,
            max_conns_per_ip: 0,
            per_ip_rps: 0,
            per_ip_burst: 0,
            rate_limit_rps_default: 0,
            rate_limit_burst_default: 0,
            rate_limit_fetch_interval: Duration::from_secs(60),
            quota_fetch_interval: Duration::from_secs(30),
            control_plane_api_url: "http://localhost:8080".into(),
            internal_token: None,
            stale_timeout: Duration::from_secs(60),
            prune_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(3),
            health_check_uri: "/healthz".into(),
            health_check_max_fails: 2,
            rate_limit_rps_tenant_default: 0,
            rate_limit_burst_tenant_default: 0,
            tenant_rate_limit_fetch_interval: Duration::from_secs(30),
            global_rate_limit_rps: 0,
            global_rate_limit_burst: 0,
        };
        let table = std::sync::Arc::new(RoutingTable::new());

        // The `unused import: CertKey` warning from the test helper
        // on line 357 does not affect CI — it fires only in
        // `#[cfg(test)]` and the Caddy JSON builder never produces
        // a key/cert order bug. Marked `#[allow(dead_code)]` on the
        // struct in caddy.rs.
        let notify = std::sync::Arc::new(Notify::new());
        run(cfg, table, notify, CancellationToken::new())
            .await
            .unwrap();
    }

    /// Three consecutive 401/403 responses from the control plane
    /// must cause `run` to fail-fast (return Err) so the operator
    /// notices the rotated `INGRESS_SERVICE_TOKEN`. Without this,
    /// a stale token would silently keep the FQDN table empty for
    /// hours — the routes would just stop resolving, with no
    /// error in the logs beyond a recurring `warn!`.
    ///
    /// We use a real-time 50ms interval (not `tokio::time::pause`)
    /// because `pause` + `tokio::spawn` + `advance` interactions are
    /// racy: the spawned `interval` is on a different scheduling
    /// path that the test's `advance` calls don't always wake in
    /// time. 50ms × 3 ticks = 150ms wall clock, which is acceptable
    /// for a unit test.
    #[tokio::test]
    async fn run_fails_fast_after_three_consecutive_401s() {
        // The wiremock server returns 401 on every call. `.expect(3)`
        // is a tight pin on the budget constant — if
        // `MAX_CONSECUTIVE_AUTH_ERRORS` changes, this test signals
        // the change to the next reader.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .respond_with(ResponseTemplate::new(401))
            .expect(3)
            .mount(&server)
            .await;

        let cfg = test_cfg_with_poll_interval(server.uri(), Duration::from_millis(50));
        let table = std::sync::Arc::new(RoutingTable::new());
        let notify = std::sync::Arc::new(Notify::new());

        // Spawn `run` and wait up to 2s for it to return Err.
        let run_handle =
            tokio::spawn(async move { run(cfg, table, notify, CancellationToken::new()).await });
        let result = tokio::time::timeout(Duration::from_secs(2), run_handle)
            .await
            .expect("run loop didn't return within 2s")
            .expect("run task panicked");
        let err = result.expect_err("run must return Err on 3 consecutive 401s");
        // Typed match on PollError::HttpStatus — the run() function
        // returns `anyhow::Error` (via `.into()`), so the
        // PollError variant is wrapped in anyhow. Use a downcast
        // helper to assert on the variant.
        let poll_err = err
            .downcast_ref::<PollError>()
            .expect("run must surface PollError");
        match poll_err {
            PollError::HttpStatus { status, .. } => {
                assert_eq!(*status, 401, "expected status 401, got {status}");
            }
            other => panic!("expected PollError::HttpStatus, got {other:?}"),
        }
    }

    /// Typed-error variant: a 403 (token good, role wrong) must
    /// also trip the fail-fast path, just like 401 (token
    /// missing/expired). Without the typed match, a future
    /// status code that just happens to contain "401" or "403"
    /// in a JSON body would have falsely tripped (or falsely
    /// missed) the substring gate. Pins the typed variant
    /// matches on the literal HTTP status.
    #[tokio::test]
    async fn run_fails_fast_after_three_consecutive_403s_typed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .respond_with(ResponseTemplate::new(403))
            .expect(3)
            .mount(&server)
            .await;

        let cfg = test_cfg_with_poll_interval(server.uri(), Duration::from_millis(50));
        let table = std::sync::Arc::new(RoutingTable::new());
        let notify = std::sync::Arc::new(Notify::new());

        let run_handle =
            tokio::spawn(async move { run(cfg, table, notify, CancellationToken::new()).await });
        let result = tokio::time::timeout(Duration::from_secs(2), run_handle)
            .await
            .expect("run loop didn't return within 2s")
            .expect("run task panicked");
        let err = result.expect_err("run must return Err on 3 consecutive 403s");
        let poll_err = err
            .downcast_ref::<PollError>()
            .expect("run must surface PollError");
        match poll_err {
            PollError::HttpStatus { status, .. } => {
                assert_eq!(*status, 403, "expected status 403, got {status}");
            }
            other => panic!("expected PollError::HttpStatus, got {other:?}"),
        }
    }

    /// Shared-Notify wiring test (PR #133 review finding #1).
    /// Spawns the poller with a shared `Arc<Notify>` and a listener
    /// task that consumes one permit from it. If the wiring regresses
    /// (e.g. main.rs creates a fresh Notify that only the poller
    /// knows about) this listener never wakes and the test times
    /// out. Without it, the regression only surfaces at runtime as
    /// "FQDN routes never reach Caddy on cold start."
    #[tokio::test]
    async fn run_signals_shared_notify_on_poll() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/domains"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": "d_1", "tenant_id": "t_a", "app_name": "api", "fqdn": "api.acme.com"}
            ])))
            .expect(1)
            .mount(&server)
            .await;

        let cfg = test_cfg_with_poll_interval(server.uri(), Duration::from_millis(50));
        let table = std::sync::Arc::new(RoutingTable::new());
        let notify = std::sync::Arc::new(Notify::new());
        let wakeups = std::sync::Arc::new(AtomicUsize::new(0));
        let wakeups_clone = wakeups.clone();

        // Listener: wait for the shared Notify to fire. The
        // AtomicUsize lets us assert *that* it fired (vs. timing
        // out) regardless of debounce/render logic downstream.
        let notify_for_listener = notify.clone();
        let listener = tokio::spawn(async move {
            notify_for_listener.notified().await;
            wakeups_clone.store(1, Ordering::SeqCst);
        });

        // Poller: spawn run() so it loops on its own interval.
        let notify_for_poller = notify.clone();
        let _run_handle = tokio::spawn(async move {
            let _ = run(cfg, table, notify_for_poller, CancellationToken::new()).await;
        });

        // Bound the wait — if the Notify never fires, the listener
        // task is still alive and we surface a clear failure.
        let result = tokio::time::timeout(Duration::from_secs(2), listener).await;
        assert!(
            result.is_ok(),
            "shared Notify was never signalled — the poller's notify_one() \
             did not reach the renderer-side listener. This is the cold-start \
             FQDN-routes-never-reach-Caddy bug from PR #133 review finding #1."
        );
        result.unwrap().expect("listener task panicked");

        assert_eq!(
            wakeups.load(Ordering::SeqCst),
            1,
            "listener must observe exactly one wakeup from the poller's notify_one()"
        );
    }

    /// Build a minimal Config for the run() tests. Filled in with
    /// dummy file paths and a short poll interval.
    fn test_cfg_with_poll_interval(control_plane_url: String, poll: Duration) -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            caddy_admin_url: "http://127.0.0.1:2019".into(),
            region: "test".into(),
            cert_file: "/tmp/c.pem".into(),
            key_file: "/tmp/k.pem".into(),
            cert_file_2: None,
            key_file_2: None,
            listen_http: ":80".into(),
            listen_https: ":443".into(),
            refresh_debounce_ms: 1000,
            http_to_https: true,
            admin_token: None,
            control_plane_url,
            service_token: "stale-token".into(),
            domain_poll_interval: poll,
            caddy_admin_listen: "localhost:2019".into(),
            metrics_listen: ":9091".into(),
            max_conns: 0,
            max_conns_per_ip: 0,
            per_ip_rps: 0,
            per_ip_burst: 0,
            rate_limit_rps_default: 0,
            rate_limit_burst_default: 0,
            rate_limit_fetch_interval: Duration::from_secs(60),
            quota_fetch_interval: Duration::from_secs(30),
            control_plane_api_url: "http://localhost:8080".into(),
            internal_token: None,
            stale_timeout: Duration::from_secs(60),
            prune_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(3),
            health_check_uri: "/healthz".into(),
            health_check_max_fails: 2,
            rate_limit_rps_tenant_default: 0,
            rate_limit_burst_tenant_default: 0,
            tenant_rate_limit_fetch_interval: Duration::from_secs(30),
            global_rate_limit_rps: 0,
            global_rate_limit_burst: 0,
        }
    }
}
