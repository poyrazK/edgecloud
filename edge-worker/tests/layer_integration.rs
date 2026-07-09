//! Phase E — L1–L10 integration tests.
//!
//! These tests exercise the end-to-end FaaS dispatch path:
//!
//!   * The handler fixture (`edge-worker/tests/fixtures/handler.wasm`)
//!     is parsed through the runtime linker and instantiated.
//!   * A `HandlerDispatch` binds an HTTP/1 server on a free port.
//!   * `reqwest::Client` makes real HTTP requests against it.
//!
//! Each test is gated on the fixture file being present (skip with a
//! clear message otherwise) and on the runtime being buildable. They
//! do **not** require Docker / NATS / the supervisor — the harness
//! instantiates `HandlerDispatch` directly so we exercise the proxy
//! path without spinning up the full worker.
//!
//! ## Layer index
//!
//! - L1–L4: linker-state tests, in `edge-runtime/tests/v0_2_smoke.rs`
//! - L5: dispatch round-trip — `l5_handler_dispatch_round_trip` (this file)
//! - L6: body cap — `l6_request_body_over_cap_returns_413` (this file)
//! - L6b: body cap under — `l6b_request_body_under_cap_reaches_guest` (this file)
//! - L7: per-request timeout — `l7_per_request_timeout_returns_500` (this file)
//! - L8: long-running self-host — deferred (long_running fixture not yet built)
//! - L9: tenant filesystem isolation — deferred (fixture fs paths not yet impl)
//! - L10: deadline interrupts outbound — deferred (outgoing-handler path pending)
//! - L11: process.get-env — `l11_guest_calls_process_get_env` (this file)
//! - L12: time.now — `l12_guest_calls_time_now` (this file)
//! - L13: kv-store round-trip — `l13_guest_calls_kv_store_round_trip` (this file)
//! - L14: cache round-trip — `l14_guest_calls_cache_round_trip` (this file)
//! - L15: observe.emit-log — `l15_guest_emit_log_reaches_sink` (this file)
//! - L16: scheduling.schedule-once — `l16_guest_schedules_task` (this file)
//! - L17: kv-store exists — `l17_kv_store_exists` (this file)
//! - L18: kv-store list-keys — `l18_kv_store_list_keys` (this file)
//! - L19: kv-store clear — `l19_kv_store_clear` (this file)
//! - L20: kv-store batch ops — `l20_kv_store_batch_ops` (this file)
//! - L21: cache size — `l21_cache_size` (this file)
//! - L22: cache exists/list/clear — `l22_cache_exists_and_list` (this file)
//! - L23: cache batch ops — `l23_cache_batch_ops` (this file)
//! - L24: observe counter/gauge/histogram — `l24_observe_counter_gauge_histogram` (this file)
//! - L25: time resolution — `l25_time_resolution` (this file)
//! - L26: scheduling repeat/cancel — `l26_scheduling_repeat_and_cancel` (this file)
//! - L27: process get-all-env — `l27_process_get_all_env` (this file)
//! - L28: process get-args — `l29_process_get_args` (this file)
//! - L29: process get-cwd — `l30_process_get_cwd` (this file)
//! - L31: wasi:sockets BlockAll default — `l31_socket_egress_block_all_denies_under_default` (this file)
//! - L32: wasi:sockets AllowList + hard-deny target — `l32_socket_egress_allowlist_blocks_hard_deny_ip` (this file)
//! - L33: wasi:sockets AllowList + public IP — `l33_socket_egress_allowlist_permits_public_ip` (this file)
//! - L34: wasi:http TLS handshake — `l34_handler_dispatch_completes_real_tls_handshake` (this file)
//! - L35: ALPN h2 routing — `l35_handler_dispatch_h2_alpn_routes_to_h2_dispatcher` (this file)
//! - L51: wasi:sockets/ip-name-lookup + connect (BlockAll) — `l51_dns_resolve_and_connect_block_all_denies` (this file; issue #309 follow-up)
//! - L52: wasi:sockets/ip-name-lookup + connect (HostnamePinned, dormant) — `l52_dns_resolve_and_connect_hostname_pinned_dormant_denies` (this file; issue #309 follow-up)
//!
//! ## Skip policy
//!
//! Set `SKIP_INTEGRATION_TESTS=1` (or `CI=1`) to skip all tests in this
//! file without taking down the rest of the suite. Tests also self-skip
//! when the handler fixture is missing.
//!
//! Run with:
//!   cargo test --manifest-path edge-worker/Cargo.toml --test layer_integration
//! Skip:
//!   SKIP_INTEGRATION_TESTS=1 cargo test --manifest-path edge-worker/Cargo.toml

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
use edge_runtime::socket_egress::SocketEgressPolicy;
use edge_runtime::{
    create_component_linker_handler, create_engine, EgressPolicy, RequestMeter, RuntimeState,
};
use edge_worker::dispatch::{try_load_tls_config, HandlerConfig, HandlerDispatch};
use rcgen::generate_simple_self_signed;
use reqwest::StatusCode;
use std::io::Write;
use tokio::sync::broadcast;
use wasmtime::component::{Component, InstancePre};

/// Open a `127.0.0.1:0` listener and snapshot the kernel-assigned
/// port, then drop the listener. The brief window between drop and
/// the dispatch's own bind in `HandlerDispatch::serve` is small
/// enough that no test in this binary can race for the same port
/// (each test process binds its own ephemeral port, and the kernel
/// never re-allocates a port to a process that already holds one).
///
/// Replaces the prior static-counter allocator (`NEXT_PORT` started
/// at 30000 and incremented atomically), which raced across nextest
/// subprocesses and triggered EADDRINUSE on the v0.2 PR CI lane.
/// See issue #212 for the broader architectural fix on the
/// production side (move the bind into `HandlerDispatch::new`).
///
/// Returns `Err` on bind failure; bind-success is the only signal
/// that the kernel had a free ephemeral port, which it essentially
/// always does on Linux/macOS.
fn ephemeral_port() -> Option<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).ok()?;
    let port = listener.local_addr().ok()?.port();
    drop(listener);
    Some(port)
}

/// Skip predicate for the layer integration tests. Unlike the supervisor
/// integration tests, these don't need Docker — but they're skipped when
/// the fixture is missing. The fixture is committed to the repo so CI runs
/// these tests on every PR.
fn should_skip_layer_tests() -> bool {
    let candidates = [
        "tests/fixtures/handler.wasm",
        "edge-worker/tests/fixtures/handler.wasm",
    ];
    !candidates.iter().any(|p| PathBuf::from(p).exists())
}

/// Locate the pre-built `handler.wasm` fixture. Returns `None` if the
/// fixture is missing in any of the expected paths.
fn handler_fixture_path() -> Option<PathBuf> {
    [
        "tests/fixtures/handler.wasm",
        "edge-worker/tests/fixtures/handler.wasm",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|p| p.exists())
}

/// No-op log sink for tests — the L5/L7 tests don't observe log output
/// but the HandlerConfig requires an `Arc<dyn LogSink>`.
struct NullSink;

impl LogSink for NullSink {
    fn push(&self, _record: LogRecord, _ctx: AppLogContext) {}
}

/// Per-test harness. Holds the engine + linker + instance_pre + the
/// spawned dispatch server + its shutdown handle.
///
/// Drop tears the server down via the broadcast channel.
struct LayerHarness {
    url_base: String,
    client: reqwest::Client,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    /// Wall-clock start of the most recent request, used by `elapsed()`.
    request_started: StdMutex<Option<Instant>>,
}

impl LayerHarness {
    /// Spin up a new handler dispatch on a free port using the pre-built
    /// `handler.wasm` fixture. The returned harness owns a reqwest
    /// client ready to fire requests.
    async fn spawn() -> anyhow::Result<Self> {
        let path = handler_fixture_path().context("handler.wasm fixture missing")?;

        let engine = create_engine().context("create_engine")?;
        let linker =
            create_component_linker_handler(&engine).context("create_component_linker_handler")?;

        let bytes = std::fs::read(&path).context("read handler.wasm")?;
        let component = Component::from_binary(&engine, &bytes).map_err(anyhow::Error::from)?;

        // Pre-compile the component into an InstancePre. The per-request
        // path rebuilds its own store+state; this pre-compilation step
        // is what makes HandlerDispatch::serve fast on the hot path.
        let instance_pre: InstancePre<RuntimeState> = linker
            .instantiate_pre(&component)
            .map_err(anyhow::Error::from)?;

        // Port allocation: prefer 8192+ (ephemeral range, less likely
        // to clash with a developer's local services).
        let port = ephemeral_port().expect("bind ephemeral port");

        let config = HandlerConfig {
            tenant_id: "test-tenant".to_string(),
            egress: Arc::new(EgressPolicy::allow_all()),
            log_sink: Arc::new(NullSink),
            app_ctx: AppLogContext {
                app_name: "l5".to_string(),
                tenant_id: "test-tenant".to_string(),
                deployment_id: "l5-deployment".to_string(),
            },
            meter: Arc::new(RequestMeter::new(
                "test-tenant".to_string(),
                "l7-deployment".to_string(),
            )),
            env: HashMap::new(),
            max_request_body_bytes: 10 * 1024 * 1024,
            metrics_acc: None,
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
                std::time::Instant::now(),
            ))),
            max_memory_mb: 256,
            cpu_budget_ms: 1000,
            preview_id: None,
            preview_pr_number: None,
        };

        let state = std::sync::Arc::new(tokio::sync::RwLock::new(
            edge_worker::state::WorkerState::new(engine.clone()),
        ));

        let dispatch = Arc::new({
            let d = HandlerDispatch::new(
                port,
                1_000,
                1,
                config,
                None,
                std::sync::Arc::new(edge_worker::downloader::Downloader::new(
                    "http://localhost".to_string(),
                    std::path::PathBuf::from("/tmp"),
                    edge_worker::auth::WorkerJwtSigner::new(vec![], None, "", "", "", ""),
                    None,
                )),
                "test-deploy".to_string(),
                std::sync::Arc::new(edge_worker::supervisor::StandbyPool::new(0).unwrap()),
                state,
            )
            .unwrap();
            d.set_proxy_pre(wasmtime_wasi_http::p2::bindings::ProxyPre::new(instance_pre).unwrap())
                .await;
            d
        });

        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let shutdown_rx = shutdown_tx.subscribe();
        let dispatch_for_serve = dispatch.clone();
        tokio::spawn(async move {
            if let Err(e) = dispatch_for_serve.serve(shutdown_rx).await {
                tracing::error!(err = %e, "HandlerDispatch serve failed");
            }
        });

        // Wait for the TCP listener to be ready. Retry a few times because
        // `tokio::spawn` may not schedule the `serve` task immediately.
        let addr = format!("127.0.0.1:{port}");
        for _ in 0..20 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(Self {
            url_base: format!("http://127.0.0.1:{port}"),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("reqwest::Client::builder")?,
            shutdown_tx,
            request_started: StdMutex::new(None),
        })
    }

    /// Fire a GET against the harness and return (status, body).
    async fn get(&self, path: &str) -> anyhow::Result<(StatusCode, String)> {
        let url = format!("{}{}", self.url_base, path);
        if let Ok(mut guard) = self.request_started.lock() {
            *guard = Some(Instant::now());
        }
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        Ok((status, body))
    }

    #[allow(dead_code)]
    fn elapsed(&self) -> Duration {
        self.request_started
            .lock()
            .ok()
            .and_then(|t| *t)
            .map(|started| started.elapsed())
            .unwrap_or(Duration::ZERO)
    }
}

impl Drop for LayerHarness {
    fn drop(&mut self) {
        // Best-effort shutdown. The dispatch's `serve` loop exits on
        // the next select! poll.
        let _ = self.shutdown_tx.send(());
    }
}

// ---- L5: handler dispatch round-trip ------------------------------------

/// L5: a Handler component is dispatched through `HandlerDispatch` and
/// the guest's `handle(req, out)` produces a real HTTP response that
/// matches the contract documented in
/// `edge-worker/tests/fixtures/README.md`.
#[tokio::test(flavor = "multi_thread")]
async fn l5_handler_dispatch_round_trip() {
    if should_skip_layer_tests() {
        eprintln!(
            "SKIPPED: layer integration tests disabled (CI=1 / \
             SKIP_INTEGRATION_TESTS=1 / handler.wasm fixture missing)"
        );
        return;
    }

    let harness = LayerHarness::spawn().await.expect("LayerHarness::spawn");

    let (status, body) = harness
        .get("/")
        .await
        .expect("GET / against the handler dispatch");
    assert_eq!(status, StatusCode::OK, "GET / status was {body}");
    let parsed: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("body {body:?} not JSON: {e}"));
    assert_eq!(parsed["hello"], "handler");
    assert_eq!(parsed["path"], "/");
}

/// L5b: paths the fixture doesn't implement must return 404 (not 200,
/// not a connection drop). The fixture's documented contract is "all
/// other paths return 404". This catches a regression where the guest
/// panics and the dispatch returns 500 instead.
#[tokio::test(flavor = "multi_thread")]
async fn l5b_handler_dispatch_unknown_path_returns_404() {
    if should_skip_layer_tests() {
        return;
    }
    let harness = LayerHarness::spawn().await.expect("LayerHarness::spawn");
    let (status, _body) = harness
        .get("/does-not-exist")
        .await
        .expect("GET /does-not-exist");
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "unknown path should return 404"
    );
}

// ---- L6: body cap enforcement -------------------------------------------

/// L6: a request whose `Content-Length` exceeds the per-app body cap
/// receives a synthetic 413 response BEFORE the guest is invoked.
///
/// The body cap is enforced in `HandlerDispatch::handle_request` via a
/// pre-check on the `Content-Length` header. This test wires a dispatch
/// with a tiny cap (100 bytes) and asserts that a POST with a 1 KiB
/// body returns 413.
///
/// A companion test (l6b) verifies that requests under the cap reach
/// the guest normally (and get the guest's response, not a 413).
#[tokio::test(flavor = "multi_thread")]
async fn l6_request_body_over_cap_returns_413() {
    if should_skip_layer_tests() {
        return;
    }

    // Spawn a harness with a tight 100-byte body cap.
    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l6".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l6-deployment".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l6-deployment".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 100,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client::builder");

    let url = format!("http://127.0.0.1:{port}/");
    let large_body = "x".repeat(1024); // 1 KiB — well over the 100-byte cap.
    let resp = client
        .post(&url)
        .body(large_body)
        .send()
        .await
        .expect("POST with large body");
    assert_eq!(
        resp.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "over-cap request should receive 413, got {}",
        resp.status()
    );
}

/// L6b: a request whose `Content-Length` is under the per-app body cap
/// reaches the guest normally and returns the guest's response (not a 413).
#[tokio::test(flavor = "multi_thread")]
async fn l6b_request_body_under_cap_reaches_guest() {
    if should_skip_layer_tests() {
        return;
    }

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l6b".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l6b-deployment".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l6b-deployment".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None, // 10 MB — generous
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256, // 10 MB — generous
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client::builder");

    // The handler fixture only accepts GET. A POST will hit the
    // guest (under cap) and return 405 (method not allowed). The key
    // assertion is that we get an HTTP response from the guest, not
    // a 413 from the cap pre-check.
    let url = format!("http://127.0.0.1:{port}/");
    let small_body = "hello";
    let resp = client
        .post(&url)
        .body(small_body)
        .send()
        .await
        .expect("POST with small body");
    assert_eq!(
        resp.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "under-cap POST should reach guest and return 405, got {}",
        resp.status()
    );
}

// ---- L7: per-request timeout -------------------------------------------

/// L7: a handler that exceeds the per-request epoch deadline returns
/// 500 to the client. The fixture's `/busy` path busy-loops a counter
/// for ~5s of Wasm execution; the harness sets a 100ms request budget
/// and asserts the response returns well before the busy loop would
/// naturally complete.
#[tokio::test(flavor = "multi_thread")]
async fn l7_per_request_timeout_returns_500() {
    if should_skip_layer_tests() {
        return;
    }

    // Build a harness with a tight 100ms budget instead of the
    // 1000ms default. We can't change `LayerHarness::spawn`'s
    // hardcoded budget without exposing another constructor, so
    // duplicate the spawn path here.
    let path = handler_fixture_path().expect("handler.wasm fixture missing");
    let engine = create_engine().expect("create_engine");
    let linker = create_component_linker_handler(&engine).expect("create_component_linker_handler");
    let bytes = std::fs::read(&path).expect("read handler.wasm");
    let component = Component::from_binary(&engine, &bytes).expect("Component::from_binary");

    let instance_pre: InstancePre<RuntimeState> = linker
        .instantiate_pre(&component)
        .expect("linker.instantiate_pre");

    let port = ephemeral_port().expect("bind ephemeral port");

    let config = HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l7".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l7-deployment".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l7-deployment".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 100,
        preview_id: None,
        preview_pr_number: None,
    };

    let state = std::sync::Arc::new(tokio::sync::RwLock::new(
        edge_worker::state::WorkerState::new(engine.clone()),
    ));

    let dispatch = Arc::new({
        let d = HandlerDispatch::new(
            port,
            /* request_budget_ms */ 100,
            1,
            config,
            None,
            std::sync::Arc::new(edge_worker::downloader::Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                edge_worker::auth::WorkerJwtSigner::new(vec![], None, "", "", "", ""),
                None,
            )),
            "test-deploy".to_string(),
            std::sync::Arc::new(edge_worker::supervisor::StandbyPool::new(0).unwrap()),
            state,
        )
        .unwrap();
        d.set_proxy_pre(wasmtime_wasi_http::p2::bindings::ProxyPre::new(instance_pre).unwrap())
            .await;
        d
    });

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let shutdown_rx = shutdown_tx.subscribe();
    let dispatch_for_serve = dispatch.clone();
    tokio::spawn(async move {
        let _ = dispatch_for_serve.serve(shutdown_rx).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client::builder");

    let url = format!("http://127.0.0.1:{port}/busy");
    let started = Instant::now();
    let resp = client.get(&url).send().await.expect("GET /busy");
    let elapsed = started.elapsed();

    // The dispatch's `Err(_dropped)` arm wraps the guest trap into a
    // 500. hyper's `serve_connection` returns the error to the client
    // as a 500-class response.
    assert!(
        resp.status().is_server_error() || resp.status() == StatusCode::INTERNAL_SERVER_ERROR,
        "/busy should return 5xx after deadline; got {}",
        resp.status()
    );

    // Budget is 100ms. The busy loop would otherwise run for ~5s. If
    // the deadline fired correctly the request returned in well under
    // 1s.
    assert!(
        elapsed < Duration::from_secs(2),
        "request should have been interrupted at ~100ms, not run for \
         the full busy loop (elapsed: {elapsed:?})"
    );

    let _ = shutdown_tx.send(());
}

// ---- Shared helpers -----------------------------------------------------

/// Spawn a `HandlerDispatch` on a free port with a supplied `HandlerConfig`.
/// Returns the port number the server is listening on.
///
/// Duplicates the spawn logic from `LayerHarness::spawn` but accepts an
/// arbitrary `HandlerConfig` so tests can vary body caps, timeouts, and
/// other knobs without repeating the full setup.
async fn spawn_handler_with_config(config: HandlerConfig) -> (u16, broadcast::Sender<()>) {
    spawn_handler_with_tls_config(config, None).await
}

/// Same as `spawn_handler_with_config` but lets the test opt into TLS.
/// Mirrors the production supervisor's `try_load_tls_config` +
/// `tls_config` wiring at `edge-worker/src/supervisor.rs:433-441` (issue
/// #209 + follow-up #272). Used by the L34/L35 e2e TLS tests.
async fn spawn_handler_with_tls_config(
    config: HandlerConfig,
    tls_config: Option<Arc<rustls::ServerConfig>>,
) -> (u16, broadcast::Sender<()>) {
    let path = handler_fixture_path().expect("handler.wasm fixture missing");
    let engine = create_engine().expect("create_engine");
    let linker = create_component_linker_handler(&engine).expect("create_component_linker_handler");
    let bytes = std::fs::read(&path).expect("read handler.wasm");
    let component = Component::from_binary(&engine, &bytes).expect("Component::from_binary");

    let instance_pre: InstancePre<RuntimeState> = linker
        .instantiate_pre(&component)
        .expect("linker.instantiate_pre");

    let port = ephemeral_port().expect("bind ephemeral port");

    let state = std::sync::Arc::new(tokio::sync::RwLock::new(
        edge_worker::state::WorkerState::new(engine.clone()),
    ));

    let dispatch = Arc::new({
        let d = HandlerDispatch::new(
            port,
            5_000,
            10,
            config,
            tls_config,
            std::sync::Arc::new(edge_worker::downloader::Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                edge_worker::auth::WorkerJwtSigner::new(vec![], None, "", "", "", ""),
                None,
            )),
            "test-deploy".to_string(),
            std::sync::Arc::new(edge_worker::supervisor::StandbyPool::new(0).unwrap()),
            state,
        )
        .unwrap();
        d.set_proxy_pre(wasmtime_wasi_http::p2::bindings::ProxyPre::new(instance_pre).unwrap())
            .await;
        d
    });

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    tokio::spawn(async move {
        let result = dispatch.serve(shutdown_rx).await;
        eprintln!("[TEST] HandlerDispatch::serve returned: {result:?}");
    });

    // Wait for the TCP listener to be ready.
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..20 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return (port, shutdown_tx);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("HandlerDispatch on {addr} did not start listening after 1s");
}

// ---- Concurrent request isolation ---------------------------------------

/// Fires N concurrent `GET /` requests against a single `HandlerDispatch`
/// and asserts all return 200. Verifies the per-request isolation path
/// (`ProxyPre::instantiate_async` called concurrently with fresh
/// `RuntimeState` per request) doesn't deadlock or produce garbled
/// responses.
#[tokio::test(flavor = "multi_thread")]
async fn l5c_concurrent_requests_all_succeed() {
    if should_skip_layer_tests() {
        return;
    }

    let harness = LayerHarness::spawn().await.expect("LayerHarness::spawn");
    let mut handles = Vec::new();

    for i in 0..10 {
        let base = harness.url_base.clone();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest::Client");
        handles.push(tokio::spawn(async move {
            let url = format!("{base}/");
            let resp = client.get(&url).send().await.expect("GET /");
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            (i, status, body)
        }));
    }

    for handle in handles {
        let (i, status, body) = handle.await.expect("concurrent request joined");
        assert_eq!(
            status,
            StatusCode::OK,
            "concurrent request {i} should return 200, got {status}: {body}"
        );
    }
}

// ---- E2E edge:cloud interface tests (L11–L16) --------------------------

/// A `LogSink` that records every push for later inspection.
#[derive(Clone)]
struct RecordingLogSink {
    records: Arc<std::sync::Mutex<Vec<(LogRecord, AppLogContext)>>>,
}

impl RecordingLogSink {
    fn new() -> Self {
        Self {
            records: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    #[allow(dead_code)]
    fn take(&self) -> Vec<(LogRecord, AppLogContext)> {
        std::mem::take(&mut self.records.lock().unwrap())
    }
}

impl LogSink for RecordingLogSink {
    fn push(&self, record: LogRecord, ctx: AppLogContext) {
        self.records.lock().unwrap().push((record, ctx));
    }
}

/// L11: call process.get-env from the guest with a custom env var.
/// Assert the response body matches the injected value.
#[tokio::test(flavor = "multi_thread")]
async fn l11_guest_calls_process_get_env() {
    if should_skip_layer_tests() {
        return;
    }

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l11".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l11".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l11".to_string(),
        )),
        env: HashMap::from([("KV_KEY".into(), "hello-from-host".into())]),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client");

    let resp = client
        .get(format!("http://127.0.0.1:{port}/env/KV_KEY"))
        .send()
        .await
        .expect("GET /env/KV_KEY");
    assert_eq!(resp.status(), StatusCode::OK, "expected 200");
    let body = resp.text().await.expect("body");
    assert_eq!(
        body, "hello-from-host",
        "env var should match injected value"
    );
}

/// L12: call time.now() from the guest. Assert the response is a
/// parseable u64 > 0 (a valid Unix millisecond timestamp).
#[tokio::test(flavor = "multi_thread")]
async fn l12_guest_calls_time_now() {
    if should_skip_layer_tests() {
        return;
    }

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l12".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l12".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l12".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client");

    let resp = client
        .get(format!("http://127.0.0.1:{port}/time/now"))
        .send()
        .await
        .expect("GET /time/now");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.expect("body");
    let ts: u64 = body
        .trim()
        .parse()
        .expect("time.now() should return a u64 timestamp");
    assert!(
        ts > 1_700_000_000,
        "timestamp should be reasonable (> 2023)"
    );
}

/// L13: kv-store round-trip from the guest. Set a key, then get it
/// back in a second request.
#[tokio::test(flavor = "multi_thread")]
async fn l13_guest_calls_kv_store_round_trip() {
    if should_skip_layer_tests() {
        return;
    }

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l13".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l13".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l13".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client");

    let base = format!("http://127.0.0.1:{port}");

    // Set key=color, val=blue
    let resp = client
        .get(format!("{base}/kv/set?key=color&val=blue"))
        .send()
        .await
        .expect("GET /kv/set");
    assert_eq!(resp.status(), StatusCode::OK, "set should succeed");

    // Get key=color
    let resp = client
        .get(format!("{base}/kv/get?key=color"))
        .send()
        .await
        .expect("GET /kv/get");
    assert_eq!(resp.status(), StatusCode::OK, "get should succeed");
    let body = resp.text().await.expect("body");
    assert_eq!(body, "blue", "kv-store should return the value we set");

    // Delete key=color
    let resp = client
        .get(format!("{base}/kv/del?key=color"))
        .send()
        .await
        .expect("GET /kv/del");
    assert_eq!(resp.status(), StatusCode::OK, "del should succeed");

    // Get again — should 404
    let resp = client
        .get(format!("{base}/kv/get?key=color"))
        .send()
        .await
        .expect("GET /kv/get (after delete)");
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "deleted key should 404"
    );
}

/// L14: cache round-trip from the guest. Same pattern as L13 but
/// exercises the cache interface instead of kv-store.
#[tokio::test(flavor = "multi_thread")]
async fn l14_guest_calls_cache_round_trip() {
    if should_skip_layer_tests() {
        return;
    }

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l14".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l14".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l14".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client");

    let base = format!("http://127.0.0.1:{port}");

    // Set key=lang, val=rust
    let resp = client
        .get(format!("{base}/cache/set?key=lang&val=rust"))
        .send()
        .await
        .expect("GET /cache/set");
    assert_eq!(resp.status(), StatusCode::OK);

    // Get key=lang
    let resp = client
        .get(format!("{base}/cache/get?key=lang"))
        .send()
        .await
        .expect("GET /cache/get");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.expect("body");
    assert_eq!(body, "rust");

    // Delete key=lang
    let resp = client
        .get(format!("{base}/cache/del?key=lang"))
        .send()
        .await
        .expect("GET /cache/del");
    assert_eq!(resp.status(), StatusCode::OK);

    // Get again — should 404
    let resp = client
        .get(format!("{base}/cache/get?key=lang"))
        .send()
        .await
        .expect("GET /cache/get (after delete)");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// L15: call observe.emit-log from the guest. Verify the log record
/// reaches the host's LogSink with the correct message.
#[tokio::test(flavor = "multi_thread")]
async fn l15_guest_emit_log_reaches_sink() {
    if should_skip_layer_tests() {
        return;
    }

    let sink = Arc::new(RecordingLogSink::new());
    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: sink.clone() as Arc<dyn LogSink>,
        app_ctx: AppLogContext {
            app_name: "l15".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l15".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l15".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client");

    let resp = client
        .get(format!("http://127.0.0.1:{port}/log?msg=hello-from-wasm"))
        .send()
        .await
        .expect("GET /log");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "log endpoint should return 200"
    );

    // Allow a brief moment for the record to propagate through the observer.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let records = sink.take();
    assert!(
        !records.is_empty(),
        "should have received at least one log record"
    );
    let (record, ctx) = &records[0];
    assert_eq!(record.message, "hello-from-wasm");
    assert_eq!(ctx.app_name, "l15");
    assert_eq!(ctx.tenant_id, "test-tenant");
}

/// L16: call scheduling.schedule-once from the guest. Assert the
/// response is a non-empty string (a UUID).
#[tokio::test(flavor = "multi_thread")]
async fn l16_guest_schedules_task() {
    if should_skip_layer_tests() {
        return;
    }

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l16".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l16".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l16".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client");

    let resp = client
        .get(format!("http://127.0.0.1:{port}/sched/once?ms=60000"))
        .send()
        .await
        .expect("GET /sched/once");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.expect("body");
    assert!(!body.is_empty(), "schedule-once should return a task ID");
    // The ID is a UUID v4: 36 hex chars with hyphens.
    assert_eq!(
        body.len(),
        36,
        "task ID should be a UUID (36 chars), got: {body:?}"
    );
}

// ── L17–L35: remaining edge:cloud interface functions ───────────────────
//
// These tests exercise every function of every edge:cloud interface that
// is not yet covered by L5–L16. Each function gets its own test.

/// Minimal test config — reduces boilerplate for the remaining tests.
fn test_config(app_name: &str) -> HandlerConfig {
    HandlerConfig {
        tenant_id: app_name.to_string(), // unique per test for store isolation
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: app_name.to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: app_name.to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            app_name.to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    }
}

fn make_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client")
}

// ── kv-store (remaining: exists, list-keys, clear, get-many, set-many,
//              delete-many) ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn l17_kv_store_exists() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l17")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    cl.get(b("/kv/set?key=a&val=1")).send().await.unwrap();
    let resp = cl.get(b("/kv/exists?key=a")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "true");

    let resp = cl.get(b("/kv/exists?key=none")).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap(), "false");
}

#[tokio::test(flavor = "multi_thread")]
async fn l18_kv_store_list_keys() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l18")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    cl.get(b("/kv/set?key=a&val=1")).send().await.unwrap();
    cl.get(b("/kv/set?key=b&val=2")).send().await.unwrap();
    cl.get(b("/kv/set?key=ab&val=3")).send().await.unwrap();

    let resp = cl.get(b("/kv/list?prefix=")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let keys: Vec<String> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert!(keys.contains(&"a".to_string()));
    assert!(keys.contains(&"b".to_string()));
    assert!(keys.contains(&"ab".to_string()));

    let resp = cl.get(b("/kv/list?prefix=a")).send().await.unwrap();
    let keys: Vec<String> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(keys.len(), 2, "prefix 'a' should match 'a' and 'ab'");
    assert!(keys.contains(&"a".to_string()));
    assert!(keys.contains(&"ab".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn l19_kv_store_clear() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l19")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    cl.get(b("/kv/set?key=a&val=1")).send().await.unwrap();
    cl.get(b("/kv/set?key=b&val=2")).send().await.unwrap();
    cl.get(b("/kv/clear")).send().await.unwrap();
    let resp = cl.get(b("/kv/get?key=a")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let resp = cl.get(b("/kv/get?key=b")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn l20_kv_store_batch_ops() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l20")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    // set-many
    let resp = cl
        .get(b("/kv/set-many?keys=x,y,z&vals=1,2,3"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // get-many
    let resp = cl.get(b("/kv/get-many?keys=x,y,z")).send().await.unwrap();
    let vals: Vec<Option<String>> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(
        vals,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );

    // Delete two, get-many again
    cl.get(b("/kv/del-many?keys=x,y")).send().await.unwrap();
    let resp = cl.get(b("/kv/get-many?keys=x,y,z")).send().await.unwrap();
    let vals: Vec<Option<String>> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(vals, vec![None, None, Some("3".into())]);
}

// ── cache (remaining: size, exists, list-keys, clear, get-many, set-many,
//            delete-many) ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn l21_cache_size() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l21")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/cache/size")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap().parse::<u32>().unwrap(), 0);

    cl.get(b("/cache/set?key=a&val=1")).send().await.unwrap();
    cl.get(b("/cache/set?key=b&val=2")).send().await.unwrap();
    let resp = cl.get(b("/cache/size")).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap().parse::<u32>().unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn l22_cache_exists_and_list() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l22")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    cl.get(b("/cache/set?key=a&val=1")).send().await.unwrap();

    let resp = cl.get(b("/cache/exists?key=a")).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap(), "true");
    let resp = cl.get(b("/cache/exists?key=none")).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap(), "false");

    let resp = cl.get(b("/cache/list?prefix=a")).send().await.unwrap();
    let keys: Vec<String> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(keys, vec!["a"]);

    cl.get(b("/cache/clear")).send().await.unwrap();
    let resp = cl.get(b("/cache/exists?key=a")).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap(), "false");
}

#[tokio::test(flavor = "multi_thread")]
async fn l23_cache_batch_ops() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l23")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    cl.get(b("/cache/set-many?keys=a,b,c&vals=x,y,z"))
        .send()
        .await
        .unwrap();

    let resp = cl
        .get(b("/cache/get-many?keys=a,b,c"))
        .send()
        .await
        .unwrap();
    let vals: Vec<Option<String>> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(
        vals,
        vec![Some("x".into()), Some("y".into()), Some("z".into())]
    );

    cl.get(b("/cache/del-many?keys=a,b")).send().await.unwrap();
    let resp = cl
        .get(b("/cache/get-many?keys=a,b,c"))
        .send()
        .await
        .unwrap();
    let vals: Vec<Option<String>> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(vals, vec![None, None, Some("z".into())]);
}

// ── observe (remaining: increment-counter, record-gauge, record-histogram,
//             emit-log-record) ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn l24_observe_counter_gauge_histogram() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l24")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    // These are fire-and-forget — we just assert they don't error.
    let resp = cl
        .get(b("/observe/counter?name=hits&val=3"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = cl
        .get(b("/observe/gauge?name=temp&val=36.5"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = cl
        .get(b("/observe/histogram?name=latency&val=42.0"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── time (remaining: resolution — sleep is tested in unit tests only
//            because `time::sleep` calls block_on which panics inside
//            a tokio runtime) ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn l25_time_resolution() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l25")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/time/resolution")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let r: u64 = resp.text().await.unwrap().trim().parse().expect("u64");
    assert!(r > 0, "resolution should be > 0");
}

// ── scheduling (remaining: schedule-repeating, cancel) ──────────────────

#[tokio::test(flavor = "multi_thread")]
async fn l26_scheduling_repeat_and_cancel() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l26")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/sched/repeat?ms=60000")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let id = resp.text().await.unwrap();
    assert_eq!(id.len(), 36, "repeat should return UUID");

    let cancel_url = format!("/sched/cancel?id={id}");
    let resp = cl.get(b(&cancel_url)).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── process (remaining: get-all-env, get-args, get-cwd) ─────────────────

#[tokio::test(flavor = "multi_thread")]
async fn l27_process_get_all_env() {
    if should_skip_layer_tests() {
        return;
    }
    let mut env = HashMap::new();
    env.insert("TEST_VAR".to_string(), "hello".to_string());
    env.insert("ANOTHER_VAR".to_string(), "world".to_string());
    let (port, _tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "l27".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l27".to_string(),
            tenant_id: "l27".to_string(),
            deployment_id: "l27".to_string(),
        },
        meter: Arc::new(RequestMeter::new("l27".to_string(), "l27".to_string())),
        env,
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/env")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let envs: Vec<Vec<String>> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    let pairs: std::collections::HashMap<String, String> = envs
        .into_iter()
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect();
    assert_eq!(pairs.get("TEST_VAR"), Some(&"hello".to_string()));
    assert_eq!(pairs.get("ANOTHER_VAR"), Some(&"world".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn l29_process_get_args() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l29")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/args")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let args: Vec<String> = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert!(
        !args.is_empty(),
        "args should contain at least the binary path"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn l30_process_get_cwd() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l30")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/cwd")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cwd = resp.text().await.unwrap();
    assert!(!cwd.is_empty(), "cwd should not be empty");
    assert!(
        std::path::Path::new(&cwd).is_absolute(),
        "cwd should be absolute: {cwd}"
    );
}

// ── Outbound Metering (L45) ────────────────────────────────────────────

/// L45: outbound byte metering is restored. Fire a GET that returns a
/// known-size response body and assert that
/// `meter.snapshot().outbound_bytes` reflects it (fixes issue #210).
#[tokio::test(flavor = "multi_thread")]
async fn l45_outbound_metering_counts_response_bytes() {
    if should_skip_layer_tests() {
        return;
    }

    let meter = Arc::new(RequestMeter::new(
        "test-tenant".to_string(),
        "l45-deployment".to_string(),
    ));
    let meter_for_config = meter.clone();

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l45".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l45-deployment".to_string(),
        },
        meter: meter_for_config,
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest::Client");

    let url = format!("http://127.0.0.1:{port}/");

    // The handler fixture's "/" path returns JSON ~50-100 bytes.
    let resp = client.get(&url).send().await.expect("GET /");
    assert_eq!(resp.status(), StatusCode::OK);
    let first_body = resp.text().await.expect("body");
    let first_len = first_body.len() as u64;

    // After the first request, outbound bytes should at minimum
    // cover the response body.
    let snap = meter.snapshot();
    assert!(
        snap.outbound_bytes >= first_len,
        "outbound_bytes ({}) should be >= response body size ({})",
        snap.outbound_bytes,
        first_len
    );

    // Fire 99 more and verify accumulation.
    for _ in 0..99 {
        let resp = client.get(&url).send().await.expect("GET /");
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = resp.text().await.expect("body");
    }

    let snap = meter.snapshot();
    assert!(
        snap.outbound_bytes >= 100 * first_len,
        "after 100 requests, outbound_bytes ({}) should be >= {}",
        snap.outbound_bytes,
        100 * first_len
    );
}

// ── L31-L50: System-level behavioral tests ─────────────────────────────
//
// These tests exercise concurrency, multi-tenancy, TTL expiry, resource
// limits, cross-interface interaction, persistence, and time consistency.

// ── Concurrency & Data Races (L31-L35) ────────────────────────────────

/// 100 concurrent kv-store sets — all must succeed.
#[tokio::test(flavor = "multi_thread")]
async fn l31_concurrent_kv_sets() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l31")).await;
    let cl = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let mut handles = Vec::new();
    for i in 0..100 {
        let url = b(&format!("/kv/set?key=k{i}&val=v{i}"));
        let cl = cl.clone();
        handles.push(tokio::spawn(async move { cl.get(&url).send().await }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let resp = handle.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "concurrent set {i} failed");
    }
}

/// 50 concurrent readers and 50 concurrent writers to the same key.
#[tokio::test(flavor = "multi_thread")]
async fn l32_concurrent_kv_read_write() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l32")).await;
    let cl = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let mut handles = Vec::new();
    // 50 writers
    for i in 0..50 {
        let url = b(&format!("/kv/set?key=shared&val=writer-{i}"));
        let cl = cl.clone();
        handles.push(tokio::spawn(async move { cl.get(&url).send().await }));
    }
    // 50 readers
    for _ in 0..50 {
        let url = b("/kv/get?key=shared");
        let cl = cl.clone();
        handles.push(tokio::spawn(async move { cl.get(&url).send().await }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let resp = handle.await.unwrap().unwrap();
        assert!(
            resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND,
            "concurrent read/write request {i} failed: {}",
            resp.status()
        );
    }
}

/// 50 concurrent observers incrementing the same counter.
#[tokio::test(flavor = "multi_thread")]
async fn l33_concurrent_observe_counter() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l33")).await;
    let cl = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let mut handles = Vec::new();
    for _ in 0..50 {
        let url = b("/observe/counter?name=systest&val=1");
        let cl = cl.clone();
        handles.push(tokio::spawn(async move { cl.get(&url).send().await }));
    }
    for handle in handles {
        let resp = handle.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "concurrent counter failed");
    }
}

/// 50 concurrent schedule-once calls — all must return unique UUIDs.
#[tokio::test(flavor = "multi_thread")]
async fn l34_concurrent_scheduling() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l34")).await;
    let cl = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let mut handles = Vec::new();
    for _ in 0..50 {
        let url = b("/sched/once?ms=60000");
        let cl = cl.clone();
        handles.push(tokio::spawn(async move { cl.get(&url).send().await }));
    }

    let mut ids = std::collections::HashSet::new();
    for handle in handles {
        let resp = handle.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let id = resp.text().await.unwrap();
        assert!(ids.insert(id), "duplicate scheduling UUID returned");
    }
    assert_eq!(ids.len(), 50, "all 50 scheduling IDs must be unique");
}

// ── Multi-Tenant Isolation (L36-L38) ───────────────────────────────────

/// Two tenants writing to the same key must not see each other's data.
#[tokio::test(flavor = "multi_thread")]
async fn l35_tenant_isolation_kv_store() {
    if should_skip_layer_tests() {
        return;
    }

    let (port_a, _tx_a) = spawn_handler_with_config(test_config("tenant-a")).await;
    let (port_b, _tx_b) = spawn_handler_with_config(test_config("tenant-b")).await;
    let cl = make_client();

    // Tenant A writes a secret
    cl.get(format!(
        "http://127.0.0.1:{port_a}/kv/set?key=secret&val=a-data"
    ))
    .send()
    .await
    .unwrap();

    // Tenant B should NOT see it
    let resp = cl
        .get(format!("http://127.0.0.1:{port_b}/kv/get?key=secret"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "tenant B should not see tenant A's kv-store data"
    );
}

/// Same isolation test for cache.
#[tokio::test(flavor = "multi_thread")]
async fn l36_tenant_isolation_cache() {
    if should_skip_layer_tests() {
        return;
    }

    let (port_a, _tx_a) = spawn_handler_with_config(test_config("tenant-cache-a")).await;
    let (port_b, _tx_b) = spawn_handler_with_config(test_config("tenant-cache-b")).await;
    let cl = make_client();

    cl.get(format!(
        "http://127.0.0.1:{port_a}/cache/set?key=token&val=a-token"
    ))
    .send()
    .await
    .unwrap();

    let resp = cl
        .get(format!("http://127.0.0.1:{port_b}/cache/get?key=token"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "tenant B should not see tenant A's cache data"
    );
}

// ── TTL / Expiry (L39-L40) ──────────────────────────────────────────────

/// kv-store TTL: key set with 2s TTL must be gone after 3s.
#[tokio::test(flavor = "multi_thread")]
async fn l37_kv_store_ttl_expiry() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l37")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    // Set with 2s TTL
    cl.get(b("/kv/set?key=ttl-key&val=ephemeral&ttl=2"))
        .send()
        .await
        .unwrap();

    // Immediately exists
    let resp = cl.get(b("/kv/get?key=ttl-key")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Wait for expiry
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Should be gone
    let resp = cl.get(b("/kv/get?key=ttl-key")).send().await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "TTL key should have expired"
    );
}

/// cache TTL: same pattern.
#[tokio::test(flavor = "multi_thread")]
async fn l38_cache_ttl_expiry() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l38")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    cl.get(b("/cache/set?key=ttl-cache&val=ephemeral&ttl=2"))
        .send()
        .await
        .unwrap();

    let resp = cl.get(b("/cache/get?key=ttl-cache")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let resp = cl.get(b("/cache/get?key=ttl-cache")).send().await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "TTL cache entry should have expired"
    );
}

// ── Resource Limits (L41-L44) ───────────────────────────────────────────

/// Cancel a non-existent scheduling ID — should not panic.
#[tokio::test(flavor = "multi_thread")]
async fn l39_scheduling_cancel_unknown() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l39")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sched/cancel?id=00000000-0000-0000-0000-000000000000"))
        .send()
        .await
        .unwrap();
    // Cancel on unknown ID should succeed (no-op).
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Missing env var returns 404.
#[tokio::test(flavor = "multi_thread")]
async fn l40_process_get_env_missing() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l40")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/env/DOES_NOT_EXIST_XYZ")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── Cross-Interface Interaction (L45-L46) ───────────────────────────────

/// KV set + observe log in sequence — both must work.
#[tokio::test(flavor = "multi_thread")]
async fn l41_kv_and_log_together() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l41")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/kv/set?key=a&val=1")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = cl.get(b("/log?msg=set-a=1")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify the kv-store value is still there
    let resp = cl.get(b("/kv/get?key=a")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "1");
}

/// Schedule + KV + log — all three interfaces in sequence.
#[tokio::test(flavor = "multi_thread")]
async fn l42_schedule_kv_log_sequence() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l42")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/sched/once?ms=60000")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let task_id = resp.text().await.unwrap();

    let resp = cl.get(b("/kv/set?key=task&val=ok")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = cl.get(b("/log?msg=scheduled")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = cl.get(b("/kv/get?key=task")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "ok");

    // The task ID should be a valid UUID
    assert_eq!(task_id.len(), 36);
}

// ── Time Consistency (L50) ──────────────────────────────────────────────

/// Two sequential time.now() calls must return increasing values.
#[tokio::test(flavor = "multi_thread")]
async fn l43_time_now_monotonic() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l43")).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl.get(b("/time/now")).send().await.unwrap();
    let t1: u64 = resp.text().await.unwrap().trim().parse().unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    let resp = cl.get(b("/time/now")).send().await.unwrap();
    let t2: u64 = resp.text().await.unwrap().trim().parse().unwrap();

    assert!(t2 > t1, "time.now() should be monotonic: t1={t1}, t2={t2}");
}

/// Two concurrent time.now() calls — both must return valid timestamps.
#[tokio::test(flavor = "multi_thread")]
async fn l44_concurrent_time_now() {
    if should_skip_layer_tests() {
        return;
    }
    let (port, _tx) = spawn_handler_with_config(test_config("l44")).await;
    let cl = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let mut handles = Vec::new();
    for _ in 0..10 {
        let url = b("/time/now");
        let cl = cl.clone();
        handles.push(tokio::spawn(async move { cl.get(&url).send().await }));
    }

    for handle in handles {
        let resp = handle.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ts: u64 = resp.text().await.unwrap().trim().parse().unwrap();
        assert!(ts > 1_700_000_000, "unreasonable timestamp: {ts}");
    }
}

// ── Streaming Response Bodies (L46) ─────────────────────────────────────

/// L46: the guest calls ResponseOutparam::set early (headers-only)
/// and continues writing body chunks. The host must start serving the
/// response immediately — proving the streaming path works end-to-end
/// for SSE, long-polling, and progressive chunked responses (issue #312).
#[tokio::test(flavor = "multi_thread")]
async fn l46_sse_endpoint_streams_headers_then_body_chunks() {
    if should_skip_layer_tests() {
        return;
    }

    let (port, _shutdown_tx) = spawn_handler_with_config(HandlerConfig {
        tenant_id: "test-tenant".to_string(),
        egress: Arc::new(EgressPolicy::allow_all()),
        log_sink: Arc::new(NullSink),
        app_ctx: AppLogContext {
            app_name: "l46".to_string(),
            tenant_id: "test-tenant".to_string(),
            deployment_id: "l46-deployment".to_string(),
        },
        meter: Arc::new(RequestMeter::new(
            "test-tenant".to_string(),
            "l46-deployment".to_string(),
        )),
        env: HashMap::new(),
        max_request_body_bytes: 10 * 1024 * 1024,
        metrics_acc: None,
        socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
        last_request_at: std::sync::Arc::new(tokio::sync::Mutex::new(Some(
            std::time::Instant::now(),
        ))),
        max_memory_mb: 256,
        cpu_budget_ms: 1000,
        preview_id: None,
        preview_pr_number: None,
    })
    .await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest::Client");

    let url = format!("http://127.0.0.1:{port}/sse?count=3");
    let resp = client.get(&url).send().await.expect("GET /sse?count=3");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap()),
        Some("text/event-stream")
    );

    // Read the body — it should contain SSE-formatted data lines
    // emitted by the guest after the headers were already sent.
    let body = resp.text().await.expect("streaming body");
    let event_count = body.lines().filter(|l| l.starts_with("data:")).count();
    assert!(
        event_count >= 3,
        "SSE response should contain at least 3 data: lines, got {event_count}\nbody:\n{body}"
    );
}
// ── L31–L33: wasi:sockets egress (issue #309) ──────────────────────────
//
// These tests prove the runtime's `WasiCtxBuilder::socket_addr_check`
// closure (see `edge-runtime/src/socket_egress.rs`) is wired into the
// linker. Each test instantiates the handler fixture, calls a new
// `/sockets/tcp/connect?ip=...&port=...` endpoint, and asserts the
// response body matches the expected policy decision.
//
//   * `l31_block_all_denies_under_default` — default mode (env unset ⇒
//     `BlockAll`). Any `start-connect` returns a deny from the closure.
//   * `l32_allowlist_blocks_hard_deny_ip` — `EDGE_EGRESS_SOCKET_MODE=
//     allowlist` + `EgressPolicy::new(vec!["api.example.com"])`. Target
//     `127.0.0.1` is in the hard-deny list ⇒ closure returns `false`
//     even though the allowlist is non-empty (hard-deny wins).
//   * `l33_allowlist_permits_public_ip` — same policy but target a
//     public IP. Closure returns `true`; the response body prefix is
//     `"allow"`. (The actual TCP connect may fail at the kernel level;
//     we only assert the policy decision here.)
//
#[tokio::test(flavor = "multi_thread")]
async fn l31_socket_egress_block_all_denies_under_default() {
    // `SocketEgressPolicy::BlockAll` is the runtime default — closure
    // denies every `start-connect`. Pass the mode explicitly through
    // `HandlerConfig`; no env mutation required (and no UB on
    // Rust 1.86+).
    //
    // (No `SOCKET_EGRESS_ENV_LOCK` / `ScopedSocketEgressMode` helper
    // — see `l33` for the replace-test-this PR.)

    // can't leak the var into this test.
    if should_skip_layer_tests() {
        return;
    }
    let mut cfg = test_config("l31");
    cfg.socket_mode_for_app = SocketEgressPolicy::BlockAll;
    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    // 8.8.8.8:53 is a public, non-hard-denied IP. Under BlockAll the
    // closure returns `false` regardless; wasmtime maps the closure's
    // `false` to `ErrorCode::AccessDenied`. The fixture surfaces that
    // as body prefix `deny:access-denied`.
    let resp = cl
        .get(b("/sockets/tcp/connect?ip=8.8.8.8&port=53"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.starts_with("deny:"),
        "BlockAll must deny wasi:sockets connect (got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn l32_socket_egress_allowlist_blocks_hard_deny_ip() {
    // mode=allowlist + `EgressPolicy::new(vec!["api.example.com"])`.
    // Target `127.0.0.1:80` is in the hard-deny range (loopback) so
    // the closure returns `false` — hard-deny wins over the non-empty
    // allowlist, same posture as the HTTP egress layer.
    if should_skip_layer_tests() {
        return;
    }
    let mut cfg = test_config("l32");
    cfg.socket_mode_for_app = SocketEgressPolicy::AllowList;
    cfg.egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));

    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sockets/tcp/connect?ip=127.0.0.1&port=80"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    // The fixture surfaces the `ErrorCode` variant that wasmtime
    // returns when the closure denies; the EgressPolicy's textual
    // reason ("hostname resolved to blocked IP") is logged but not
    // propagated to the guest. Body prefix `deny:` is the assertion
    // of the policy decision.
    assert!(
        body.starts_with("deny:"),
        "loopback must be hard-denied (got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn l33_socket_egress_allowlist_permits_public_ip() {
    // mode=allowlist + non-empty allowlist + public IP target.
    // Closure returns `true` (per the documented asymmetry — see
    // `EgressPolicy::check_address` in `egress.rs`); the fixture's
    // path returns `"allow"`. We don't assert on whether the
    // underlying TCP succeeded (no listener at 8.8.8.8:53 in CI);
    // only on the policy decision.
    if should_skip_layer_tests() {
        return;
    }
    let mut cfg = test_config("l33");
    cfg.socket_mode_for_app = SocketEgressPolicy::AllowList;
    cfg.egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));

    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sockets/tcp/connect?ip=8.8.8.8&port=53"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    // This test asserts the *closure decision*: under non-empty
    // allowlist + AllowList, a non-hard-denied IP must be permitted by
    // the policy. The actual TCP connect to 8.8.8.8:53 may then
    // fail at the kernel level in a sandboxed CI (seccomp, missing
    // outbound, etc.) — surface as a `deny:connection-*` or
    // `deny:address-*`, never a closure-policy deny. Accept either
    // the policy-permit or any kernel-level denial here.
    let closure_permitted = body.starts_with("allow");
    let kernel_level_deny = body.starts_with("deny:connection-")
        || body.starts_with("deny:address-")
        || body.starts_with("deny:invalid-state")
        || body.starts_with("deny:timeout");
    assert!(
        closure_permitted || kernel_level_deny,
        "non-hard-denied IP under non-empty allowlist + AllowList must \
         either be permitted by the closure or fail at the kernel level \
         (not a policy denial like 'deny:hostname-resolved-to-...'; got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}

// ── L34–L35: wasi:http TLS handshake (issue #345) ───────────────────────
//
// These tests prove the runtime-side TLS plumbing in
// `edge-worker/src/dispatch.rs::HandlerDispatch` (issue #209) actually
// completes a real handshake and dispatches to the right h1/h2 path
// based on ALPN. The unit tests in `dispatch.rs::tls_tests` only
// cover `try_load_tls_config` (loading). End-to-end handshake coverage
// starts here.

/// L34: spin up a `HandlerDispatch` with TLS enabled, point a
/// `reqwest::Client` at it, and assert the handshake completes and
/// the fixture's hello JSON returns 200 OK. Uses `rcgen` to mint a
/// self-signed cert at test time (no fixtures in the repo).
#[tokio::test(flavor = "multi_thread")]
async fn l34_handler_dispatch_completes_real_tls_handshake() {
    if should_skip_layer_tests() {
        return;
    }
    // rustls 0.23 requires a CryptoProvider before constructing any
    // ServerConfig. Install the ring-based default (matches the unit
    // tests in `dispatch.rs::tls_tests`).
    let _ = rustls::crypto::ring::default_provider().install_default();
    // 1. Mint an `localhost`-SAN self-signed cert + key.
    let cert = generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen self-signed");
    let cert_der = cert.cert.der().clone();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    let mut cert_file = tempfile::NamedTempFile::new().expect("cert tempfile");
    cert_file
        .write_all(cert_pem.as_bytes())
        .expect("write cert pem");
    let mut key_file = tempfile::NamedTempFile::new().expect("key tempfile");
    key_file
        .write_all(key_pem.as_bytes())
        .expect("write key pem");

    // 2. Load + wire into a HandlerConfig. Mirrors the production
    //    supervisor wiring at `supervisor.rs:433-441`.
    let tls_config = try_load_tls_config(
        &Some(cert_file.path().to_str().unwrap().into()),
        &Some(key_file.path().to_str().unwrap().into()),
    )
    .expect("try_load_tls_config parsed the synthetic PEMs");

    let cfg = test_config("l34");
    let (port, shutdown_tx) = spawn_handler_with_tls_config(cfg, Some(tls_config)).await;

    // 3. HTTPS GET via rustls-tls, pinning the self-signed cert.
    let cl = reqwest::Client::builder()
        .https_only(true)
        .add_root_certificate(
            reqwest::Certificate::from_der(&cert_der).expect("cert as reqwest::Certificate"),
        )
        .build()
        .expect("reqwest::Client");
    let resp = cl
        .get(format!("https://localhost:{port}/"))
        .send()
        .await
        .expect("HTTPS GET");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("\"hello\":\"handler\""),
        "expected the fixture's hello JSON, got: {body:?}"
    );

    let _ = shutdown_tx.send(());
}

/// L35: same setup as L34, but negotiate HTTP/2 via ALPN (the
/// dispatcher's `try_load_tls_config` advertises `[h2, http/1.1]`),
/// routing to `serve_connection_h2` at
/// `edge-worker/src/dispatch.rs:441`. Verifies the h2 dispatcher
/// branch doesn't panic and serves the same 200.
///
/// (Earlier draft used `http2_prior_knowledge()` to skip ALPN, but
/// that sends a direct h2 client preface while the dispatcher
/// still inspects ALPN to pick between h1/h2 — the connection was
/// torn down because the server side expected the h1 path.)
#[tokio::test(flavor = "multi_thread")]
async fn l35_handler_dispatch_h2_alpn_routes_to_h2_dispatcher() {
    if should_skip_layer_tests() {
        return;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen self-signed");
    let cert_der = cert.cert.der().clone();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    let mut cert_file = tempfile::NamedTempFile::new().expect("cert tempfile");
    cert_file
        .write_all(cert_pem.as_bytes())
        .expect("write cert pem");
    let mut key_file = tempfile::NamedTempFile::new().expect("key tempfile");
    key_file
        .write_all(key_pem.as_bytes())
        .expect("write key pem");

    let tls_config = try_load_tls_config(
        &Some(cert_file.path().to_str().unwrap().into()),
        &Some(key_file.path().to_str().unwrap().into()),
    )
    .expect("try_load_tls_config parsed the synthetic PEMs");

    let cfg = test_config("l35");
    let (port, shutdown_tx) = spawn_handler_with_tls_config(cfg, Some(tls_config)).await;

    // Default reqwest advertises ALPN `[h2, http/1.1]`; the
    // dispatcher's rustls config (built by `try_load_tls_config`)
    // advertises the same. ALPN picks `h2`, the dispatcher detects
    // it (dispatch.rs:353-365), and the request is dispatched to
    // `serve_connection_h2`.
    let cl = reqwest::Client::builder()
        .https_only(true)
        .add_root_certificate(
            reqwest::Certificate::from_der(&cert_der).expect("cert as reqwest::Certificate"),
        )
        .build()
        .expect("reqwest::Client");
    let resp = cl
        .get(format!("https://localhost:{port}/"))
        .send()
        .await
        .expect("HTTPS GET via ALPN h2");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("\"hello\":\"handler\""),
        "expected the fixture's hello JSON via ALPN h2, got: {body:?}"
    );

    let _ = shutdown_tx.send(());
}

// ── L51–L52: wasi:sockets/ip-name-lookup → connect (issue #309 follow-up)
//
// These tests exercise the new `/sockets/dns-resolve-and-connect` fixture
// path. The fixture calls `wasi:sockets/ip-name-lookup.resolve_addresses`
// → `resolve_next_address` → `tcp_connect`, surfacing the policy decision
// for each step in the response body.
//
// Both tests target `127.0.0.1` (loopback) — the local resolver parses
// the literal as-is and returns it as the first address. The runtime's
// resolve hook (see `docs/upstream-wasmtime-resolve-check.patch` and
// `edge-runtime/src/socket_egress.rs::HostnamePinning`) is **dormant**
// today because the upstream patch hasn't merged. The
// connect-side gate (`EgressPolicy::hostname_pinned_match` or
// `BlockAll`'s short-circuit) is what these tests assert on.
//
// L51: `socket_mode = BlockAll` (the worker's default). The runtime
// closure for `TcpConnect` always returns `false` under BlockAll, so
// the body must start with `deny:`. The exact error code from
// wasmtime 45 is `access-denied` (the bindgen discriminant that the
// fixture maps verbatim — see `error_code_name` in the fixture).
#[tokio::test(flavor = "multi_thread")]
async fn l51_dns_resolve_and_connect_block_all_denies() {
    if should_skip_layer_tests() {
        return;
    }
    let mut cfg = test_config("l51");
    cfg.socket_mode_for_app = SocketEgressPolicy::BlockAll;
    cfg.hostname_pinning_enabled = false;
    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sockets/dns-resolve-and-connect?host=127.0.0.1&port=80"))
        .send()
        .await
        .expect("send resolve-and-connect");
    let body = resp.text().await.expect("body");
    assert!(
        body.starts_with("deny:"),
        "BlockAll must deny the connect side (got: {body:?})"
    );
    // The fixture's `error_code_name` should map the runtime's
    // `Err(ErrorCode::AccessDenied)` to the literal "access-denied"
    // — the bindgen discriminant is the source of truth (see
    // `edge-worker/tests/fixtures/handler/src/lib.rs::error_code_name`).
    assert!(
        body.contains("access-denied"),
        "BlockAll must surface access-denied (got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}

// L52: `socket_mode = HostnamePinned` + `hostname_pinning_enabled = true`.
// This is the new dispatch arm (see commit 9195a43) — the per-request
// `RuntimeState` swap uses `SocketEgressPolicy::HostnamePinned` instead
// of the worker-wide `socket_mode`. The fixture's per-request
// `HostnamePinning` cache is empty (the upstream resolve hook hasn't
// merged), so the connect side closes the door — body
// `deny:access-denied`.
//
// This test pins the **dormant** state semantics: the new path +
// variant + Arc+Closure wiring all line up. Once the upstream patch
// merges and the resolve hook populates the cache, the test will
// start producing `ok:127.0.0.1:80` and must be updated (or replaced
// with a parallel `l52b_dns_resolve_and_connect_hostname_pinned_permits_observed_ip`
// test that pre-populates the cache before the request).
#[tokio::test(flavor = "multi_thread")]
async fn l52_dns_resolve_and_connect_hostname_pinned_dormant_denies() {
    if should_skip_layer_tests() {
        return;
    }
    let mut cfg = test_config("l52");
    // The new variant — see `edge-runtime/src/socket_egress.rs::SocketEgressPolicy::HostnamePinned`.
    cfg.socket_mode_for_app = SocketEgressPolicy::HostnamePinned;
    // The per-request `RuntimeState` swap in `dispatch.rs::handle_request`
    // consults this toggle: true → HostnamePinned + the app-wide shared
    // cache; false → worker-wide socket_mode + a fresh empty cache.
    cfg.hostname_pinning_enabled = true;
    // The default `cfg.hostname_pinning` from `test_config` is a fresh
    // empty `Arc<HostnamePinning>` — the dormant state. We add an
    // explicit assertion here so future readers see the contract.
    assert!(
        cfg.hostname_pinning.snapshot().is_empty(),
        "l52 explicitly tests the empty-cache dormant state; the cache \
         must be empty for the connect-side closure to deny"
    );

    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sockets/dns-resolve-and-connect?host=127.0.0.1&port=80"))
        .send()
        .await
        .expect("send resolve-and-connect");
    let body = resp.text().await.expect("body");
    assert!(
        body.starts_with("deny:access-denied"),
        "HostnamePinned + empty cache must deny the connect side \
         (dormant state; got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn l53_socket_mode_per_app_block_all_overrides_worker_allowlist() {
    if should_skip_layer_tests() {
        return;
    }
    // Per-app selector (issue #412): even though the worker-wide `socket_mode`
    // is `AllowList`, this app's `AppSpec.socket_mode = BlockAll` must win.
    // The `allowlist` here is irrelevant — the runtime never consults it once
    // the per-app mode is `BlockAll`.
    let mut cfg = test_config("l53");
    cfg.egress = Arc::new(EgressPolicy::allow_all());
    cfg.socket_mode_for_app = SocketEgressPolicy::BlockAll;

    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sockets/dns-resolve-and-connect?host=127.0.0.1&port=80"))
        .send()
        .await
        .expect("send resolve-and-connect");
    let body = resp.text().await.expect("body");
    assert!(
        body.starts_with("deny:access-denied"),
        "per-app BlockAll must deny the connect side regardless of the \
         worker-wide socket_mode (got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn l54_socket_mode_per_app_hostname_pinned_requires_worker_knob() {
    if should_skip_layer_tests() {
        return;
    }
    // Compose rule (issue #412, user-confirmed): the per-app selector is
    // `AppSpec.socket_mode`, but the worker-wide `hostname_pinning_enabled`
    // remains a hard gate for `HostnamePinned`. With the knob OFF, the
    // HostnamePinned arm is unreachable and the request must deny — without
    // ever touching the empty cache.
    let mut cfg = test_config("l54");
    cfg.socket_mode_for_app = SocketEgressPolicy::HostnamePinned;
    // Explicitly NOT setting `cfg.hostname_pinning_enabled = true` —
    // this is the load-bearing assertion that the worker-wide knob still
    // gates the HostnamePinned arm.
    assert!(
        !cfg.hostname_pinning_enabled,
        "l54 explicitly tests the compose rule: HostnamePinned must be \
         unreachable when the worker-wide knob is off"
    );

    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sockets/dns-resolve-and-connect?host=127.0.0.1&port=80"))
        .send()
        .await
        .expect("send resolve-and-connect");
    let body = resp.text().await.expect("body");
    assert!(
        body.starts_with("deny:access-denied"),
        "HostnamePinned with worker-wide knob OFF must deny (compose rule; \
         got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn l55_socket_mode_per_app_hostname_pinned_both_required_lights_up() {
    if should_skip_layer_tests() {
        return;
    }
    // Compose rule with BOTH gates active: per-app `HostnamePinned` AND
    // worker-wide `hostname_pinning_enabled = true`. The dispatch site
    // reaches the HostnamePinned arm and uses the app-wide shared
    // `HostnamePinning` cache. With an empty cache (dormant today), the
    // connect-side closure must deny — same shape as l52.
    let mut cfg = test_config("l55");
    cfg.socket_mode_for_app = SocketEgressPolicy::HostnamePinned;
    cfg.hostname_pinning_enabled = true;
    assert!(
        cfg.hostname_pinning.snapshot().is_empty(),
        "l55 explicitly tests the empty-cache dormant state, same as l52"
    );

    let (port, shutdown_tx) = spawn_handler_with_config(cfg).await;
    let cl = make_client();
    let b = |p: &str| format!("http://127.0.0.1:{port}{p}");

    let resp = cl
        .get(b("/sockets/dns-resolve-and-connect?host=127.0.0.1&port=80"))
        .send()
        .await
        .expect("send resolve-and-connect");
    let body = resp.text().await.expect("body");
    assert!(
        body.starts_with("deny:access-denied"),
        "HostnamePinned + empty cache must deny (per-app + knob both \
         active; dormant state; got: {body:?})"
    );

    let _ = shutdown_tx.send(());
}
