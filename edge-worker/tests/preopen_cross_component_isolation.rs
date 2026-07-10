//! Real two-component filesystem isolation test (issue #558 / PR #599
//! review follow-up).
//!
//! `tests/preopen_per_app_isolation.rs` in `edge-runtime` only verifies
//! the host-side path construction — it does NOT exercise the
//! `wasi:filesystem/types::open-at("/", ...)` surface against a real
//! guest. This file closes that gap: a real `handler.wasm` guest is
//! instantiated against TWO `RuntimeState`s for the same tenant with
//! different app_names. App A's `/fs/write?path=sentinel&body=...` lands
//! in `base/{tenant}/app-a/sentinel` and is read back via
//! `/fs/read?path=sentinel`. App B's `/fs/read?path=sentinel` against
//! its own preopen must return 404 — the file lives in app-a's
//! subdirectory, not app-b's.
//!
//! Two distinct `HandlerDispatch` instances each bind a random port on
//! `127.0.0.1`, so the test fires two real HTTP requests through the
//! runtime's per-app preopen plumbing.
//!
//! ## Skip policy
//!
//! Skipped when the `handler.wasm` fixture is missing (the fixture must
//! be rebuilt with the new `/fs/write` and `/fs/read` routes — see the
//! build procedure in `edge-worker/tests/fixtures/README.md`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
use edge_runtime::socket_egress::{HostnamePinning, SocketEgressPolicy};
use edge_runtime::{
    create_component_linker_handler, create_engine, EgressPolicy, RequestMeter, RuntimeState,
};
use edge_worker::dispatch::{HandlerConfig, HandlerDispatch};
use reqwest::StatusCode;
use wasmtime::component::{Component, InstancePre};

fn fixture_present() -> bool {
    [
        "tests/fixtures/handler.wasm",
        "edge-worker/tests/fixtures/handler.wasm",
    ]
    .iter()
    .any(|p| PathBuf::from(p).exists())
}

fn fixture_path() -> PathBuf {
    [
        "tests/fixtures/handler.wasm",
        "edge-worker/tests/fixtures/handler.wasm",
    ]
    .iter()
    .map(PathBuf::from)
    .find(|p| p.exists())
    .expect("fixture present")
}

struct NullSink;
impl LogSink for NullSink {
    fn push(&self, _r: LogRecord, _c: AppLogContext) {}
}

fn ephemeral_port() -> Option<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).ok()?;
    let port = listener.local_addr().ok()?.port();
    drop(listener);
    Some(port)
}

/// Per-test harness modeled after `LayerHarness` in
/// `tests/layer_integration.rs`. We keep a separate copy here because
/// `LayerHarness` is private to that test file (cargo `tests/*.rs` are
/// independent binaries; pub(crate) doesn't reach across them).
struct IsoHarness {
    url_base: String,
    client: reqwest::Client,
    _dispatch: Arc<HandlerDispatch>,
    _shutdown_tx: tokio::sync::broadcast::Sender<()>,
    _request_started: StdMutex<Option<Instant>>,
}

impl IsoHarness {
    async fn spawn(
        engine: &wasmtime::Engine,
        component: &Component,
        tenant_id: &str,
        app_name: &str,
        deployment_id: &str,
    ) -> anyhow::Result<Self> {
        let linker = create_component_linker_handler(engine).context("linker")?;
        let instance_pre: InstancePre<RuntimeState> = linker
            .instantiate_pre(component)
            .map_err(anyhow::Error::from)?;
        let port = ephemeral_port().expect("ephemeral port");

        let config = HandlerConfig {
            tenant_id: tenant_id.to_string(),
            egress: Arc::new(EgressPolicy::allow_all()),
            log_sink: Arc::new(NullSink),
            app_ctx: AppLogContext {
                app_name: app_name.to_string(),
                tenant_id: tenant_id.to_string(),
                deployment_id: deployment_id.to_string(),
            },
            meter: Arc::new(RequestMeter::new(
                tenant_id.to_string(),
                deployment_id.to_string(),
            )),
            env: HashMap::new(),
            max_request_body_bytes: 10 * 1024 * 1024,
            metrics_acc: None,
            socket_mode_for_app: SocketEgressPolicy::default(),
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(Some(Instant::now()))),
            max_memory_mb: 256,
            cpu_budget_ms: 1000,
            preview_id: None,
            preview_pr_number: None,
        };

        let dispatch = Arc::new({
            HandlerDispatch::new(
                port,
                1_000,
                1,
                config,
                None,
                Arc::new(edge_worker::downloader::Downloader::new(
                    "http://localhost".to_string(),
                    PathBuf::from("/tmp"),
                    edge_worker::auth::WorkerJwtSigner::new(vec![], None, "", "", "", ""),
                    None,
                )),
                deployment_id.to_string(),
                Arc::new(edge_worker::supervisor::StandbyPool::new(0).unwrap()),
                Arc::new(tokio::sync::RwLock::new(
                    edge_worker::state::WorkerState::new(engine.clone()),
                )),
            )
            .context("HandlerDispatch::new")?
        });

        dispatch
            .set_proxy_pre(wasmtime_wasi_http::p2::bindings::ProxyPre::new(instance_pre).unwrap())
            .await;

        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let shutdown_rx = shutdown_tx.subscribe();
        let dispatch_for_serve = dispatch.clone();
        tokio::spawn(async move {
            if let Err(e) = dispatch_for_serve.serve(shutdown_rx).await {
                tracing::error!(err = %e, "HandlerDispatch serve failed");
            }
        });

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
            _dispatch: dispatch,
            _shutdown_tx: shutdown_tx,
            _request_started: StdMutex::new(None),
        })
    }

    async fn get(&self, path: &str) -> anyhow::Result<(StatusCode, String)> {
        let url = format!("{}{}", self.url_base, path);
        if let Ok(mut guard) = self._request_started.lock() {
            *guard = Some(Instant::now());
        }
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        Ok((status, body))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn app_a_write_is_invisible_from_app_b() {
    if !fixture_present() {
        eprintln!(
            "SKIPPED: handler.wasm fixture missing — rebuild with the new \
             /fs/write and /fs/read routes. See edge-worker/tests/fixtures/README.md."
        );
        return;
    }

    let base = tempfile::tempdir().expect("tempdir");
    let base_path = base.path().to_path_buf();
    std::env::set_var("EDGE_FS_PATH", &base_path);

    let engine = create_engine().expect("engine");
    let bytes = std::fs::read(fixture_path()).expect("read handler.wasm");
    let component = Component::from_binary(&engine, &bytes).expect("parse component");

    let tenant = "tenant-cross";

    let h_a = IsoHarness::spawn(&engine, &component, tenant, "app-a", "deploy-a")
        .await
        .expect("h_a");
    let h_b = IsoHarness::spawn(&engine, &component, tenant, "app-b", "deploy-b")
        .await
        .expect("h_b");

    // Sanity: both harness GET / should return 200.
    let (sa, _) = h_a.get("/").await.expect("GET / app-a");
    let (sb, _) = h_b.get("/").await.expect("GET / app-b");
    assert_eq!(sa, StatusCode::OK, "app-a GET / status was {sa}");
    assert_eq!(sb, StatusCode::OK, "app-b GET / status was {sb}");

    // 1. app-a writes /sentinel.
    let (write_status, write_body) = h_a
        .get("/fs/write?path=sentinel&body=cross-component-app-a")
        .await
        .expect("GET /fs/write against app-a");
    assert_eq!(
        write_status,
        StatusCode::OK,
        "app-a /fs/write did not return 200 — body was {write_body:?}"
    );
    assert_eq!(
        write_body, "ok",
        "app-a /fs/write should report ok; got {write_body:?}"
    );

    // 2. app-a reads /sentinel back — sanity check that the write landed.
    let (read_status, read_body) = h_a
        .get("/fs/read?path=sentinel")
        .await
        .expect("GET /fs/read against app-a");
    assert_eq!(
        read_status,
        StatusCode::OK,
        "app-a /fs/read should succeed after app-a /fs/write — body={read_body:?}"
    );
    assert_eq!(
        read_body, "cross-component-app-a",
        "app-a should read its own write; got {read_body:?}"
    );

    // 3. app-b reads /sentinel — must return 404, NOT the body.
    let (b_status, b_body) = h_b
        .get("/fs/read?path=sentinel")
        .await
        .expect("GET /fs/read against app-b");
    assert_eq!(
        b_status,
        StatusCode::NOT_FOUND,
        "app-b MUST NOT see app-a's sentinel file (issue #558). \
         Got status={b_status} body={b_body:?} — this means the per-app \
         preopen isolation is broken."
    );
    assert_ne!(
        b_body, "cross-component-app-a",
        "app-b MUST NOT see app-a's sentinel body (issue #558)"
    );
}
