//! Supervisor wiring helpers shared by every integration test.
//!
//! `build_supervisor` was previously inlined (with subtle drift) in two
//! `edge-worker` test files. Centralizing the wiring means:
//! - Adding a new `Config` field no longer requires editing two literal
//!   blocks; the `test_config` factory below carries the defaults in
//!   one place.
//! - The two test files can no longer drift: both go through the same
//!   `build_supervisor` body.
//!
//! Out of scope: `edge-ingress` has its own `Config` type
//! (`edge_ingress::config::Config`) and does not use a `Supervisor`; it
//! cannot use `build_supervisor`. It can use `should_skip_integration_tests`
//! and `nats_container` from the `nats` module.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use edge_worker::auth::WorkerJwtSigner;
use edge_worker::config::Config;
use edge_worker::downloader::Downloader;
use edge_worker::log_forwarder::LogForwarder;
use edge_worker::nats::{NatsClient as NatsClientTrait, NatsClientImpl};
use edge_worker::port_pool::PortPool;
use edge_worker::state::WorkerState;
use edge_worker::supervisor::Supervisor;
use tokio::sync::Mutex as TokioMutex;

/// Construct a `Config` populated with the test defaults shared by
/// every consumer of `build_supervisor`. The caller supplies the four
/// test-specific inputs (`worker_id`, `region`, `nats_url`,
/// `control_plane_url`); the rest take the values the worker
/// integration tests have been using since the testcontainers helpers
/// were introduced.
///
/// Pinning the defaults here (rather than inlining them in every
/// `Config { ... }` literal) means a future change to the default
/// memory cap, epoch tick, or JWT issuer happens in one place.
///
/// `worker_addr` defaults to `"test-host:0"` — a placeholder that
/// won't actually receive traffic. Tests that exercise the
/// heartbeat/ingress wire contract (which carry `worker_addr` on the
/// wire) override this field after the call.
pub fn test_config(
    worker_id: &str,
    region: &str,
    nats_url: String,
    control_plane_url: String,
) -> Config {
    Config {
        worker_id: worker_id.to_string(),
        region: region.to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url,
        control_plane_url,
        cache_dir: PathBuf::from("/tmp/edge-worker-test-cache"),
        heartbeat_interval_secs: 30,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 18_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        queue_group: "test-group".to_string(),
        consumer_name: format!("test-{}", worker_id),
        nats_max_deliver: 20i64,
        // JWT fields: required by Config, but in tests we construct
        // Config directly and never hit the auth path against a real
        // control plane (the mock server accepts anything on
        // /api/internal/*). Any non-empty placeholder works.
        worker_jwt_secret: "test-secret".to_string(),
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        // Phase 4 bootstrap: tests don't configure the bootstrap path
        // by default — they use the legacy static-secret signer
        // (which is now `#[deprecated]`, hence the `#[allow]` on the
        // build_supervisor call below). Tests that exercise the
        // bootstrap path override `worker_bootstrap_psk` and call
        // `build_supervisor_with_signer` with their own signer.
        worker_bootstrap_psk: None,
        jwt_cache_path: PathBuf::from("/tmp/edge-worker-test-jwt-cache.json"),
        // Spool defaults: the helper opens a Spool rooted at a fresh
        // tempdir during build_supervisor; the Config's spool_dir is
        // preserved so a test that introspects the supervisor's
        // config still sees a sensible value. The 1 GiB cap matches
        // the production default (config.rs::from_env) — tests that
        // exercise overflow can override it after the call.
        //
        // R4: per-process spool dir (instead of the prior
        // `/tmp/edge-worker-test-spool` shared across every test
        // worker). cargo test runs test files in parallel across
        // multiple processes; sharing a single dir means a 503
        // response in test A's mock control plane could write
        // pending batches to the spool that test B's
        // `LogForwarder::new` would then replay into its in-memory
        // buffer and POST to test B's mock. Suffixing with the
        // process id gives each test runner its own spool root;
        // the OS cleans up `/tmp` on reboot. Tests that need
        // stricter isolation (one spool per test) override
        // `spool_dir` with a `tempfile::TempDir` after the call —
        // that pattern stays valid.
        spool_dir: std::env::temp_dir()
            .join(format!("edge-worker-test-spool-{}", std::process::id())),
        spool_max_bytes: 1u64 << 30,
    }
}

/// Build a `Supervisor` from a fully-formed `Config`. Wires the
/// wasmtime engine, the worker state, the JWT signer, the downloader,
/// the port pool, the NATS client, and the log forwarder. The caller
/// is responsible for `Config` field overrides specific to each test
/// (e.g., `cache_dir` for cache-isolation tests, `worker_addr` for the
/// ingress-wire test, `queue_group`/`consumer_name` for the
/// queue-group pinning test).
///
/// `cache_dir`, `nats_url`, and `spool_dir` come from the `Config`; all
/// three are required to be set by the caller. If `cache_dir`'s parent
/// doesn't exist, `Downloader::new` will create it on first artifact
/// download. `spool_dir` is opened via `Spool::open`, which creates
/// the directory if missing.
///
/// **JWT signer:** R5 — branches on
/// `config.worker_bootstrap_psk`. When `Some(psk)`, constructs a
/// `WorkerJwtSigner::new_with_callback(...)` whose callback fires
/// against the configured `control_plane_url`. When `None`, falls
/// back to the legacy `WorkerJwtSigner::new` (static secret) so
/// existing tests that don't exercise the bootstrap path keep
/// working. Tests that need a custom callback or a pre-seeded
/// signer call `build_supervisor_with_signer` directly.
pub async fn build_supervisor(config: Config) -> anyhow::Result<Arc<Supervisor>> {
    let signer = build_signer_for_config(&config);
    build_supervisor_inner(config, signer).await
}

/// Pick the right JWT signer for the given `Config`. When
/// `worker_bootstrap_psk` is set, returns a callback-based signer
/// whose callback hits the control plane's
/// `POST /api/internal/auth/token` endpoint on cache miss. When
/// unset, returns the legacy static-secret signer (deprecated for
/// production use; here it preserves the pre-R5 behavior for tests
/// that don't exercise the bootstrap path).
///
/// `pub(crate)` so the smoke tests in `lib.rs::tests` can introspect
/// the chosen path (R5) without going through the full
/// `build_supervisor` wiring (which would require a working NATS
/// container).
pub(crate) fn build_signer_for_config(config: &Config) -> Arc<WorkerJwtSigner> {
    if let Some(psk) = config.worker_bootstrap_psk.as_deref() {
        // R5: honor `worker_bootstrap_psk` so tests that set it
        // actually exercise the bootstrap path (the prior code
        // silently used `worker_jwt_secret` regardless, giving
        // false coverage).
        let psk_bytes: Vec<u8> = pkcs7_pad_psk(psk).into_bytes();
        let control_plane_url = config.control_plane_url.clone();
        let worker_id = config.worker_id.clone();
        let region = config.region.clone();
        let tenant_id = config.worker_tenant_id.clone();
        let issuer = config.worker_jwt_issuer.clone();
        let http_client = Arc::new(
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("build reqwest client for bootstrap callback"),
        );
        // `new_with_callback` takes the four identity strings by
        // value; clone them so they're also available to the
        // closure body (the closure needs `worker_id` etc. for
        // both the request headers and the body JSON).
        WorkerJwtSigner::new_with_callback(
            issuer.clone(),
            worker_id.clone(),
            region.clone(),
            tenant_id.clone(),
            move || {
                // The signer holds this closure via Arc<dyn Fn>;
                // cloning the captured Arcs into the request keeps
                // the closure 'static without leaking the
                // surrounding Config. Sync→async bridge.
                //
                // Production wires this through
                // `Handle::current().block_on(...)` from main()'s
                // non-async context. Tests may run in either
                // context: a test that calls `sign()` from inside
                // its own `block_on` is already on a tokio runtime
                // thread (in which case building a fresh runtime
                // here would panic with "Cannot start a runtime
                // from within a runtime"). A test that calls
                // `sign()` from a plain thread has no active
                // runtime, so `Handle::current()` would also panic.
                //
                // F6 (PR #165 review): prefer the current runtime
                // when one exists (`Handle::try_current`) and only
                // build a fresh single-threaded runtime as a
                // fallback. This matches the production code path
                // for in-runtime callers (no extra runtime
                // construction) and preserves correctness for the
                // no-runtime case.
                let url = format!("{control_plane_url}/api/internal/auth/token");
                let req = http_client
                    .post(&url)
                    .header("X-Worker-Id", worker_id.clone())
                    .header("X-Worker-Region", region.clone())
                    .header(
                        "X-Bootstrap-Signature",
                        edge_worker::bootstrap::sign_with_psk(
                            &psk_bytes, &worker_id, &region, &tenant_id,
                        ),
                    )
                    .json(&serde_json::json!({
                        "worker_id": worker_id,
                        "region": region,
                        "tenant_id": tenant_id,
                    }));
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.block_on(async move {
                        let resp = req.send().await?;
                        let bundle: edge_worker::bootstrap::JwtBundle = resp.json().await?;
                        Ok(bundle)
                    })
                } else {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("build runtime for bootstrap callback");
                    rt.block_on(async move {
                        let resp = req.send().await?;
                        let bundle: edge_worker::bootstrap::JwtBundle = resp.json().await?;
                        Ok(bundle)
                    })
                }
            },
        )
    } else {
        #[allow(deprecated)]
        WorkerJwtSigner::new(
            config.worker_jwt_secret.clone(),
            config.worker_jwt_issuer.clone(),
            config.worker_id.clone(),
            config.region.clone(),
            config.tenant_id_for_signer(),
        )
    }
}

/// Pad the PSK up to the 32-byte minimum the server enforces
/// (`BOOTSTRAP_PSK` length floor). The worker accepts any byte
/// length in production but warns on < 32; for tests we want the
/// callback to be sent without server-side rejection so the test
/// exercises the callback path, not the rejection path. Pads with
/// ASCII `!` characters (preserves ASCII-only contract — server
/// doesn't actually require that, but it keeps the test PSK
/// printable in logs).
fn pkcs7_pad_psk(psk: &str) -> String {
    if psk.len() >= 32 {
        psk.to_string()
    } else {
        let mut s = String::with_capacity(32);
        s.push_str(psk);
        for _ in 0..(32 - psk.len()) {
            s.push('!');
        }
        s
    }
}

/// Like `build_supervisor` but takes a pre-built JWT signer. Tests
/// that exercise the bootstrap path construct their own
/// `WorkerJwtSigner::new_with_callback` (with a wiremock-backed
/// closure) and hand it in here, sidestepping the legacy
/// `worker_jwt_secret` field.
pub async fn build_supervisor_with_signer(
    config: Config,
    signer: Arc<WorkerJwtSigner>,
) -> anyhow::Result<Arc<Supervisor>> {
    build_supervisor_inner(config, signer).await
}

/// Shared wiring — both `build_supervisor` and `build_supervisor_with_signer`
/// use the same body so the supervisor construction can't drift between
/// the two paths.
async fn build_supervisor_inner(
    config: Config,
    jwt_signer: Arc<WorkerJwtSigner>,
) -> anyhow::Result<Arc<Supervisor>> {
    let engine = edge_runtime::create_engine().context("create engine")?;
    let state = Arc::new(tokio::sync::RwLock::new(WorkerState::new(engine)));

    // Downloader::new and LogForwarder::new both take Arc<WorkerJwtSigner>;
    // clone the Arc so we can hand it to both.
    let downloader = Arc::new(Downloader::new(
        config.control_plane_url.clone(),
        config.cache_dir.clone(),
        jwt_signer.clone(),
    ));

    let port_pool = Arc::new(TokioMutex::new(PortPool::new(
        config.starting_port,
        config.port_cooldown_secs,
    )));

    let nats =
        Arc::new(NatsClientImpl::connect(&config.nats_url).await?) as Arc<dyn NatsClientTrait>;

    // Open the log spool rooted at config.spool_dir. The default in
    // test_config is /tmp/edge-worker-test-spool, but tests that need
    // a fresh per-test spool (e.g. assertions on spool contents)
    // should override config.spool_dir before calling build_supervisor.
    let spool = Arc::new(
        edge_spool::Spool::open(&config.spool_dir)
            .await
            .with_context(|| format!("open spool at {}", config.spool_dir.display()))?,
    );

    // Shared reqwest::Client for the test supervisor's outbound HTTP.
    // In production main() constructs the client once and shares it
    // across the bootstrap callback + LogForwarder (finding B2). Tests
    // keep the same shape so the helper exercises the production API.
    let http_client = Arc::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .with_context(|| "build test reqwest client".to_string())?,
    );

    let log_forwarder = LogForwarder::new(
        config.control_plane_url.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        jwt_signer,
        http_client,
        spool,
        config.spool_max_bytes,
    )
    .await;

    Ok(Arc::new(Supervisor {
        config,
        state,
        downloader,
        port_pool,
        nats,
        log_forwarder,
    }))
}

/// Trait extension on `Config` for tests — currently exposes
/// `tenant_id` for the legacy signer constructor.
trait ConfigTestExt {
    fn tenant_id_for_signer(&self) -> String;
}

impl ConfigTestExt for Config {
    fn tenant_id_for_signer(&self) -> String {
        self.worker_tenant_id.clone()
    }
}
