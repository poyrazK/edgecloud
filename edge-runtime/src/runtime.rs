//! Root runtime state — implements all WIT Host traits.
//!
//! v0.2 changes (breaking vs. v0.1):
//!   * `http_client`, `http_server`, `networking`, and the custom `streams`
//!     interfaces are removed. Components that need them now go through
//!     the standard `wasi:http`, `wasi:sockets`, `wasi:io` imports instead.
//!   * Implements `WasiView` so the linker can wire the canonical wasi:
//!     interfaces. Each per-`Store` state has a fresh `ResourceTable`.
//!   * `Clone` is implemented for future `wasmtime_wasi_http::ProxyPre`
//!     per-request use. The persistent store handles are `Arc`-cloned;
//!     the `WasiCtx` is rebuilt from a stored env `HashMap` because
//!     `wasmtime_wasi::WasiCtx` does not implement `Clone` in 25.x.
//!   * `egress` is kept as internal state for future `wasi:http` outgoing
//!     enforcement (deferred to the linker task — not yet wired here).
//!
//! v0.2 NOTE on async: wasmtime 25 binds `wit-parser 0.217`, which does
//! NOT recognize the `async func` syntax. All WIT is plain `func(...)`
//! and the generated Host trait methods are SYNC. host-internal async
//! work (Tokio sleep, scheduling timers) is bridged with
//! `tokio::runtime::Handle::current().block_on(...)` inside the per-
//! interface Rust modules (see `interfaces/time.rs::Clock::sleep`).

// Same package, two bindgens = two distinct Host traits per interface.
// We alias the long-running-world ones (used by the linker task) and
// import the handler-world ones directly. The macro_rules! block at the
// bottom generates an `impl` for each world in a single call.
use crate::edge_runtime_handler::edge::cloud::{
    cache::Host as CacheHost,
    kv_store::Host as KvStoreHost,
    observe::{
        Host as ObserveHost, LogLevel as HandlerObserveLogLevel,
        LogRecord as HandlerObserveLogRecord,
    },
    process::Host as ProcessHost,
    scheduling::Host as SchedulingHost,
    time::Host as TimeHost,
};
use crate::edge_runtime_long::edge::cloud::{
    cache::Host as LongCacheHost,
    kv_store::Host as LongKvStoreHost,
    observe::{
        Host as LongObserveHost, LogLevel as LongObserveLogLevel, LogRecord as LongObserveLogRecord,
    },
    process::Host as LongProcessHost,
    scheduling::Host as LongSchedulingHost,
    time::Host as LongTimeHost,
};
use crate::egress::EgressPolicy;
use crate::interfaces::{cache, kv_store, observe, process, scheduling, time};
use crate::metering::RequestMeter;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::types::{
    default_send_request, HostFutureIncomingResponse, OutgoingRequestConfig,
};
use wasmtime_wasi_http::{HttpError, HttpResult, WasiHttpCtx, WasiHttpView};

/// Process-wide per-tenant store registries. Each tenant gets its own
/// `Arc<KvStore>` / `Arc<Cache>` / `Scheduler`, cached here so state is
/// preserved across `execute_app` calls for the same tenant.
static KV_STORES: std::sync::LazyLock<RwLock<HashMap<String, Arc<kv_store::KvStore>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));
static CACHE_STORES: std::sync::LazyLock<RwLock<HashMap<String, Arc<cache::Cache>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));
static SCHEDULERS: std::sync::LazyLock<RwLock<HashMap<String, Arc<scheduling::Scheduler>>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// State for one guest invocation. Cheap to clone (Arc-heavy), fresh
/// `ResourceTable` per clone so the per-`Store` resource handles don't
/// leak across guest calls.
pub struct RuntimeState {
    pub kv_store: Arc<kv_store::KvStore>,
    pub cache: Arc<cache::Cache>,
    // Wrapped in `Arc` so `Clone` is trivial — each `Observer`/`Clock`/
    // `Process` is Arc-shared across clones.
    pub observe: Arc<observe::Observer>,
    pub time: Arc<time::Clock>,
    pub scheduling: Arc<scheduling::Scheduler>,
    pub process: Arc<process::Process>,

    /// wasi: state — required by `WasiView`. The `WasiCtx` is rebuilt
    /// per `Clone` from `wasi_env_for_clone` because `WasiCtx` does not
    /// implement `Clone` in wasmtime 25. The `ResourceTable` is fresh
    /// per clone so resource handles from one request don't leak to the
    /// next.
    pub wasi_ctx: WasiCtx,
    /// wasi:http state — required by `WasiHttpView`. Zero-sized in
    /// wasmtime 25 (`WasiHttpCtx { _priv: PhantomData<()> }`), so
    /// `Clone` is free. Per-instance state lives in `resource_table`.
    pub wasi_http_ctx: WasiHttpCtx,
    pub resource_table: ResourceTable,
    wasi_env_for_clone: Arc<HashMap<String, String>>,

    /// Tenant that owns this runtime instance.
    pub tenant_id: String,

    /// Per-deployment egress policy. Will be re-applied on the
    /// `wasi:http/outgoing-handler` host impl (deferred to the linker task).
    pub egress: Arc<EgressPolicy>,

    /// Shared exit-code flag set by `Process::exit` when the guest calls
    /// `process.exit`. Allows `execute_app` to distinguish a clean guest
    /// exit from a wasm trap.
    pub exit_code: Arc<AtomicU32>,
}

impl RuntimeState {
    /// Test-only constructor. Ephemeral in-memory stores, no preopens,
    /// permissive egress.
    #[cfg(test)]
    pub fn new() -> Self {
        let env = Arc::new(process::filter_env_vars(std::env::vars()).collect::<HashMap<_, _>>());
        let wasi_ctx = WasiCtxBuilder::new()
            .envs(
                &env.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>(),
            )
            .build();
        let exit_code = Arc::new(AtomicU32::new(0));
        let process = process::Process::with_env_and_exit_code(env.clone(), exit_code.clone());
        Self {
            kv_store: Arc::new(kv_store::KvStore::new()),
            cache: Arc::new(cache::Cache::new(1000)),
            observe: Arc::new(observe::Observer::new()),
            time: Arc::new(time::Clock::new()),
            scheduling: Arc::new(scheduling::Scheduler::new()),
            process: Arc::new(process),
            wasi_ctx,
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
            wasi_env_for_clone: env,
            tenant_id: String::new(),
            egress: Arc::new(EgressPolicy::allow_all()),
            exit_code,
        }
    }

    /// Production constructor. Builds per-tenant persistent stores (KV,
    /// cache, scheduler) and a `WasiCtx` for the tenant's preopens.
    ///
    /// `egress` is stored but not yet enforced on any host call — that
    /// wiring lands in the linker task when `wasi:http/outgoing-handler`
    /// is added.
    ///
    /// `log_sink` and `app_ctx` are wired into the per-tenant `Observer`
    /// so guest `emit_log` calls reach the worker's `LogForwarder`. The
    /// v0.2 sprint initially dropped these from the constructor (with the
    /// intent to wire them via the linker), but the production worker
    /// constructs `RuntimeState` per-request inside the supervisor and
    /// needs to inject the per-app sink + app context at construction
    /// time. Restoring them here keeps the linker concern separate from
    /// the per-invocation wiring concern.
    #[allow(clippy::too_many_arguments)]
    pub fn with_env_and_meter(
        env: std::collections::HashMap<String, String>,
        _meter: Option<Arc<RequestMeter>>,
        tenant_id: String,
        egress: Arc<EgressPolicy>,
        log_sink: Arc<dyn observe::LogSink>,
        app_ctx: observe::AppLogContext,
    ) -> Self {
        let env = Arc::new(env);
        let exit_code = Arc::new(AtomicU32::new(0));
        let kv_store = get_or_create_kv_store(&tenant_id);
        let cache_store = get_or_create_cache(&tenant_id);
        let scheduling = get_or_create_scheduler(&tenant_id);

        let wasi_ctx = build_wasi_ctx_for_tenant(&env, &tenant_id);

        let observer = observe::Observer::from_config(
            observe::ObserveConfig::new()
                .with_log_sink(log_sink)
                .with_app_ctx(app_ctx),
        );

        Self {
            kv_store,
            cache: cache_store,
            observe: Arc::new(observer),
            time: Arc::new(time::Clock::new()),
            scheduling,
            process: Arc::new(process::Process::with_env_and_exit_code(
                env.clone(),
                exit_code.clone(),
            )),
            wasi_ctx,
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
            wasi_env_for_clone: env,
            tenant_id,
            egress,
            exit_code,
        }
    }

    /// Returns `Some(code)` if the guest WASM component called
    /// `process.exit(code)`, `None` if no exit was requested.
    pub fn exit_requested(&self) -> Option<u32> {
        let code = self.exit_code.load(Ordering::SeqCst);
        if code == 0 {
            None
        } else {
            Some(code)
        }
    }
}

/// Validate a tenant ID. Returns true iff the ID is non-empty, ≤ 64 chars,
/// and contains only `[a-zA-Z0-9_-]`. Used by the worker to refuse path-
/// traversal attacks before any filesystem or store operations.
pub fn is_safe_tenant_id(tenant_id: &str) -> bool {
    !tenant_id.is_empty()
        && tenant_id.len() <= 64
        && tenant_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

impl Clone for RuntimeState {
    fn clone(&self) -> Self {
        // Persistent stores — Arc-clone (cheap, shared with other tenants).
        // Per-app simple types — must be Clone. Each is Arc-based internally.
        // wasi: state — `WasiCtx` is rebuilt from the stored env `HashMap`
        // because `wasmtime_wasi::WasiCtx` is not `Clone` in 25.x.
        // `ResourceTable` is fresh so per-`Store` resource handles from
        // one request don't leak to the next.
        Self {
            kv_store: self.kv_store.clone(),
            cache: self.cache.clone(),
            scheduling: self.scheduling.clone(),
            observe: self.observe.clone(),
            time: self.time.clone(),
            process: self.process.clone(),
            wasi_ctx: build_wasi_ctx_for_tenant(&self.wasi_env_for_clone, &self.tenant_id),
            // WasiHttpCtx is zero-sized in wasmtime 25 (`PhantomData`)
            // so this is a no-op clone. The per-Store resources still
            // live in `resource_table` (fresh below) which is what
            // matters for handler isolation.
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
            wasi_env_for_clone: self.wasi_env_for_clone.clone(),
            tenant_id: self.tenant_id.clone(),
            egress: self.egress.clone(),
            exit_code: Arc::new(AtomicU32::new(0)),
        }
    }
}

/// `WasiView` lets the linker wire all wasi: imports with a single call
/// to `wasmtime_wasi::add_to_linker_async`. The trait is the canonical
/// integration point: it pairs the `WasiCtx` (filesystem, env, args)
/// with the `ResourceTable` (handles to streams, files, sockets).
/// wasmtime 25 split this into two separate methods (no `WasiCtxView`
/// tuple type).
impl WasiView for RuntimeState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resource_table
    }
}

/// `WasiHttpView` — required by `wasmtime_wasi_http::add_only_http_to_linker_async`.
/// The default `send_request` is overridden to enforce the tenant's
/// `EgressPolicy` (Phase C-3). Per-request outbound requests go through
/// this path: `guest.wasi:http/outgoing-handler::handle(req, out)` →
/// `WasiHttpImpl<&mut T>::send_request` → `T::send_request` →
/// `EgressPolicy::check(url)` → either denied (returns `Err`) or
/// forwarded to the canonical `default_send_request` impl.
///
/// The check runs PRE-DNS, so a denied host NEVER leaves the worker.
/// The DNS-rebinding guard (`EgressPolicy::check_resolved_ip`) is best-
/// effort in v0.2 — `wasmtime-wasi-http` 25.0.3 doesn't expose the
/// hyper `connect_hook` we'd need to actually inspect the resolved IP
/// before connect. v0.3 will land this via a hyper upgrade. See the
/// `egress` field and the plan §C-3 "DNS-rebinding guard" decision.
impl WasiHttpView for RuntimeState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http_ctx
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resource_table
    }
    fn send_request(
        &mut self,
        request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let url_str = request.uri().to_string();
        if let Err(reason) = self.egress.check(&url_str) {
            tracing::warn!(
                tenant_id = %self.tenant_id,
                url = %url_str,
                reason = %reason,
                "egress denied"
            );
            // Convert to `ErrorCode::InternalError` (the public
            // `From<ErrorCode> for HttpError` impl is the only way to
            // build a typed `HttpError` from outside the crate). The
            // guest sees an InternalError-equivalent 5xx with the
            // reason embedded in the diagnostics payload.
            let diagnostics = format!("egress denied: {reason}");
            return Err(HttpError::from(ErrorCode::InternalError(Some(diagnostics))));
        }
        // Egress allowlist passed — defer to the canonical hyper-based
        // send_request implementation that wasmtime-wasi-http ships.
        Ok(default_send_request(request, config))
    }
    // `is_forbidden_header` falls back to the WasiHttpView default which
    // strips the canonical hop-by-hop / connection-state header set
    // (Connection, Keep-Alive, Proxy-Authenticate, Proxy-Authorization,
    // TE, Trailers, Transfer-Encoding, Upgrade, Host, Http2-Settings).
    // Adding egress-specific stripping here would require new
    // EgressPolicy methods that we don't need for v0.2 — every header a
    // tenant wants blocked is already enforced by the URL-level
    // `EgressPolicy::check` above.
}

#[cfg(test)]
impl Default for RuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod send_request_tests {
    //! Phase C-8: prove that `WasiHttpView::send_request` enforces the
    //! tenant's `EgressPolicy`. The full DNS path isn't exercised here
    //! because `default_send_request` would actually open a TCP socket;
    //! we short-circuit by testing only the URL-level deny path. The
    //! resolved-IP DNS-rebinding guard is covered in `egress.rs` because
    //! it sits on the EgressPolicy itself, not the `send_request` shim.
    use super::*;
    use crate::interfaces::observe::{AppLogContext, LogRecord, LogSink};
    use hyper::Request;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use wasmtime_wasi_http::WasiHttpView;

    /// A no-op `LogSink` for tests that only exercise `send_request`.
    struct NoopSink;
    impl LogSink for NoopSink {
        fn push(&self, _r: LogRecord, _c: AppLogContext) {}
    }

    /// Build a `RuntimeState` wired with the supplied `EgressPolicy`.
    fn state_with_egress(policy: Arc<EgressPolicy>) -> RuntimeState {
        RuntimeState::with_env_and_meter(
            std::collections::HashMap::new(),
            None,
            "phase-c8-test".to_string(),
            policy,
            Arc::new(NoopSink) as Arc<dyn LogSink>,
            AppLogContext {
                app_name: "phase-c8".to_string(),
                tenant_id: "phase-c8-test".to_string(),
                deployment_id: "phase-c8-test".to_string(),
            },
        )
    }

    /// Build a minimal `hyper::Request<HyperOutgoingBody>` with the
    /// supplied URL. The body is empty — we never get past the URL
    /// check so no real network IO happens.
    fn make_request(uri: &str) -> Request<wasmtime_wasi_http::body::HyperOutgoingBody> {
        Request::builder()
            .uri(uri)
            .method("GET")
            .body(wasmtime_wasi_http::body::HyperOutgoingBody::default())
            .expect("test request build")
    }

    #[test]
    fn send_request_blocks_denied_host() {
        // Policy only allows api.stripe.com — request to 127.0.0.1 is
        // denied on TWO grounds: the IP is hard-blocked by
        // is_blocked_ipv4 (loopback range), AND it's not in the
        // allowlist. The test asserts an Err is returned.
        let mut state = state_with_egress(Arc::new(EgressPolicy::new(vec![
            "api.stripe.com".to_string()
        ])));
        let req = make_request("http://127.0.0.1/");
        let config = wasmtime_wasi_http::types::OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: std::time::Duration::from_secs(60),
            first_byte_timeout: std::time::Duration::from_secs(60),
            between_bytes_timeout: std::time::Duration::from_secs(60),
        };
        let result = state.send_request(req, config);
        assert!(result.is_err(), "expected Err for denied host, got Ok");
        let msg = format!("{:?}", result.unwrap_err()).to_lowercase();
        assert!(
            msg.contains("egress") || msg.contains("internalerror"),
            "expected egress denial in error chain, got: {msg}"
        );
    }

    #[test]
    fn send_request_allows_allowlisted_host() {
        // Policy allows api.stripe.com — request should reach
        // `default_send_request`. We don't actually want to open a TCP
        // socket in unit tests, so we only assert that the call DID
        // NOT error on the policy check (it'll either succeed with a
        // HostFuture or fail with a connection error — either way the
        // `Result` is `Ok`).
        let mut state = state_with_egress(Arc::new(EgressPolicy::new(vec![
            "api.stripe.com".to_string()
        ])));
        let req = make_request("https://api.stripe.com/v1/charges");
        let config = wasmtime_wasi_http::types::OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: std::time::Duration::from_secs(60),
            first_byte_timeout: std::time::Duration::from_secs(60),
            between_bytes_timeout: std::time::Duration::from_secs(60),
        };
        let result = state.send_request(req, config);
        assert!(
            result.is_ok(),
            "expected Ok from send_request for allowlisted host, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn send_request_blocks_empty_allowlist() {
        // Empty allowlist = default-deny. Even hitting a public host
        // must be denied.
        let mut state = state_with_egress(Arc::new(EgressPolicy::new(vec![])));
        let req = make_request("https://example.com/");
        let config = wasmtime_wasi_http::types::OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: std::time::Duration::from_secs(60),
            first_byte_timeout: std::time::Duration::from_secs(60),
            between_bytes_timeout: std::time::Duration::from_secs(60),
        };
        let result = state.send_request(req, config);
        assert!(result.is_err(), "expected Err for empty allowlist, got Ok");
    }

    #[test]
    fn send_request_allow_all_passes_non_blocked() {
        // Sentinel policy (`EgressPolicy::allow_all()`) — public hosts
        // pass, only hard-denied IPs would still error.
        let mut state = state_with_egress(Arc::new(EgressPolicy::allow_all()));
        let req = make_request("http://127.0.0.1/");
        let config = wasmtime_wasi_http::types::OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: std::time::Duration::from_secs(60),
            first_byte_timeout: std::time::Duration::from_secs(60),
            between_bytes_timeout: std::time::Duration::from_secs(60),
        };
        let result = state.send_request(req, config);
        // Loopback still hard-denied even under allow_all() — this
        // confirms the hard-deny layer precedes the allowlist.
        assert!(
            result.is_err(),
            "expected hard-deny Err for loopback under allow_all, got Ok"
        );
    }

    #[test]
    fn send_request_wildcard_suffix_allowed() {
        // Wildcard *.stripe.com should let api.stripe.com through.
        let mut state =
            state_with_egress(Arc::new(EgressPolicy::new(
                vec!["*.stripe.com".to_string()],
            )));
        let req = make_request("https://api.stripe.com/v1/charges");
        let config = wasmtime_wasi_http::types::OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: std::time::Duration::from_secs(60),
            first_byte_timeout: std::time::Duration::from_secs(60),
            between_bytes_timeout: std::time::Duration::from_secs(60),
        };
        let result = state.send_request(req, config);
        assert!(
            result.is_ok(),
            "expected Ok for wildcard-matched host, got: {:?}",
            result.err()
        );
    }

    /// Lint that the CountingSink trait object wires correctly —
    /// protects against the test infra regressing.
    #[test]
    fn log_sink_counting_compiles() {
        struct CountingSink {
            #[allow(dead_code)]
            pushes: AtomicUsize,
            #[allow(dead_code)]
            records: Mutex<Vec<LogRecord>>,
        }
        impl LogSink for CountingSink {
            fn push(&self, _r: LogRecord, _c: AppLogContext) {
                self.pushes.fetch_add(1, Ordering::Relaxed);
            }
        }
        let _sink: Arc<dyn LogSink> = Arc::new(CountingSink {
            pushes: AtomicUsize::new(0),
            records: Mutex::new(Vec::new()),
        });
    }
}

// ── Tenant store registries ────────────────────────────────────────────────

fn get_or_create_kv_store(tenant_id: &str) -> Arc<kv_store::KvStore> {
    {
        let stores = KV_STORES.read();
        if let Some(store) = stores.get(tenant_id) {
            return Arc::clone(store);
        }
    }
    let mut stores = KV_STORES.write();
    if let Some(store) = stores.get(tenant_id) {
        return Arc::clone(store);
    }
    let store = make_kv_store_for_tenant(tenant_id);
    let arc = Arc::new(store);
    stores.insert(tenant_id.to_string(), Arc::clone(&arc));
    arc
}

fn make_kv_store_for_tenant(tenant_id: &str) -> kv_store::KvStore {
    match std::env::var("EDGE_KV_STORE_PATH") {
        Ok(base) => {
            let path = Path::new(&base).join(tenant_id);
            match kv_store::KvStore::with_persistence(&path) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        tenant_id,
                        err = %e,
                        "KV store persistence unavailable for tenant, using ephemeral"
                    );
                    kv_store::KvStore::new()
                }
            }
        }
        Err(_) => kv_store::KvStore::new(),
    }
}

fn get_or_create_cache(tenant_id: &str) -> Arc<cache::Cache> {
    {
        let stores = CACHE_STORES.read();
        if let Some(cache) = stores.get(tenant_id) {
            return Arc::clone(cache);
        }
    }
    let mut stores = CACHE_STORES.write();
    if let Some(cache) = stores.get(tenant_id) {
        return Arc::clone(cache);
    }
    let cache = make_cache_for_tenant(tenant_id);
    let arc = Arc::new(cache);
    stores.insert(tenant_id.to_string(), Arc::clone(&arc));
    arc
}

fn make_cache_for_tenant(tenant_id: &str) -> cache::Cache {
    match std::env::var("EDGE_CACHE_PATH") {
        Ok(base) => {
            let path = Path::new(&base).join(tenant_id);
            match cache::Cache::with_persistence(&path, 1000) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        tenant_id,
                        err = %e,
                        "cache persistence unavailable for tenant, using ephemeral"
                    );
                    cache::Cache::new(1000)
                }
            }
        }
        Err(_) => cache::Cache::new(1000),
    }
}

fn get_or_create_scheduler(tenant_id: &str) -> Arc<scheduling::Scheduler> {
    {
        let schedulers = SCHEDULERS.read();
        if let Some(s) = schedulers.get(tenant_id) {
            return Arc::clone(s);
        }
    }
    let mut schedulers = SCHEDULERS.write();
    if let Some(s) = schedulers.get(tenant_id) {
        return Arc::clone(s);
    }
    let scheduler = make_scheduler_for_tenant(tenant_id);
    let arc = Arc::new(scheduler);
    schedulers.insert(tenant_id.to_string(), Arc::clone(&arc));
    arc
}

fn make_scheduler_for_tenant(tenant_id: &str) -> scheduling::Scheduler {
    match std::env::var("EDGE_SCHEDULING_PATH") {
        Ok(base) => {
            let path = Path::new(&base).join(tenant_id);
            match scheduling::Scheduler::with_persistence(&path) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        tenant_id,
                        err = %e,
                        "scheduling persistence unavailable for tenant, using ephemeral"
                    );
                    scheduling::Scheduler::new()
                }
            }
        }
        Err(_) => scheduling::Scheduler::new(),
    }
}

/// Build a `WasiCtx` for a tenant from the supplied env `HashMap`.
///
/// Per-tenant preopens (Phase C-5): if `EDGE_FS_PATH` is set, the
/// tenant's directory `{EDGE_FS_PATH}/{tenant_id}/` is mounted at the
/// guest's `/` so it can call `wasi:filesystem/types::open-at("/",
/// ...)` from any handler/long-running component. The directory is
/// created on first use (idempotent — `create_dir_all`). If the base
/// path is missing or `create_dir_all` fails (read-only mount, EACCES),
/// the ctx falls through without the preopen so the guest still runs
/// (no filesystem access) rather than refusing to start.
fn build_wasi_ctx_for_tenant(env: &Arc<HashMap<String, String>>, tenant_id: &str) -> WasiCtx {
    // Apply the env blocklist BEFORE handing the env to `WasiCtx`. The
    // host exposes the env to the guest via two paths:
    //
    //   * `edge:cloud/process::get-env`/`get-all-env` — tenant-filtered
    //     by the worker before it reaches here (sanity layer).
    //   * `wasi:cli/environment::get-environment()` — the canonical
    //     WASI Preview 2 path that any guest can call directly.
    //
    // Without filtering here, an operator who sets e.g. `AWS_ACCESS_KEY_ID`
    // in the per-app env leaks the credential to the guest through the
    // wasi:cli path. The blocklist (`AWS_*`, `*SECRET*`, `*API_KEY*`, …)
    // lives in `interfaces/process.rs::filter_env_vars` and is reused
    // here — single source of truth.
    let env_strings: Vec<(String, String)> = process::filter_env_vars(
        env.iter().map(|(k, v)| (k.clone(), v.clone())),
    )
    .collect();
    let mut builder = WasiCtxBuilder::new();
    builder.envs(&env_strings);

    if let Some(base) = std::env::var_os("EDGE_FS_PATH") {
        let tenant_dir = std::path::Path::new(&base).join(tenant_id);
        match std::fs::create_dir_all(&tenant_dir) {
            Ok(()) => {
                // wasmtime-wasi 25 requires explicit DirPerms/FilePerms
                // for preopened_dir — read-write is the canonical edge-
                // cloud semantic (tenants upload and serve from their own
                // directory). Refusing READ would block every fixture that
                // reads `index.html` etc. WRITE is required so e.g. the
                // migrate flow can persist generated `.c` to disk.
                if let Err(e) =
                    builder.preopened_dir(&tenant_dir, "/", DirPerms::all(), FilePerms::all())
                {
                    tracing::warn!(
                        tenant_id,
                        dir = ?tenant_dir,
                        err = %e,
                        "EDGE_FS_PATH preopen failed; running without filesystem access"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    tenant_id,
                    dir = ?tenant_dir,
                    err = %e,
                    "EDGE_FS_PATH directory create failed; running without filesystem access"
                );
            }
        }
    }

    builder.build()
}

// ── WIT Host trait impls (all sync) ───────────────────────────────────────
//
// Both worlds share the `edge:cloud@0.2.0` package but bindgen emits
// distinct Host traits per submodule (`edge_runtime_long::...::Host` vs
// `edge_runtime_handler::...::Host`). The WIT bodies are identical, so
// each `impl_*!` macro below generates the two parallel impls in one
// call. Bodies delegate to the inner per-tenant store structs and never
// cross the sync/async boundary — host-internal async work (Tokio sleep,
// scheduling timers) is handled inside `interfaces/{time,scheduling}.rs`.

macro_rules! impl_kv_store {
    ($host:path, $record_ty:ty) => {
        impl $host for RuntimeState {
            fn get(&mut self, key: String) -> Option<Vec<u8>> {
                self.kv_store.get(&key).ok().flatten()
            }
            fn set(&mut self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) {
                let _ = self.kv_store.set(key, value, ttl_secs);
            }
            fn delete(&mut self, key: String) {
                let _ = self.kv_store.delete(&key);
            }
            fn list_keys(&mut self, prefix: String) -> Vec<String> {
                self.kv_store.list_keys(&prefix).ok().unwrap_or_default()
            }
            fn get_many(&mut self, keys: Vec<String>) -> Vec<Option<Vec<u8>>> {
                self.kv_store.get_many(&keys)
            }
            fn set_many(&mut self, items: Vec<(String, Vec<u8>, Option<u32>)>) {
                let _ = self.kv_store.set_many(&items);
            }
            fn delete_many(&mut self, keys: Vec<String>) {
                let _ = self.kv_store.delete_many(&keys);
            }
            fn exists(&mut self, key: String) -> bool {
                self.kv_store.exists(&key)
            }
            fn clear(&mut self) {
                self.kv_store.clear();
            }
        }
    };
}

macro_rules! impl_cache {
    ($host:path) => {
        #[allow(unused_must_use)]
        impl $host for RuntimeState {
            fn get(&mut self, key: String) -> Option<Vec<u8>> {
                self.cache.get(&key).ok().flatten()
            }
            fn set(&mut self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) {
                let _ = self.cache.set(key, value, ttl_secs);
            }
            fn delete(&mut self, key: String) {
                let _ = self.cache.delete(&key);
            }
            fn clear(&mut self) {
                self.cache.clear();
            }
            fn size(&mut self) -> u32 {
                self.cache.size().unwrap_or(0)
            }
            fn exists(&mut self, key: String) -> bool {
                self.cache.exists(&key)
            }
            fn list_keys(&mut self, prefix: String) -> Vec<String> {
                self.cache.list_keys(&prefix)
            }
            fn get_many(&mut self, keys: Vec<String>) -> Vec<Option<Vec<u8>>> {
                self.cache.get_many(&keys)
            }
            fn set_many(&mut self, items: Vec<(String, Vec<u8>, Option<u32>)>) {
                let _ = self.cache.set_many(&items);
            }
            fn delete_many(&mut self, keys: Vec<String>) {
                let _ = self.cache.delete_many(&keys);
            }
        }
    };
}

// Observe has the trickiest trait: `emit_log_record` takes a `LogRecord`
// enum that is namespaced per world. The macro takes the records/enum
// paths explicitly.
macro_rules! impl_observe {
    ($host:path, $record_ty:path, $log_level_ty:path) => {
        impl $host for RuntimeState {
            fn increment_counter(&mut self, name: String, labels: Vec<(String, String)>) {
                self.observe.increment_counter(&name, &labels);
            }
            fn record_gauge(&mut self, name: String, value: f64, labels: Vec<(String, String)>) {
                self.observe.record_gauge(&name, value, &labels);
            }
            fn record_histogram(
                &mut self,
                name: String,
                value: f64,
                labels: Vec<(String, String)>,
            ) {
                self.observe.record_histogram(&name, value, &labels);
            }
            fn emit_log(&mut self, level: String, message: String, labels: Vec<(String, String)>) {
                self.observe.emit_log(&level, &message, &labels);
            }
            fn emit_log_record(&mut self, r: $record_ty) {
                self.observe.emit_log_record(&observe::LogRecord {
                    timestamp_ms: r.timestamp_ms,
                    level: match r.level {
                        <$log_level_ty>::Error => observe::LogLevel::Error,
                        <$log_level_ty>::Warn => observe::LogLevel::Warn,
                        <$log_level_ty>::Info => observe::LogLevel::Info,
                        <$log_level_ty>::Debug => observe::LogLevel::Debug,
                        <$log_level_ty>::Trace => observe::LogLevel::Trace,
                    },
                    message: r.message,
                    labels: r.labels,
                });
            }
        }
    };
}

macro_rules! impl_time {
    ($host:path) => {
        impl $host for RuntimeState {
            fn now(&mut self) -> u64 {
                self.time.now()
            }
            fn sleep(&mut self, duration_ms: u64) {
                let _ = self.time.sleep(duration_ms);
            }
            fn resolution(&mut self) -> u64 {
                self.time.resolution()
            }
        }
    };
}

macro_rules! impl_scheduling {
    ($host:path) => {
        impl $host for RuntimeState {
            fn schedule_once(&mut self, delay_ms: u64, payload: Vec<u8>) -> String {
                self.scheduling
                    .schedule_once(delay_ms, payload)
                    .unwrap_or_default()
            }
            fn schedule_repeating(&mut self, interval_ms: u64, payload: Vec<u8>) -> String {
                self.scheduling
                    .schedule_repeating(interval_ms, payload)
                    .unwrap_or_default()
            }
            fn cancel_scheduled(&mut self, id: String) {
                let _ = self.scheduling.cancel(&id);
            }
        }
    };
}

macro_rules! impl_process {
    ($host:path) => {
        impl $host for RuntimeState {
            fn get_env(&mut self, key: String) -> Option<String> {
                self.process.get_env(&key)
            }
            fn get_all_env(&mut self) -> Vec<(String, String)> {
                self.process.get_all_env()
            }
            fn get_args(&mut self) -> Vec<String> {
                self.process.get_args()
            }
            fn get_cwd(&mut self) -> Result<String, String> {
                self.process.get_cwd()
            }
            fn exit(&mut self, code: u32) {
                self.process.exit(code)
            }
        }
    };
}

// Generate the per-world impls. Each macro invocation emits TWO impl
// blocks (one for long-running, one for handler) with identical bodies.
// The bindgen! call for each world generates a separate Host trait per
// interface in its own submodule; the trait objects are distinct Rust
// types but have the same shape.

impl_kv_store!(LongKvStoreHost, LongObserveLogRecord);
impl_kv_store!(KvStoreHost, HandlerObserveLogRecord);

impl_cache!(LongCacheHost);
impl_cache!(CacheHost);

impl_observe!(LongObserveHost, LongObserveLogRecord, LongObserveLogLevel);
impl_observe!(ObserveHost, HandlerObserveLogRecord, HandlerObserveLogLevel);

impl_time!(LongTimeHost);
impl_time!(TimeHost);

impl_scheduling!(LongSchedulingHost);
impl_scheduling!(SchedulingHost);

impl_process!(LongProcessHost);
impl_process!(ProcessHost);
