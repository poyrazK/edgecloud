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
//!   * `egress` is enforced on `wasi:http/outgoing-handler` via
//!     `EgressHttpHooks::send_request` (see the `http_hooks` field),
//!     and on `wasi:sockets/{tcp,udp}` connect-side via
//!     `WasiCtxBuilder::socket_addr_check` (see `socket_egress`). Both
//!     layers share the same `Arc<EgressPolicy>` so a policy swap is
//!     reflected in both at once.
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
use crate::socket_egress::{make_socket_addr_check, SocketEgressPolicy};
use crate::store::HasStoreLimits;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use wasmtime::component::ResourceTable;
use wasmtime::{ResourceLimiter, StoreLimits};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::types::{HostFutureIncomingResponse, OutgoingRequestConfig};
use wasmtime_wasi_http::p2::{HttpError, HttpResult, WasiHttpCtxView, WasiHttpHooks, WasiHttpView};
use wasmtime_wasi_http::WasiHttpCtx;

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

    /// Per-deployment egress policy. Enforced on:
    ///   * `wasi:http/outgoing-handler` via `EgressHttpHooks::send_request`
    ///     (URL/hostname + hard-deny).
    ///   * `wasi:sockets/{tcp,udp}` connect-side via the per-tenant
    ///     `WasiCtx` `socket_addr_check` closure (IP-level + hard-deny).
    ///
    /// Both layers share the same `Arc<EgressPolicy>` so a policy swap
    /// is reflected in both at once.
    pub egress: Arc<EgressPolicy>,

    /// Per-deployment socket-egress mode (issue #309). `Copy`, so each
    /// `RuntimeState::clone` copies it cheaply without consulting the
    /// env var. The runtime does **not** read `EDGE_EGRESS_SOCKET_MODE`
    /// itself; the bootstrap site is `edge-worker/src/config.rs::Config::from_env`,
    /// which sets it once at worker startup and threads it through
    /// `HandlerConfig` → `RuntimeState::with_env_and_meter`.
    pub socket_mode: SocketEgressPolicy,

    /// wasmtime 45 moved `send_request` off the `WasiHttpView` trait and
    /// onto a new `WasiHttpHooks` trait, referenced by
    /// `WasiHttpCtxView::hooks`. We store the concrete `EgressHttpHooks`
    /// struct here; `WasiHttpView::http()` coerces `&mut self.http_hooks`
    /// to `&mut dyn WasiHttpHooks` for the linker. Concrete storage
    /// avoids the per-request `Box::new` + `String::clone` that a boxed
    /// trait object would force.
    pub(crate) http_hooks: EgressHttpHooks,

    /// Shared exit-code flag set by `Process::exit` when the guest calls
    /// `process.exit`. Allows `execute_app` to distinguish a clean guest
    /// exit from a wasm trap.
    pub exit_code: Arc<AtomicU32>,

    /// Memory limits embedded here so `create_store` can wire wasmtime's
    /// resource-limiter callback with a proper lifetime-bounded closure
    /// rather than a `Box::leak`-based `'static` reference. Set by
    /// `create_store` before the `Store` is constructed; `None` until then.
    store_limits: Option<StoreLimits>,
}

impl RuntimeState {
    /// Test-only constructor. Ephemeral in-memory stores, no preopens,
    /// permissive egress, sockets in `BlockAll` mode (the test-seam
    /// default; tests that exercise the closure directly call
    /// `socket_egress::make_socket_addr_check` instead).
    #[cfg(test)]
    pub fn new() -> Self {
        let env = Arc::new(process::filter_env_vars(std::env::vars()).collect::<HashMap<_, _>>());
        let egress = Arc::new(EgressPolicy::allow_all());
        let wasi_ctx = build_wasi_ctx_for_tenant(&env, "", &egress, SocketEgressPolicy::BlockAll);
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
            egress: egress.clone(),
            socket_mode: SocketEgressPolicy::BlockAll,
            http_hooks: EgressHttpHooks::new(egress, String::new()),
            exit_code,
            store_limits: None,
        }
    }

    /// Production constructor. Builds per-tenant persistent stores (KV,
    /// cache, scheduler) and a `WasiCtx` for the tenant's preopens.
    ///
    /// `egress` is enforced on `wasi:http/outgoing-handler` via the
    /// `EgressHttpHooks` stored in `http_hooks` and on
    /// `wasi:sockets/{tcp,udp}` connect-side via the closure installed
    /// in `build_wasi_ctx_for_tenant`. `socket_mode` is threaded in as a
    /// parameter (no env reads on the per-request hot path); the
    /// bootstrap site that reads `EDGE_EGRESS_SOCKET_MODE` is
    /// `edge-worker/src/config.rs::Config::from_env`.
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
        metrics_acc: Option<Arc<observe::MetricsAccumulator>>,
        socket_mode: SocketEgressPolicy,
    ) -> Self {
        let env = Arc::new(env);
        let exit_code = Arc::new(AtomicU32::new(0));
        let kv_store = get_or_create_kv_store(&tenant_id);
        let cache_store = get_or_create_cache(&tenant_id);
        let scheduling = get_or_create_scheduler(&tenant_id);

        let wasi_ctx = build_wasi_ctx_for_tenant(&env, &tenant_id, &egress, socket_mode);

        let mut observe_cfg = observe::ObserveConfig::new()
            .with_log_sink(log_sink)
            .with_app_ctx(app_ctx);
        if let Some(acc) = metrics_acc {
            observe_cfg = observe_cfg.with_metrics_accumulator(acc);
        }
        let observer = observe::Observer::from_config(observe_cfg);

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
            tenant_id: tenant_id.clone(),
            egress: egress.clone(),
            socket_mode,
            // The hooks box shares the same Arc-shared EgressPolicy and
            // tenant id as the top-level fields, so a future mid-flight
            // policy swap (returning a new Arc) only updates one place.
            http_hooks: EgressHttpHooks::new(egress, tenant_id),
            exit_code,
            store_limits: None,
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
        // one request don't leak to the next. The `socket_addr_check`
        // closure is `Arc`-backed inside `SocketAddrCheck`, so the
        // re-built `WasiCtx` shares the same closure with the original.
        Self {
            kv_store: self.kv_store.clone(),
            cache: self.cache.clone(),
            scheduling: self.scheduling.clone(),
            observe: self.observe.clone(),
            time: self.time.clone(),
            process: self.process.clone(),
            wasi_ctx: build_wasi_ctx_for_tenant(
                &self.wasi_env_for_clone,
                &self.tenant_id,
                &self.egress,
                self.socket_mode,
            ),
            // WasiHttpCtx is zero-sized in wasmtime 25 (`PhantomData`)
            // so this is a no-op clone. The per-Store resources still
            // live in `resource_table` (fresh below) which is what
            // matters for handler isolation.
            wasi_http_ctx: WasiHttpCtx::new(),
            resource_table: ResourceTable::new(),
            wasi_env_for_clone: self.wasi_env_for_clone.clone(),
            tenant_id: self.tenant_id.clone(),
            egress: self.egress.clone(),
            socket_mode: self.socket_mode,
            http_hooks: EgressHttpHooks::new(self.egress.clone(), self.tenant_id.clone()),
            exit_code: Arc::new(AtomicU32::new(0)),
            store_limits: None, // set fresh by create_store for each new Store
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
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

/// Per-tenant `WasiHttpHooks` impl. wasmtime 45 split the outgoing-HTTP
/// customization off the `WasiHttpView` trait into a separate
/// `WasiHttpHooks` trait, referenced from `WasiHttpCtxView::hooks`.
/// `send_request` is the only hook we override — it enforces the tenant's
/// `EgressPolicy` before opening any TCP connection.
///
/// Per-request outbound requests go through this path:
/// `guest.wasi:http/outgoing-handler::handle(req, out)` →
/// `WasiHttpImpl<&mut T>::send_request` → `T::http().hooks.send_request`
/// (where `T::http()` returns the `WasiHttpCtxView` and `hooks` is our
/// `&mut EgressHttpHooks`) → `EgressPolicy::check(url)` → either
/// denied (returns `Err`) or forwarded to `egress_transport`,
/// which pre-resolves the hostname, validates every resolved IP against
/// `EgressPolicy::check_resolved_ip`, and connects to the validated IP
/// literal — closing the TOCTOU window between the pre-check and the
/// kernel's own resolver inside `default_send_request`.
///
/// The host check runs PRE-DNS, so a denied host NEVER leaves the
/// worker. The DNS-rebinding guard is enforced at IP granularity inside
/// `egress_transport`, not via a separate `connect_hook` (which
/// wasmtime-wasi-http 45 does not expose). With the `egress-tls`
/// feature enabled, the same module also terminates TLS itself so SNI
/// keeps using the hostname while the underlying TCP socket binds to
/// the validated literal — closing the second-order TOCTOU between
/// TCP-connect and TLS-handshake.
///
/// Follow-up for v0.3: `wasi:sockets/ip-name-lookup` is a separate
/// egress surface that bypasses `wasi:http` entirely (raw DNS APIs the
/// guest calls directly). It needs its own `Host` override that funnels
/// through the same `EgressPolicy::check_resolved_ip` gate; today it is
/// left to wasmtime-wasi-http's default resolver, which is unguarded.
pub(crate) struct EgressHttpHooks {
    pub(crate) egress: Arc<EgressPolicy>,
    pub(crate) tenant_id: String,
}

impl EgressHttpHooks {
    pub(crate) fn new(egress: Arc<EgressPolicy>, tenant_id: String) -> Self {
        Self { egress, tenant_id }
    }
}

impl WasiHttpHooks for EgressHttpHooks {
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
            let diagnostics = format!("egress denied: {reason}");
            return Err(HttpError::from(ErrorCode::InternalError(Some(diagnostics))));
        }
        // Egress allowlist passed — defer to our custom DNS-rebinding-
        // aware send_request handler in `egress_transport`. It
        // pre-resolves via `tokio::net::lookup_host`, validates each
        // candidate IP against `egress.check_resolved_ip`, then connects
        // to the validated IP literal — so the kernel cannot re-resolve
        // and serve a poisoned record on the second query. This also
        // subsumes the inline `std::net::ToSocketAddrs` rebinding check
        // that PR #288 added on main; having both would be redundant
        // (and the inline check races the actual `default_send_request`
        // resolver, while egress_transport connects to a literal IP and
        // — when the `egress-tls` feature is on — terminates TLS itself
        // to also close the second-order TOCTOU).
        Ok(crate::egress_transport::spawn_send_request_handler(
            request,
            config,
            self.egress.clone(),
            self.tenant_id.clone(),
        ))
    }
    // `is_forbidden_header` falls back to the `WasiHttpHooks` default
    // which strips the canonical hop-by-hop / connection-state header
    // set (Connection, Keep-Alive, Proxy-Authenticate,
    // Proxy-Authorization, TE, Trailers, Transfer-Encoding, Upgrade,
    // Host, Http2-Settings). Adding egress-specific stripping here
    // would require new `EgressPolicy` methods we don't need for v0.2 —
    // every header a tenant wants blocked is already enforced by the
    // URL-level `EgressPolicy::check` above.
}

/// `WasiHttpView` — required by `wasmtime_wasi_http::p2::add_only_http_to_linker_async`.
/// wasmtime 45 collapsed the trait from `{ctx, table}` into a single
/// `http()` method returning a `WasiHttpCtxView` bundle (mirrors the
/// `WasiView` change in wasmtime 36). The `hooks` field is the
/// `EgressHttpHooks` box stored on `RuntimeState`.
///
/// Sibling egress surface: `wasi:sockets/{tcp,udp}` connect-side is
/// gated by the `socket_addr_check` closure installed in
/// `build_wasi_ctx_for_tenant`. See `socket_egress` for the dispatch
/// table. Binds (`TcpBind`/`UdpBind`) are local-only and unconditionally
/// permitted; only the connect-side is policy-gated.
impl WasiHttpView for RuntimeState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        // `&mut self.http_hooks` (a `&mut EgressHttpHooks`) coerces to
        // `&mut dyn WasiHttpHooks` at the struct-literal site via the
        // `WasiHttpHooks for EgressHttpHooks` impl above. No Box, no
        // heap indirection.
        WasiHttpCtxView {
            ctx: &mut self.wasi_http_ctx,
            table: &mut self.resource_table,
            hooks: &mut self.http_hooks,
        }
    }
}

impl HasStoreLimits for RuntimeState {
    fn set_store_limits(&mut self, limits: StoreLimits) {
        self.store_limits = Some(limits);
    }

    fn store_limits_mut(&mut self) -> &mut dyn ResourceLimiter {
        self.store_limits
            .as_mut()
            .expect("store_limits not set — create_store must call set_store_limits first")
    }
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

    /// Default request config used by every `send_request` test below.
    /// Hoisted out of the test bodies so a future field addition to
    /// `OutgoingRequestConfig` only updates one site (and so the
    /// tests don't drift to inconsistent defaults).
    const TEST_REQUEST_CONFIG: wasmtime_wasi_http::p2::types::OutgoingRequestConfig =
        wasmtime_wasi_http::p2::types::OutgoingRequestConfig {
            use_tls: false,
            connect_timeout: std::time::Duration::from_secs(60),
            first_byte_timeout: std::time::Duration::from_secs(60),
            between_bytes_timeout: std::time::Duration::from_secs(60),
        };

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
            None,
            crate::socket_egress::SocketEgressPolicy::default(),
        )
    }

    /// Build a minimal `hyper::Request<HyperOutgoingBody>` with the
    /// supplied URL. The body is empty — we never get past the URL
    /// check so no real network IO happens.
    fn make_request(uri: &str) -> Request<wasmtime_wasi_http::p2::body::HyperOutgoingBody> {
        Request::builder()
            .uri(uri)
            .method("GET")
            .body(wasmtime_wasi_http::p2::body::HyperOutgoingBody::default())
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
        let result = state.http().hooks.send_request(req, TEST_REQUEST_CONFIG);
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
        let result = state.http().hooks.send_request(req, TEST_REQUEST_CONFIG);
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
        let result = state.http().hooks.send_request(req, TEST_REQUEST_CONFIG);
        assert!(result.is_err(), "expected Err for empty allowlist, got Ok");
    }

    #[test]
    fn send_request_allow_all_passes_non_blocked() {
        // Sentinel policy (`EgressPolicy::allow_all()`) — public hosts
        // pass, only hard-denied IPs would still error.
        let mut state = state_with_egress(Arc::new(EgressPolicy::allow_all()));
        let req = make_request("http://127.0.0.1/");
        let result = state.http().hooks.send_request(req, TEST_REQUEST_CONFIG);
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
        let result = state.http().hooks.send_request(req, TEST_REQUEST_CONFIG);
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

/// Resolved once per process from `EDGE_FS_PATH`. Reading env vars on
/// the FaaS hot path (one read per accepted HTTP request) was costing
/// ~1k syscalls/sec at 1k RPS in the v0.2 review. The var is process-
/// static by design (operators don't reload it without a restart), so
/// a `OnceLock` is the right tool.
static EDGE_FS_PATH: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();

fn resolve_edge_fs_path() -> Option<&'static std::path::Path> {
    EDGE_FS_PATH
        .get_or_init(|| std::env::var_os("EDGE_FS_PATH").map(std::path::PathBuf::from))
        .as_deref()
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
///
/// Sockets egress (issue #309): calls `builder.socket_addr_check(...)`
/// with a closure sourced from `socket_egress::make_socket_addr_check`.
/// The closure consults the tenant `EgressPolicy` (closed over from
/// `egress`) and the per-tenant `SocketEgressPolicy` mode (`mode`).
/// Mode = `BlockAll` keeps the wasmtime 45 default close behavior
/// (guests cannot use `wasi:sockets` for connect/send); mode =
/// `AllowList` consults `EgressPolicy::check_address` (hard-deny +
/// allowlist); mode = `AllowAll` is equivalent to
/// `WasiCtxBuilder::inherit_network(true)` and is **off by default**.
fn build_wasi_ctx_for_tenant(
    env: &Arc<HashMap<String, String>>,
    tenant_id: &str,
    egress: &Arc<EgressPolicy>,
    mode: SocketEgressPolicy,
) -> WasiCtx {
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
    let env_strings: Vec<(String, String)> =
        process::filter_env_vars(env.iter().map(|(k, v)| (k.clone(), v.clone()))).collect();
    let mut builder = WasiCtxBuilder::new();
    builder.envs(&env_strings);

    if let Some(base) = resolve_edge_fs_path() {
        let tenant_dir = base.join(tenant_id);
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

    // Socket-level egress policy: install the closure before `.build()`.
    // The closure is the only public injection point in wasmtime-wasi
    // 45.0.3 — `WasiSocketsCtx`'s fields are `pub(crate)`. See
    // `socket_egress.rs` for the dispatch table per `SocketEgressPolicy`
    // × `SocketAddrUse`.
    builder.socket_addr_check(make_socket_addr_check(
        egress.clone(),
        mode,
        tenant_id.to_string(),
    ));

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

#[cfg(test)]
mod with_env_and_meter_tests {
    use super::*;
    use crate::interfaces::observe::{AppLogContext, LogRecord, LogSink};

    struct NoopSink;
    impl LogSink for NoopSink {
        fn push(&self, _r: LogRecord, _c: AppLogContext) {}
    }

    fn state_with_env(env: HashMap<String, String>, tenant_id: &str) -> RuntimeState {
        RuntimeState::with_env_and_meter(
            env,
            None,
            tenant_id.to_string(),
            Arc::new(EgressPolicy::allow_all()),
            Arc::new(NoopSink) as Arc<dyn LogSink>,
            AppLogContext {
                app_name: "test".to_string(),
                tenant_id: tenant_id.to_string(),
                deployment_id: "test".to_string(),
            },
            None,
            crate::socket_egress::SocketEgressPolicy::default(),
        )
    }

    // ── exit_requested ─────────────────────────────────────────────────

    #[test]
    fn exit_requested_returns_none_on_zero() {
        let state = RuntimeState::new();
        assert_eq!(state.exit_requested(), None);
    }

    #[test]
    fn exit_requested_returns_code_after_exit() {
        let state = RuntimeState::new();
        state.process.exit(42);
        assert_eq!(state.exit_requested(), Some(42));
    }

    // ── Env passthrough ────────────────────────────────────────────────
    //
    // `with_env_and_meter` stores the raw env in `Process` as-is. The
    // worker is responsible for pre-filtering before calling this
    // constructor. The defense-in-depth env blocklist is applied inside
    // `build_wasi_ctx_for_tenant` to the wasi:cli path (which we cannot
    // easily inspect from tests), not to the process::get-all-env path.

    #[test]
    fn env_passed_through_to_process() {
        let mut env = HashMap::new();
        env.insert("SAFE_VAR".into(), "safe".into());
        env.insert("AWS_SECRET_KEY".into(), "leaked".into());
        env.insert("DB_API_KEY".into(), "secret123".into());

        let state = state_with_env(env.clone(), &format!("env-pass-{}", uuid::Uuid::new_v4()));
        let all_env: HashMap<String, String> = state.process.get_all_env().into_iter().collect();

        // All vars are passed through — runtime does NOT filter the Process.
        assert_eq!(all_env.get("SAFE_VAR"), Some(&"safe".into()));
        assert_eq!(all_env.get("AWS_SECRET_KEY"), Some(&"leaked".into()));
        assert_eq!(all_env.get("DB_API_KEY"), Some(&"secret123".into()));
    }

    // ── Persistence env var wiring ────────────────────────────────────

    #[tokio::test]
    async fn persistence_env_vars_create_persistent_stores() {
        let kv_dir = tempfile::TempDir::new().expect("kv temp dir");
        let cache_dir = tempfile::TempDir::new().expect("cache temp dir");
        let sched_dir = tempfile::TempDir::new().expect("sched temp dir");
        let tenant_id = format!("persist-{}", uuid::Uuid::new_v4());

        let kv_str = kv_dir.path().to_string_lossy().to_string();
        let cache_str = cache_dir.path().to_string_lossy().to_string();
        let sched_str = sched_dir.path().to_string_lossy().to_string();

        let result = tokio::task::spawn_blocking(move || {
            temp_env::with_var("EDGE_KV_STORE_PATH", Some(&kv_str), || {
                temp_env::with_var("EDGE_CACHE_PATH", Some(&cache_str), || {
                    temp_env::with_var("EDGE_SCHEDULING_PATH", Some(&sched_str), || {
                        let state = state_with_env(HashMap::new(), &tenant_id);

                        state.kv_store.set("k".into(), b"v".to_vec(), None).unwrap();
                        assert_eq!(state.kv_store.get("k").unwrap(), Some(b"v".to_vec()));

                        state.cache.set("ck".into(), b"cv".to_vec(), None).unwrap();
                        assert_eq!(state.cache.get("ck").unwrap(), Some(b"cv".to_vec()));

                        let sched_id = state
                            .scheduling
                            .schedule_once(60_000, b"sp".to_vec())
                            .unwrap();
                        state.scheduling.cancel(&sched_id).unwrap();
                    });
                });
            });
        })
        .await;

        result.expect("spawn_blocking panicked");
    }

    #[tokio::test]
    async fn persistence_env_var_fallback_on_bad_path() {
        let bad_dir = tempfile::TempDir::new().expect("temp dir");
        let tenant_id = format!("fallback-{}", uuid::Uuid::new_v4());

        // Make the directory read-only so with_persistence fails.
        let mut perms = std::fs::metadata(bad_dir.path())
            .expect("metadata")
            .permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(bad_dir.path(), perms).expect("set read-only");

        let bad_str = bad_dir.path().to_string_lossy().to_string();

        let result = tokio::task::spawn_blocking(move || {
            temp_env::with_var("EDGE_KV_STORE_PATH", Some(&bad_str), || {
                // Should not panic — fallback to ephemeral store.
                let state = state_with_env(HashMap::new(), &tenant_id);
                state.kv_store.set("k".into(), b"v".to_vec(), None).unwrap();
                assert_eq!(state.kv_store.get("k").unwrap(), Some(b"v".to_vec()));
            });
        })
        .await;

        result.expect("spawn_blocking panicked");
    }

    // ── Registry caching ───────────────────────────────────────────────

    #[test]
    fn same_tenant_reuses_registry_stores() {
        let tenant_id = format!("reuse-{}", uuid::Uuid::new_v4());
        let state1 = state_with_env(HashMap::new(), &tenant_id);
        let state2 = state_with_env(HashMap::new(), &tenant_id);

        // Both calls must return the same Arc allocations.
        assert!(
            Arc::ptr_eq(&state1.kv_store, &state2.kv_store),
            "same tenant must reuse kv_store Arc"
        );
        assert!(
            Arc::ptr_eq(&state1.cache, &state2.cache),
            "same tenant must reuse cache Arc"
        );
        assert!(
            Arc::ptr_eq(&state1.scheduling, &state2.scheduling),
            "same tenant must reuse scheduling Arc"
        );
    }

    #[test]
    fn different_tenants_have_different_stores() {
        let tenant_a = format!("diff-a-{}", uuid::Uuid::new_v4());
        let tenant_b = format!("diff-b-{}", uuid::Uuid::new_v4());
        let state1 = state_with_env(HashMap::new(), &tenant_a);
        let state2 = state_with_env(HashMap::new(), &tenant_b);

        assert!(
            !Arc::ptr_eq(&state1.kv_store, &state2.kv_store),
            "different tenants must have different kv_store Arcs"
        );
        assert!(
            !Arc::ptr_eq(&state1.cache, &state2.cache),
            "different tenants must have different cache Arcs"
        );
        assert!(
            !Arc::ptr_eq(&state1.scheduling, &state2.scheduling),
            "different tenants must have different scheduling Arcs"
        );
    }
}
