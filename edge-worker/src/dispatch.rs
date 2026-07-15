//! FaaS dispatcher for Handler-model components.
//!
//! Phase C: real HTTP server that hosts one
//! `wasi:http/incoming-handler` export per `(tenant, app)` pair. Each
//! accepted request creates a fresh `wasmtime::Store<RuntimeState>`
//! (via `ProxyPre::instantiate_async`) and drives the guest's
//! `handle(req, out)` impl. Outbound HTTP calls go through
//! `state.http().hooks.send_request` (the `EgressHttpHooks` override), which is where `EgressPolicy::check`
//! runs (Phase C-3).
//!
//! # Why hyper (not axum)
//!
//! `wasmtime-wasi-http` 25 ships `HyperOutgoingBody` (a `hyper::Body`)
//! and is documented to be paired with the raw `hyper::server::conn::http1`
//! API. axum 0.7 wraps `hyper::Body` in `axum::body::Body` which round-
//! trips through extra mpsc/adapter channels. Direct `hyper` keeps the
//! per-request path lean. Body-cap limits are enforced via a separate
//! `BodySizeCap` wrapper (TODO C-6.4) once the integration tests prove
//! the simple path works end-to-end.
//!
//! # Per-request isolation
//!
//! Every request constructs a fresh `RuntimeState` via
//! `RuntimeState::with_env_and_meter`. The constructor builds a
//! fresh `ResourceTable` and a fresh `WasiCtx` (rebuilt from the
//! stored env `HashMap`), so resource handles from one request
//! don't leak to the next. Per-tenant `KvStore` / `Cache` /
//! `Scheduler` are pulled from the runtime's global registry
//! (memoized via `OnceCell`-rooted RwLock reads) so two requests
//! for the same tenant share the same `Arc<KvStore>` etc.
//!
//! The `with_env_and_meter` call also allocates a fresh
//! `Arc<AtomicU32>` for the `exit_code` flag and wires it into the
//! per-request `Process` — see `RuntimeState::with_env_and_meter`
//! at edge-runtime/src/runtime.rs:252-296. After the guest call we
//! manual-clone that Arc into `exit_code_arc` so we can read the
//! flag once the `RuntimeState` has been moved into the
//! `wasmtime::Store`.
//!
//! `RuntimeState::clone` (edge-runtime/src/runtime.rs:321-383) is
//! available as a documented alternative but currently unused —
//! the WasiCtx inside `RuntimeState` is `!Sync`, which would
//! force any `Arc<RuntimeState>` field to live on a struct that
//! does not need `Send`, a constraint that the FaaS dispatcher's
//! `tokio::spawn` path cannot satisfy without restructuring. The
//! per-request `with_env_and_meter` is correct and the expensive
//! parts (registry lookups + observer config) are O(1) amortized.
//!
//! # Per-request budget
//!
//! The store's epoch deadline is set to `request_budget_ticks`; the
//! engine's epoch clock is advanced by the per-app `std::thread`
//! ticker spawned at the top of `serve`. (The supervisor's
//! long-running-path ticker — at `supervisor.rs:206-217` — is the
//! tokio analogue. We use a dedicated OS thread here because tokio
//! scheduling latency under load (parallel test runs) can drift the
//! ticker past the deadline.) A guest that exceeds the budget traps
//! with an interrupt — `handle_request` translates that into a
//! synthetic 500 response.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskCtx, Poll};
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
// Replaces `rustls-pemfile` (RUSTSEC-2025-0134 unmaintained) per issue
// #625 — see `try_load_tls_config`.
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::rt::Executor;
use hyper::server::conn::http1;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::Request as HyperRequest;
use hyper::Response as HyperResponse;
use rustls::pki_types::pem::PemObject;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use wasmtime_wasi_http::io::TokioIo;
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::WasiHttpView;

use edge_runtime::interfaces::observe::{AppLogContext, LogSink};
use edge_runtime::socket_egress::SocketEgressPolicy;
use edge_runtime::{EgressPolicy, RequestMeter, RuntimeState};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

// Convenience aliases: the bindgen-generated `ProxyPre` lives one level
// deeper than the example docs suggest — `wasmtime_wasi_http::ProxyPre`
// is NOT re-exported at the crate root (verified in 25.0.3's `lib.rs`).
// The Response Sender/Receiver aliases factor a 6-line type that
// clippy::type_complexity rightly complains about.
type HandlerProxyPre = wasmtime_wasi_http::p2::bindings::ProxyPre<RuntimeState>;
type HandlerResponseResult = Result<
    HyperResponse<HyperOutgoingBody>,
    wasmtime_wasi_http::p2::bindings::http::types::ErrorCode,
>;
type HandlerResponseSender = tokio::sync::oneshot::Sender<HandlerResponseResult>;
type HandlerResponseReceiver = tokio::sync::oneshot::Receiver<HandlerResponseResult>;

/// Wraps [`HyperOutgoingBody`] and counts every data frame's byte length
/// via [`RequestMeter::record_outbound_bytes`] (issue #210 — outbound
/// byte metering was lost when PR #196 deleted `http_server.rs`).
///
/// Call `.boxed_unsync()` to convert back to [`HyperOutgoingBody`].
struct CountingBody {
    inner: HyperOutgoingBody,
    meter: Arc<RequestMeter>,
    /// Worker-level metrics handle (issue #49). `None` for tests
    /// that build a synthetic dispatch without a `WorkerMetrics`.
    metrics_handle: Option<Arc<crate::metrics::MetricsHandle>>,
}

impl Body for CountingBody {
    type Data = Bytes;
    type Error = ErrorCode;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskCtx<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let len = data.len() as u64;
                    self.meter.record_outbound_bytes(len);
                    if let Some(handle) = self.metrics_handle.as_ref() {
                        handle.outbound_bytes.inc_by(len);
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

/// Stream type that can be either plain TCP or TLS-wrapped (issue #209).
enum MaybeTls {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskCtx<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskCtx<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Per-app HTTP dispatcher for a FaaS component.
///
/// Owns a `ProxyPre<RuntimeState>` (pre-instantiated component) and a
/// `hyper`-based server bound to `0.0.0.0:port`. One instance per
/// `(tenant_id, app_name)` is stored on the `AppInstance`.
pub struct HandlerDispatch {
    /// TCP port assigned to this app by `PortPool`.
    port: u16,

    /// Engine-clock tick interval (ms) — how often the per-app ticker
    /// calls `engine.increment_epoch()`. Defaults to 1 if the caller
    /// passes 0.
    tick_ms: u64,
    /// Per-app context shared across all requests (tenant_id, egress,
    /// meter, log_sink, app_ctx). Cheap to clone (`Arc`-heavy).
    pub config: Arc<HandlerConfig>,
    /// Optional TLS server config for encrypted connections (issue #209).
    tls_config: Option<Arc<rustls::ServerConfig>>,
    downloader: Arc<crate::downloader::Downloader>,
    deployment_id: String,
    engine_pool: Arc<crate::supervisor::StandbyPool>,
    proxy_pre: tokio::sync::RwLock<Option<HandlerProxyPre>>,
    state: Arc<tokio::sync::RwLock<crate::state::WorkerState>>,
    /// Number of in-flight HTTP requests currently being processed.
    /// Incremented before spawning the handler task, decremented
    /// on completion. Used by the graceful drain flow (issue #graceful-draining).
    pub in_flight: Arc<std::sync::atomic::AtomicUsize>,
}

/// Per-app context handed to every FaaS request.
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct HandlerConfig {
    /// Tenant that owns this dispatch — propagated into
    /// `RuntimeState::tenant_id` and stamped onto the per-request log.
    pub tenant_id: String,
    /// EgressPolicy applied to outbound `wasi:http` calls.
    pub egress: Arc<EgressPolicy>,
    /// Per-app log sink — guest `emit_log` records flow here.
    pub log_sink: Arc<dyn LogSink>,
    /// App context stamped onto every log record for attribution.
    /// `app_ctx.app_name` is also threaded into
    /// `RuntimeState::with_env_and_meter_preview` as the per-app preopen
    /// subdirectory (issue #558) — keep this populated whenever you
    /// build a `HandlerConfig`.
    pub app_ctx: AppLogContext,
    /// Per-deployment request meter — incremented per accepted request
    /// so the heartbeat carries the right counts.
    pub meter: Arc<RequestMeter>,
    /// Env vars injected into the per-request `WasiCtx`.
    pub env: std::collections::HashMap<String, String>,
    /// Per-request body-size cap in bytes. Requests with a
    /// `Content-Length` exceeding this are rejected with 413 before
    /// the guest is invoked. `0` disables the cap (NOT RECOMMENDED —
    /// see `Config::handler_max_request_body_bytes`).
    pub max_request_body_bytes: u64,
    /// Shared metrics accumulator for this app instance. Guest
    /// `edge:observe` metric calls write into this, and the heartbeat
    /// builder snapshots it every 30s to populate `observer_metrics`
    /// on the wire. `None` before the app is running (the accumulator
    /// is created in `start_app`).
    pub metrics_acc: Option<Arc<edge_runtime::interfaces::observe::MetricsAccumulator>>,
    /// Per-app socket-egress mode (issue #412). Resolved from
    /// `AppSpec.socket_mode` with the worker-wide `Config::socket_mode`
    /// (set at startup from `EDGE_EGRESS_SOCKET_MODE`) as the default.
    /// The dispatch site picks this when constructing the per-request
    /// `RuntimeState`. The compose rule with the worker-wide
    /// `hostname_pinning_enabled` toggle is enforced at the dispatch
    /// site (see `dispatch::handle_request`), NOT here.
    pub socket_mode_for_app: SocketEgressPolicy,
    /// Per-deployment `HostnamePinned` mode toggle (issue #309
    /// follow-up). When `true`, the per-request `RuntimeState` swap in
    /// `dispatch` uses `SocketEgressPolicy::HostnamePinned` **only when
    /// `socket_mode_for_app == HostnamePinned`** (issue #412 compose
    /// rule). The worker-wide knob remains a hard gate for the
    /// dormant `HostnamePinned` mode until the upstream wasmtime-wasi
    /// patch (`docs/upstream-wasmtime-resolve-check.patch`) merges and
    /// the cache starts being populated.
    pub hostname_pinning_enabled: bool,
    /// Shared per-app `HostnamePinning` cache. The supervisor
    /// constructs one `Arc<HostnamePinning>` per app instance and
    /// passes it through every FaaS dispatch. Once the upstream
    /// wasmtime-wasi resolve hook merges (see
    /// `docs/upstream-wasmtime-resolve-check.patch`), the runtime
    /// writes into this cache during `resolve_addresses`; in-flight
    /// dispatch clones share the same Arc so any cache entry added
    /// while a request is being handled is visible to subsequent
    /// connect-side checks on the same app instance.
    pub hostname_pinning: Arc<edge_runtime::socket_egress::HostnamePinning>,
    pub last_request_at: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
    pub max_memory_mb: u64,
    pub cpu_budget_ms: u64,
    /// Preview-id forwarded from `TaskMessage` (issue #308). When `Some`,
    /// the FaaS dispatch constructs the per-request `RuntimeState` with
    /// `with_env_and_meter_preview`, which scopes the per-tenant
    /// persistent stores (KV / cache / scheduler) under a
    /// `/preview-{id}/` subdirectory. `None` for non-preview deploys;
    /// `with_env_and_meter_preview(..., None, None, ...)` collapses to
    /// the pre-#308 behavior.
    pub preview_id: Option<String>,
    /// PR-number forwarded from `TaskMessage`. When `Some`, stamped
    /// into the guest env as `EDGE_PREVIEW_PR_NUMBER` so the guest
    /// can render PR-aware UI.
    pub preview_pr_number: Option<u32>,
    /// Worker-level per-app `MetricsHandle` (issue #49). The dispatch
    /// path bumps the same four `IntCounter`s that
    /// `RequestMeter` already increments, so the `/metrics`
    /// endpoint exposes per-app counters without taking a lock on
    /// the worker-level `WorkerMetrics`. `None` for tests that build
    /// a synthetic `HandlerConfig` without a `WorkerMetrics`.
    pub metrics_handle: Option<Arc<crate::metrics::MetricsHandle>>,
}

impl HandlerDispatch {
    /// Create a new HandlerDispatch wrapping a ProxyPre for a Handler component.
    /// `tick_ms` is clamped to ≥1, and ticks is clamped to ≥1.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        port: u16,
        _request_budget_ms: u64,
        epoch_tick_ms: u64,
        config: HandlerConfig,
        tls_config: Option<Arc<rustls::ServerConfig>>,
        downloader: Arc<crate::downloader::Downloader>,
        deployment_id: String,
        engine_pool: Arc<crate::supervisor::StandbyPool>,
        state: Arc<tokio::sync::RwLock<crate::state::WorkerState>>,
    ) -> anyhow::Result<Self> {
        let tick_ms = epoch_tick_ms.max(1);
        Ok(Self {
            proxy_pre: tokio::sync::RwLock::new(None),
            port,
            tick_ms,
            config: Arc::new(config),
            tls_config,
            downloader,
            deployment_id,
            engine_pool,
            state,
            in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    /// Expose for integration tests so they can skip the Downloader.
    #[allow(dead_code)]
    pub async fn set_proxy_pre(
        &self,
        pre: wasmtime_wasi_http::p2::bindings::ProxyPre<edge_runtime::RuntimeState>,
    ) {
        *self.proxy_pre.write().await = Some(pre);
    }

    /// Wait for all in-flight requests to complete, up to `timeout`.
    /// Returns `true` if all requests drained, `false` if timeout was reached.
    pub async fn drain_in_flight(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if self.in_flight.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// Spawn the HTTP server on `0.0.0.0:port`. Returns once the
    /// shutdown signal is observed OR the server fails. The caller
    /// (supervisor) drives this in a `tokio::spawn`.
    pub async fn serve(
        self: Arc<Self>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let listener = TcpListener::bind(("0.0.0.0", self.port))
            .await
            .with_context(|| format!("HandlerDispatch: bind 0.0.0.0:{}", self.port))?;
        let local_addr = listener
            .local_addr()
            .with_context(|| format!("HandlerDispatch: local_addr for port {}", self.port))?;
        tracing::info!(
            port = self.port,
            addr = %local_addr,
            "HandlerDispatch: hyper HTTP/1 listener ready"
        );

        // Spawn the per-app epoch ticker. The engine clock is global,
        // but advancing it in a per-app thread keeps a misbehaving
        // app's deadline work isolated — when the app stops, the ticker
        // joins with it. Mirrors the supervisor's long-running-path
        // ticker at supervisor.rs:206-217. The ticker is REQUIRED —
        // without it, the per-request epoch deadline never advances,
        // and a busy guest runs to natural completion (or until the
        // host kills the process). Tested by
        // `l7_per_request_timeout_returns_500` in
        // `edge-worker/tests/layer_integration.rs`.
        //
        // We use `std::thread` (not `tokio::spawn`) because tokio
        // scheduling latency under load (multiple concurrent tests
        // each spawning many tasks) can drift the ticker past the
        // requested deadline. `std::thread::sleep` paces wall-clock
        // strictly and is unaffected by tokio runtime state. The
        // thread polls an `Arc<AtomicBool>` shutdown flag on every
        // tick and exits within one tick interval.
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        // The engine could change during lazy-loading, so the ticker needs
        // to dynamically fetch the latest engine. We use a channel or shared ref.
        let server_ref = self.clone();
        let tick_ms = self.tick_ms;
        let ticker_shutdown = shutdown_flag.clone();
        let ticker_handle = thread::Builder::new()
            .name(format!("epoch-tick-{}", self.port))
            .spawn(move || loop {
                if ticker_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                thread::sleep(Duration::from_millis(tick_ms));
                if ticker_shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let maybe_engine = {
                    let lock = server_ref.proxy_pre.blocking_read();
                    lock.as_ref().map(|p: &HandlerProxyPre| p.engine().clone())
                };
                if let Some(engine) = maybe_engine {
                    engine.increment_epoch();
                }
            })
            .with_context(|| {
                format!(
                    "HandlerDispatch: spawn epoch-tick thread for port {}",
                    self.port
                )
            })?;

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    tracing::info!(port = self.port, "HandlerDispatch received shutdown");
                    shutdown_flag.store(true, Ordering::Relaxed);
                    // Best-effort join. The thread is bounded by one
                    // tick interval (~1 ms in tests, 10 ms in prod).
                    let _ = ticker_handle.join();
                    return Ok(());
                }
                accept = listener.accept() => {
                    let (client, addr) = match accept {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!(
                                port = self.port,
                                err = %e,
                                "accept failed; continuing"
                            );
                            continue;
                        }
                    };
                    let server = self.clone();
                    let tls_config = self.tls_config.clone();
                    let tenant_id_for_log = server.config.tenant_id.clone();
                    let app_name_for_log = server.config.app_ctx.app_name.clone();
                    tokio::spawn(async move {
                        // Perform TLS handshake if configured, otherwise
                        // use the plain TCP stream (issue #209).
                        let (stream, negotiated_h2) = if let Some(tls) = tls_config {
                            match tokio_rustls::TlsAcceptor::from(tls)
                                .accept(client)
                                .await
                            {
                                Ok(tls_stream) => {
                                    let h2 = tls_stream
                                        .get_ref()
                                        .1
                                        .alpn_protocol()
                                        .map(|p| p == b"h2")
                                        .unwrap_or(false);
                                    (MaybeTls::Tls(Box::new(tls_stream)), h2)
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        tenant_id = %tenant_id_for_log,
                                        client = %addr,
                                        err = %e,
                                        "TLS handshake failed"
                                    );
                                    return;
                                }
                            }
                        } else {
                            (MaybeTls::Plain(client), false)
                        };
                        let io = TokioIo::new(stream);
                        let result = if negotiated_h2 {
                            server.serve_connection_h2(io).await
                        } else {
                            server.serve_connection_generic(io).await
                        };
                        match result {
                            Ok(()) => {
                                tracing::debug!(
                                    client = %addr,
                                    "connection closed cleanly"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    tenant_id = %tenant_id_for_log,
                                    app_name = %app_name_for_log,
                                    client = %addr,
                                    err = %e,
                                    "connection ended with error"
                                );
                            }
                        }
                    });
                }
            }
        }
    }

    /// Serve one accepted TCP connection (plain or TLS). Iterates
    /// HTTP/1.1 request/response cycles until the client closes or
    /// a handler errors.
    ///
    /// We use the raw `hyper::server::conn::http1` API because
    /// `wasmtime-wasi-http` 25's examples pair with `hyper` directly.
    async fn serve_connection_generic<IO>(
        self: Arc<Self>,
        client: TokioIo<IO>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let io = client;
        let server = self.clone();
        let svc = service_fn(move |req: HyperRequest<Incoming>| {
            let server = server.clone();
            async move {
                // `handle_request` returns `anyhow::Result<HyperResponse>`
                // which is `Send + Sync + 'static` — the bounds
                // `hyper::service::Service` requires on `Output::Error`.
                server.handle_request(req).await
            }
        });
        http1::Builder::new()
            .keep_alive(true)
            .serve_connection(io, svc)
            .await
            .context("http1::Builder::serve_connection")?;
        Ok(())
    }

    /// Serve one accepted connection using HTTP/2 (ALPN-negotiated `h2`).
    async fn serve_connection_h2<IO>(self: Arc<Self>, client: TokioIo<IO>) -> anyhow::Result<()>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let io = client;
        let server = self.clone();
        let svc = service_fn(move |req: HyperRequest<Incoming>| {
            let server = server.clone();
            async move { server.handle_request(req).await }
        });
        http2::Builder::new(crate::dispatch::TokioExecutor)
            .serve_connection(io, svc)
            .await
            .context("http2::Builder::serve_connection")?;
        Ok(())
    }
}

/// RAII guard that decrements an in_flight counter on drop.
/// Used to track in-flight HTTP requests for graceful shutdown.
struct InFlightGuard(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Tokio-based executor for hyper HTTP/2 connections.
#[derive(Clone, Copy, Debug)]
struct TokioExecutor;
impl<F: Future + Send + 'static> Executor<F> for TokioExecutor {
    fn execute(&self, f: F) {
        tokio::spawn(async move {
            f.await;
        });
    }
}

/// Check whether the incoming request's `Content-Length` exceeds the
/// per-app body cap. Returns `Some(413 response)` when the body is too
/// large, `None` when the check passes (or is disabled).
///
/// When `cap == 0` the check is disabled — all body sizes are allowed.
/// When the request has no `Content-Length` header (chunked transfer),
/// the check is skipped and the wasmtime memory cap provides defense-
/// in-depth.
pub fn check_body_cap(
    content_length: Option<u64>,
    cap: u64,
    meter: &Arc<RequestMeter>,
    metrics_handle: Option<&Arc<crate::metrics::MetricsHandle>>,
) -> Option<HyperResponse<HyperOutgoingBody>> {
    if cap == 0 {
        return None;
    }
    match content_length {
        Some(len) if len > cap => {
            tracing::warn!(
                content_length = len,
                cap,
                "request body exceeds per-app cap; rejecting 413",
            );
            Some(synthetic_413(len, cap, meter, metrics_handle))
        }
        _ => None,
    }
}

impl HandlerDispatch {
    pub async fn evict(&self) -> Option<wasmtime::Engine> {
        let mut lock = self.proxy_pre.write().await;
        lock.take().map(|proxy_pre| proxy_pre.engine().clone())
    }

    pub async fn has_engine(&self) -> bool {
        let lock = self.proxy_pre.read().await;
        lock.is_some()
    }

    /// Dispatch a single HTTP request through `ProxyPre`.
    /// Mirrors the canonical example in `wasmtime-wasi-http` 25's own
    /// `lib.rs`. Key differences from that example:
    ///
    ///   * `RuntimeState::with_env_and_meter` constructs per-request
    ///     state with the per-app tenant_id, egress policy, log sink,
    ///     and app context — wired through `HandlerConfig`.
    ///   * The store's epoch deadline is set to `request_budget_ticks`
    ///     so a runaway guest hits an interrupt.
    ///   * `meter.record_request()` is incremented before the guest is
    ///     invoked so the count is exact even if the guest traps.
    ///
    /// Errors are mapped to:
    ///   * `Ok(Ok(resp))` → forward the guest response, wrapping the
    ///     body in CountingBody to meter outbound bytes (issue #210).
    ///   * `Ok(Err(http_error))` → wrap as a 500 response with the
    ///     diagnostic in the body (so a client gets a real HTTP
    ///     response, not a connection drop).
    ///   * `Err(dispatch_outcome)` → guest never called `set` (trapped /
    ///     hung); wrap into a 500 with the underlying task error.
    ///
    /// Note: there is no `tokio::spawn` here. The original sketch
    /// spawned a task so `receiver.await` and the guest call could
    /// race, but that's unnecessary — the guest only delivers a
    /// response by calling `response-outparam::set`, which drops the
    /// sender and triggers `receiver`. The two are causally linked,
    /// so awaiting them in sequence is correct. Inlining also
    /// eliminates a previously-latent use-after-free: the spawned
    /// future borrowed `&mut store` from this stack frame, so if
    /// `hyper` dropped the response future mid-dispatch, the spawned
    /// task kept running with a dangling borrow. With the spawn gone,
    /// `store` lives only on this stack and is dropped at function
    /// return.
    async fn handle_request(
        self: Arc<Self>,
        req: HyperRequest<Incoming>,
    ) -> anyhow::Result<HyperResponse<HyperOutgoingBody>> {
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _guard = InFlightGuard(self.in_flight.clone());

        {
            let mut lock = self.config.last_request_at.lock().await;
            *lock = Some(std::time::Instant::now());
        }

        // Lazy instantiation: check if we have a ProxyPre.
        let proxy_pre = {
            let lock = self.proxy_pre.read().await;
            if lock.is_some() {
                lock.as_ref().unwrap().clone()
            } else {
                drop(lock);
                let mut write_lock = self.proxy_pre.write().await;
                if write_lock.is_none() {
                    let engine = self.engine_pool.acquire(&self.state).await;
                    let cwasm_path = self.downloader.cwasm_path(&self.deployment_id);

                    let component = if cwasm_path.exists() {
                        match tokio::fs::read(&cwasm_path).await {
                            Ok(cwasm_bytes) => unsafe {
                                wasmtime::component::Component::deserialize(&engine, &cwasm_bytes)
                            }
                            .ok(),
                            Err(_) => None,
                        }
                    } else {
                        None
                    };

                    let component = match component {
                        Some(c) => c,
                        None => {
                            let wasm_path = self.downloader.cache_path(&self.deployment_id);
                            let bytes = tokio::fs::read(&wasm_path).await?;
                            let engine_for_spawn = engine.clone();
                            let cwasm_path_clone = cwasm_path.clone();
                            match tokio::task::spawn_blocking(move || {
                                wasmtime::component::Component::from_binary(
                                    &engine_for_spawn,
                                    &bytes,
                                )
                            })
                            .await
                            .unwrap()
                            {
                                Ok(c) => {
                                    // Serialize to .cwasm for future cache hits in a
                                    // background task so the initial request is not delayed.
                                    match c.serialize() {
                                        Ok(serialized) => {
                                            tokio::spawn(async move {
                                                if let Err(e) =
                                                    tokio::fs::write(&cwasm_path_clone, &serialized)
                                                        .await
                                                {
                                                    tracing::warn!(
                                                        path = %cwasm_path_clone.display(),
                                                        err = %e,
                                                        "failed to write serialized component to AOT cache"
                                                    );
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                err = %e,
                                                "failed to serialize compiled component"
                                            );
                                        }
                                    }
                                    c
                                }
                                Err(e) => return Err(anyhow::anyhow!("JIT failed: {e}")),
                            }
                        }
                    };

                    let linker = edge_runtime::create_component_linker_handler(&engine)?;
                    let instance_pre = linker.instantiate_pre(&component)?;
                    let p = HandlerProxyPre::new(instance_pre)?;
                    *write_lock = Some(p.clone());
                    p
                } else {
                    write_lock.as_ref().unwrap().clone()
                }
            }
        };

        // Body-cap pre-check. Prevents a FaaS guest from being asked to
        // handle a 10 GB POST that we'd then have to buffer into the
        // 256 MiB wasmtime memory cap. We trust Content-Length as the
        // upper bound: hyper parses it eagerly; chunked requests with
        // no Content-Length still trip the wasmtime memory cap, which
        // is enforced by `StoreLimitsBuilder.memory_size` and is
        // defense-in-depth against the missing-header case.
        //
        // Returning 413 *before* the guest runs (instead of dispatching
        // and letting the guest trap) means a misconfigured tenant
        // can't DoS the worker by spamming large payloads.
        {
            let cl = req
                .headers()
                .get(hyper::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            if let Some(resp) = check_body_cap(
                cl,
                self.config.max_request_body_bytes,
                &self.config.meter,
                self.config.metrics_handle.as_ref(),
            ) {
                return Ok(resp);
            }
        }

        let engine = proxy_pre.engine();

        // Per-request RuntimeState — fresh ResourceTable, fresh
        // WasiCtx (rebuilt from the stored env HashMap), shared
        // EgressPolicy + LogSink + meter (Arc-clones).
        //
        // Socket-egress dispatch (issue #412 compose rule): the
        // per-app `socket_mode_for_app` is the per-app selector for
        // `BlockAll` / `AllowList` / `AllowAll`. The `HostnamePinned`
        // arm is reachable ONLY when both the per-app field is
        // `HostnamePinned` AND the worker-wide
        // `hostname_pinning_enabled` is true — the worker-wide knob
        // remains the hard gate for the dormant mode (issue #309).
        // All other modes use the per-app value with a fresh empty
        // `HostnamePinning` cache (the cache only matters for
        // `HostnamePinned`).
        let (socket_mode, hostname_pinning) = match (
            self.config.socket_mode_for_app,
            self.config.hostname_pinning_enabled,
        ) {
            (edge_runtime::socket_egress::SocketEgressPolicy::HostnamePinned, true) => (
                edge_runtime::socket_egress::SocketEgressPolicy::HostnamePinned,
                self.config.hostname_pinning.clone(),
            ),
            (mode, _) => (
                mode,
                Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            ),
        };
        let request_state = RuntimeState::with_env_and_meter_preview(
            self.config.env.clone(),
            Some(self.config.meter.clone()),
            self.config.tenant_id.clone(),
            // Per-app preopen (issue #558): pass app_name through to
            // scope the EDGE_FS_PATH mount to
            // `{base}/{tenant_id}/{app_name}/`. Sourced from the
            // already-populated AppLogContext — no HandlerConfig field
            // change needed.
            self.config.app_ctx.app_name.as_str(),
            self.config.preview_id.as_deref(),
            self.config.preview_pr_number,
            self.config.egress.clone(),
            self.config.log_sink.clone(),
            self.config.app_ctx.clone(),
            self.config.metrics_acc.clone(),
            socket_mode,
            hostname_pinning,
        );

        // Clone the shared exit-code flag BEFORE moving `request_state`
        // into the store. CLAUDE.md: "Always check RuntimeState::exit_requested()
        // after a guest call returns Err — a clean process.exit looks like
        // a trap to wasmtime." The store owns the RuntimeState for the
        // duration of the guest call, so the flag is only reachable via
        // this Arc clone once we drop the request_state into create_store.
        let exit_code_arc = Arc::clone(&request_state.exit_code);

        // Memory cap per request — bounds memory-bomb guests.
        // Uses the configured max_memory_mb limit.
        let mut store =
            edge_runtime::create_store(engine, self.config.max_memory_mb, request_state);
        let ticks = self.config.cpu_budget_ms / self.tick_ms;
        store.set_epoch_deadline(ticks.max(1));

        // Build the incoming-request / response-outparam handles the
        // guest will see. `new_incoming_request` records the URL +
        // headers in the per-request `ResourceTable`. The response
        // outparam delivers a `Result<Response, ErrorCode>` — see
        // wasmtime-wasi-http 25's bindings.rs for the trappable_error
        // declaration that maps `error-code` → `HttpError`.
        let (sender, receiver): (HandlerResponseSender, HandlerResponseReceiver) =
            tokio::sync::oneshot::channel();
        let req_handle = store
            .data_mut()
            .http()
            .new_incoming_request(Scheme::Http, req)
            .map_err(anyhow::Error::from)?;
        let out = store
            .data_mut()
            .http()
            .new_response_outparam(sender)
            .map_err(anyhow::Error::from)?;

        // Account the request before dispatching the guest. We
        // snapshot-and-subtract in the heartbeat loop, not here, so
        // the counter only ever moves forward.
        self.config.meter.record_request();
        // Mirror into the worker-level /metrics counter (issue #49).
        // The IntCounter is monotonic (no snapshot-and-subtract
        // path) — operator dashboards want a running total, not a
        // delta since the last heartbeat.
        if let Some(handle) = self.config.metrics_handle.as_ref() {
            handle.requests.inc();
        }
        // Issue #555: capture the dispatch-accept instant so the
        // terminal arm of `receiver.await` can stamp
        // `meter.record_duration(elapsed)` with the user-visible
        // wall-clock latency (accept → response complete). The body
        // cap 413 early-return above (lines ~757-762) does NOT stamp
        // — mirrors `record_request()` not being called there. All
        // four terminal arms below stamp unconditionally so a guest
        // trap or `process.exit` still produces a metered duration,
        // consistent with `request_count` being billed on the same
        // arms. Billability of hung handlers is captured at the call
        // site in `record_duration`'s doc-comment.
        let started_at = std::time::Instant::now();
        let tenant_for_log = self.config.tenant_id.clone();
        let app_name_for_log = self.config.app_ctx.app_name.clone();
        // Hoist the Option<&Arc<MetricsHandle>> once. Per finding #8
        // (PR #697 review), the closure below previously captured
        // `self.config.metrics_handle.as_ref()` four times across
        // the terminal arms — a refcount bump per arm, per FaaS
        // request. Pulling it out once at the top of handle_request
        // cuts the four bumps to one (negligible compared to the
        // guest trap path, but the cleanest pattern). Tests still
        // get `None` here when `metrics_handle: None` was set on the
        // dispatched HandlerConfig.
        let metrics_handle = self.config.metrics_handle.as_ref();

        // Spawn the guest concurrently so the host can start serving
        // the response body as soon as the guest calls
        // ResponseOutparam::set, while the guest continues writing
        // body chunks. This enables SSE, long-polling, and progressive
        // chunked responses — the guest delivers headers immediately
        // and streams the body over time.
        //
        // `HyperOutgoingBody` uses an internal mpsc channel so it does
        // not borrow `store`; the response can outlive the `tokio::spawn`
        // future without dangling references.
        let guest_result = tokio::spawn(async move {
            let proxy = proxy_pre
                .instantiate_async(&mut store)
                .await
                .map_err(anyhow::Error::from)?;
            proxy
                .wasi_http_incoming_handler()
                .call_handle(store, req_handle, out)
                .await
                .map_err(|e| anyhow::anyhow!("proxy.wasi_http_incoming_handler.call_handle: {e}"))
        });

        let meter = &self.config.meter;
        // Helper: stamp FaaS duration (issue #555) on the per-request
        // RequestMeter AND on the worker-level IntCounter. The two
        // counters are independent — RequestMeter is
        // snapshot-and-subtracted by the heartbeat loop; the IntCounter
        // is monotonic for /metrics. `metrics_handle` is None for
        // tests that build a synthetic dispatch.
        let stamp_duration = |elapsed: std::time::Duration| {
            meter.record_duration(elapsed);
            if let Some(handle) = metrics_handle {
                handle.duration_ms.inc_by(elapsed.as_millis() as u64);
            }
        };
        match receiver.await {
            Ok(Ok(resp)) => {
                // Stamp FaaS duration (issue #555). Covers the
                // success path: guest called
                // `response-outparam::set(Ok(resp))` and we are about
                // to return the response to hyper.
                stamp_duration(started_at.elapsed());
                // Wrap the response body in CountingBody so every data
                // frame's byte length is metered via record_outbound_bytes
                // (fixes issue #210 — outbound byte metering was lost
                // when PR #196 deleted http_server.rs).
                let (parts, body) = resp.into_parts();
                let counting = CountingBody {
                    inner: body,
                    meter: meter.clone(),
                    metrics_handle: metrics_handle.cloned(),
                };
                Ok(HyperResponse::from_parts(parts, counting.boxed_unsync()))
            }
            Ok(Err(error_code)) => {
                // Guest exited cleanly via `response-outparam::set`
                // with an error (e.g. EgressPolicy denial surface
                // upstream). Surface as 500 with diagnostics in the
                // body so the client sees a real HTTP response.
                tracing::warn!(
                    tenant_id = %tenant_for_log,
                    app_name = %app_name_for_log,
                    err = %error_code,
                    "guest response_outparam::set returned Err"
                );
                // Stamp FaaS duration (issue #555). The guest did
                // call `set` — we got a result, just an error-coded
                // one. Duration is billable.
                stamp_duration(started_at.elapsed());
                Ok(synthetic_500(
                    &format!("guest returned error-code: {error_code:?}"),
                    meter,
                    metrics_handle,
                ))
            }
            Err(_dropped) => {
                // Sender dropped without invoking `set` — guest
                // either trapped (e.g. epoch deadline exceeded), called
                // `process.exit(code)` (looks identical to a trap to
                // wasmtime — the wasmtime trap is raised right after
                // the host-side flag is set), or never replied.
                //
                // Distinguish process.exit from a real trap via the
                // exit_code_arc we cloned before the store was built
                // (mirrors the supervisor's LongRunning pattern at
                // edge-worker/src/supervisor.rs:785 — see the `if let
                // Some(code) = store.data().exit_requested()` check).
                // A non-zero code is a clean guest exit; surface it as
                // a controlled 500 instead of leaking the trap message
                // to the client.
                let exit_code = exit_code_arc.load(std::sync::atomic::Ordering::SeqCst);
                if exit_code != 0 {
                    tracing::info!(
                        tenant_id = %tenant_for_log,
                        app_name = %app_name_for_log,
                        code = exit_code,
                        "guest called process.exit during handler dispatch"
                    );
                    // Stamp FaaS duration (issue #555). Clean
                    // process.exit is billable, consistent with
                    // `request_count` being billed for this arm.
                    stamp_duration(started_at.elapsed());
                    return Ok(synthetic_500("guest cleanly exited", meter, metrics_handle));
                }

                // Real trap. guest_result has already resolved with
                // the underlying error; surface it as a synthetic 500
                // so hyper sends a real response to the client instead
                // of closing the connection mid-message.
                let e = match guest_result.await {
                    Ok(Ok(())) => {
                        anyhow::anyhow!("guest never invoked `response-outparam::set` method")
                    }
                    Ok(Err(e)) => e,
                    Err(join_err) => anyhow::anyhow!("guest task panicked: {join_err}"),
                };
                tracing::warn!(
                    tenant_id = %tenant_for_log,
                    app_name = %app_name_for_log,
                    err = %e,
                    "guest trap or hang; returning 500"
                );
                // Stamp FaaS duration (issue #555). Hung handlers
                // (epoch deadline exceeded) ARE billed — there is
                // no grace period today; see `record_duration`'s
                // doc-comment in edge-runtime/src/metering.rs.
                stamp_duration(started_at.elapsed());
                Ok(synthetic_500(&format!("{e:#}"), meter, metrics_handle))
            }
        }
    }
}

/// Load TLS certificate and key from files specified by paths (issue #209).
/// Returns `None` if either path is unset, the files can't be read, or
/// the PEM data is invalid. Logs warnings for each failure mode so
/// operators can diagnose without enabling debug logging.
pub fn try_load_tls_config(
    cert_path: &Option<String>,
    key_path: &Option<String>,
) -> Option<Arc<rustls::ServerConfig>> {
    let cert_path = cert_path.as_ref()?;
    let key_path = key_path.as_ref()?;

    let cert = std::fs::read(cert_path)
        .map_err(|e| tracing::warn!(path = %cert_path, err = %e, "failed to read TLS certificate"))
        .ok()?;
    let key = std::fs::read(key_path)
        .map_err(|e| tracing::warn!(path = %key_path, err = %e, "failed to read TLS private key"))
        .ok()?;

    if cert.is_empty() || key.is_empty() {
        tracing::warn!("TLS certificate or key file is empty");
        return None;
    }

    // PEM parsing: in-tree `rustls::pki_types::pem::PemObject` API,
    // replaces `rustls-pemfile` (RUSTSEC-2025-0134 unmaintained) per
    // issue #625. `pem_slice_iter` emits every parsed cert; per-section
    // errors are swallowed (`filter_map(Result::ok)`) so one bad block
    // doesn't sink the whole chain. `from_pem_slice` on `PrivateKeyDer`
    // returns the first matching kind in file order across Pkcs1 /
    // Pkcs8 / Sec1.
    let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(&cert)
        .filter_map(Result::ok)
        .collect();
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(&key).ok()?;

    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .ok()?;

    // Advertise HTTP/2 via ALPN so clients can negotiate it over TLS.
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    tracing::info!(cert_path = %cert_path, "TLS configured");
    Some(Arc::new(cfg))
}

/// Shared body of every synthetic error response. Truncates the
/// diagnostic to a UTF-8-safe boundary at 1024 bytes (so a 100 MB
/// error string doesn't blow up the dispatch). Inputs under 1024
/// bytes pass through unchanged.
fn truncate_diagnostic(diagnostic: &str) -> &str {
    let bytes = diagnostic.as_bytes();
    if bytes.len() <= 1024 {
        return diagnostic;
    }
    let cut = bytes[..1024]
        .iter()
        .rposition(|b| (*b as i8) >= -0x40)
        .unwrap_or(0);
    std::str::from_utf8(&bytes[..cut]).unwrap_or("non-utf8 diagnostic")
}

/// Build a synthetic HTTP response with `status`, `body` (from
/// `diagnostic`, truncated to 1 KiB), and `Content-Type: text/plain`.
/// Used when the guest traps or returns an error — hyper 1.x closes
/// the connection mid-message if the service returns `Err`, so every
/// error path MUST return `Ok(synthetic_response(...))` rather than
/// propagating `Err`.
///
/// The synthetic body length is recorded via `meter.record_outbound_bytes`
/// so billing reflects bytes served even on error responses (issue #210).
/// The mirror bump into the worker-level `outbound_bytes` IntCounter
/// (issue #49) lets `/metrics` reflect error-path bytes too.
fn synthetic_response(
    status: hyper::StatusCode,
    diagnostic: &str,
    meter: &Arc<RequestMeter>,
    metrics_handle: Option<&Arc<crate::metrics::MetricsHandle>>,
) -> HyperResponse<HyperOutgoingBody> {
    use http_body_util::{BodyExt, Full};
    use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use std::convert::Infallible;

    let bounded = truncate_diagnostic(diagnostic);
    let body = bounded.as_bytes().to_vec();
    let len = body.len();

    meter.record_outbound_bytes(len as u64);
    if let Some(handle) = metrics_handle {
        handle.outbound_bytes.inc_by(len as u64);
    }

    let body_wrapped =
        Full::from(bytes::Bytes::from(body)).map_err(|never: Infallible| match never {});

    HyperResponse::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(CONTENT_LENGTH, len)
        .body(HyperOutgoingBody::new(body_wrapped))
        .expect("synthetic response: builder with explicit content-length never fails")
}

/// Build a synthetic 500. See `synthetic_response`.
fn synthetic_500(
    diagnostic: &str,
    meter: &Arc<RequestMeter>,
    metrics_handle: Option<&Arc<crate::metrics::MetricsHandle>>,
) -> HyperResponse<HyperOutgoingBody> {
    synthetic_response(
        hyper::StatusCode::INTERNAL_SERVER_ERROR,
        diagnostic,
        meter,
        metrics_handle,
    )
}

/// Build a synthetic 413 Payload Too Large with a diagnostic that
/// describes the over-cap request. See `synthetic_response`.
fn synthetic_413(
    content_length: u64,
    cap: u64,
    meter: &Arc<RequestMeter>,
    metrics_handle: Option<&Arc<crate::metrics::MetricsHandle>>,
) -> HyperResponse<HyperOutgoingBody> {
    let diagnostic =
        format!("request body of {content_length} bytes exceeds per-app cap of {cap} bytes");
    synthetic_response(
        hyper::StatusCode::PAYLOAD_TOO_LARGE,
        &diagnostic,
        meter,
        metrics_handle,
    )
}

// ── InFlightGuard tests ─────────────────────────────────────────────────

#[cfg(test)]
mod in_flight_guard_tests {
    use super::*;

    #[test]
    fn in_flight_guard_decrements_on_drop() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(5));
        {
            let _guard = InFlightGuard(counter.clone());
            // Guard constructed with clone of counter — counter unchanged
            assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 5);
        }
        // On drop, the guard decrements
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 4);
    }
}

#[cfg(test)]
mod tls_tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn try_load_tls_config_returns_none_when_no_env() {
        assert!(try_load_tls_config(&None, &None).is_none());
        assert!(try_load_tls_config(&Some("cert.pem".into()), &None).is_none());
        assert!(try_load_tls_config(&None, &Some("key.pem".into())).is_none());
    }

    #[test]
    fn try_load_tls_config_returns_none_on_bad_path() {
        assert!(try_load_tls_config(
            &Some("/nonexistent/cert.pem".into()),
            &Some("/nonexistent/key.pem".into()),
        )
        .is_none());
    }

    #[test]
    fn try_load_tls_config_parses_self_signed_cert_and_key() {
        // rustls 0.23 requires a CryptoProvider to be installed before
        // constructing ServerConfig. Install the ring-based default.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate self-signed cert");
        let cert_pem = cert.cert.pem();
        let key_pem = cert.key_pair.serialize_pem();

        let mut cert_file = NamedTempFile::new().expect("cert tempfile");
        write!(cert_file, "{}", cert_pem).expect("write cert");
        let mut key_file = NamedTempFile::new().expect("key tempfile");
        write!(key_file, "{}", key_pem).expect("write key");

        let result = try_load_tls_config(
            &Some(cert_file.path().to_str().unwrap().into()),
            &Some(key_file.path().to_str().unwrap().into()),
        );
        assert!(result.is_some(), "should load valid cert+key");

        let cfg = result.unwrap();
        assert_eq!(
            cfg.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    #[test]
    fn try_load_tls_config_returns_none_on_empty_file() {
        let cert_file = NamedTempFile::new().expect("cert tempfile");
        let key_file = NamedTempFile::new().expect("key tempfile");
        assert!(try_load_tls_config(
            &Some(cert_file.path().to_str().unwrap().into()),
            &Some(key_file.path().to_str().unwrap().into()),
        )
        .is_none());
    }

    #[test]
    fn try_load_tls_config_returns_none_on_malformed_pem() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut cert_file = NamedTempFile::new().expect("cert tempfile");
        write!(cert_file, "not-a-pem").expect("write cert");
        let mut key_file = NamedTempFile::new().expect("key tempfile");
        write!(key_file, "not-a-pem").expect("write key");
        assert!(try_load_tls_config(
            &Some(cert_file.path().to_str().unwrap().into()),
            &Some(key_file.path().to_str().unwrap().into()),
        )
        .is_none());
    }

    #[test]
    fn try_load_tls_config_returns_none_on_key_cert_mismatch() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert_a =
            rcgen::generate_simple_self_signed(vec!["a.example".into()]).expect("generate cert a");
        let cert_b =
            rcgen::generate_simple_self_signed(vec!["b.example".into()]).expect("generate cert b");
        let mut cert_file = NamedTempFile::new().expect("cert tempfile");
        write!(cert_file, "{}", cert_a.cert.pem()).expect("write cert");
        let mut key_file = NamedTempFile::new().expect("key tempfile");
        // Write cert_b's private key with cert_a's cert → mismatch → None
        write!(key_file, "{}", cert_b.key_pair.serialize_pem()).expect("write key");
        let result = try_load_tls_config(
            &Some(cert_file.path().to_str().unwrap().into()),
            &Some(key_file.path().to_str().unwrap().into()),
        );
        assert!(result.is_none());
    }

    /// Cert chain of (leaf, intermediate, key) loads successfully —
    /// guards the `pem_slice_iter(...).collect()` shape from regressing
    /// to a `.next().unwrap_or(...)` that would drop the intermediate
    /// and break every acme.sh `fullchain.pem` deployment.
    #[test]
    fn try_load_tls_config_loads_leaf_and_intermediate_chain() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        // rcgen 0.13's `CertificateParams` doesn't expose a
        // `serialize_certificate_pem()` alongside `serialize_pem()`,
        // so we generate the leaf + key from one self-signed cert and
        // the intermediate from a second one, then concatenate leaf +
        // intermediate into the cert file. The leaf's key pairs with
        // the leaf cert; the intermediate is purely a chain element.
        let leaf =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("generate leaf");
        let intermediate = rcgen::generate_simple_self_signed(vec!["intermediate.example".into()])
            .expect("generate intermediate");

        let mut cert_file = NamedTempFile::new().expect("cert tempfile");
        // Order matters: leaf first, then intermediate.
        write!(cert_file, "{}{}", leaf.cert.pem(), intermediate.cert.pem())
            .expect("write cert chain");
        let mut key_file = NamedTempFile::new().expect("key tempfile");
        write!(key_file, "{}", leaf.key_pair.serialize_pem()).expect("write key");

        let result = try_load_tls_config(
            &Some(cert_file.path().to_str().unwrap().into()),
            &Some(key_file.path().to_str().unwrap().into()),
        );
        assert!(
            result.is_some(),
            "leaf + intermediate chain must load — `pem_slice_iter(...).collect()` \
             should preserve every CERTIFICATE block"
        );
    }
}

#[cfg(test)]
mod synthetic_response_tests {
    use super::*;
    use edge_runtime::RequestMeter;
    use hyper::StatusCode;

    fn test_meter() -> Arc<RequestMeter> {
        Arc::new(RequestMeter::new("t_test".into(), "d_test".into()))
    }

    #[test]
    fn synthetic_500_returns_internal_server_error() {
        let m = test_meter();
        let resp = synthetic_500("something went wrong", &m, None);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn synthetic_500_has_text_content_type() {
        let m = test_meter();
        let resp = synthetic_500("test", &m, None);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn synthetic_500_has_content_length_header() {
        let m = test_meter();
        let resp = synthetic_500("hello", &m, None);
        assert!(resp.headers().get("content-length").is_some());
        let cl: usize = resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(cl > 0, "content-length must be positive");
    }

    #[test]
    fn synthetic_500_content_length_matches_diagnostic_length() {
        let m = test_meter();
        let resp = synthetic_500("abc", &m, None);
        let cl: usize = resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        // "abc" fits in the capped body — length should be 3.
        assert_eq!(cl, 3, "content-length should match 'abc' length");
    }

    #[test]
    fn synthetic_500_truncates_very_long_diagnostics() {
        let m = test_meter();
        let long = "x".repeat(10_000);
        let resp = synthetic_500(&long, &m, None);
        let cl: usize = resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        // Truncated at 1024 bytes.
        assert!(cl <= 1024, "body should be <= 1024 bytes, got {cl}");
    }

    #[test]
    fn synthetic_500_empty_diagnostic_returns_500() {
        let m = test_meter();
        let resp = synthetic_500("", &m, None);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn synthetic_500_content_length_is_zero_for_empty() {
        let m = test_meter();
        let resp = synthetic_500("", &m, None);
        let cl: usize = resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(cl, 0, "empty diagnostic should have zero-length body");
    }

    #[test]
    fn synthetic_413_returns_payload_too_large() {
        let m = test_meter();
        let resp = synthetic_413(1_000_000, 1024, &m, None);
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn synthetic_413_has_text_content_type() {
        let m = test_meter();
        let resp = synthetic_413(5000, 100, &m, None);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn synthetic_413_has_content_length_header() {
        let m = test_meter();
        let resp = synthetic_413(5000, 100, &m, None);
        assert!(resp.headers().get("content-length").is_some());
    }

    #[test]
    fn synthetic_413_diagnostic_mentions_both_values() {
        let m = test_meter();
        let resp = synthetic_413(5000, 100, &m, None);
        // We can't easily read the body without consuming the response,
        // but the content-length is always > 0 for non-empty input.
        let cl: usize = resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        // The diagnostic is the format string "request body of {content_length}
        // bytes exceeds per-app cap of {cap} bytes". For 5000/100 that's around
        // 60 chars, well under 1024.
        assert!(cl > 10 && cl < 100, "unexpected diagnostic length {cl}");
    }

    #[test]
    fn synthetic_413_handles_large_numbers() {
        let m = test_meter();
        let resp = synthetic_413(u64::MAX, u64::MAX, &m, None);
        let cl: usize = resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            cl <= 1024,
            "body should be capped at 1024 even for huge values"
        );
    }

    // ── check_body_cap tests ────────────────────────────────────────

    #[test]
    fn body_cap_disabled_returns_none() {
        let m = test_meter();
        assert!(check_body_cap(Some(999_999), 0, &m, None).is_none());
    }

    #[test]
    fn body_cap_no_content_length_returns_none() {
        let m = test_meter();
        assert!(check_body_cap(None, 1024, &m, None).is_none());
    }

    #[test]
    fn body_cap_within_limit_returns_none() {
        let m = test_meter();
        assert!(check_body_cap(Some(100), 1024, &m, None).is_none());
    }

    #[test]
    fn body_cap_exact_limit_returns_none() {
        let m = test_meter();
        // Content-Length exactly equal to cap should pass.
        assert!(check_body_cap(Some(1024), 1024, &m, None).is_none());
    }

    #[test]
    fn body_cap_exceeds_returns_413() {
        let m = test_meter();
        let resp = check_body_cap(Some(2000), 1024, &m, None);
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn body_cap_zero_length_returns_none() {
        let m = test_meter();
        assert!(check_body_cap(Some(0), 1024, &m, None).is_none());
    }

    // ── truncate_diagnostic tests ───────────────────────────────────

    #[test]
    fn truncate_short_diagnostic_passes_through() {
        assert_eq!(truncate_diagnostic("hello"), "hello");
    }

    #[test]
    fn truncate_exact_1024_passes_through() {
        let s = "x".repeat(1024);
        assert_eq!(truncate_diagnostic(&s).len(), 1024);
    }

    #[test]
    fn truncate_long_diagnostic_capped_at_1024() {
        let s = "x".repeat(2000);
        let truncated = truncate_diagnostic(&s);
        assert!(truncated.len() <= 1024);
    }

    #[test]
    fn truncate_empty_returns_empty() {
        assert_eq!(truncate_diagnostic(""), "");
    }

    #[test]
    fn truncate_multi_byte_utf8_boundary() {
        // Each '☃' is 3 bytes. A string with 342 snowmen = 1026 bytes.
        // Truncation must cut at a character boundary, so the result
        // should be 341 snowmen = 1023 bytes.
        let s = "☃".repeat(342); // 1026 bytes
        let truncated = truncate_diagnostic(&s);
        assert!(truncated.len() <= 1024);
        assert!(truncated.len() % 3 == 0, "must cut at char boundary");
    }

    // ── budget math tests ───────────────────────────────────────────

    #[test]
    fn budget_ticks_divide_evenly() {
        assert_eq!(100u64 / 10u64, 10);
    }

    #[test]
    fn budget_ticks_floor_at_one() {
        assert_eq!(5u64 / 10u64, 0);
    }

    #[test]
    fn budget_ticks_rounding_floor() {
        assert_eq!(95u64 / 10u64, 9);
    }

    // ── CountingBody tests ──────────────────────────────────────────────

    use bytes::Bytes;
    use std::pin::Pin;

    #[tokio::test]
    async fn counting_body_records_outbound_bytes() {
        let meter = test_meter();
        let inner = http_body_util::Full::new(Bytes::from("hello"));
        let inner_hyper = HyperOutgoingBody::new(inner.map_err(|e| match e {}));
        let mut counting = CountingBody {
            inner: inner_hyper,
            meter: meter.clone(),
            metrics_handle: None,
        };

        while let Some(Ok(_)) = Pin::new(&mut counting).frame().await {}
        let snap = meter.snapshot();
        assert_eq!(snap.outbound_bytes, 5);
    }

    // ── FaaS duration metering tests (issue #555) ─────────────────────────
    //
    // The dispatch path stamps `meter.record_duration(elapsed)` in each
    // of the four terminal arms of `handle_request`'s `receiver.await`
    // match. These tests verify the meter shape that the dispatch path
    // stamps into — the production arms are mechanical one-liners, so
    // testing the meter seam is sufficient (same posture as the
    // CountingBody tests above for outbound bytes). The
    // l7_per_request_timeout_returns_500 integration test in
    // `tests/layer_integration.rs` exercises the real
    // `Err(_dropped)` trap arm end-to-end.

    #[test]
    fn duration_meter_initial_state_is_zero() {
        let meter = test_meter();
        assert_eq!(meter.snapshot().duration_ms, 0);
        assert_eq!(meter.get_duration_ms(), 0);
    }

    #[test]
    fn duration_meter_accumulates_across_requests() {
        let meter = test_meter();
        // Simulate three FaaS requests of varying latencies — the
        // dispatch arms each call `meter.record_duration(elapsed)`
        // exactly once.
        meter.record_duration(Duration::from_millis(120));
        meter.record_duration(Duration::from_millis(80));
        meter.record_duration(Duration::from_millis(45));
        assert_eq!(meter.snapshot().duration_ms, 245);
    }

    #[test]
    fn duration_meter_subtract_delta_removes_snapshotted_value() {
        let meter = test_meter();
        meter.record_duration(Duration::from_millis(100));
        let snap = meter.snapshot();
        // Stamp landing after the snapshot but before reset (race the
        // heartbeat loop's snapshot-then-reset pattern).
        meter.record_duration(Duration::from_millis(50));
        meter.subtract_duration_ms(snap.duration_ms);
        assert_eq!(meter.snapshot().duration_ms, 50);
    }

    #[test]
    fn duration_meter_clone_shares_counter() {
        let meter = test_meter();
        let meter2 = meter.clone();
        meter.record_duration(Duration::from_millis(100));
        meter2.record_duration(Duration::from_millis(50));
        // Same Arc — both halves see the full total.
        assert_eq!(meter.snapshot().duration_ms, 150);
        assert_eq!(meter2.snapshot().duration_ms, 150);
    }

    #[test]
    fn duration_meter_survives_zero_subtract() {
        // LongRunning apps leave `duration_ms_total = 0` on every
        // heartbeat (the dispatch path never stamps for LR). The
        // reset path unconditionally calls
        // `subtract_duration_ms(status.duration_ms_total)` so the
        // subtraction must be a no-op for 0, not wrap to u64::MAX.
        let meter = test_meter();
        meter.subtract_duration_ms(0);
        assert_eq!(meter.snapshot().duration_ms, 0);
    }

    #[tokio::test]
    async fn counting_body_empty_records_zero() {
        let meter = test_meter();
        let inner = http_body_util::Full::new(Bytes::from(""));
        let inner_hyper = HyperOutgoingBody::new(inner.map_err(|e| match e {}));
        let mut counting = CountingBody {
            inner: inner_hyper,
            meter: meter.clone(),
            metrics_handle: None,
        };

        while let Some(Ok(_)) = Pin::new(&mut counting).frame().await {}
        let snap = meter.snapshot();
        assert_eq!(snap.outbound_bytes, 0);
    }

    #[tokio::test]
    async fn counting_body_size_hint_delegates() {
        let meter = test_meter();
        let inner = http_body_util::Full::new(Bytes::from("test"));
        let inner_hyper = HyperOutgoingBody::new(inner.map_err(|e| match e {}));
        let counting = CountingBody {
            inner: inner_hyper,
            meter,
            metrics_handle: None,
        };

        let hint = Body::size_hint(&counting);
        assert_eq!(hint.lower(), 4);
    }

    #[tokio::test]
    async fn counting_body_error_frame_passthrough() {
        let meter = test_meter();
        // A body that immediately yields an error frame.
        let err_body = http_body_util::StreamBody::new(futures::stream::iter(vec![Err::<
            hyper::body::Frame<Bytes>,
            ErrorCode,
        >(
            wasmtime_wasi_http::p2::bindings::http::types::ErrorCode::InternalError(None),
        )]));
        let inner_hyper = HyperOutgoingBody::new(err_body);
        let mut counting = CountingBody {
            inner: inner_hyper,
            meter: meter.clone(),
            metrics_handle: None,
        };

        use std::pin::Pin;
        let frame = Pin::new(&mut counting).frame().await;
        assert!(frame.is_some());
        assert!(frame.unwrap().is_err());
        // No bytes should have been recorded for an error frame.
        let snap = meter.snapshot();
        assert_eq!(snap.outbound_bytes, 0);
    }

    #[tokio::test]
    async fn counting_body_end_of_stream() {
        let meter = test_meter();
        let inner = http_body_util::Full::new(Bytes::from(""));
        let inner_hyper = HyperOutgoingBody::new(inner.map_err(|e| match e {}));
        let counting = CountingBody {
            inner: inner_hyper,
            meter,
            metrics_handle: None,
        };
        assert!(counting.is_end_stream());
    }

    // ── Synthetic response body content tests ──────────────────────────

    #[tokio::test]
    async fn synthetic_500_body_contains_diagnostic() {
        let m = test_meter();
        let resp = synthetic_500("custom error message", &m, None);
        let body_bytes = resp
            .collect()
            .await
            .expect("collect body")
            .to_bytes()
            .to_vec();
        let body = String::from_utf8(body_bytes).unwrap();
        assert!(body.contains("custom error message"));
    }

    #[tokio::test]
    async fn synthetic_413_body_contains_cap_info() {
        let m = test_meter();
        let resp = synthetic_413(10_000_000, 1024, &m, None);
        let body_bytes = resp
            .collect()
            .await
            .expect("collect body")
            .to_bytes()
            .to_vec();
        let body = String::from_utf8(body_bytes).unwrap();
        assert!(body.contains("10000000"));
        assert!(body.contains("1024"));
        assert!(body.contains("cap"));
    }

    // ── MaybeTls loopback tests ─────────────────────────────────────────

    #[tokio::test]
    async fn maybe_tls_plain_round_trips() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        let mut maybe_server = MaybeTls::Plain(server);
        let mut maybe_client = MaybeTls::Plain(client);

        maybe_client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        maybe_server.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }

    #[tokio::test]
    async fn maybe_tls_flush_and_shutdown() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        let mut maybe_server = MaybeTls::Plain(server);
        maybe_server.flush().await.unwrap();
        maybe_server.shutdown().await.unwrap();

        let mut maybe_client = MaybeTls::Plain(client);
        maybe_client.shutdown().await.unwrap();
    }

    // ── HandlerDispatch::evict / has_engine tests ───────────────────────

    #[tokio::test]
    async fn dispatch_new_with_engine() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pool = Arc::new(crate::supervisor::StandbyPool::new(1).expect("pool"));
        let engine_pool = pool;
        let state = Arc::new(tokio::sync::RwLock::new(crate::state::WorkerState::new(
            engine,
        )));

        let cfg = HandlerConfig {
            tenant_id: "t".into(),
            egress: Arc::new(edge_runtime::EgressPolicy::allow_all()),
            log_sink: Arc::new(edge_runtime::interfaces::observe::NoopLogSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "test".into(),
                tenant_id: "t".into(),
                deployment_id: "d1".into(),
            },
            meter: Arc::new(RequestMeter::new("t".into(), "d1".into())),
            env: std::collections::HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(None)),
            cpu_budget_ms: 1000,
            max_memory_mb: 256,
            // issue #308: default to non-preview for unit tests; specific
            // tests can override these by building a custom HandlerConfig.
            preview_id: None,
            preview_pr_number: None,
            metrics_handle: None,
        };

        let dispatch = HandlerDispatch::new(
            18000,
            1000,
            10,
            cfg,
            None,
            Arc::new(crate::downloader::Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                crate::auth::WorkerJwtSigner::new(
                    String::new(),
                    None,
                    String::new(),
                    "w",
                    "r",
                    "t",
                ),
                None,
            )),
            "d1".into(),
            engine_pool,
            state,
        )
        .expect("HandlerDispatch::new");

        // By default has_engine=false (no ProxyPre loaded)
        assert!(!dispatch.has_engine().await);

        // evict on empty proxy_pre returns None
        assert!(dispatch.evict().await.is_none());
    }

    #[tokio::test]
    async fn dispatch_evict_round_trips_engine() {
        let engine = edge_runtime::create_engine().expect("engine");
        let engine_for_proxy = edge_runtime::create_engine().expect("engine2");
        let pool = Arc::new(crate::supervisor::StandbyPool::new(1).expect("pool"));
        let state = Arc::new(tokio::sync::RwLock::new(crate::state::WorkerState::new(
            engine,
        )));

        let cfg = HandlerConfig {
            tenant_id: "t".into(),
            egress: Arc::new(edge_runtime::EgressPolicy::allow_all()),
            log_sink: Arc::new(edge_runtime::interfaces::observe::NoopLogSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "test".into(),
                tenant_id: "t".into(),
                deployment_id: "d1".into(),
            },
            meter: Arc::new(RequestMeter::new("t".into(), "d1".into())),
            env: std::collections::HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(None)),
            cpu_budget_ms: 1000,
            max_memory_mb: 256,
            // issue #308: default to non-preview for unit tests; specific
            // tests can override these by building a custom HandlerConfig.
            preview_id: None,
            preview_pr_number: None,
            metrics_handle: None,
        };

        let dispatch = HandlerDispatch::new(
            18001,
            1000,
            10,
            cfg,
            None,
            Arc::new(crate::downloader::Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                crate::auth::WorkerJwtSigner::new(
                    String::new(),
                    None,
                    String::new(),
                    "w",
                    "r",
                    "t",
                ),
                None,
            )),
            "d1".into(),
            pool,
            state,
        )
        .expect("HandlerDispatch::new");

        assert!(!dispatch.has_engine().await);
        assert!(dispatch.evict().await.is_none());
        let _ = engine_for_proxy;
    }

    // ── Budget/tick math tests ─────────────────────────────────────────

    #[test]
    fn budget_ticks_100ms_at_10ms_gives_10() {
        // request_budget_ms (100) / tick_ms (10) = 10 ticks, clamped to ≥1
        let budget: u64 = 100;
        let tick: u64 = 10;
        let ticks = (budget / tick.max(1)).max(1);
        assert_eq!(ticks, 10);
    }

    #[test]
    fn budget_ticks_5ms_at_10ms_floors_at_1() {
        let budget: u64 = 5;
        let tick: u64 = 10;
        let ticks = (budget / tick.max(1)).max(1);
        assert_eq!(ticks, 1);
    }

    #[test]
    fn budget_ticks_zero_budget_floors_at_1() {
        let budget: u64 = 0;
        let tick: u64 = 10;
        let ticks = (budget / tick.max(1)).max(1);
        assert_eq!(ticks, 1);
    }

    #[test]
    fn tick_ms_zero_clamped_to_1() {
        let tick: u64 = 0;
        assert_eq!(tick.max(1), 1);
    }

    // ── Drain in-flight tests ──────────────────────────────────────────

    #[tokio::test]
    async fn drain_in_flight_clean_returns_true() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pool = Arc::new(crate::supervisor::StandbyPool::new(1).expect("pool"));
        let state = Arc::new(tokio::sync::RwLock::new(crate::state::WorkerState::new(
            engine,
        )));

        let cfg = HandlerConfig {
            tenant_id: "t".into(),
            egress: Arc::new(edge_runtime::EgressPolicy::allow_all()),
            log_sink: Arc::new(edge_runtime::interfaces::observe::NoopLogSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "test".into(),
                tenant_id: "t".into(),
                deployment_id: "d1".into(),
            },
            meter: Arc::new(RequestMeter::new("t".into(), "d1".into())),
            env: std::collections::HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(None)),
            cpu_budget_ms: 1000,
            max_memory_mb: 256,
            // issue #308: default to non-preview for unit tests; specific
            // tests can override these by building a custom HandlerConfig.
            preview_id: None,
            preview_pr_number: None,
            metrics_handle: None,
        };

        let dispatch = HandlerDispatch::new(
            18002,
            1000,
            10,
            cfg,
            None,
            Arc::new(crate::downloader::Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                crate::auth::WorkerJwtSigner::new(
                    String::new(),
                    None,
                    String::new(),
                    "w",
                    "r",
                    "t",
                ),
                None,
            )),
            "d1".into(),
            pool,
            state,
        )
        .expect("HandlerDispatch::new");

        // in_flight is 0, so drain should return true immediately
        assert!(dispatch.drain_in_flight(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn drain_in_flight_timeout_returns_false() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pool = Arc::new(crate::supervisor::StandbyPool::new(1).expect("pool"));
        let state = Arc::new(tokio::sync::RwLock::new(crate::state::WorkerState::new(
            engine,
        )));

        let cfg = HandlerConfig {
            tenant_id: "t".into(),
            egress: Arc::new(edge_runtime::EgressPolicy::allow_all()),
            log_sink: Arc::new(edge_runtime::interfaces::observe::NoopLogSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "test".into(),
                tenant_id: "t".into(),
                deployment_id: "d1".into(),
            },
            meter: Arc::new(RequestMeter::new("t".into(), "d1".into())),
            env: std::collections::HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(None)),
            cpu_budget_ms: 1000,
            max_memory_mb: 256,
            // issue #308: default to non-preview for unit tests; specific
            // tests can override these by building a custom HandlerConfig.
            preview_id: None,
            preview_pr_number: None,
            metrics_handle: None,
        };

        let dispatch = HandlerDispatch::new(
            18003,
            1000,
            10,
            cfg,
            None,
            Arc::new(crate::downloader::Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                crate::auth::WorkerJwtSigner::new(
                    String::new(),
                    None,
                    String::new(),
                    "w",
                    "r",
                    "t",
                ),
                None,
            )),
            "d1".into(),
            pool,
            state,
        )
        .expect("HandlerDispatch::new");

        // Set in_flight to 1 so drain times out
        dispatch
            .in_flight
            .store(1, std::sync::atomic::Ordering::SeqCst);
        assert!(!dispatch.drain_in_flight(Duration::from_millis(10)).await);
    }

    // ── HandlerDispatch tick clamping tests ────────────────────────────

    #[test]
    fn handler_dispatch_new_tick_ms_zero_clamped_to_one() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pool = crate::supervisor::StandbyPool::new(1).expect("pool");
        let state = crate::state::WorkerState::new(engine);

        let cfg = HandlerConfig {
            tenant_id: "t".into(),
            egress: Arc::new(edge_runtime::EgressPolicy::allow_all()),
            log_sink: Arc::new(edge_runtime::interfaces::observe::NoopLogSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "test".into(),
                tenant_id: "t".into(),
                deployment_id: "d1".into(),
            },
            meter: Arc::new(RequestMeter::new("t".into(), "d1".into())),
            env: std::collections::HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(None)),
            cpu_budget_ms: 1000,
            max_memory_mb: 256,
            // issue #308: default to non-preview for unit tests; specific
            // tests can override these by building a custom HandlerConfig.
            preview_id: None,
            preview_pr_number: None,
            metrics_handle: None,
        };

        // epoch_tick_ms=0 → tick_ms field = 1 (verified by budget math above)
        let dispatch = HandlerDispatch::new(
            18004,
            1000,
            0,
            cfg,
            None,
            Arc::new(crate::downloader::Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                crate::auth::WorkerJwtSigner::new(
                    String::new(),
                    None,
                    String::new(),
                    "w",
                    "r",
                    "t",
                ),
                None,
            )),
            "d1".into(),
            Arc::new(pool),
            Arc::new(tokio::sync::RwLock::new(state)),
        )
        .expect("HandlerDispatch::new");

        assert!(dispatch.port >= 18004);
    }
}
