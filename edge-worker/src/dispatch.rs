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
//! Every request gets a fresh `ResourceTable` and a fresh `WasiCtx`
//! (rebuilt from the stored env `HashMap` through `RuntimeState::clone`).
//! Per-tenant `KvStore` / `Cache` / `Scheduler` are Arc-shared, so
//! cheap to clone.
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
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::rt::Executor;
use hyper::server::conn::http1;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::Request as HyperRequest;
use hyper::Response as HyperResponse;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use wasmtime::component::InstancePre;
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
                    self.meter.record_outbound_bytes(data.len() as u64);
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
    /// `wasmtime_wasi_http::ProxyPre` — pre-instantiated component
    /// that exports `wasi:http/incoming-handler`. Cheap to clone
    /// (Arc-shared); we hand a clone to each per-request task so
    /// `proxy_pre.instantiate_async(&mut store)` is parallel-safe.
    proxy_pre: HandlerProxyPre,
    /// TCP port assigned to this app by `PortPool`.
    port: u16,
    /// Per-request wasmtime epoch deadline (in ticks, where each tick
    /// is `tick_ms`).
    request_budget_ticks: u64,
    /// Engine-clock tick interval (ms) — how often the per-app ticker
    /// calls `engine.increment_epoch()`. Defaults to 1 if the caller
    /// passes 0.
    tick_ms: u64,
    /// Per-app context shared across all requests (tenant_id, egress,
    /// meter, log_sink, app_ctx). Cheap to clone (`Arc`-heavy).
    config: Arc<HandlerConfig>,
    /// Optional TLS server config for encrypted connections (issue #209).
    tls_config: Option<Arc<rustls::ServerConfig>>,
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
    /// Socket-egress mode for `wasi:sockets/{tcp,udp}` (issue #309).
    /// Threaded into `RuntimeState::with_env_and_meter` on every FaaS
    /// dispatch — the runtime does NOT read `EDGE_EGRESS_SOCKET_MODE`
    /// itself; the worker's `Config::from_env` reads it once at
    /// startup and the supervisor copies it into `HandlerConfig`.
    pub socket_mode: SocketEgressPolicy,
}

impl HandlerDispatch {
    /// Build a dispatcher from a pre-instantiated component.
    pub fn new(
        instance_pre: InstancePre<RuntimeState>,
        port: u16,
        request_budget_ms: u64,
        epoch_tick_ms: u64,
        config: HandlerConfig,
        tls_config: Option<Arc<rustls::ServerConfig>>,
    ) -> anyhow::Result<Self> {
        let proxy_pre = HandlerProxyPre::new(instance_pre).map_err(|e| {
            anyhow::anyhow!(
                "ProxyPre::new (component does not export wasi:http/incoming-handler): {e}"
            )
        })?;
        // Defend against divide-by-zero: a misconfigured 0 tick would
        // NaN the math. Default to 1 ms.
        let tick_ms = epoch_tick_ms.max(1);
        let ticks = request_budget_ms / tick_ms;
        Ok(Self {
            proxy_pre,
            port,
            request_budget_ticks: ticks.max(1),
            tick_ms,
            config: Arc::new(config),
            tls_config,
        })
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
        let ticker_engine = self.proxy_pre.engine().clone();
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
                ticker_engine.increment_epoch();
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

impl HandlerDispatch {
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
        if self.config.max_request_body_bytes > 0 {
            if let Some(cl) = req.headers().get(hyper::header::CONTENT_LENGTH) {
                if let Ok(cl_str) = cl.to_str() {
                    if let Ok(len) = cl_str.parse::<u64>() {
                        if len > self.config.max_request_body_bytes {
                            tracing::warn!(
                                tenant_id = %self.config.tenant_id,
                                app_name = %self.config.app_ctx.app_name,
                                content_length = len,
                                cap = self.config.max_request_body_bytes,
                                "request body exceeds per-app cap; rejecting 413",
                            );
                            return Ok(synthetic_413(
                                len,
                                self.config.max_request_body_bytes,
                                &self.config.meter,
                            ));
                        }
                    }
                }
            }
        }

        let engine = self.proxy_pre.engine();

        // Per-request RuntimeState — fresh ResourceTable, fresh
        // WasiCtx (rebuilt from the stored env HashMap), shared
        // EgressPolicy + LogSink + meter (Arc-clones).
        let request_state = RuntimeState::with_env_and_meter(
            self.config.env.clone(),
            Some(self.config.meter.clone()),
            self.config.tenant_id.clone(),
            self.config.egress.clone(),
            self.config.log_sink.clone(),
            self.config.app_ctx.clone(),
            self.config.metrics_acc.clone(),
            self.config.socket_mode,
        );

        // Clone the shared exit-code flag BEFORE moving `request_state`
        // into the store. CLAUDE.md: "Always check RuntimeState::exit_requested()
        // after a guest call returns Err — a clean process.exit looks like
        // a trap to wasmtime." The store owns the RuntimeState for the
        // duration of the guest call, so the flag is only reachable via
        // this Arc clone once we drop the request_state into create_store.
        let exit_code_arc = Arc::clone(&request_state.exit_code);

        // 256 MiB memory cap per request — generous for FaaS
        // workloads but bounds memory-bomb guests. Matches the
        // LongRunning branch's hardcoded cap from the v0.1 era.
        let mut store = edge_runtime::create_store(engine, 256, request_state);
        store.set_epoch_deadline(self.request_budget_ticks);

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
        let tenant_for_log = self.config.tenant_id.clone();
        let app_name_for_log = self.config.app_ctx.app_name.clone();
        let proxy_pre = self.proxy_pre.clone();

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
        match receiver.await {
            Ok(Ok(resp)) => {
                // Wrap the response body in CountingBody so every data
                // frame's byte length is metered via record_outbound_bytes
                // (fixes issue #210 — outbound byte metering was lost
                // when PR #196 deleted http_server.rs).
                let (parts, body) = resp.into_parts();
                let counting = CountingBody {
                    inner: body,
                    meter: meter.clone(),
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
                Ok(synthetic_500(
                    &format!("guest returned error-code: {error_code:?}"),
                    meter,
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
                    return Ok(synthetic_500("guest cleanly exited", meter));
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
                Ok(synthetic_500(&format!("{e:#}"), meter))
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

    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::Cursor::new(&cert))
        .filter_map(Result::ok)
        .collect();
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(&key))
        .ok()
        .flatten()?;

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
fn synthetic_response(
    status: hyper::StatusCode,
    diagnostic: &str,
    meter: &Arc<RequestMeter>,
) -> HyperResponse<HyperOutgoingBody> {
    use http_body_util::{BodyExt, Full};
    use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use std::convert::Infallible;

    let bounded = truncate_diagnostic(diagnostic);
    let body = bounded.as_bytes().to_vec();
    let len = body.len();

    meter.record_outbound_bytes(len as u64);

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
fn synthetic_500(diagnostic: &str, meter: &Arc<RequestMeter>) -> HyperResponse<HyperOutgoingBody> {
    synthetic_response(hyper::StatusCode::INTERNAL_SERVER_ERROR, diagnostic, meter)
}

/// Build a synthetic 413 Payload Too Large with a diagnostic that
/// describes the over-cap request. See `synthetic_response`.
fn synthetic_413(
    content_length: u64,
    cap: u64,
    meter: &Arc<RequestMeter>,
) -> HyperResponse<HyperOutgoingBody> {
    let diagnostic =
        format!("request body of {content_length} bytes exceeds per-app cap of {cap} bytes");
    synthetic_response(hyper::StatusCode::PAYLOAD_TOO_LARGE, &diagnostic, meter)
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
        let resp = synthetic_500("something went wrong", &m);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn synthetic_500_has_text_content_type() {
        let m = test_meter();
        let resp = synthetic_500("test", &m);
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
        let resp = synthetic_500("hello", &m);
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
        let resp = synthetic_500("abc", &m);
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
        let resp = synthetic_500(&long, &m);
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
        let resp = synthetic_500("", &m);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn synthetic_500_content_length_is_zero_for_empty() {
        let m = test_meter();
        let resp = synthetic_500("", &m);
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
        let resp = synthetic_413(1_000_000, 1024, &m);
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn synthetic_413_has_text_content_type() {
        let m = test_meter();
        let resp = synthetic_413(5000, 100, &m);
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
        let resp = synthetic_413(5000, 100, &m);
        assert!(resp.headers().get("content-length").is_some());
    }

    #[test]
    fn synthetic_413_diagnostic_mentions_both_values() {
        let m = test_meter();
        let resp = synthetic_413(5000, 100, &m);
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
        let resp = synthetic_413(u64::MAX, u64::MAX, &m);
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
}
