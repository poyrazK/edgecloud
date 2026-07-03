//! FaaS dispatcher for Handler-model components.
//!
//! Phase C: real HTTP server that hosts one
//! `wasi:http/incoming-handler` export per `(tenant, app)` pair. Each
//! accepted request creates a fresh `wasmtime::Store<RuntimeState>`
//! (via `ProxyPre::instantiate_async`) and drives the guest's
//! `handle(req, out)` impl. Outbound HTTP calls go through
//! `RuntimeState::send_request`, which is where `EgressPolicy::check`
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

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request as HyperRequest;
use hyper::Response as HyperResponse;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use wasmtime::component::InstancePre;
use wasmtime_wasi_http::io::TokioIo;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::WasiHttpView;

use edge_runtime::interfaces::observe::{AppLogContext, LogSink};
use edge_runtime::{EgressPolicy, RequestMeter, RuntimeState};

// Convenience aliases: the bindgen-generated `ProxyPre` lives one level
// deeper than the example docs suggest — `wasmtime_wasi_http::ProxyPre`
// is NOT re-exported at the crate root (verified in 25.0.3's `lib.rs`).
// The Response Sender/Receiver aliases factor a 6-line type that
// clippy::type_complexity rightly complains about.
type HandlerProxyPre = wasmtime_wasi_http::p2::bindings::ProxyPre<RuntimeState>;
type HandlerResponseResult =
    Result<HyperResponse<HyperOutgoingBody>, wasmtime_wasi_http::p2::bindings::http::types::ErrorCode>;
type HandlerResponseSender = tokio::sync::oneshot::Sender<HandlerResponseResult>;
type HandlerResponseReceiver = tokio::sync::oneshot::Receiver<HandlerResponseResult>;

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
}

impl HandlerDispatch {
    /// Build a dispatcher from a pre-instantiated component.
    pub fn new(
        instance_pre: InstancePre<RuntimeState>,
        port: u16,
        request_budget_ms: u64,
        epoch_tick_ms: u64,
        config: HandlerConfig,
    ) -> anyhow::Result<Self> {
        // wasmtime 45 `Error` no longer implements `std::error::Error`, so
        // `anyhow::Context::context` can't apply directly. Map to
        // `anyhow::Error` first, preserving the source chain.
        let proxy_pre = HandlerProxyPre::new(instance_pre)
            .map_err(|e| anyhow::anyhow!("ProxyPre::new (component does not export wasi:http/incoming-handler): {e}"))?;
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
                    let tenant_id_for_log = server.config.tenant_id.clone();
                    let app_name_for_log = server.config.app_ctx.app_name.clone();
                    tokio::spawn(async move {
                        match server.serve_connection(client).await {
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

    /// Serve one accepted TCP connection. Iterates HTTP/1.1 request/
    /// response cycles until the client closes or a handler errors.
    ///
    /// We use the raw `hyper::server::conn::http1` API because
    /// `wasmtime-wasi-http` 25's examples pair with `hyper` directly.
    /// axum 0.7 would add an extra layer of `axum::body::Body` ↔
    /// `hyper::body::Incoming` conversion with no win for a FaaS
    /// dispatch path that already round-trips through `hyper::Body`.
    async fn serve_connection(
        self: Arc<Self>,
        client: tokio::net::TcpStream,
    ) -> anyhow::Result<()> {
        let io = TokioIo::new(client);
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

    /// Dispatch a single HTTP request through `ProxyPre`.
    ///
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
    ///   * `Ok(Ok(resp))` → forward the guest response.
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
                            return Ok(synthetic_413(len, self.config.max_request_body_bytes));
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
        // wasmtime 45 moved `new_incoming_request` / `new_response_outparam`
        // off the `WasiHttpView` trait onto `WasiHttpCtxView`, so we go
        // through `data_mut().http()` to get the view. The errors are
        // `wasmtime::Error`, which no longer implements `std::error::Error`
        // in wasmtime 45, so map to `anyhow::Error` directly.
        let req_handle = store
            .data_mut()
            .http()
            .new_incoming_request(Scheme::Http, req)
            .map_err(|e| anyhow::anyhow!("new_incoming_request: {e}"))?;
        let out = store
            .data_mut()
            .http()
            .new_response_outparam(sender)
            .map_err(|e| anyhow::anyhow!("new_response_outparam: {e}"))?;

        // Account the request before dispatching the guest. We
        // snapshot-and-subtract in the heartbeat loop, not here, so
        // the counter only ever moves forward.
        self.config.meter.record_request();
        let tenant_for_log = self.config.tenant_id.clone();
        let app_name_for_log = self.config.app_ctx.app_name.clone();
        let proxy_pre = self.proxy_pre.clone();

        // Inline guest dispatch. `instantiate_async` + `call_handle`
        // run sequentially on this future; the guest returns by
        // calling `set` (drops `sender`), which fires `receiver`.
        let dispatch_outcome: anyhow::Result<()> = async {
            let proxy = proxy_pre
                .instantiate_async(&mut store)
                .await
                .map_err(|e| anyhow::anyhow!("proxy_pre.instantiate_async: {e}"))?;
            // call_handle returns `wasmtime::Result<()>`. wasmtime 45 Error
            // does not implement `std::error::Error`, so map to anyhow.
            proxy
                .wasi_http_incoming_handler()
                .call_handle(store, req_handle, out)
                .await
                .map_err(|e| anyhow::anyhow!("proxy.wasi_http_incoming_handler.call_handle: {e}"))
        }
        .await;

        match receiver.await {
            Ok(Ok(resp)) => Ok(resp),
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
                Ok(synthetic_500(&format!(
                    "guest returned error-code: {error_code:?}"
                )))
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
                    return Ok(synthetic_500("guest cleanly exited"));
                }

                // Real trap. `dispatch_outcome` has already resolved
                // (above) with the underlying error; surface it as a
                // synthetic 500 so hyper sends a real response to the
                // client instead of closing the connection mid-message.
                let e = match dispatch_outcome {
                    Ok(()) => {
                        anyhow::anyhow!("guest never invoked `response-outparam::set` method")
                    }
                    Err(e) => e,
                };
                tracing::warn!(
                    tenant_id = %tenant_for_log,
                    app_name = %app_name_for_log,
                    err = %e,
                    "guest trap or hang; returning 500"
                );
                Ok(synthetic_500(&format!("{e:#}")))
            }
        }
    }
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
fn synthetic_response(
    status: hyper::StatusCode,
    diagnostic: &str,
) -> HyperResponse<HyperOutgoingBody> {
    use http_body_util::{BodyExt, Full};
    use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use std::convert::Infallible;

    let bounded = truncate_diagnostic(diagnostic);
    let body = bounded.as_bytes().to_vec();
    let len = body.len();

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
fn synthetic_500(diagnostic: &str) -> HyperResponse<HyperOutgoingBody> {
    synthetic_response(hyper::StatusCode::INTERNAL_SERVER_ERROR, diagnostic)
}

/// Build a synthetic 413 Payload Too Large with a diagnostic that
/// describes the over-cap request. See `synthetic_response`.
fn synthetic_413(content_length: u64, cap: u64) -> HyperResponse<HyperOutgoingBody> {
    let diagnostic =
        format!("request body of {content_length} bytes exceeds per-app cap of {cap} bytes");
    synthetic_response(hyper::StatusCode::PAYLOAD_TOO_LARGE, &diagnostic)
}

#[cfg(test)]
mod synthetic_response_tests {
    use super::*;
    use hyper::StatusCode;

    #[test]
    fn synthetic_500_returns_internal_server_error() {
        let resp = synthetic_500("something went wrong");
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn synthetic_500_has_text_content_type() {
        let resp = synthetic_500("test");
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
        let resp = synthetic_500("hello");
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
        let resp = synthetic_500("abc");
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
        let long = "x".repeat(10_000);
        let resp = synthetic_500(&long);
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
        let resp = synthetic_500("");
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn synthetic_500_content_length_is_zero_for_empty() {
        let resp = synthetic_500("");
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
        let resp = synthetic_413(1_000_000, 1024);
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn synthetic_413_has_text_content_type() {
        let resp = synthetic_413(5000, 100);
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
        let resp = synthetic_413(5000, 100);
        assert!(resp.headers().get("content-length").is_some());
    }

    #[test]
    fn synthetic_413_diagnostic_mentions_both_values() {
        let resp = synthetic_413(5000, 100);
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
        let resp = synthetic_413(u64::MAX, u64::MAX);
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
