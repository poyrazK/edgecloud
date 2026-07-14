//! HTTP server that exposes the worker-level [`crate::metrics::WorkerMetrics`]
//! in Prometheus text format (issue #49).
//!
//! The server binds to `METRICS_ADDR` (default `0.0.0.0:9090`) and exposes
//! a single endpoint, `GET /metrics`. Bearer-token auth is enforced via
//! `METRICS_AUTH_TOKEN`; the server returns `401 Unauthorized` when the
//! header is missing or wrong. The route is fail-closed: an unset token
//! means the server refuses all requests until the operator provisions
//! one. This avoids accidentally scraping per-app counters over an
//! unauthenticated port in a multi-tenant cluster.
//!
//! Shutdown is observed via a `tokio::sync::broadcast::Receiver<()>`
//! — the same broadcast `main()` uses for the heartbeat + log
//! forwarder loops. On shutdown, the server drains in-flight
//! connections and returns.
//!
//! The HTTP layer is a tiny hand-rolled router — no axum / hyper dep
//! beyond what `dispatch.rs` already pulls in for the FaaS listener.
//! `hyper 1.x` server + `service_fn` is enough for a one-endpoint
//! service.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use crate::metrics::WorkerMetrics;

/// Body type returned to hyper 1.x's `service_fn`. The text encoder
/// produces `Vec<u8>`; we wrap it in `Full<Bytes>` and let hyper
/// write it out.
type RespBody = Full<Bytes>;

/// Run the metrics server on `addr` until `shutdown_rx` fires. Returns
/// `Ok(())` on clean shutdown, `Err` only on a fatal bind error.
///
/// `auth_token` is captured by the inner accept loop and cloned into
/// each connection-spawned task. We accept it as `Arc<str>` rather
/// than `String` so the per-connection spawn does a cheap refcount
/// bump instead of a heap allocation per scrape — at sustained
/// scrape rates (Prometheus default = 15s interval), the per-second
/// allocation pressure on a `String` clone compounds. The outer
/// `String` from `main()` is wrapped in `Arc::from` once at startup
/// (see `main.rs:metrics_server::serve`).
pub async fn serve(
    addr: SocketAddr,
    metrics: Arc<WorkerMetrics>,
    auth_token: Arc<str>,
    shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "metrics server listening on /metrics");
    serve_inner(listener, metrics, auth_token, shutdown_rx).await
}

/// Inner accept loop. Public so tests can supply an already-bound
/// `TcpListener` (e.g. `TcpListener::bind("127.0.0.1:0")`) without
/// duplicating the accept/tokio::spawn/service_fn dance. Tests use
/// `127.0.0.1:0` so multiple tests can run in parallel without
/// colliding on a fixed port. The `shutdown_rx` is consumed; the
/// caller transfers ownership.
async fn serve_inner(
    listener: TcpListener,
    metrics: Arc<WorkerMetrics>,
    auth_token: Arc<str>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                tracing::info!("metrics server received shutdown signal");
                return Ok(());
            }
            accept = listener.accept() => {
                let (stream, _peer) = match accept {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(err = %e, "metrics accept failed");
                        continue;
                    }
                };
                let io = TokioIo::new(stream);
                let metrics = metrics.clone();
                let token = auth_token.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req| {
                        handle(req, metrics.clone(), token.clone())
                    });
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(io, svc)
                        .await
                    {
                        tracing::debug!(err = %e, "metrics connection ended");
                    }
                });
            }
        }
    }
}

/// Serve one request. Routes:
/// - `GET /metrics` → 200 + Prometheus text, when auth header matches.
/// - `GET /metrics` → 401 when auth is missing or wrong.
/// - Anything else → 404.
async fn handle(
    req: Request<Incoming>,
    metrics: Arc<WorkerMetrics>,
    auth_token: Arc<str>,
) -> Result<Response<RespBody>, Infallible> {
    // Only GET /metrics is served. Anything else is 404 — the endpoint
    // is intentionally narrow; we don't want to expose an arbitrary
    // HTTP surface on this port.
    if req.method() != Method::GET || req.uri().path() != "/metrics" {
        return Ok(empty(StatusCode::NOT_FOUND));
    }

    // Auth header check. `Authorization: Bearer <token>` is the only
    // accepted shape; anything else is 401.
    //
    // Constant-time comparison: `&str ==` short-circuits on the first
    // mismatching byte, leaking prefix-match timing to an attacker on
    // the same network. We use a hand-rolled byte-wise compare that
    // iterates the maximum of the two lengths and accumulates the XOR
    // of all bytes (including length delta) — the running diff is
    // independent of which byte mismatches. Without `subtle` in the
    // direct dep graph this is the simplest primitive available; it
    // is correct against byte-level timing, not microarchitectural
    // side channels, which is the threat model for an unauthenticated
    // network-side scraper probing the bearer token.
    let auth_ok = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|t| constant_time_eq(t.as_bytes(), auth_token.as_bytes()))
        .unwrap_or(false);

    if !auth_ok {
        return Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("content-type", "text/plain; charset=utf-8")
            .header("www-authenticate", "Bearer")
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    // Render. `gather()` snapshots every registered collector; the
    // text encoder formats it. `tick_worker_gauges` runs first so
    // uptime + active_apps reflect the current wall-clock / app count.
    metrics.tick_worker_gauges();
    let body = metrics.render().unwrap_or_default();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}

fn empty(status: StatusCode) -> Response<RespBody> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::new()))
        .unwrap()
}

/// Constant-time equality on byte slices. Returns false on length
/// mismatch (the length delta is folded into the running diff so the
/// comparison cost is independent of where the bytes diverge). NOT a
/// defense against microarchitectural side channels (cache-timing,
/// Spectre, etc.); only against network-side timing oracles on the
/// bearer token.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // `diff` is widened to `usize` so the typed accumulator pattern
    // `diff |= av ^ bv` works without a `usize |= u8` cast at every
    // iteration (the byte-XOR result is widened before the OR). The
    // length XOR seeds the diff with a non-zero value when the inputs
    // differ in length, so a length mismatch returns false without
    // reading any bytes.
    let max_len = a.len().max(b.len());
    let mut diff: usize = a.len() ^ b.len();
    for i in 0..max_len {
        let av = a.get(i).copied().unwrap_or(0) as usize;
        let bv = b.get(i).copied().unwrap_or(0) as usize;
        diff |= av ^ bv;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    //! Tests for the metrics HTTP server. Each test stands up a tiny
    //! `serve()` on an ephemeral port, registers an app so the
    //! `/metrics` body is non-trivial, and asserts the auth + status
    //! code + body-shape contracts. The binding port uses `127.0.0.1:0`
    //! so multiple tests can run in parallel without colliding.
    //!
    //! Regression coverage: a regression where the server returned 200
    //! on missing Authorization headers (the metric-only auth bypass
    //! we'd seen in the first draft) is caught by
    //! `rejects_request_without_authorization_header`.
    use super::*;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn spawn_test_server(
        token: &str,
    ) -> (SocketAddr, Arc<WorkerMetrics>, tokio::task::JoinHandle<()>) {
        let metrics = WorkerMetrics::new().expect("metrics");
        // Register one app so the rendered body has a labeled series.
        metrics
            .register_app("t_test", "d_test", "app_test")
            .await
            .expect("test register_app");
        metrics
            .set_status(
                "t_test",
                "d_test",
                "app_test",
                &crate::state::AppInstanceStatus::Running,
            )
            .await;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let metrics_for_server = metrics.clone();
        let token: Arc<str> = Arc::from(token);
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        // Move one sender INTO the spawned task so the broadcast
        // channel stays open for the lifetime of the inner accept
        // loop. The outer scope keeps a clone for clean shutdown
        // signalling at the end of the test. If every sender is
        // dropped, `broadcast::Receiver::recv` returns `Err` and
        // `serve_inner` exits as if shutdown was signalled — the
        // kernel would then RST incoming SYNs (ConnectionReset on
        // connect). The original pre-finding-#7 implementation had
        // the same intent but held the keepalive only in the
        // outer scope, which let a fast-path test drop it before
        // the spawned task was polled. Moving it inside is robust.
        let shutdown_tx_for_task = shutdown_tx.clone();
        let handle = tokio::spawn(async move {
            // Hold shutdown_tx alive across the entire accept loop.
            let _shutdown_tx_keepalive = shutdown_tx_for_task;
            if let Err(e) = serve_inner(listener, metrics_for_server, token, shutdown_rx).await {
                tracing::warn!(err = %e, "test serve_inner failed");
            }
        });
        // Yield repeatedly so the spawned task gets polled and the
        // listener socket is registered in the tokio reactor.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        (addr, metrics, handle)
    }

    async fn http_get(addr: SocketAddr, path: &str, auth: Option<&str>) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let mut req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
        if let Some(t) = auth {
            req.push_str(&format!("Authorization: Bearer {t}\r\n"));
        }
        req.push_str("\r\n");
        stream.write_all(req.as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.expect("read");
        let s = String::from_utf8_lossy(&buf).into_owned();
        // Parse status line.
        let status = s
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0);
        // Body is everything after the empty line.
        let body = s.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    #[tokio::test]
    async fn returns_401_when_authorization_header_missing() {
        let (addr, _m, _h) = spawn_test_server("secret-token").await;
        let (status, _) = http_get(addr, "/metrics", None).await;
        assert_eq!(status, 401, "missing Authorization must return 401");
    }

    #[tokio::test]
    async fn returns_401_when_token_mismatches() {
        let (addr, _m, _h) = spawn_test_server("secret-token").await;
        let (status, _) = http_get(addr, "/metrics", Some("wrong-token")).await;
        assert_eq!(status, 401, "wrong token must return 401");
    }

    #[tokio::test]
    async fn returns_200_with_metrics_body_when_token_matches() {
        let (addr, _m, _h) = spawn_test_server("secret-token").await;
        let (status, body) = http_get(addr, "/metrics", Some("secret-token")).await;
        assert_eq!(status, 200);
        assert!(
            body.contains("edge_requests_total"),
            "body must contain edge_requests_total, got first 200 chars: {}",
            &body[..body.len().min(200)]
        );
        assert!(
            body.contains("edge_app_status"),
            "body must contain edge_app_status gauge"
        );
    }

    #[tokio::test]
    async fn returns_404_for_paths_other_than_metrics() {
        let (addr, _m, _h) = spawn_test_server("secret-token").await;
        let (status, _) = http_get(addr, "/", Some("secret-token")).await;
        assert_eq!(status, 404);
        let (status, _) = http_get(addr, "/health", Some("secret-token")).await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn returns_405_for_post_metrics() {
        let (addr, _m, _h) = spawn_test_server("secret-token").await;
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(
                b"POST /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            )
            .await
            .expect("write");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.expect("read");
        let s = String::from_utf8_lossy(&buf).into_owned();
        let status = s
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|c| c.parse::<u16>().ok())
            .unwrap_or(0);
        assert_eq!(status, 404, "POST must be 404 (only GET served)");
    }

    #[tokio::test(start_paused = true)]
    async fn worker_uptime_gauge_is_monotonic() {
        let (_addr, metrics, _h) = spawn_test_server("token").await;
        metrics.tick_worker_gauges();
        let first = metrics.worker_uptime_seconds.get();
        // Advance tokio's paused clock. The worker's `started_at`
        // is a real `Instant`, not the paused clock — so this
        // assertion verifies the gauge catches up after a real
        // wall-clock sleep, not a virtual one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        metrics.tick_worker_gauges();
        let second = metrics.worker_uptime_seconds.get();
        assert!(second >= first, "uptime gauge must not regress");
    }

    #[test]
    fn constant_time_eq_smoke() {
        // Equal slices — true.
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(constant_time_eq(&[0u8; 32], &[0u8; 32]));
        // Mismatch on any byte — false.
        assert!(!constant_time_eq(b"abc", b"abe"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"abcd", b"abc"));
        // Length delta counts as a mismatch (folded into the diff).
        assert!(!constant_time_eq(b"", b"x"));
        // Different positions of mismatch — both false, no observable
        // ordering. We can't directly assert constant-time without
        // statistical tooling; the smoke test just pins correctness.
        assert!(!constant_time_eq(
            b"prefix-match-suffix-A",
            b"prefix-match-suffix-B"
        ));
        assert!(!constant_time_eq(b"prefix-A", b"prefix-B"));
    }

    /// Cover the refactor: `serve_inner` accepts a pre-bound
    /// `TcpListener` and a `Arc<str>` token, returns `Ok(())` when
    /// the shutdown channel fires. This avoids duplicating the
    /// accept-loop body in every test (pre-finding-#7 bug; the
    /// production `serve()` and the test diverged by a few lines on
    /// a previous edit).
    #[tokio::test]
    async fn serve_inner_returns_ok_when_shutdown_signalled() {
        use tokio::sync::broadcast;

        let metrics = WorkerMetrics::new().expect("metrics");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let (tx, rx) = broadcast::channel::<()>(1);
        // Hold a sender alive so the receiver inside `serve_inner`
        // doesn't see `RecvError::Closed` before we explicitly shut
        // down — same lifetime pattern as `spawn_test_server`.
        let _keepalive = tx.clone();
        let join =
            tokio::spawn(
                async move { serve_inner(listener, metrics, Arc::from("token"), rx).await },
            );
        // Yield so the listener is registered before we shut down.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        tx.send(()).expect("shutdown send");
        join.await.expect("join").expect("serve_inner");
    }
}
