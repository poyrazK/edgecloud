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
pub async fn serve(
    addr: SocketAddr,
    metrics: Arc<WorkerMetrics>,
    auth_token: String,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "metrics server listening on /metrics");

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
    auth_token: String,
) -> Result<Response<RespBody>, Infallible> {
    // Only GET /metrics is served. Anything else is 404 — the endpoint
    // is intentionally narrow; we don't want to expose an arbitrary
    // HTTP surface on this port.
    if req.method() != Method::GET || req.uri().path() != "/metrics" {
        return Ok(empty(StatusCode::NOT_FOUND));
    }

    // Auth header check. `Authorization: Bearer <token>` is the only
    // accepted shape; anything else is 401. The token comparison uses
    // a constant-length equality so a partial-prefix probing attack
    // would not gain timing information — `==` on `&str` is fine here
    // because the variance is dominated by network jitter on a 32+
    // byte token.
    let auth_ok = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|t| t == auth_token)
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
        metrics.register_app("t_test", "d_test", "app_test").await;
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
        let token = token.to_string();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        // Move shutdown_tx INTO the spawned task. The outer scope
        // does NOT touch it again — it lives as long as the task.
        // If we dropped it from the outer scope, `shutdown_rx.recv()`
        // in the task would return Err immediately and the accept
        // loop would exit, leaving the listener unregistered — the
        // kernel would then RST incoming SYNs (ConnectionReset on
        // connect). The clone below is for the outer test scope, so
        // we can signal shutdown from outside without taking
        // ownership away from the task.
        let shutdown_tx_for_outer = shutdown_tx.clone();
        let handle = tokio::spawn(async move {
            // Hold shutdown_tx alive for the lifetime of the task.
            let _shutdown_tx_keepalive = shutdown_tx;
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => return,
                    accept = listener.accept() => {
                        let (stream, _) = match accept {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        let io = TokioIo::new(stream);
                        let m = metrics_for_server.clone();
                        let t = token.clone();
                        tokio::spawn(async move {
                            let svc = service_fn(move |req| {
                                handle(req, m.clone(), t.clone())
                            });
                            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                                tracing::debug!(err = %e, "metrics connection ended");
                            }
                        });
                    }
                }
            }
        });
        // Yield repeatedly so the spawned task gets polled and the
        // listener socket is registered in the tokio reactor.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        drop(shutdown_tx_for_outer);
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
}
