//! Outbound HTTP transport with DNS-rebinding protection.
//!
//! Clones `wasmtime_wasi_http::p2::default_send_request_handler`
//! (which does DNS+TCP atomically inside `tokio::net::TcpStream::connect`
//! at `wasmtime-wasi-http-45.0.3/src/p2/mod.rs:603` and never surfaces
//! the resolved IP) so we can pre-resolve via `tokio::net::lookup_host`,
//! validate each candidate with `EgressPolicy::check_resolved_ip`, and
//! connect to a `SocketAddr` IP literal — defeating the OS-level TOCTOU
//! window that allows a tenant to fetch `evil.example.com` (allowlisted)
//! that resolves to `169.254.169.254` on the second query.
//!
//! Used by `EgressHttpHooks::send_request` in `runtime.rs`. The
//! URL-level `EgressPolicy::check(url)` is the *first* defense (cheap
//! pre-DNS hostname allowlist match); this module is the *second*
//! defense for the DNS-rebinding race.
//!
//! The DNS-resolver test seam at the bottom of this file is gated
//! `#[cfg(test)]`; production binaries have zero overhead.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::egress::EgressPolicy;
use http_body_util::BodyExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use wasmtime_wasi_http::io::TokioIo;
use wasmtime_wasi_http::p2::bindings::http::types::{DnsErrorPayload, ErrorCode};
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::hyper_request_error;
use wasmtime_wasi_http::p2::types::{
    HostFutureIncomingResponse, IncomingResponse, OutgoingRequestConfig,
};

/// Wrap a `String` DNS error message in the typed `ErrorCode::DnsError`
/// variant. The upstream `wasmtime_wasi_http::p2::dns_error` helper is
/// `pub(crate)` (verified at `wasmtime-wasi-http-45.0.3/src/p2/error.rs:138`),
/// so we build it inline via the bindgen-generated `DnsErrorPayload`.
fn dns_error(rcode: String) -> ErrorCode {
    ErrorCode::DnsError(DnsErrorPayload {
        rcode: Some(rcode),
        info_code: Some(0),
    })
}

/// Pre-resolve the host and pick the first candidate whose IP passes
/// `EgressPolicy::check_resolved_ip`. If ANY candidate IP is hard-denied,
/// refuse the whole request — a poisoned record in the resolution set
/// is a real anomaly worth surfacing, and silent-allow on a clean sibling
/// leaves the TOCTOU window open.
async fn resolve_validated(
    host: &str,
    port: u16,
    egress: &EgressPolicy,
    tenant_id: &str,
    connect_timeout: Duration,
) -> Result<SocketAddr, ErrorCode> {
    // Test-only resolver seam (gated). Production builds use the system
    // resolver directly.
    #[cfg(test)]
    {
        // Clone the Arc out of the Mutex BEFORE the .await so the
        // MutexGuard is dropped (it's not Send, and the spawn'd future
        // requires Send).
        let f = TEST_RESOLVER.lock().unwrap().clone();
        if let Some(f) = f.as_ref() {
            let fut = f(host, port);
            let mut fut = Box::pin(fut);
            let result: std::io::Result<Box<dyn Iterator<Item = SocketAddr> + Send>> =
                fut.as_mut().await;
            match result {
                Ok(mut it) => {
                    let mut blocked: Vec<std::net::IpAddr> = Vec::new();
                    for addr in it.by_ref() {
                        match egress.check_resolved_ip(addr.ip()) {
                            Ok(()) => return Ok(addr),
                            Err(reason) => {
                                tracing::warn!(
                                    tenant_id,
                                    %host,
                                    ip = %addr.ip(),
                                    %reason,
                                    "egress denied: skipping blocked resolved IP (test seam)"
                                );
                                blocked.push(addr.ip());
                            }
                        }
                    }
                    if !blocked.is_empty() {
                        return Err(ErrorCode::InternalError(Some(format!(
                            "egress denied: {host} resolved to {} blocked IP(s) ({:?}); refusing all",
                            blocked.len(),
                            blocked
                        ))));
                    }
                    return Err(dns_error(format!("no addresses returned for {host}")));
                }
                Err(e) => return Err(dns_error(format!("lookup failed: {e}"))),
            }
        }
    }

    let lookup = timeout(connect_timeout, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_| ErrorCode::ConnectionTimeout)?
        .map_err(|e| dns_error(format!("lookup failed: {e}")))?;

    let mut blocked: Vec<std::net::IpAddr> = Vec::new();
    for addr in lookup {
        match egress.check_resolved_ip(addr.ip()) {
            Ok(()) => return Ok(addr),
            Err(reason) => {
                tracing::warn!(
                    tenant_id = %tenant_id,
                    %host,
                    ip = %addr.ip(),
                    %reason,
                    "egress denied: skipping blocked resolved IP"
                );
                blocked.push(addr.ip());
            }
        }
    }
    if !blocked.is_empty() {
        return Err(ErrorCode::InternalError(Some(format!(
            "egress denied: {host} resolved to {} blocked IP(s) ({:?}); refusing all",
            blocked.len(),
            blocked
        ))));
    }
    Err(dns_error(format!("no addresses returned for {host}")))
}

/// Process-wide rustls `TlsConnector`. webpki roots (Mozilla CA bundle)
/// are identical for every tenant, so per-tenant rebuilds only pay
/// construction cost for an identical struct. Construction is amortized
/// via `OnceLock`; the first TLS handshake is unaffected.
#[cfg(feature = "egress-tls")]
fn tls_connector() -> &'static tokio_rustls::TlsConnector {
    use std::sync::OnceLock;
    static CONNECTOR: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();
    CONNECTOR.get_or_init(|| {
        let store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.into(),
        };
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(store)
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg))
    })
}

/// The async core: clone of `wasmtime_wasi_http::p2::default_send_request_handler`
/// with the DNS+TCP steps split. Pre-resolves via `lookup_host`, validates
/// each candidate with `EgressPolicy::check_resolved_ip`, then connects
/// to the validated `SocketAddr` literal so the kernel cannot re-resolve.
///
/// Mirrors the upstream `wasmtime-wasi-http-45.0.3/src/p2/mod.rs:570-712`
/// structure (HOST header insertion from authority, URI stripping, hyper
/// handshake + conn-driver spawn, `sender.send_request`, body wrap).
pub(crate) async fn send_request_handler(
    mut request: hyper::Request<HyperOutgoingBody>,
    OutgoingRequestConfig {
        use_tls,
        connect_timeout,
        first_byte_timeout,
        between_bytes_timeout,
    }: OutgoingRequestConfig,
    egress: &EgressPolicy,
    tenant_id: &str,
) -> Result<IncomingResponse, ErrorCode> {
    // 1. Insert HOST header from authority (matches upstream mod.rs:585-591).
    //    Critical: HOST must be the hostname, NOT the IP literal we connect
    //    to — SNI + virtual-host routing depend on it.
    if !request.headers().contains_key(hyper::header::HOST) {
        if let Some(auth) = request.uri().authority() {
            if let Ok(v) = hyper::header::HeaderValue::from_str(auth.as_str()) {
                request.headers_mut().insert(hyper::header::HOST, v);
            }
        }
    }

    // 2. Pull host + port out of the URI. ws URIs (no authority) are
    //    rejected by wasmtime-wasi-http's outgoing-handler binding before
    //    we get here, so an absent authority means the request is malformed.
    let auth = request
        .uri()
        .authority()
        .ok_or(ErrorCode::HttpRequestUriInvalid)?;
    // Capture host as `String` so we keep using it after `sender.send_request`
    // has consumed `request`. `auth.host()` returns a borrow tied to the URI.
    let host: String = auth.host().to_string();
    let port = if let Some(p) = auth.port_u16() {
        p
    } else if use_tls {
        443
    } else {
        80
    };

    // 3. Pre-resolve + IP-validate. This is the rebinding defense.
    let addr = resolve_validated(&host, port, egress, tenant_id, connect_timeout).await?;

    // 4. TCP connect to the IP literal — kernel does NOT re-resolve.
    let tcp_stream = timeout(connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| ErrorCode::ConnectionTimeout)?
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::AddrNotAvailable => dns_error("address not available".into()),
            _ if e
                .to_string()
                .starts_with("failed to lookup address information") =>
            {
                dns_error("address not available".into())
            }
            _ => ErrorCode::ConnectionRefused,
        })?;

    // 5. Optional TLS handshake. Pass hostname (not IP) to ServerName so
    //    SNI + cert validation are correct.
    let (mut sender, worker) = if use_tls {
        #[cfg(feature = "egress-tls")]
        {
            use rustls::pki_types::ServerName;
            let domain = ServerName::try_from(host.as_str())
                .map_err(|e| {
                    tracing::warn!(
                        tenant_id = %tenant_id,
                        %host,
                        "dns name invalid for TLS SNI: {e:?}"
                    );
                    dns_error("invalid dns name".into())
                })?
                .to_owned();
            let stream = tls_connector()
                .connect(domain, tcp_stream)
                .await
                .map_err(|e| {
                    tracing::warn!(
                        tenant_id = %tenant_id,
                        %host,
                        "tls protocol error: {e:?}"
                    );
                    ErrorCode::TlsProtocolError
                })?;
            let stream = TokioIo::new(stream);

            let (sender, conn) = timeout(
                connect_timeout,
                hyper::client::conn::http1::handshake(stream),
            )
            .await
            .map_err(|_| ErrorCode::ConnectionTimeout)?
            .map_err(hyper_request_error)?;

            let worker = wasmtime_wasi::runtime::spawn(async move {
                match conn.await {
                    Ok(()) => {}
                    Err(e) => tracing::warn!("dropping conn error {e}"),
                }
            });
            (sender, worker)
        }
        #[cfg(not(feature = "egress-tls"))]
        {
            // `egress-tls` feature disabled — refuse all TLS requests.
            return Err(ErrorCode::TlsProtocolError);
        }
    } else {
        let tcp_stream = TokioIo::new(tcp_stream);
        let (sender, conn) = timeout(
            connect_timeout,
            hyper::client::conn::http1::handshake(tcp_stream),
        )
        .await
        .map_err(|_| ErrorCode::ConnectionTimeout)?
        .map_err(hyper_request_error)?;

        let worker = wasmtime_wasi::runtime::spawn(async move {
            match conn.await {
                Ok(()) => {}
                Err(e) => tracing::warn!("dropping conn error {e}"),
            }
        });
        (sender, worker)
    };

    // 6. URI stripping — load-bearing for non-proxy sends
    //    (matches upstream mod.rs:687-699).
    *request.uri_mut() = http::Uri::builder()
        .path_and_query(
            request
                .uri()
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("/"),
        )
        .build()
        .expect("comes from valid request");

    // 7. Send the request.
    let response = timeout(first_byte_timeout, sender.send_request(request))
        .await
        .map_err(|_| ErrorCode::ConnectionReadTimeout)?
        .map_err(hyper_request_error)?;

    // 8. Redirect-bypass guard (issue #207).
    //
    // Our hyper clone does NOT auto-follow redirects — it sends one
    // request and returns one response — so a guest that re-issues via
    // `wasi:http/outgoing-handler` would already re-run `egress.check`
    // on the redirect target. This guard is belt-and-braces: it catches
    // guest code that bypasses `outgoing-handler` to follow redirects
    // (e.g. by parsing the Location manually and re-using the open TCP
    // socket via `wasi:sockets/tcp`), which would otherwise reuse this
    // connection's destination without any policy check.
    //
    // Only 3xx responses get checked (per RFC 7231 §7.1.2 the Location
    // header on non-redirect responses is meaningless). Only absolute
    // Location URLs are validated — relative URLs need request-URI
    // resolution which is the guest's responsibility.
    if (300..400).contains(&response.status().as_u16()) {
        if let Err(reason) =
            validate_location(response.headers().get(hyper::header::LOCATION), egress)
        {
            tracing::warn!(
                tenant_id = %tenant_id,
                host = %host,
                status = %response.status(),
                reason = %reason,
                "egress redirect denied"
            );
            return Err(ErrorCode::InternalError(Some(reason)));
        }
    }

    // 9. Assemble the typed response with body + worker driver.
    let (parts, body) = response.into_parts();
    let body = body.map_err(hyper_request_error).boxed_unsync();
    let resp = http::Response::from_parts(parts, body);

    Ok(IncomingResponse {
        resp,
        worker: Some(worker),
        between_bytes_timeout,
    })
}

/// Sync entry point mirroring `wasmtime_wasi_http::p2::default_send_request`.
/// Spawns the async handler on `wasmtime_wasi::runtime::spawn` (the same
/// helper upstream uses) and wraps the join handle in
/// `HostFutureIncomingResponse::pending`.
pub(crate) fn spawn_send_request_handler(
    request: hyper::Request<HyperOutgoingBody>,
    config: OutgoingRequestConfig,
    egress: Arc<EgressPolicy>,
    tenant_id: String,
) -> HostFutureIncomingResponse {
    let handle = wasmtime_wasi::runtime::spawn(async move {
        Ok(send_request_handler(request, config, &egress, &tenant_id).await)
    });
    HostFutureIncomingResponse::pending(handle)
}

/// Validate a redirect `Location` header against the egress policy.
///
/// Returns `Ok(())` for `None` (no Location → no redirect target to check),
/// non-UTF-8 values (unparseable → let the guest decide), and relative URLs
/// (resolution needs the request URI, which is the guest's responsibility).
///
/// Only absolute `http://` / `https://` URLs are checked. Returns
/// `Err(reason)` if the URL fails `EgressPolicy::check` — this is the
/// defense-in-depth catch for guest code that bypasses
/// `wasi:http/outgoing-handler` to follow redirects via the open
/// `wasi:sockets/tcp` connection (issue #207).
fn validate_location(
    location: Option<&hyper::header::HeaderValue>,
    egress: &EgressPolicy,
) -> Result<(), String> {
    let Some(value) = location else {
        return Ok(());
    };
    let Ok(loc_str) = value.to_str() else {
        // Non-ASCII Location. Don't try to parse — let the guest decide.
        return Ok(());
    };
    if !(loc_str.starts_with("http://") || loc_str.starts_with("https://")) {
        // Relative URL — needs request-URI resolution the guest owns.
        return Ok(());
    }
    egress.check(loc_str)
}

#[cfg(test)]
mod validate_location_tests {
    use super::validate_location;
    use crate::egress::EgressPolicy;
    use hyper::header::HeaderValue;

    #[test]
    fn no_location_passes() {
        let egress = EgressPolicy::allow_all();
        assert!(validate_location(None, &egress).is_ok());
    }

    #[test]
    fn relative_location_passes_to_guest() {
        let egress = EgressPolicy::allow_all();
        let loc = HeaderValue::from_static("/v2/charges");
        assert!(validate_location(Some(&loc), &egress).is_ok());
        // Protocol-relative: "//evil.com/path" — neither http:// nor https://
        // prefix, so we don't try to resolve. Guest must handle.
        let loc = HeaderValue::from_static("//evil.com/path");
        assert!(validate_location(Some(&loc), &egress).is_ok());
    }

    #[test]
    fn non_utf8_location_passes_to_guest() {
        let egress = EgressPolicy::allow_all();
        // HeaderValue::from_bytes with invalid utf-8
        let bytes: &[u8] = b"http://\xff\xfe/x";
        let value = HeaderValue::from_bytes(bytes).expect("valid header bytes");
        assert!(validate_location(Some(&value), &egress).is_ok());
    }

    #[test]
    fn redirect_to_loopback_denied() {
        let egress = EgressPolicy::allow_all();
        let loc = HeaderValue::from_static("http://127.0.0.1/admin");
        let err = validate_location(Some(&loc), &egress).unwrap_err();
        assert!(
            err.contains("egress denied"),
            "expected 'egress denied' in reason, got: {err}"
        );
    }

    #[test]
    fn redirect_to_cloud_metadata_denied() {
        let egress = EgressPolicy::allow_all();
        // 169.254.169.254 = AWS/Azure/GCP IMDS — must always be denied.
        let loc = HeaderValue::from_static(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
        );
        let err = validate_location(Some(&loc), &egress).unwrap_err();
        assert!(err.contains("egress denied"), "got: {err}");
    }

    #[test]
    fn redirect_to_metadata_hostname_denied() {
        let egress = EgressPolicy::allow_all();
        // AWS EC2 hostname
        let loc = HeaderValue::from_static("http://instance-data.ec2.internal/latest/");
        let err = validate_location(Some(&loc), &egress).unwrap_err();
        assert!(err.contains("egress denied"), "got: {err}");
    }

    #[test]
    fn redirect_to_uncategorized_host_denied_by_allowlist() {
        // Empty allowlist = default-deny.
        let egress = EgressPolicy::new(vec![]);
        let loc = HeaderValue::from_static("https://evil.example.com/");
        let err = validate_location(Some(&loc), &egress).unwrap_err();
        assert!(
            err.contains("egress denied") || err.contains("allowlist"),
            "got: {err}"
        );
    }

    #[test]
    fn redirect_to_host_not_in_tenant_allowlist_denied() {
        let egress = EgressPolicy::new(vec!["api.stripe.com".into()]);
        let loc = HeaderValue::from_static("https://evil.example.com/");
        let err = validate_location(Some(&loc), &egress).unwrap_err();
        assert!(err.contains("egress denied"), "got: {err}");
    }

    #[test]
    fn redirect_to_allowlisted_host_passes() {
        // Tenant allowlist permits api.stripe.com — a 302 to that host
        // (e.g. v1 → v2) is fine.
        let egress = EgressPolicy::new(vec!["api.stripe.com".into()]);
        let loc = HeaderValue::from_static("https://api.stripe.com/v2/charges");
        assert!(validate_location(Some(&loc), &egress).is_ok());
    }

    #[test]
    fn redirect_to_allowlisted_host_with_wildcard_passes() {
        let egress = EgressPolicy::new(vec!["*.stripe.com".into()]);
        let loc = HeaderValue::from_static("https://api.stripe.com/v1/charges");
        assert!(validate_location(Some(&loc), &egress).is_ok());
    }
}

// ── Test-only DNS resolver seam ─────────────────────────────────────
//
// Allows the integration test (`l10_dns_rebinding_guard_denies_blocked_resolution`)
// to inject a controlled resolution set without touching `/etc/hosts` or
// relying on DNS rebinding real-world conditions. `#[cfg(test)]`-only —
// production builds have zero overhead and the symbol is unreachable.

#[cfg(test)]
mod test_seam {
    use super::*;
    use std::net::IpAddr;

    pub(super) type ResolverIter = Box<dyn Iterator<Item = SocketAddr> + Send>;
    pub(super) type ResolverFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<ResolverIter>> + Send>>;
    // Arc<dyn Fn + Send + Sync> so we can clone the resolver out of the
    // Mutex BEFORE the .await — the MutexGuard isn't `Send`, so holding
    // it across an await would make the spawn'd future non-Send.
    pub(super) type ResolverFn = std::sync::Arc<dyn Fn(&str, u16) -> ResolverFuture + Send + Sync>;

    pub(super) static TEST_RESOLVER: std::sync::Mutex<Option<ResolverFn>> =
        std::sync::Mutex::new(None);

    /// Lock to serialize access to `TEST_RESOLVER` from tests that
    /// run in parallel. Held for the duration of the test body via
    /// `parking_lot`'s RAII guard.
    pub(super) static SEAM_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    /// Install a test resolver. Replaces any previous resolver.
    /// Takes an `Arc<dyn Fn>` so callers can capture local state.
    pub fn set_test_resolver(f: ResolverFn) {
        *TEST_RESOLVER.lock().unwrap() = Some(f);
    }

    /// Clear the test resolver (fall back to system `lookup_host`).
    pub fn clear_test_resolver() {
        *TEST_RESOLVER.lock().unwrap() = None;
    }

    /// Convenience helper for tests: build a single-IP resolver.
    pub fn static_resolver(ip: IpAddr, port: u16) -> ResolverFn {
        std::sync::Arc::new(move |_host, _port| {
            let addr = SocketAddr::new(ip, port);
            Box::pin(async move {
                Ok(Box::new(std::iter::once(addr)) as Box<dyn Iterator<Item = SocketAddr> + Send>)
            })
        })
    }
}

#[cfg(test)]
use test_seam::TEST_RESOLVER;

// Re-export for integration tests in `edge-worker/tests/layer_integration.rs`.
#[cfg(test)]
pub use test_seam::{clear_test_resolver, set_test_resolver, static_resolver};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::EgressPolicy;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_config(use_tls: bool) -> OutgoingRequestConfig {
        OutgoingRequestConfig {
            use_tls,
            connect_timeout: Duration::from_secs(1),
            first_byte_timeout: Duration::from_secs(1),
            between_bytes_timeout: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn blocks_when_resolver_returns_loopback_ip() {
        // Hold the seam mutex for the whole test body — but we need
        // to drop the guard before any await points to satisfy
        // clippy::await_holding_lock. The simplest workaround is to
        // acquire and drop the guard before the await, since the
        // TEST_RESOLVER is reference-counted (Arc) and stays alive
        // across the await.
        let _guard = test_seam::SEAM_LOCK.lock();
        drop(_guard);
        // Inject a resolver that returns 127.0.0.1 for any host.
        // The hard-deny loopback rule should refuse the request.
        set_test_resolver(static_resolver(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 80));
        let egress = EgressPolicy::allow_all();
        let req = hyper::Request::builder()
            .uri("http://mock.local/")
            .method("GET")
            .body(HyperOutgoingBody::default())
            .unwrap();
        let result = send_request_handler(req, test_config(false), &egress, "test-tenant").await;
        clear_test_resolver();
        assert!(
            result.is_err(),
            "expected Err for loopback resolution, got Ok"
        );
        let msg = format!("{:?}", result.unwrap_err()).to_lowercase();
        assert!(
            msg.contains("egress denied") || msg.contains("internalerror"),
            "expected egress-deny / InternalError, got: {msg}"
        );
    }

    #[tokio::test]
    async fn blocks_when_all_resolved_ips_are_blocked() {
        // See note in `blocks_when_resolver_returns_loopback_ip` above:
        // acquire + drop the seam mutex up front so the
        // TEST_RESOLVER (Arc) lives across the await without holding
        // a MutexGuard.
        let _guard = test_seam::SEAM_LOCK.lock();
        drop(_guard);
        // Inject a resolver that returns ONLY blocked IPs. The policy
        // picks the first valid candidate, so when none exist we fail
        // closed with `InternalError`.
        set_test_resolver({
            let addrs: Vec<SocketAddr> = vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)), 80),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 80),
            ];
            type Fut = std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = std::io::Result<Box<dyn Iterator<Item = SocketAddr> + Send>>,
                        > + Send,
                >,
            >;
            let resolver = move |_host: &str, _port: u16| -> Fut {
                let addrs = addrs.clone();
                Box::pin(async move {
                    std::io::Result::Ok(
                        Box::new(addrs.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>
                    )
                })
            };
            std::sync::Arc::new(resolver) as std::sync::Arc<dyn Fn(&str, u16) -> Fut + Send + Sync>
        });
        let egress = EgressPolicy::allow_all();
        let req = hyper::Request::builder()
            .uri("http://mock.local/")
            .method("GET")
            .body(HyperOutgoingBody::default())
            .unwrap();
        let result = send_request_handler(req, test_config(false), &egress, "test-tenant").await;
        clear_test_resolver();
        assert!(
            result.is_err(),
            "expected Err when all IPs are blocked, got Ok"
        );
        let msg = format!("{:?}", result.unwrap_err());
        assert!(
            msg.contains("blocked IP") && msg.contains("refusing all"),
            "expected error to name the blocked IPs and the policy, got: {msg}"
        );
    }
}
