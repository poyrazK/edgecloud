//! `wasi:sockets` egress policy hook for `wasmtime-wasi` 45.0.3.
//!
//! Closes the bypass where raw `wasi:sockets/tcp-connect` and
//! `wasi:sockets/udp-send` could ignore the per-tenant `EgressPolicy`
//! (GitHub issue #309).
//!
//! ## Why a separate module
//!
//! `wasmtime-wasi` 45.0.3's `WasiSocketsCtx` fields are `pub(crate)`
//! (verified at `wasmtime-wasi-45.0.3/src/sockets/mod.rs:70-72`) and
//! `SocketAddrCheck::new` is also `pub(crate)` (mod.rs:140-147). The
//! only public injection point is `WasiCtxBuilder::socket_addr_check(...)`
//! (ctx.rs:397-406), which must be called **before** `.build()`. This
//! module builds the closure that the builder consumes inside
//! `build_wasi_ctx_for_tenant`.
//!
//! The blanket `impl<T: WasiView> WasiSocketsView for T` from
//! `wasmtime-wasi-45.0.3/src/view.rs:87-95` already gives every
//! `WasiView`-implementing store a sockets projection — we do **not**
//! shadow it with a manual `WasiSocketsView for RuntimeState`.
//!
//! ## Bind vs. connect-side
//!
//! `TcpBind` / `UdpBind` are local-only operations (the kernel reserves
//! a local port); we allow all binds. Only the *connect-side* is gated:
//! `TcpConnect`, `UdpConnect`, `UdpOutgoingDatagram`.
//!
//! ## Modes
//!
//! Behavior is configured via `SocketEgressPolicy`, which is read from
//! the `EDGE_EGRESS_SOCKET_MODE` env var. Default is `BlockAll`, matching
//! the effective deny-all behavior of wasmtime's `SocketAddrCheck::default()`.
//! See [`SocketEgressPolicy::from_env`] for the parsing rules.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use wasmtime_wasi::sockets::SocketAddrUse;

use crate::egress::EgressPolicy;

/// Per-tenant socket-egress mode, configured via `EDGE_EGRESS_SOCKET_MODE`.
///
/// # Mode semantics
///
/// - [`BlockAll`] (default): closure always returns `false`. Same posture
///   as wasmtime's `SocketAddrCheck::default()` — guests cannot use
///   `wasi:sockets` for any operation: every `SocketAddrUse` variant
///   (`TcpBind`, `TcpConnect`, `UdpBind`, `UdpConnect`,
///   `UdpOutgoingDatagram`) returns `access-denied`.
/// - [`AllowList`]: closure consults `EgressPolicy::check_address` for
///   connect-side operations; allows all binds. `Bind` (local-only) is
///   always permitted. **Asymmetry vs. HTTP:** because the closure only
///   receives a `SocketAddr` (an IP literal), we cannot match the
///   tenant's hostname allowlist (e.g. `api.stripe.com`) against a raw
///   IP. A non-empty allowlist opts the tenant into raw-socket egress
///   to any non-hard-denied IP. Hostname-pinned enforcement happens at
///   the HTTP layer in `EgressHttpHooks::send_request`.
/// - [`AllowAll`]: closure always returns `true`. Equivalent to
///   `WasiCtxBuilder::inherit_network(true)`. The hard-deny layer in
///   `EgressPolicy::check_address` is bypassed (the closure short-
///   circuits before consulting the policy) — use with caution.
/// - [`HostnamePinned`]: closure consults
///   `EgressPolicy::hostname_pinned_match` against the per-`Network`
///   [`HostnamePinning`] cache (issue #309 follow-up). **Dormant
///   today** — until the upstream wasmtime-wasi PR in
///   `docs/upstream-wasmtime-resolve-check.patch` merges, the cache
///   stays empty. `HostnamePinned` therefore equals `BlockAll`: every
///   connect-side call is denied. Once the patch ships, set
///   `EDGE_EGRESS_HOSTNAME_PINNING=true` and the mode lights up.
///
/// # Per-app selection (issue #412)
///
/// The per-app selector is `AppSpec.socket_mode` on the worker
/// (`edge-worker/src/messages.rs`). Each app's `RuntimeState::socket_mode`
/// is set from that field, falling back to the worker-wide
/// `EDGE_EGRESS_SOCKET_MODE` knob when the field is absent. For the
/// `HostnamePinned` arm specifically, the **compose rule** is that the
/// mode activates only when **both** the per-app field is `HostnamePinned`
/// AND the worker-wide `EDGE_EGRESS_HOSTNAME_PINNING=true`. The
/// worker-wide knob remains a hard gate so the dormant arm can never
/// be reached without an explicit operator opt-in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SocketEgressPolicy {
    #[default]
    BlockAll,
    AllowList,
    AllowAll,
    HostnamePinned,
}

impl SocketEgressPolicy {
    /// Parse the `EDGE_EGRESS_SOCKET_MODE` env var. Returns `BlockAll`
    /// if unset or invalid. Logs only when the resolved value differs
    /// from the last-seen value (change-detection via a single
    /// `AtomicU8`), so per-request calls (per-RuntimeState, per-Clone)
    /// do not spam the log.
    ///
    /// **Process-static by design.** Operators don't reload this knob
    /// without restarting the worker — the worker reads it once at
    /// startup via `edge-worker/src/config.rs::Config::from_env` and
    /// threads the resolved mode through `HandlerConfig::socket_mode`
    /// into every `RuntimeState::with_env_and_meter` call. This method
    /// remains as a bootstrap helper for any standalone-runtime user
    /// who doesn't go through the worker.
    pub fn from_env() -> Self {
        let parsed = match std::env::var("EDGE_EGRESS_SOCKET_MODE") {
            Ok(s) => s.parse::<Self>().unwrap_or_else(|e: String| {
                tracing::warn!(
                    value = %s,
                    error = %e,
                    "EDGE_EGRESS_SOCKET_MODE: invalid value; falling back to block-all"
                );
                Self::BlockAll
            }),
            Err(_) => Self::BlockAll,
        };
        log_if_changed(parsed);
        parsed
    }
}

/// Process-static "last value we logged" — encoded as a `u8` so we can
/// use a single `AtomicU8` for thread-safe change detection without
/// needing a `Mutex` or `OnceLock`. The three modes map to `0/1/2`;
/// 255 is the "no previous value logged" sentinel.
fn log_if_changed(mode: SocketEgressPolicy) {
    use std::sync::atomic::{AtomicU8, Ordering};
    static LAST_LOGGED: AtomicU8 = AtomicU8::new(255);
    // The `_` arm is required because `SocketEgressPolicy` is
    // `#[non_exhaustive]` — it makes the match future-proof against
    // variants added in later releases. Today the match is exhaustive
    // (all 4 variants covered above), so the `_` is dead. Suppress the
    // warning rather than remove the arm so future contributors see
    // the pattern and don't accidentally drop the wildcard.
    let next = match mode {
        SocketEgressPolicy::BlockAll => 0,
        SocketEgressPolicy::AllowList => 1,
        SocketEgressPolicy::AllowAll => 2,
        SocketEgressPolicy::HostnamePinned => 3,
    };
    let prev = LAST_LOGGED.swap(next, Ordering::Relaxed);
    if prev == next {
        return;
    }
    match mode {
        SocketEgressPolicy::BlockAll => {
            tracing::info!(mode = %mode, "edge-runtime socket egress")
        }
        SocketEgressPolicy::AllowList => {
            tracing::info!(mode = %mode, "edge-runtime socket egress")
        }
        SocketEgressPolicy::AllowAll => tracing::warn!(
            mode = %mode,
            "edge-runtime socket egress: hard-deny bypassed — use with caution"
        ),
        SocketEgressPolicy::HostnamePinned => tracing::info!(
            mode = %mode,
            "edge-runtime socket egress: HostnamePinned — dormant until upstream \
             wasmtime-wasi resolve hook (docs/upstream-wasmtime-resolve-check.patch) merges; \
             closing every connect-side call today."
        ),
    }
}

impl FromStr for SocketEgressPolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "block-all" => Ok(Self::BlockAll),
            "allowlist" => Ok(Self::AllowList),
            "allow-all" => Ok(Self::AllowAll),
            "hostname-pinned" => Ok(Self::HostnamePinned),
            other => Err(format!(
                "unknown mode {:?} (expected one of: block-all, allowlist, allow-all, hostname-pinned)",
                other
            )),
        }
    }
}

impl std::fmt::Display for SocketEgressPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::BlockAll => "block-all",
            Self::AllowList => "allowlist",
            Self::AllowAll => "allow-all",
            Self::HostnamePinned => "hostname-pinned",
        })
    }
}

/// Type that `WasiCtxBuilder::socket_addr_check` accepts on wasmtime-wasi
/// 45.0.3 (verified at `wasmtime-wasi-45.0.3/src/ctx.rs:397-406`).
type SocketAddrCheckFuture = Pin<Box<dyn Future<Output = bool> + Send + Sync>>;

// ── Per-tenant hostname-pinning cache (issue #309 follow-up) ─────────────
//
// Backs the dormant `SocketEgressPolicy::HostnamePinned` mode. Each
// entry records `(hostname → set of resolved IPs)` as observed by the
// host impl `wasi:sockets/ip-name-lookup::resolve-addresses`.
//
// Today the cache is populated manually by tests only (the upstream
// resolve-side closure hook documented in
// `docs/upstream-wasmtime-resolve-check.patch` hasn't been merged yet).
// When the upstream PR lands, the runtime's
// `WasiSocketsCtxView::resolve_addresses` impl calls
// `HostnamePinning::record` before the upstream resolver runs, and the
// connect-side closure consults `HostnamePinning::contains`.
//
// `Arc<Mutex<...>>` so a fresh `RuntimeState` (per-request in the
// dispatch path) shares one cache for the lifetime of the dispatch
// instance. The `Mutex` is short-held (a single hashmap insert or
// lookup) and the cache is read-mostly; contention is negligible.

/// Per-`Network` resolution cache backing `SocketEgressPolicy::HostnamePinned`.
#[derive(Debug, Default)]
pub struct HostnamePinning {
    by_hostname: Mutex<HashMap<String, HashSet<IpAddr>>>,
}

impl HostnamePinning {
    /// Construct an empty cache. `Default::default()` is the same.
    pub fn new() -> Self {
        Self {
            by_hostname: Mutex::new(HashMap::new()),
        }
    }

    /// Record `hostname → {ips}` as observed by the resolve-side hook.
    /// Replaces any prior entry for `hostname`. The `Mutex` is held
    /// briefly — single insert.
    pub fn record(&self, hostname: &str, ips: impl IntoIterator<Item = IpAddr>) {
        let mut guard = self
            .by_hostname
            .lock()
            .expect("HostnamePinning mutex poisoned");
        let set: HashSet<IpAddr> = ips.into_iter().collect();
        guard.insert(hostname.to_string(), set);
    }

    /// Snapshot the full cache (used by the connect-side closure and
    /// by tests). Cheap clone of a `HashMap<String, HashSet<IpAddr>>`.
    pub fn snapshot(&self) -> HashMap<String, HashSet<IpAddr>> {
        self.by_hostname
            .lock()
            .expect("HostnamePinning mutex poisoned")
            .clone()
    }
}

/// Build the closure consumed by `WasiCtxBuilder::socket_addr_check`.
///
/// The returned closure is `Send + Sync + 'static` so `WasiCtxBuilder`
/// accepts it. It dispatches per-call on the captured `mode` and the
/// `SocketAddrUse` variant:
/// - `BlockAll` → always `false`.
/// - `AllowAll` → always `true`.
/// - `AllowList` + `TcpBind` / `UdpBind` → `true` (binds are local-only).
/// - `AllowList` + `TcpConnect` / `UdpConnect` / `UdpOutgoingDatagram`
///   → `EgressPolicy::check_address(addr).is_ok()`. Denials are logged
///   with `tracing::warn!` in the same shape as
///   `EgressHttpHooks::send_request` (see `runtime.rs:339-343`); allows
///   are silent.
///
/// `hostname_pinning` is reserved for the dormant
/// `SocketEgressPolicy::HostnamePinned` mode (commit 3 wires the
/// consult path; commit 2 only plumbs the Arc through). The closure
/// captures it so a future body change doesn't re-touch every call
/// site, but does not read it yet. The cache stays empty until the
/// upstream wasmtime-wasi patch in
/// `docs/upstream-wasmtime-resolve-check.patch` merges.
#[allow(clippy::too_many_arguments)]
pub(crate) fn make_socket_addr_check(
    egress: Arc<EgressPolicy>,
    mode: SocketEgressPolicy,
    tenant_id: String,
    hostname_pinning: Arc<HostnamePinning>,
) -> impl Fn(SocketAddr, SocketAddrUse) -> SocketAddrCheckFuture + Send + Sync + 'static {
    move |addr: SocketAddr, use_: SocketAddrUse| -> SocketAddrCheckFuture {
        match (mode, use_) {
            // `BlockAll` — close every bind/connect/send path.
            (SocketEgressPolicy::BlockAll, _) => Box::pin(async { false }),
            // `AllowAll` — open every bind/connect/send path. Equivalent
            // to `WasiCtxBuilder::inherit_network(true)`. Hard-deny is
            // bypassed here by design; this is the operator opt-in.
            (SocketEgressPolicy::AllowAll, _) => Box::pin(async { true }),
            // `AllowList` — binds are local-only, always permitted.
            (SocketEgressPolicy::AllowList, SocketAddrUse::TcpBind)
            | (SocketEgressPolicy::AllowList, SocketAddrUse::UdpBind) => Box::pin(async { true }),
            // `AllowList` — connect-side consults the policy. Log on deny
            // in the same shape as `EgressHttpHooks::send_request`.
            (
                SocketEgressPolicy::AllowList,
                SocketAddrUse::TcpConnect
                | SocketAddrUse::UdpConnect
                | SocketAddrUse::UdpOutgoingDatagram,
            ) => {
                let egress = egress.clone();
                let tenant_id = tenant_id.clone();
                Box::pin(async move {
                    match egress.check_address(addr) {
                        Ok(()) => true,
                        Err(reason) => {
                            tracing::warn!(
                                tenant_id = %tenant_id,
                                addr = %addr,
                                use_ = ?use_,
                                reason = %reason,
                                "egress denied (wasi:sockets)"
                            );
                            false
                        }
                    }
                })
            }
            // `HostnamePinned` — binds are local-only, always permitted.
            (SocketEgressPolicy::HostnamePinned, SocketAddrUse::TcpBind)
            | (SocketEgressPolicy::HostnamePinned, SocketAddrUse::UdpBind) => {
                Box::pin(async { true })
            }
            // `HostnamePinned` — connect-side consults the per-tenant
            // resolution cache via `EgressPolicy::hostname_pinned_match`.
            //
            // Cache is empty today (dormant): upstream resolve hook in
            // `docs/upstream-wasmtime-resolve-check.patch` hasn't merged,
            // so `hostname_pinned_match` returns `false` for every
            // observed addr. This arm equals the `BlockAll` arm in
            // observable behavior until that hook ships; once it does,
            // set `EDGE_EGRESS_HOSTNAME_PINNING=true` and admit paths
            // light up. Hard-deny applies first so loopback/private IPs
            // never reach the cache lookup.
            (
                SocketEgressPolicy::HostnamePinned,
                SocketAddrUse::TcpConnect
                | SocketAddrUse::UdpConnect
                | SocketAddrUse::UdpOutgoingDatagram,
            ) => {
                let egress = egress.clone();
                let tenant_id = tenant_id.clone();
                let cache = hostname_pinning.clone();
                Box::pin(async move {
                    // Hard-deny short-circuit — preserves the existing
                    // posture for loopback/private/metadata IPs even
                    // when the cache would otherwise admit.
                    if let Err(reason) = egress.check_resolved_ip(addr.ip()) {
                        tracing::warn!(
                            tenant_id = %tenant_id,
                            addr = %addr,
                            use_ = ?use_,
                            reason = %reason,
                            "egress denied (wasi:sockets hostname-pinned, hard-deny)"
                        );
                        return false;
                    }
                    let snapshot = cache.snapshot();
                    let admitted = egress.hostname_pinned_match(addr, &snapshot);
                    if !admitted {
                        tracing::warn!(
                            tenant_id = %tenant_id,
                            addr = %addr,
                            use_ = ?use_,
                            "egress denied (wasi:sockets hostname-pinned, no cache hit)"
                        );
                    }
                    admitted
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::EgressPolicy;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback_v4_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn public_v4_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), port)
    }

    fn metadata_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)), 80)
    }

    /// Empty per-`Network` resolution cache. Today (dormant) every
    /// `make_socket_addr_check` test passes this; once the upstream
    /// resolve-side closure hook ships, tests that want to exercise
    /// `HostnamePinned` admit paths will populate it.
    fn empty_cache() -> Arc<HostnamePinning> {
        Arc::new(HostnamePinning::new())
    }

    // ── mode dispatch: BlockAll ──────────────────────────────────────────

    #[tokio::test]
    async fn block_all_denies_all_use_variants() {
        let egress = Arc::new(EgressPolicy::allow_all());
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::BlockAll,
            "t_test".to_string(),
            empty_cache(),
        );
        for use_ in [
            SocketAddrUse::TcpBind,
            SocketAddrUse::TcpConnect,
            SocketAddrUse::UdpBind,
            SocketAddrUse::UdpConnect,
            SocketAddrUse::UdpOutgoingDatagram,
        ] {
            let result = check(public_v4_addr(80), use_).await;
            assert!(!result, "BlockAll must deny {use_:?} on public IP");
        }
    }

    // ── mode dispatch: AllowAll ──────────────────────────────────────────

    #[tokio::test]
    async fn allow_all_permits_all_use_variants() {
        let egress = Arc::new(EgressPolicy::allow_all());
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::AllowAll,
            "t_test".to_string(),
            empty_cache(),
        );
        for use_ in [
            SocketAddrUse::TcpBind,
            SocketAddrUse::TcpConnect,
            SocketAddrUse::UdpBind,
            SocketAddrUse::UdpConnect,
            SocketAddrUse::UdpOutgoingDatagram,
        ] {
            let result = check(public_v4_addr(80), use_).await;
            assert!(result, "AllowAll must permit {use_:?} on public IP");
        }
    }

    #[tokio::test]
    async fn allow_all_bypasses_hard_deny() {
        // `AllowAll` is operator opt-in: even hard-deny IPs are permitted.
        // Document this explicitly so reviewers don't mistake the design.
        let egress = Arc::new(EgressPolicy::allow_all());
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::AllowAll,
            "t_test".to_string(),
            empty_cache(),
        );
        let result = check(loopback_v4_addr(80), SocketAddrUse::TcpConnect).await;
        assert!(
            result,
            "AllowAll short-circuits to true; hard-deny does not apply"
        );
    }

    // ── mode dispatch: AllowList ─────────────────────────────────────────

    #[tokio::test]
    async fn allowlist_empty_allowlist_denies_connect_side() {
        let egress = Arc::new(EgressPolicy::new(vec![]));
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::AllowList,
            "t_test".to_string(),
            empty_cache(),
        );
        // Connect-side on a public IP: empty allowlist ⇒ deny.
        assert!(!check(public_v4_addr(80), SocketAddrUse::TcpConnect).await);
        assert!(!check(public_v4_addr(80), SocketAddrUse::UdpConnect).await);
        assert!(!check(public_v4_addr(80), SocketAddrUse::UdpOutgoingDatagram).await);
    }

    #[tokio::test]
    async fn allowlist_nonempty_allowlist_permits_public_connect_side() {
        // The documented asymmetry: tenant hostname allowlist opts into
        // raw-socket egress to non-hard-denied IPs.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::AllowList,
            "t_test".to_string(),
            empty_cache(),
        );
        assert!(check(public_v4_addr(80), SocketAddrUse::TcpConnect).await);
        assert!(check(public_v4_addr(80), SocketAddrUse::UdpConnect).await);
        assert!(check(public_v4_addr(80), SocketAddrUse::UdpOutgoingDatagram).await);
    }

    #[tokio::test]
    async fn allowlist_hard_deny_overrides_allowlist_on_connect_side() {
        // Hard-deny ALWAYS wins over the allowlist, even on a non-empty
        // allowlist. Same posture as the HTTP layer.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::AllowList,
            "t_test".to_string(),
            empty_cache(),
        );
        assert!(!check(loopback_v4_addr(80), SocketAddrUse::TcpConnect).await);
        assert!(!check(metadata_addr(), SocketAddrUse::TcpConnect).await);
    }

    #[tokio::test]
    async fn allowlist_bind_variants_are_always_permitted() {
        // User decision: bind is local-only, allow always.
        let egress = Arc::new(EgressPolicy::new(vec![]));
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::AllowList,
            "t_test".to_string(),
            empty_cache(),
        );
        assert!(check(loopback_v4_addr(0), SocketAddrUse::TcpBind).await);
        assert!(check(loopback_v4_addr(0), SocketAddrUse::UdpBind).await);
    }

    #[tokio::test]
    async fn allowlist_block_all_mode_denies_bind_too() {
        // Sanity: when mode is BlockAll, even binds are denied.
        let egress = Arc::new(EgressPolicy::allow_all());
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::BlockAll,
            "t_test".to_string(),
            empty_cache(),
        );
        assert!(!check(public_v4_addr(0), SocketAddrUse::TcpBind).await);
        assert!(!check(public_v4_addr(0), SocketAddrUse::UdpBind).await);
    }

    // ── from_env parsing ────────────────────────────────────────────────

    #[test]
    fn from_env_parses_block_all() {
        assert_eq!(
            "block-all".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::BlockAll
        );
        assert_eq!(
            "BLOCK-ALL".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::BlockAll
        );
    }

    #[test]
    fn from_env_parses_allowlist() {
        assert_eq!(
            "allowlist".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::AllowList
        );
        assert_eq!(
            "ALLOWLIST".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::AllowList
        );
    }

    #[test]
    fn from_env_parses_allow_all() {
        assert_eq!(
            "allow-all".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::AllowAll
        );
        assert_eq!(
            "ALLOW-ALL".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::AllowAll
        );
    }

    #[test]
    fn from_env_rejects_unknown_values() {
        let err = "bogus".parse::<SocketEgressPolicy>().unwrap_err();
        assert!(
            err.contains("bogus"),
            "error message should mention the offending value: {err}"
        );
        assert!(
            err.contains("block-all") && err.contains("allowlist") && err.contains("allow-all"),
            "error message should name the valid options: {err}"
        );
    }

    #[test]
    fn display_matches_from_str_roundtrip() {
        for mode in [
            SocketEgressPolicy::BlockAll,
            SocketEgressPolicy::AllowList,
            SocketEgressPolicy::AllowAll,
        ] {
            assert_eq!(
                mode.to_string().parse::<SocketEgressPolicy>().unwrap(),
                mode
            );
        }
    }

    #[test]
    fn default_is_block_all() {
        assert_eq!(SocketEgressPolicy::default(), SocketEgressPolicy::BlockAll);
    }

    #[test]
    fn from_env_parses_hostname_pinned() {
        // Round-trips through `EDGE_EGRESS_SOCKET_MODE=hostname-pinned`.
        std::env::set_var("EDGE_EGRESS_SOCKET_MODE", "hostname-pinned");
        let parsed = SocketEgressPolicy::from_env();
        std::env::remove_var("EDGE_EGRESS_SOCKET_MODE");
        assert_eq!(parsed, SocketEgressPolicy::HostnamePinned);
    }

    #[test]
    fn from_env_rejects_unknown_values_includes_hostname_pinned() {
        // Ensure the error message advertises the new variant. Operator
        // misconfiguration gotcha: silently falling back to BlockAll is
        // the safe behavior, but a clearer error wins over hidden fail.
        let err = "garbage-mode".parse::<SocketEgressPolicy>().unwrap_err();
        assert!(err.contains("hostname-pinned"));
    }

    #[test]
    fn display_matches_from_str_roundtrip_includes_hostname_pinned() {
        let parsed = "hostname-pinned".parse::<SocketEgressPolicy>().unwrap();
        assert_eq!(parsed.to_string(), "hostname-pinned");
        assert_eq!(parsed, SocketEgressPolicy::HostnamePinned);
    }

    // ── mode dispatch: HostnamePinned (dormant) ─────────────────────────
    //
    // Dormant contract: with an empty cache, HostnamePinned denies every
    // connect-side call regardless of the addr (parity with BlockAll).
    // Once the upstream resolve hook in
    // `docs/upstream-wasmtime-resolve-check.patch` ships, the cache is
    // populated and admit paths light up. These tests pin the dormant
    // contract today; follow-up tests will pin the live contract.

    #[tokio::test]
    async fn hostname_pinned_empty_cache_denies_all_use_variants() {
        let egress = Arc::new(EgressPolicy::allow_all());
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            empty_cache(),
        );
        for use_ in [
            SocketAddrUse::TcpBind,
            SocketAddrUse::UdpBind,
            SocketAddrUse::TcpConnect,
            SocketAddrUse::UdpConnect,
            SocketAddrUse::UdpOutgoingDatagram,
        ] {
            let admitted = check(public_v4_addr(0), use_).await;
            // Binds admit (local-only); connect-side denies.
            match use_ {
                SocketAddrUse::TcpBind | SocketAddrUse::UdpBind => assert!(admitted, "binds admit"),
                _ => assert!(!admitted, "connect-side denies with empty cache"),
            }
        }
    }

    #[tokio::test]
    async fn hostname_pinned_populated_cache_permits_observed_ip() {
        // Populate the cache as the upstream resolve hook would. The
        // observed IP must admit. Other IPs still deny.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let cache = empty_cache();
        cache.record("api.example.com", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            cache,
        );
        // The observed IP under the allowlisted hostname admits.
        assert!(check(public_v4_addr(80), SocketAddrUse::TcpConnect).await);
        // An unrelated IP (not in cache, not under any allowlisted
        // hostname) still denies.
        let other: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 80);
        assert!(!check(other, SocketAddrUse::TcpConnect).await);
    }

    #[tokio::test]
    async fn hostname_pinned_unobserved_ip_is_denied_with_allowlist() {
        // Allowlist present, cache populated under a non-allowlisted
        // hostname → IP must deny (cache key must be allowlist-matched).
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let cache = empty_cache();
        cache.record("evil.com", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            cache,
        );
        assert!(!check(public_v4_addr(80), SocketAddrUse::TcpConnect).await);
    }

    #[tokio::test]
    async fn hostname_pinned_hostname_not_in_allowlist_denies() {
        // Mirror of the unit test above: cache has the IP under an
        // allowlisted hostname, but the connect IP itself is private
        // (hard-deny). Hard-deny short-circuits before the cache
        // lookup, so the answer is `false`.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let cache = empty_cache();
        cache.record("api.example.com", [IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]);
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            cache,
        );
        assert!(!check(loopback_v4_addr(80), SocketAddrUse::TcpConnect).await);
    }
}
