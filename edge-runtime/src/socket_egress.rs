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
//!
//! ## HostnamePinned mode (dormant)
//!
//! `SocketEgressPolicy::HostnamePinned` is the **fourth** mode and the
//! one that addresses the documented asymmetry in `AllowList`. It
//! closes the host-bypass by permitting a connect-side destination only
//! if the destination IP was previously observed under a hostname in
//! the tenant's `EgressPolicy::allowlist`. The mechanism uses a per-`RuntimeState`
//! resolution cache ([`HostnamePinning`]) that the host would populate from
//! `wasi:sockets/ip-name-lookup::resolve-addresses` at request time.
//!
//! ### Dormant state
//!
//! The mode is currently **dormant**: the upstream wasmtime-wasi 45.0.3
//! host impl does not call back into the runtime for `resolve-addresses`,
//! so the cache is empty. While dormant, `HostnamePinned` equals
//! `BlockAll` (every connect denied). The runtime-side scaffolding
//! (`HostnamePinning` cache + `Arc` plumbing + 4 dispatch arm) is live;
//! the upstream change at `docs/upstream-wasmtime-resolve-check.patch`
//! will turn it on without further runtime work.
//!
//! Operators opt into the dormant mode today via
//! `EDGE_EGRESS_SOCKET_MODE=hostname-pinned` — this is intentionally a
//! no-op (dormant == BlockAll) so future upgrades land in a single
//! coordinated cutover rather than mid-flight.

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
///   as wasmtime's `SocketAddrCheck::default()` — guests effectively
///   cannot use `wasi:sockets/tcp-connect` / `wasi:sockets/udp-send`.
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
/// - [`HostnamePinned`] (**dormant today**): the closure consults a
///   per-`RuntimeState` resolution cache ([`HostnamePinning`]) that the
///   host impl populates from `wasi:sockets/ip-name-lookup::resolve-addresses`.
///   A connect-side destination IP is permitted only if the cache says
///   it was previously observed under a hostname in the tenant's
///   `EgressPolicy::allowlist`. Binds are always permitted. With an
///   empty cache (the **dormant state** today — the upstream PR is in
///   `docs/upstream-wasmtime-resolve-check.patch`), this mode equals
///   `BlockAll`. Once the upstream PR merges, `HostnamePinned` becomes
///   active without further runtime changes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SocketEgressPolicy {
    #[default]
    BlockAll,
    AllowList,
    AllowAll,
    /// See the module-level `HostnamePinned mode (dormant)` section.
    /// Until the upstream wasmtime-wasi PR in
    /// `docs/upstream-wasmtime-resolve-check.patch` merges, this mode
    /// is dormant — every connect-side call is denied.
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
/// needing a `Mutex` or `OnceLock`. The four modes map to `0/1/2/3`;
/// 255 is the "no previous value logged" sentinel.
fn log_if_changed(mode: SocketEgressPolicy) {
    use std::sync::atomic::{AtomicU8, Ordering};
    static LAST_LOGGED: AtomicU8 = AtomicU8::new(255);
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
        // HostnamePinned is dormant until the upstream PR (see
        // docs/upstream-wasmtime-resolve-check.patch) merges.
        SocketEgressPolicy::HostnamePinned => tracing::info!(
            mode = %mode,
            "edge-runtime socket egress: HostnamePinned mode is dormant until \
             docs/upstream-wasmtime-resolve-check.patch merges"
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

/// Per-`Network` resolution cache that backs the dormant
/// `SocketEgressPolicy::HostnamePinned` mode. Each entry records
/// `(hostname → set of resolved IPs)` as observed by the host impl
/// `wasi:sockets/ip-name-lookup::resolve-addresses`.
///
/// Today the cache is populated manually by tests only (the upstream
/// closure hook documented in `docs/upstream-wasmtime-resolve-check.patch`
/// hasn't been merged yet). When the upstream PR lands, the runtime
/// `Host for WasiSocketsCtxView::resolve_addresses` impl calls
/// [`HostnamePinning::record`] before the upstream resolver runs, and
/// the connect-side closure consults [`HostnamePinning::contains`].
///
/// `Arc<Mutex<...>>` so a fresh `RuntimeState` (per-request in the
/// dispatch path) shares one cache for the lifetime of the dispatch
/// instance. The `Mutex` is short-held (a single hashmap lookup) and
/// the cache is read-mostly; contention is negligible.
#[derive(Debug, Default)]
pub struct HostnamePinning {
    by_hostname: Mutex<HashMap<String, HashSet<IpAddr>>>,
}

impl HostnamePinning {
    /// Record `ips` as observed under `hostname`. The host impl calls
    /// this from `wasi:sockets/ip-name-lookup::resolve-addresses`.
    pub fn record(&self, hostname: &str, ips: impl IntoIterator<Item = IpAddr>) {
        let mut guard = self
            .by_hostname
            .lock()
            .expect("HostnamePinning mutex poisoned");
        guard.entry(hostname.to_string()).or_default().extend(ips);
    }

    /// Returns `true` if `ip` was observed under `hostname`. The
    /// connect-side closure consults this in `HostnamePinned` mode.
    pub fn contains(&self, hostname: &str, ip: IpAddr) -> bool {
        let guard = self
            .by_hostname
            .lock()
            .expect("HostnamePinning mutex poisoned");
        guard.get(hostname).is_some_and(|set| set.contains(&ip))
    }

    /// Snapshot the full cache (for tests + debug logging). Cheap
    /// clone of a `HashMap<String, HashSet<IpAddr>>`.
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
/// `SocketAddrUse` variant. The 4-arm dispatch table:
///
/// | mode            | bind  | connect-side                                          |
/// |-----------------|-------|-------------------------------------------------------|
/// | `BlockAll`      | deny  | deny (closure always `false`)                         |
/// | `AllowAll`      | allow | allow (hard-deny bypassed; operator opt-in)           |
/// | `AllowList`     | allow | `EgressPolicy::check_address(addr)`                   |
/// | `HostnamePinned`| allow | `EgressPolicy::hostname_pinned_match(addr, &cache)`   |
///
/// Denials for both gated modes (`AllowList`, `HostnamePinned`) are
/// logged with `tracing::warn!` in the same shape as
/// `EgressHttpHooks::send_request`. Allows are silent.
///
/// `HostnamePinned` is **dormant today** — the upstream closure hook
/// (`docs/upstream-wasmtime-resolve-check.patch`) hasn't merged, so
/// the cache is empty and the connect-side arm denies everything
/// (behaves like `BlockAll`). The runtime-side machinery is live so
/// the upstream change is the only delta needed to activate it.
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
            // `AllowList` + `HostnamePinned` — binds are local-only,
            // always permitted in either mode. Connect-side is what
            // gets gated.
            (SocketEgressPolicy::AllowList, SocketAddrUse::TcpBind)
            | (SocketEgressPolicy::AllowList, SocketAddrUse::UdpBind)
            | (SocketEgressPolicy::HostnamePinned, SocketAddrUse::TcpBind)
            | (SocketEgressPolicy::HostnamePinned, SocketAddrUse::UdpBind) => {
                Box::pin(async { true })
            }
            // `AllowList` — connect-side consults the policy. Log on
            // deny in the same shape as `EgressHttpHooks::send_request`.
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
            // `HostnamePinned` (dormant) — connect-side consults the
            // cache. Permits iff the cache says this IP was previously
            // observed under a hostname in `egress.allowlist`. Today
            // the cache is empty so this equals `BlockAll`. Denials are
            // logged in the same shape as the `AllowList` connect-side
            // arm above.
            (
                SocketEgressPolicy::HostnamePinned,
                SocketAddrUse::TcpConnect
                | SocketAddrUse::UdpConnect
                | SocketAddrUse::UdpOutgoingDatagram,
            ) => {
                let cache = hostname_pinning.snapshot();
                let egress = egress.clone();
                let tenant_id = tenant_id.clone();
                Box::pin(async move {
                    // Hard-deny always wins (parity with the
                    // AllowList path).
                    if let Err(reason) = egress.check_resolved_ip(addr.ip()) {
                        tracing::warn!(
                            tenant_id = %tenant_id,
                            addr = %addr,
                            use_ = ?use_,
                            reason = %reason,
                            "egress denied (wasi:sockets, hostname-pinned)"
                        );
                        return false;
                    }
                    if egress.hostname_pinned_match(addr, &cache) {
                        return true;
                    }
                    tracing::warn!(
                        tenant_id = %tenant_id,
                        addr = %addr,
                        use_ = ?use_,
                        "egress denied (wasi:sockets, hostname-pinned: not in resolution cache)"
                    );
                    false
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

    /// Helper: empty `HostnamePinning` cache, used as a default 4th
    /// arg in tests that aren't exercising the new
    /// `SocketEgressPolicy::HostnamePinned` arm.
    fn empty_cache() -> Arc<HostnamePinning> {
        Arc::new(HostnamePinning::default())
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
    fn from_env_parses_hostname_pinned() {
        assert_eq!(
            "hostname-pinned".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::HostnamePinned
        );
        assert_eq!(
            "HOSTNAME-PINNED".parse::<SocketEgressPolicy>().unwrap(),
            SocketEgressPolicy::HostnamePinned
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
            err.contains("block-all")
                && err.contains("allowlist")
                && err.contains("allow-all")
                && err.contains("hostname-pinned"),
            "error message should name the valid options: {err}"
        );
    }

    #[test]
    fn display_matches_from_str_roundtrip() {
        for mode in [
            SocketEgressPolicy::BlockAll,
            SocketEgressPolicy::AllowList,
            SocketEgressPolicy::AllowAll,
            SocketEgressPolicy::HostnamePinned,
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

    // ── HostnamePinning cache ──────────────────────────────────────────
    //
    // The cache is dormant until the upstream wasmtime-wasi PR (see
    // docs/upstream-wasmtime-resolve-check.patch) merges. The tests
    // populate it directly via `HostnamePinning::record` to verify the
    // shape and the read API.

    #[test]
    fn hostname_pinning_default_is_empty() {
        let pinning = HostnamePinning::default();
        assert!(!pinning.contains("anything", IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))));
        assert!(pinning.snapshot().is_empty());
    }

    #[test]
    fn hostname_pinning_record_then_contains() {
        let pinning = HostnamePinning::default();
        pinning.record("api.example.com", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        assert!(pinning.contains("api.example.com", IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!pinning.contains("api.example.com", IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        // Different hostname: not found even if the IP matches.
        assert!(!pinning.contains("other.example.com", IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }

    #[test]
    fn hostname_pinning_record_dedupes_ips() {
        // `record` is idempotent for the same IP — multiple
        // `resolve-next-address` calls return the same set.
        let pinning = HostnamePinning::default();
        pinning.record("h", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        pinning.record("h", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        assert_eq!(pinning.snapshot().get("h").unwrap().len(), 1);
    }

    #[test]
    fn hostname_pinning_record_multiple_ips_same_hostname() {
        let pinning = HostnamePinning::default();
        pinning.record(
            "api.example.com",
            [
                IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
                IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)),
            ],
        );
        let snap = pinning.snapshot();
        let set = snap.get("api.example.com").unwrap();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        assert!(set.contains(&IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2))));
    }

    // ── mode dispatch: HostnamePinned (dormant) ───────────────────────────
    //
    // The HostnamePinned mode (issue #309 follow-up) consults the
    // HostnamePinning resolution cache to gate connect-side traffic.
    // Today the upstream PR (see docs/upstream-wasmtime-resolve-check.patch)
    // is unmerged, so the cache is always empty at runtime and this mode
    // behaves identically to `BlockAll` for connect-side calls.
    //
    // The tests below verify the cache + `EgressPolicy::hostname_pinned_match`
    // logic directly so the dispatch arm is exercised end-to-end:
    //
    //   * empty cache → connect-side denied (dormant state)
    //   * populated cache + matching hostname/IP → permit
    //   * populated cache + unmatched IP → deny even with non-empty allowlist
    //   * populated cache + hostname not in allowlist → deny

    #[tokio::test]
    async fn hostname_pinned_empty_cache_denies_all_use_variants() {
        // Dormant state: cache is empty → every connect-side denied
        // (parity with `BlockAll`); binds remain permitted.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let cache = empty_cache();
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            cache,
        );

        // Connect-side: denied.
        assert!(!check(public_v4_addr(80), SocketAddrUse::TcpConnect).await);
        assert!(!check(public_v4_addr(80), SocketAddrUse::UdpConnect).await);
        assert!(!check(public_v4_addr(80), SocketAddrUse::UdpOutgoingDatagram).await);

        // Binds: permitted (local-only, just like AllowList).
        assert!(check(loopback_v4_addr(0), SocketAddrUse::TcpBind).await);
        assert!(check(loopback_v4_addr(0), SocketAddrUse::UdpBind).await);
    }

    #[tokio::test]
    async fn hostname_pinned_populated_cache_permits_observed_ip() {
        // Seed the cache: "api.example.com" was resolved to 8.8.8.8
        // (a public, non-hard-denied IP). Connect to 8.8.8.8:80 →
        // allowed. Allowlist contains "api.example.com" so the
        // wildcard/exact hostname match engages.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let cache = Arc::new(HostnamePinning::default());
        cache.record("api.example.com", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            cache,
        );
        assert!(check(public_v4_addr(80), SocketAddrUse::TcpConnect).await);
        assert!(check(public_v4_addr(80), SocketAddrUse::UdpConnect).await);
        assert!(check(public_v4_addr(80), SocketAddrUse::UdpOutgoingDatagram).await);
    }

    #[tokio::test]
    async fn hostname_pinned_unobserved_ip_is_denied_with_allowlist() {
        // Cache says "api.example.com" → 8.8.8.8 (NOT 1.1.1.1). Connect
        // to 1.1.1.1:80 → denied even though the allowlist contains a
        // matching hostname (the IP wasn't seen under it). This is the
        // core "pinned" semantics: only resolved IPs are admitted.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let cache = Arc::new(HostnamePinning::default());
        cache.record("api.example.com", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            cache,
        );
        let other = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 80);
        assert!(!check(other, SocketAddrUse::TcpConnect).await);
        assert!(!check(other, SocketAddrUse::UdpOutgoingDatagram).await);
    }

    #[tokio::test]
    async fn hostname_pinned_hostname_not_in_allowlist_denies() {
        // Cache says "evil.com" → 8.8.8.8 (so the IP "appears" under a
        // hostname), but the tenant's allowlist contains only
        // "api.example.com". Because `hostname_pinned_match` requires
        // the cache hostname to match an allowlist entry, this connect
        // is denied. The cache cannot grant access for hostnames the
        // tenant did not explicitly opt into.
        let egress = Arc::new(EgressPolicy::new(vec!["api.example.com".to_string()]));
        let cache = Arc::new(HostnamePinning::default());
        cache.record("evil.com", [IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]);
        let check = make_socket_addr_check(
            egress,
            SocketEgressPolicy::HostnamePinned,
            "t_test".to_string(),
            cache,
        );
        assert!(!check(public_v4_addr(80), SocketAddrUse::TcpConnect).await);
    }
}
