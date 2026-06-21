//! Egress policy — enforces per-tenant outbound HTTP allowlists.
//!
//! Every outbound request from a guest WASM component passes through
//! `EgressPolicy::check` before the host issues it. Two independent
//! layers are applied in order:
//!
//! 1. **Hard-deny**: loopback, link-local (covers cloud metadata services),
//!    private, multicast, broadcast, and unspecified IP ranges are always
//!    blocked, regardless of the allowlist. Same for known metadata hostnames.
//! 2. **Allowlist**: an empty list means default-deny (no outbound traffic).
//!    A non-empty list must contain at least one entry matching the request
//!    host (exact or `*.suffix` wildcard). The sentinel value `"*"` allows
//!    all non-hard-denied hosts and is used only in tests.

use std::net::IpAddr;
use url::{Host, Url};

/// Per-tenant egress policy derived from `AppSpec.allowlist`.
#[derive(Debug, Clone)]
pub struct EgressPolicy {
    /// Allowed destination hosts. Empty = deny all.
    /// Each entry is either an exact hostname (`"api.stripe.com"`) or a
    /// wildcard-suffix pattern (`"*.stripe.com"`).
    /// The special value `"*"` bypasses allowlist matching (test use only).
    allowlist: Vec<String>,
}

impl EgressPolicy {
    /// Create a policy from the tenant's allowlist.
    /// An empty list enforces default-deny.
    /// The `"*"` wildcard sentinel is silently stripped — it is reserved for
    /// the test constructor and must not arrive from external data.
    pub fn new(allowlist: Vec<String>) -> Self {
        let allowlist = allowlist.into_iter().filter(|e| e != "*").collect();
        Self { allowlist }
    }

    /// Unrestricted policy — allows any host that isn't hard-denied.
    /// Used for rolling-upgrade backward compatibility when a TaskMessage has
    /// no `allowlist` field (pre-enforcement control planes) and in tests.
    pub fn allow_all() -> Self {
        Self {
            allowlist: vec!["*".to_string()],
        }
    }

    /// Check whether a resolved IP address is in a hard-deny range.
    ///
    /// Called after DNS resolution to detect rebinding attacks: an allowlisted
    /// hostname may be redirected to a private/metadata IP via a zero-TTL record.
    /// Returns `Err(reason)` if the IP is blocked.
    pub fn check_resolved_ip(&self, ip: IpAddr) -> Result<(), String> {
        if is_blocked_ip(ip) {
            Err(format!(
                "egress denied: hostname resolved to blocked IP {} \
                 (loopback/private/link-local/metadata)",
                ip
            ))
        } else {
            Ok(())
        }
    }

    /// Check whether an outbound request to `url` is permitted.
    ///
    /// Returns `Ok(())` if allowed, `Err(reason)` if denied.
    pub fn check(&self, url: &str) -> Result<(), String> {
        let parsed = Url::parse(url).map_err(|e| format!("egress denied: invalid URL: {}", e))?;

        // Use the typed Host enum so IPv6 addresses are handled correctly.
        // url::Url::host_str() returns "[::1]" (with brackets) for IPv6, which
        // would cause IpAddr::parse to fail. host() gives us the parsed value.
        let host = parsed
            .host()
            .ok_or_else(|| "egress denied: URL has no host".to_string())?;

        // Hard-deny layer: always blocked regardless of allowlist.
        match &host {
            Host::Ipv4(v4) => {
                if is_blocked_ip(IpAddr::V4(*v4)) {
                    return Err(format!(
                        "egress denied: blocked IP address {} (loopback/private/link-local/multicast)",
                        v4
                    ));
                }
            }
            Host::Ipv6(v6) => {
                if is_blocked_ip(IpAddr::V6(*v6)) {
                    return Err(format!(
                        "egress denied: blocked IP address {} (loopback/multicast)",
                        v6
                    ));
                }
            }
            Host::Domain(name) => {
                if is_blocked_hostname(name) {
                    return Err(format!(
                        "egress denied: blocked hostname {} (cloud metadata service)",
                        name
                    ));
                }
            }
        }

        // Resolve the host to a string for allowlist matching.
        // For Domain hosts this is the plain hostname; IP hosts never reach here
        // unless they passed the hard-deny check above (public routable IPs).
        let host_str: String = match &host {
            Host::Domain(name) => name.to_string(),
            Host::Ipv4(v4) => v4.to_string(),
            Host::Ipv6(v6) => v6.to_string(),
        };

        // Allowlist layer: empty list = default-deny.
        if self.allowlist.is_empty() {
            return Err(format!(
                "egress denied: no allowlist configured, outbound traffic is disabled (host: {})",
                host_str
            ));
        }

        // Sentinel: allow everything that passed the hard-deny layer.
        if self.allowlist.iter().any(|e| e == "*") {
            return Ok(());
        }

        // Match each allowlist entry against the request host.
        for entry in &self.allowlist {
            if let Some(bare) = entry.strip_prefix("*.") {
                // Wildcard suffix pattern: *.stripe.com matches api.stripe.com
                // but not a.b.stripe.com or evil-stripe.com.
                let suffix = &entry[1..];
                if host_str == bare {
                    // Bare apex: "stripe.com" matches "*.stripe.com".
                    return Ok(());
                }
                if let Some(label) = host_str.strip_suffix(suffix) {
                    if !label.is_empty() && !label.contains('.') {
                        return Ok(());
                    }
                }
            } else if entry == &host_str {
                return Ok(());
            }
        }

        Err(format!(
            "egress denied: host {} is not in the allowlist",
            host_str
        ))
    }
}

/// Returns `true` for IP addresses that must never be reachable from guest code.
fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_ipv4(v4),
        IpAddr::V6(v6) => {
            // IPv4-mapped IPv6 (::ffff:0:0/96) — apply the IPv4 rules to the
            // embedded address so ::ffff:127.0.0.1 is blocked just like 127.0.0.1.
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_ipv4(mapped);
            }
            v6.is_loopback()            // ::1
            || v6.is_multicast()        // ff00::/8
            || v6.is_unspecified()     // ::
            || v6.is_unique_local()     // fc00::/7 — includes AWS fd00:ec2::254 IMDS
            || v6.is_unicast_link_local() // fe80::/10
        }
    }
}

fn is_blocked_ipv4(v4: std::net::Ipv4Addr) -> bool {
    v4.is_loopback()       // 127.0.0.0/8
    || v4.is_link_local()  // 169.254.0.0/16 — covers 169.254.169.254 (AWS/Azure/GCP metadata)
    || v4.is_private()     // 10.x, 172.16–31.x, 192.168.x
    || v4.is_multicast()   // 224.0.0.0/4
    || v4.is_broadcast()   // 255.255.255.255
    || v4.is_unspecified() // 0.0.0.0
    // Azure Wire Server / IMDS — not in any standard blocked range above.
    || v4 == std::net::Ipv4Addr::new(168, 63, 129, 16)
}

/// Returns `true` for hostnames that resolve to cloud metadata services.
/// These must be blocked even if a tenant somehow adds them to their allowlist.
pub(crate) fn is_blocked_hostname(host: &str) -> bool {
    matches!(
        host,
        "metadata.google.internal" | "instance-data.ec2.internal" | "metadata.azure.internal"
    )
}

#[cfg(test)]
mod tests {
    use super::EgressPolicy;

    // ── hard-deny: IPs ────────────────────────────────────────────────────

    #[test]
    fn loopback_ipv4_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("http://127.0.0.1/").is_err());
        assert!(policy.check("http://127.1.2.3/path").is_err());
    }

    #[test]
    fn loopback_ipv6_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("http://[::1]/").is_err());
    }

    #[test]
    fn link_local_denied() {
        let policy = EgressPolicy::allow_all();
        // AWS / Azure / GCP metadata endpoint
        assert!(policy
            .check("http://169.254.169.254/latest/meta-data/")
            .is_err());
        assert!(policy.check("http://169.254.0.1/").is_err());
    }

    #[test]
    fn private_ranges_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("http://10.0.0.1/").is_err());
        assert!(policy.check("http://192.168.1.1/").is_err());
        assert!(policy.check("http://172.16.0.1/").is_err());
        assert!(policy.check("http://172.31.255.255/").is_err());
    }

    #[test]
    fn ipv4_mapped_ipv6_denied() {
        let policy = EgressPolicy::allow_all();
        // ::ffff:127.0.0.1 — IPv4-mapped loopback
        assert!(policy.check("http://[::ffff:127.0.0.1]/").is_err());
        // ::ffff:10.0.0.1 — IPv4-mapped private
        assert!(policy.check("http://[::ffff:10.0.0.1]/").is_err());
        // ::ffff:169.254.169.254 — IPv4-mapped link-local (metadata)
        assert!(policy.check("http://[::ffff:169.254.169.254]/").is_err());
    }

    #[test]
    fn ipv6_ula_denied() {
        let policy = EgressPolicy::allow_all();
        // fd00:ec2::254 — AWS IMDSv2 IPv6 endpoint (ULA, fc00::/7)
        assert!(policy
            .check("http://[fd00:ec2::254]/latest/meta-data/")
            .is_err());
        assert!(policy.check("http://[fc00::1]/").is_err());
    }

    #[test]
    fn ipv6_link_local_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("http://[fe80::1]/").is_err());
    }

    #[test]
    fn azure_wire_server_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy
            .check("http://168.63.129.16/metadata/instance")
            .is_err());
    }

    #[test]
    fn multicast_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("http://224.0.0.1/").is_err());
        assert!(policy.check("http://239.255.255.255/").is_err());
    }

    #[test]
    fn broadcast_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("http://255.255.255.255/").is_err());
    }

    // ── hard-deny: metadata hostnames ────────────────────────────────────

    #[test]
    fn gcp_metadata_hostname_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy
            .check("http://metadata.google.internal/computeMetadata/v1/")
            .is_err());
    }

    #[test]
    fn ec2_metadata_hostname_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("http://instance-data.ec2.internal/").is_err());
    }

    #[test]
    fn azure_metadata_hostname_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy
            .check("http://metadata.azure.internal/metadata/instance")
            .is_err());
    }

    // ── allowlist: default-deny ───────────────────────────────────────────

    #[test]
    fn empty_allowlist_denies_everything() {
        let policy = EgressPolicy::new(vec![]);
        assert!(policy.check("https://api.stripe.com/v1/charges").is_err());
        assert!(policy.check("https://example.com/").is_err());
    }

    // ── allowlist: exact match ────────────────────────────────────────────

    #[test]
    fn exact_match_allowed() {
        let policy = EgressPolicy::new(vec!["api.stripe.com".to_string()]);
        assert!(policy.check("https://api.stripe.com/v1/charges").is_ok());
    }

    #[test]
    fn exact_match_different_host_denied() {
        let policy = EgressPolicy::new(vec!["api.stripe.com".to_string()]);
        assert!(policy.check("https://evil.com/").is_err());
    }

    #[test]
    fn exact_match_subdomain_denied() {
        let policy = EgressPolicy::new(vec!["stripe.com".to_string()]);
        // "api.stripe.com" does NOT match exact entry "stripe.com"
        assert!(policy.check("https://api.stripe.com/").is_err());
    }

    // ── allowlist: wildcard suffix ───────────────────────────────────────

    #[test]
    fn wildcard_matches_subdomain() {
        let policy = EgressPolicy::new(vec!["*.stripe.com".to_string()]);
        assert!(policy.check("https://api.stripe.com/").is_ok());
        assert!(policy.check("https://dashboard.stripe.com/").is_ok());
    }

    #[test]
    fn wildcard_matches_bare_apex() {
        // "*.stripe.com" should also allow "stripe.com" itself.
        let policy = EgressPolicy::new(vec!["*.stripe.com".to_string()]);
        assert!(policy.check("https://stripe.com/").is_ok());
    }

    #[test]
    fn wildcard_does_not_match_different_domain() {
        let policy = EgressPolicy::new(vec!["*.stripe.com".to_string()]);
        assert!(policy.check("https://evil-stripe.com/").is_err());
        assert!(policy.check("https://notstripe.com/").is_err());
    }

    // ── allowlist: multiple entries ───────────────────────────────────────

    #[test]
    fn multiple_entries_any_match_allowed() {
        let policy = EgressPolicy::new(vec![
            "api.stripe.com".to_string(),
            "*.sendgrid.net".to_string(),
        ]);
        assert!(policy.check("https://api.stripe.com/").is_ok());
        assert!(policy.check("https://smtp.sendgrid.net/").is_ok());
        assert!(policy.check("https://other.com/").is_err());
    }

    #[test]
    fn wildcard_does_not_match_multi_level_subdomain() {
        let policy = EgressPolicy::new(vec!["*.stripe.com".to_string()]);
        // Two levels deep must be denied.
        assert!(policy.check("https://a.b.stripe.com/").is_err());
        assert!(policy.check("https://internal.admin.stripe.com/").is_err());
    }

    #[test]
    fn star_sentinel_stripped_from_new() {
        // EgressPolicy::new(["*"]) should strip the sentinel → empty list → default-deny.
        let policy = EgressPolicy::new(vec!["*".to_string()]);
        assert!(policy.check("https://example.com/").is_err());
    }

    #[test]
    fn star_sentinel_mixed_with_real_entries_stripped() {
        // Mixing "*" with real entries: only real entries survive.
        let policy = EgressPolicy::new(vec!["*".to_string(), "api.stripe.com".to_string()]);
        assert!(policy.check("https://api.stripe.com/").is_ok());
        assert!(policy.check("https://example.com/").is_err());
    }

    // ── allow_all sentinel ───────────────────────────────────────────────

    #[test]
    fn allow_all_passes_public_hosts() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("https://example.com/").is_ok());
        assert!(policy.check("https://api.github.com/").is_ok());
    }

    #[test]
    fn allow_all_still_blocks_private_ips() {
        let policy = EgressPolicy::allow_all();
        // Hard-deny overrides the wildcard sentinel.
        assert!(policy.check("http://192.168.0.1/").is_err());
        assert!(policy.check("http://10.0.0.1/").is_err());
    }

    // ── edge cases ───────────────────────────────────────────────────────

    #[test]
    fn invalid_url_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy.check("not a url").is_err());
        assert!(policy.check("").is_err());
    }

    #[test]
    fn url_without_host_denied() {
        let policy = EgressPolicy::allow_all();
        // "file:///etc/passwd" has no host component.
        assert!(policy.check("file:///etc/passwd").is_err());
    }

    #[test]
    fn https_scheme_works() {
        let policy = EgressPolicy::new(vec!["api.example.com".to_string()]);
        assert!(policy.check("https://api.example.com/v1/resource").is_ok());
    }

    #[test]
    fn port_in_url_ignored_for_host_matching() {
        let policy = EgressPolicy::new(vec!["api.example.com".to_string()]);
        // Port is part of authority but host_str() strips it.
        assert!(policy.check("https://api.example.com:8443/v1/").is_ok());
    }

    // ── check_resolved_ip: DNS rebinding guard ───────────────────────────

    #[test]
    fn resolved_ip_loopback_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy
            .check_resolved_ip("127.0.0.1".parse().unwrap())
            .is_err());
    }

    #[test]
    fn resolved_ip_link_local_denied() {
        let policy = EgressPolicy::allow_all();
        // AWS/GCP/Azure metadata endpoint
        assert!(policy
            .check_resolved_ip("169.254.169.254".parse().unwrap())
            .is_err());
    }

    #[test]
    fn resolved_ip_private_denied() {
        let policy = EgressPolicy::allow_all();
        assert!(policy
            .check_resolved_ip("10.0.0.1".parse().unwrap())
            .is_err());
        assert!(policy
            .check_resolved_ip("192.168.1.1".parse().unwrap())
            .is_err());
    }

    #[test]
    fn resolved_ip_ipv6_ula_denied() {
        let policy = EgressPolicy::allow_all();
        // AWS IMDSv2 IPv6 endpoint
        assert!(policy
            .check_resolved_ip("fd00:ec2::254".parse().unwrap())
            .is_err());
    }

    #[test]
    fn resolved_ip_public_allowed() {
        let policy = EgressPolicy::allow_all();
        assert!(policy
            .check_resolved_ip("93.184.216.34".parse().unwrap())
            .is_ok());
        assert!(policy
            .check_resolved_ip("8.8.8.8".parse().unwrap())
            .is_ok());
    }
}
