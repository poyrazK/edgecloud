//! Shared DNS cache — TTL-based, thread-safe.
//!
//! Used by both [`NetworkingState`](super::networking::NetworkingState) (WIT-bound `resolve()`)
//! and [`HttpClient`](super::http_client::HttpClient) (internal connection pooling).

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};
use trust_dns_resolver::TokioAsyncResolver;

struct CachedEntry {
    ips: Vec<String>,
    expires_at: Instant,
}

/// Thread-safe TTL DNS cache wrapping `TokioAsyncResolver`.
pub struct DnsCache {
    resolver: TokioAsyncResolver,
    entries: RwLock<HashMap<String, CachedEntry>>,
    ttl: Duration,
}

impl DnsCache {
    /// Create a new DnsCache with the given TTL in seconds.
    pub fn new(ttl_secs: u64) -> Self {
        let resolver =
            TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
        Self {
            resolver,
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Resolve a hostname, using the cache if a valid entry exists.
    /// Thread-safe. If no Tokio runtime is active, skips cache and returns an error.
    pub fn resolve(&self, hostname: &str) -> Result<Vec<String>, String> {
        // Fast path: check cache under read lock
        let now = Instant::now();
        {
            let entries = self.entries.read().unwrap();
            if let Some(entry) = entries.get(hostname) {
                if now < entry.expires_at {
                    return Ok(entry.ips.clone());
                }
            }
        }

        // Slow path: resolve and cache. Requires a Tokio runtime.
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.block_on(self.resolve_async(hostname))
        } else {
            Err("no Tokio runtime active".into())
        }
    }

    async fn resolve_async(&self, hostname: &str) -> Result<Vec<String>, String> {
        let ips: Vec<String> = self
            .resolver
            .lookup_ip(hostname)
            .await
            .map(|lookup| lookup.iter().map(|ip| ip.to_string()).collect())
            .map_err(|e| format!("DNS resolution failed: {}", e))?;

        let expires_at = Instant::now() + self.ttl;
        let mut entries = self.entries.write().unwrap();
        entries.insert(
            hostname.to_string(),
            CachedEntry {
                ips: ips.clone(),
                expires_at,
            },
        );

        Ok(ips)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dns_cache_creation() {
        let _cache = DnsCache::new(60);
        // Should not panic — basic smoke test
    }
}
