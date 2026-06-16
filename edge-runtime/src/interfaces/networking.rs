//! `edge:networking` — DNS resolution.

use std::sync::Arc;

#[cfg(feature = "networking")]
use crate::interfaces::dns::DnsCache;

#[cfg(feature = "networking")]
pub struct NetworkingState {
    dns_cache: Arc<DnsCache>,
}

#[cfg(feature = "networking")]
impl NetworkingState {
    pub fn new() -> Self {
        Self {
            dns_cache: Arc::new(DnsCache::new(60)),
        }
    }

    /// Expose the DNS cache so [`super::http_client::HttpClient`] can share it.
    pub fn dns_cache(&self) -> Arc<DnsCache> {
        self.dns_cache.clone()
    }

    /// Resolve a hostname to a list of IP addresses.
    pub fn resolve(&self, hostname: &str) -> Result<Vec<String>, String> {
        self.dns_cache.resolve(hostname)
    }
}

impl Default for NetworkingState {
    fn default() -> Self {
        Self::new()
    }
}
