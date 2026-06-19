//! Host function implementations for edge:* WIT interfaces.

/// Returns `true` iff `id` is safe to use as a single directory component.
/// Rejects empty strings, path separators, `.`, `..`, null bytes, and colons.
pub fn is_safe_tenant_id(id: &str) -> bool {
    !id.is_empty()
        && id != "."
        && id != ".."
        && !id.contains('/')
        && !id.contains('\\')
        && !id.contains('\0')
        && !id.contains(':')
}

#[cfg(feature = "cache")]
pub mod cache;
#[cfg(feature = "networking")]
pub mod dns;
#[cfg(feature = "http-client")]
pub mod http_client;
#[cfg(feature = "http-server")]
pub mod http_server;
#[cfg(feature = "kv-store")]
pub mod kv_store;
#[cfg(feature = "networking")]
pub mod networking;
#[cfg(feature = "observe")]
pub mod observe;
#[cfg(feature = "process")]
pub mod process;
#[cfg(feature = "scheduling")]
pub mod scheduling;
#[cfg(feature = "time")]
pub mod time;
