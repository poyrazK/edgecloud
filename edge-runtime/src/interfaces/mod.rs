//! Host function implementations for edge:* WIT interfaces.

/// Returns `true` iff `id` is safe to use as a single directory component.
/// Rejects empty strings, path separators, `.`, `..`, null bytes, colons,
/// and Windows reserved device names (CON, NUL, PRN, AUX, COM1-9, LPT1-9).
pub fn is_safe_tenant_id(id: &str) -> bool {
    if id.is_empty() || id == "." || id == ".." {
        return false;
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') || id.contains(':') {
        return false;
    }
    let upper = id.to_ascii_uppercase();
    if matches!(
        upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    ) {
        return false;
    }
    true
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
