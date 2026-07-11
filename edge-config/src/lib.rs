//! Shared helpers for reading the user's `~/.config/edgecloud/config.toml`.
//!
//! `edge-cli` and `edge-migrate-bin` both need to read the same on-disk
//! config file to discover a saved API key and base URL. Keeping the
//! loaders here means a config-schema change ships in one crate.

use std::path::PathBuf;

/// Return the platform-default config path.
///
/// `dirs::config_dir()` returns the right place on every supported OS:
///   - Linux: `$XDG_CONFIG_HOME` or `$HOME/.config`
///   - macOS: `$HOME/Library/Application Support`
///   - Windows: `%APPDATA%` (Roaming)
///
/// Returns `None` on platforms where the resolution fails.
pub fn config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|p| p.join("edgecloud").join("config.toml"))
}

/// Read the API key with precedence: `EDGE_API_KEY` env var → config file
/// → error. The empty string is treated as unset at every layer.
pub fn read_api_key() -> anyhow::Result<String> {
    if let Ok(k) = std::env::var("EDGE_API_KEY") {
        if !k.is_empty() {
            return Ok(k);
        }
    }
    if let Some(path) = config_path() {
        if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("failed to read {}: {}", path.display(), e))?;
            #[derive(serde::Deserialize)]
            struct Cfg {
                default: DefaultSection,
            }
            #[derive(serde::Deserialize)]
            struct DefaultSection {
                api_key: Option<String>,
            }
            if let Ok(cfg) = toml::from_str::<Cfg>(&content) {
                if let Some(k) = cfg.default.api_key {
                    if !k.is_empty() {
                        return Ok(k);
                    }
                }
            }
        }
    }
    anyhow::bail!("EDGE_API_KEY not set — run `edge auth signup` or `edge auth login` first")
}

/// Read the API base URL with precedence: `EDGE_API_URL` env var → config
/// file → `fallback`. Used by subcommands that have no `edge.toml` to read
/// from (e.g. `edge auth signup`, `edge-migrate`).
pub fn read_api_url(fallback: &str) -> String {
    if let Ok(u) = std::env::var("EDGE_API_URL") {
        if !u.is_empty() {
            return u;
        }
    }
    if let Some(path) = config_path() {
        if let Ok(content) = std::fs::read_to_string(&path) {
            #[derive(serde::Deserialize)]
            struct Cfg {
                default: DefaultSection,
            }
            #[derive(serde::Deserialize)]
            struct DefaultSection {
                api: Option<String>,
            }
            if let Ok(cfg) = toml::from_str::<Cfg>(&content) {
                if let Some(u) = cfg.default.api {
                    if !u.is_empty() {
                        return u;
                    }
                }
            }
        }
    }
    fallback.to_string()
}

/// Read the developer-facing web URL with precedence: `EDGE_WEB_URL` env
/// var → config file → `fallback`. Used by `edge billing portal` to
/// decide where Stripe should send the user when they leave the hosted
/// portal, and by anything else that links out to the operator's
/// user-facing console. Kept distinct from `read_api_url` because the
/// API host and the web console host are usually different subdomains
/// (e.g. `api.edgecloud.dev` vs `edgecloud.dev`).
pub fn read_web_url(fallback: &str) -> String {
    if let Ok(u) = std::env::var("EDGE_WEB_URL") {
        if !u.is_empty() {
            return u;
        }
    }
    if let Some(path) = config_path() {
        if let Ok(content) = std::fs::read_to_string(&path) {
            #[derive(serde::Deserialize)]
            struct Cfg {
                default: DefaultSection,
            }
            #[derive(serde::Deserialize)]
            struct DefaultSection {
                web: Option<String>,
            }
            if let Ok(cfg) = toml::from_str::<Cfg>(&content) {
                if let Some(u) = cfg.default.web {
                    if !u.is_empty() {
                        return u;
                    }
                }
            }
        }
    }
    fallback.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_MUTEX.lock().unwrap()
    }

    fn write_config(dir: &tempfile::TempDir, content: &str) -> std::path::PathBuf {
        let dir_path = dir.path().join("edgecloud");
        std::fs::create_dir_all(&dir_path).unwrap();
        let config_path = dir_path.join("config.toml");
        let mut f = std::fs::File::create(&config_path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.sync_all().unwrap();
        config_path
    }

    #[test]
    fn config_path_returns_something_on_supported_platform() {
        let p = config_path();
        assert!(
            p.is_some(),
            "config_path should return Some on supported platforms"
        );
        let p = p.unwrap();
        assert!(
            p.ends_with("config.toml"),
            "path should end with config.toml, got: {}",
            p.display()
        );
    }

    #[test]
    fn read_api_key_env_var_takes_priority() {
        let _guard = lock_env();
        std::env::set_var("EDGE_API_KEY", "env-key-123");
        let result = read_api_key().unwrap();
        assert_eq!(result, "env-key-123");
        std::env::remove_var("EDGE_API_KEY");
    }

    #[test]
    fn read_api_key_env_var_empty_falls_through_to_config() {
        let _guard = lock_env();
        let dir = tempfile::tempdir().unwrap();
        write_config(&dir, "[default]\napi_key = \"file-key\"\n");
        std::env::set_var("EDGE_API_KEY", "");
        // We can't redirect config_path() in this test without mocking dirs,
        // so when env is empty and config_path points to a real path that
        // doesn't exist, we expect the final error.
        let result = read_api_key();
        std::env::remove_var("EDGE_API_KEY");
        // Falls through to config file — if the real one doesn't exist, error
        assert!(
            result.is_err(),
            "empty env var should fall through, real config likely missing"
        );
    }

    #[test]
    fn read_api_key_env_var_not_set_errors_when_no_config() {
        let _guard = lock_env();
        std::env::remove_var("EDGE_API_KEY");
        let result = read_api_key();
        std::env::remove_var("EDGE_API_KEY");
        assert!(
            result.is_err(),
            "missing env var and no config file should error"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("EDGE_API_KEY"),
            "error should mention EDGE_API_KEY, got: {}",
            msg
        );
    }

    #[test]
    fn read_api_url_env_var_takes_priority() {
        let _guard = lock_env();
        std::env::set_var("EDGE_API_URL", "https://api.example.com");
        let result = read_api_url("https://fallback.example.com");
        assert_eq!(result, "https://api.example.com");
        std::env::remove_var("EDGE_API_URL");
    }

    #[test]
    fn read_api_url_env_var_empty_falls_through() {
        let _guard = lock_env();
        std::env::set_var("EDGE_API_URL", "");
        let result = read_api_url("https://fallback.example.com");
        std::env::remove_var("EDGE_API_URL");
        // Empty env + no real config file → falls through to fallback
        assert_eq!(result, "https://fallback.example.com");
    }

    #[test]
    fn read_api_url_no_env_returns_fallback() {
        let _guard = lock_env();
        std::env::remove_var("EDGE_API_URL");
        let result = read_api_url("https://default.example.com");
        std::env::remove_var("EDGE_API_URL");
        assert_eq!(result, "https://default.example.com");
    }

    #[test]
    fn read_api_url_env_var_is_not_set_uses_fallback() {
        let _guard = lock_env();
        // remove_var before and after to be safe
        std::env::remove_var("EDGE_API_URL");
        let result = read_api_url("fb");
        std::env::remove_var("EDGE_API_URL");
        assert_eq!(result, "fb");
    }

    #[test]
    fn read_web_url_env_var_takes_priority() {
        let _guard = lock_env();
        std::env::set_var("EDGE_WEB_URL", "https://web.example.com");
        let result = read_web_url("https://fallback.example.com");
        assert_eq!(result, "https://web.example.com");
        std::env::remove_var("EDGE_WEB_URL");
    }

    #[test]
    fn read_web_url_env_var_empty_falls_through() {
        let _guard = lock_env();
        std::env::set_var("EDGE_WEB_URL", "");
        let result = read_web_url("https://fallback.example.com");
        std::env::remove_var("EDGE_WEB_URL");
        // Empty env + no real config file → falls through to fallback
        assert_eq!(result, "https://fallback.example.com");
    }

    #[test]
    fn read_web_url_no_env_returns_fallback() {
        let _guard = lock_env();
        std::env::remove_var("EDGE_WEB_URL");
        let result = read_web_url("https://default.example.com");
        std::env::remove_var("EDGE_WEB_URL");
        assert_eq!(result, "https://default.example.com");
    }
}
