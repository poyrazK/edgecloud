//! API key management.
//!
//! API keys live in `~/.config/edgecloud/config.toml` (overridable via
//! `EDGE_API_KEY` env var). Writes are atomic — tempfile in the same
//! directory followed by `rename` — and on Unix the file is created with
//! `0o600` permissions so other local users cannot read the secret.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

/// API key — loaded from `EDGE_API_KEY` env var or `~/.config/edgecloud/config.toml`.
#[derive(Debug, Clone)]
pub struct ApiKey(pub String);

impl ApiKey {
    /// Load API key: first `EDGE_API_KEY` env var, then config file.
    pub fn load() -> Result<Self> {
        if let Ok(key) = env::var("EDGE_API_KEY") {
            if !key.is_empty() {
                return Ok(Self(key));
            }
        }

        if let Some(path) = Self::config_path() {
            if path.exists() {
                let content = std::fs::read_to_string(&path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                if let Ok(config) = toml::from_str::<TomlConfig>(&content) {
                    if let Some(key) = config.default.api_key {
                        if !key.is_empty() {
                            return Ok(Self(key));
                        }
                    }
                }
            }
        }

        anyhow::bail!(
            "API key not found: set EDGE_API_KEY env var or run `edge auth signup` / `edge auth login`"
        )
    }

    /// Load API key from the config file only — never the `EDGE_API_KEY`
    /// env var. Used by flows that just wrote a key to disk and need to
    /// validate the on-disk value without an ambient env var shadowing it.
    pub fn load_without_env() -> Result<Self> {
        if let Some(path) = Self::config_path() {
            if path.exists() {
                let content = std::fs::read_to_string(&path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                if let Ok(config) = toml::from_str::<TomlConfig>(&content) {
                    if let Some(key) = config.default.api_key {
                        if !key.is_empty() {
                            return Ok(Self(key));
                        }
                    }
                }
            }
        }
        anyhow::bail!(
            "no API key in config file; run `edge auth signup` or `edge auth login` first"
        )
    }

    /// Persist this key to the user's config file. Creates the parent
    /// directory if it does not exist. Any pre-existing config file is
    /// preserved — only `default.api_key` is overwritten.
    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()
            .ok_or_else(|| anyhow::anyhow!("no config directory available on this platform"))?;
        self.save_to(&path)
    }

    /// Variant of [`save`] that writes to an explicit path. Used by tests
    /// to avoid touching the real `~/.config`.
    pub fn save_to(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create config dir {}", parent.display()))?;
        }

        // If a config file already exists, preserve any other top-level
        // sections and only overwrite `default.api_key`. Otherwise start
        // from a blank document.
        let mut doc: toml::Value = if path.exists() {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            toml::from_str(&content)
                .with_context(|| format!("failed to parse {}", path.display()))?
        } else {
            toml::Value::Table(toml::map::Map::new())
        };

        {
            let root = doc
                .as_table_mut()
                .ok_or_else(|| anyhow::anyhow!("config root is not a table"))?;
            let default = root
                .entry("default".to_string())
                .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
            let default_table = default
                .as_table_mut()
                .ok_or_else(|| anyhow::anyhow!("[default] is not a table"))?;
            default_table.insert("api_key".to_string(), toml::Value::String(self.0.clone()));
        }

        let serialized = toml::to_string(&doc).context("failed to serialize config")?;

        // Atomic write: tempfile in the same directory, fsync, rename.
        // Same-directory rename is atomic on POSIX and on Windows when
        // using `MoveFileEx` (which Rust's `rename` does for files).
        let tmp = path.with_extension("toml.tmp");
        write_file_atomically(&tmp, path, serialized.as_bytes())?;
        Ok(())
    }

    /// Delete `default.api_key` from the config file. Other top-level
    /// sections and other keys under `[default]` are preserved. If the
    /// file does not exist, this is a no-op.
    pub fn clear() -> Result<()> {
        if let Some(path) = Self::config_path() {
            Self::clear_at(&path)?;
        }
        Ok(())
    }

    /// Variant of [`clear`] that targets an explicit path. Used by tests.
    pub fn clear_at(path: &Path) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut doc: toml::Value = toml::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        if let Some(default) = doc.get_mut("default") {
            if let Some(table) = default.as_table_mut() {
                table.remove("api_key");
            }
        }
        let serialized = toml::to_string(&doc).context("failed to serialize config")?;
        let tmp = path.with_extension("toml.tmp");
        write_file_atomically(&tmp, path, serialized.as_bytes())?;
        Ok(())
    }

    /// Returns `~/.config/edgecloud/config.toml` (or the platform
    /// equivalent) if `dirs` can resolve a config directory, else `None`.
    pub fn config_path() -> Option<PathBuf> {
        edge_config::config_path()
    }
}

/// Resolve the API base URL with precedence: `EDGE_API_URL` env var →
/// `~/.config/edgecloud/config.toml` `[default].api` → `fallback`.
///
/// Used by subcommands that have no `edge.toml` to read from (e.g.
/// `edge auth signup`). Delegates to the shared `edge-config` crate
/// so `edge-migrate` and the CLI stay in sync.
pub fn load_api_url(fallback: &str) -> String {
    edge_config::read_api_url(fallback)
}

fn write_file_atomically(tmp: &Path, final_path: &Path, contents: &[u8]) -> Result<()> {
    {
        let mut f = open_with_secure_mode(tmp)?;
        f.write_all(contents)
            .with_context(|| format!("failed to write {}", tmp.display()))?;
        f.sync_all()
            .with_context(|| format!("failed to fsync {}", tmp.display()))?;
    }
    std::fs::rename(tmp, final_path).with_context(|| {
        format!(
            "failed to rename {} -> {}",
            tmp.display(),
            final_path.display()
        )
    })?;
    Ok(())
}

#[cfg(unix)]
fn open_with_secure_mode(path: &Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))
}

#[cfg(not(unix))]
fn open_with_secure_mode(path: &Path) -> Result<std::fs::File> {
    // On non-Unix platforms, mode(0o600) is unavailable. Create with
    // default permissions; users on those platforms should rely on the
    // platform's own user-profile isolation.
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))
}

#[derive(Debug, Deserialize)]
struct TomlConfig {
    default: DefaultSection,
}

#[derive(Debug, Deserialize)]
struct DefaultSection {
    api_key: Option<String>,
    #[allow(dead_code)]
    api: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_to_writes_parseable_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        ApiKey("k_test_value".into()).save_to(&path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: TomlConfig = toml::from_str(&content).unwrap();
        assert_eq!(parsed.default.api_key.as_deref(), Some("k_test_value"));
    }

    #[test]
    fn save_to_preserves_existing_api_url() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[default]\napi = \"https://staging.example\"\n").unwrap();
        ApiKey("k_x".into()).save_to(&path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("api = \"https://staging.example\""));
        assert!(content.contains("api_key = \"k_x\""));
    }

    #[test]
    fn save_to_creates_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("config.toml");
        ApiKey("k_y".into()).save_to(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    #[cfg(unix)]
    fn save_to_applies_0o600_on_unix() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        ApiKey("k_z".into()).save_to(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected 0o600, got {mode:o}");
    }

    #[test]
    fn clear_at_removes_api_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        ApiKey("k_old".into()).save_to(&path).unwrap();
        ApiKey::clear_at(&path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: TomlConfig = toml::from_str(&content).unwrap();
        assert!(parsed.default.api_key.is_none());
    }

    #[test]
    fn clear_at_preserves_other_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[default]\napi_key = \"k_old\"\napi = \"https://staging\"\n\n[other]\nfoo = \"bar\"\n",
        )
        .unwrap();
        ApiKey::clear_at(&path).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(!content.contains("api_key"));
        assert!(content.contains("api = \"https://staging\""));
        assert!(content.contains("[other]"));
        assert!(content.contains("foo = \"bar\""));
    }

    #[test]
    fn clear_at_missing_file_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.toml");
        ApiKey::clear_at(&path).unwrap();
        assert!(!path.exists());
    }
}