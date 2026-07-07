//! Deployment state management.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// State file persisted after a successful deploy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub deployment_id: String,
    pub app_name: String,
    pub live_url: String,
    /// Regions this deployment is replicated to. `#[serde(default)]`
    /// so a state.json written before #82 (no `regions` field) still
    /// deserializes cleanly — the field becomes an empty vec, which
    /// the CLI treats as "use the control plane's default region".
    /// See `commands/deploy::run` and the `--regions` flag.
    #[serde(default)]
    pub regions: Vec<String>,
    /// Desired replica count (issue #316). 0 means no threshold.
    /// `#[serde(default)]` for backward compat with pre-#316 state.json.
    #[serde(default)]
    pub desired_replicas: usize,
}

impl State {
    /// Read state from .edge/state.json in the given project directory.
    pub fn load(path: &Path) -> Result<Self> {
        let path = path.join(".edge").join("state.json");
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Write state to .edge/state.json in the given project directory.
    pub fn save(&self, path: &Path) -> Result<()> {
        let dir = path.join(".edge");
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let path = dir.join("state.json");
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, content)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TestStateRoundTrip_WithRegions pins the JSON wire format of
    // state.json with the new `regions` field populated. Changing the
    // shape is a breaking change for every existing user.
    #[test]
    fn state_round_trip_with_regions() {
        let dir = tempfile::tempdir().unwrap();
        let original = State {
            deployment_id: "d_abc".to_string(),
            app_name: "myapp".to_string(),
            live_url: "https://example.test".to_string(),
            regions: vec!["us-east".to_string(), "eu-west".to_string()],
            desired_replicas: 3,
        };
        original.save(dir.path()).unwrap();
        let loaded = State::load(dir.path()).unwrap();
        assert_eq!(loaded.deployment_id, "d_abc");
        assert_eq!(loaded.app_name, "myapp");
        assert_eq!(loaded.live_url, "https://example.test");
        assert_eq!(loaded.regions, vec!["us-east", "eu-west"]);
        assert_eq!(loaded.desired_replicas, 3);
    }

    // TestStateLoad_LegacyFileWithoutRegions pins the
    // #[serde(default)] contract: a state.json written before the #82
    // schema change (no `regions` field at all) must still load, with
    // regions defaulting to an empty vec. Without this contract, every
    // existing user would see `edge status` break the moment they
    // upgrade.
    #[test]
    fn state_load_legacy_file_without_regions() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".edge")).unwrap();
        // Hand-written JSON, no `regions` field. This is the exact
        // shape a pre-#82 CLI would have written.
        std::fs::write(
            dir.path().join(".edge").join("state.json"),
            r#"{
  "deployment_id": "d_legacy",
  "app_name": "oldapp",
  "live_url": "https://old.test"
}"#,
        )
        .unwrap();

        let loaded = State::load(dir.path()).unwrap();
        assert_eq!(loaded.deployment_id, "d_legacy");
        assert_eq!(loaded.app_name, "oldapp");
        assert_eq!(loaded.live_url, "https://old.test");
        assert!(
            loaded.regions.is_empty(),
            "legacy state.json must default regions to empty, got {:?}",
            loaded.regions
        );
    }

    // TestStateSave_SerializesRegions pins the on-disk shape: the
    // `regions` field must round-trip through serde as a JSON array.
    // This guards against an accidental `#[serde(skip)]` or `omit`
    // tag that would silently drop the new field on save.
    #[test]
    fn state_save_serializes_regions() {
        let dir = tempfile::tempdir().unwrap();
        let state = State {
            deployment_id: "d_x".to_string(),
            app_name: "app".to_string(),
            live_url: "https://x.test".to_string(),
            regions: vec!["us-east".to_string(), "ap-south".to_string()],
            desired_replicas: 5,
        };
        state.save(dir.path()).unwrap();
        let raw = std::fs::read_to_string(dir.path().join(".edge").join("state.json")).unwrap();
        assert!(
            raw.contains("\"regions\""),
            "on-disk JSON missing 'regions' field: {raw}"
        );
        assert!(
            raw.contains("us-east"),
            "on-disk JSON missing region value: {raw}"
        );
        assert!(
            raw.contains("ap-south"),
            "on-disk JSON missing region value: {raw}"
        );
    }
}
