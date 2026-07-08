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

/// Build-time metadata captured at `edge build` time. Serialized to
/// `.edge/build_metadata.json` and uploaded to the control plane in
/// the multipart `build_metadata` form field as part of issue #307
/// PR2 (SLSA L1 provenance). The control plane uses these fields to
/// populate the SLSA provenance envelope's
/// `predicate.invocation.parameters` and `predicate.buildTools[]`
/// entries.
///
/// Optional tooling fields (clang, etc.) are absent for Rust-only
/// toolchains; the envelope's toolchain list then omits those tools.
/// `target` and `profile` come from the project's `edge.toml`'s
/// `[build]` section (default `wasm32-wasip2`/`release`) — see
/// `commands/build.rs`. `source_digest` is computed by hashing the
/// project's source manifest (a sorted, sha256-of-concatenated
/// paths stream) so a downstream verifier can compare against the
/// SLSA `materials[].digest.sha256` entries without trusting the
/// CLI's claim.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildMetadata {
    /// `rustc --version` output, e.g. "rustc 1.82.0 (...)".
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub toolchain_rustc: String,
    /// `cargo --version` output, e.g. "cargo 1.82.0 (...)".
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub toolchain_cargo: String,
    /// `clang --version` first line (when present on PATH) — set
    /// by the C/Rust analyzer preprocessor path inside
    /// `edge-migrate`. Empty for Rust toolchains that don't
    /// invoke clang.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub toolchain_clang: String,
    /// `rustup show active-toolchain` for projects pinned to a
    /// specific toolchain. Empty when the project uses a
    /// system-installed rustc.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub toolchain_rustup: String,
    /// Cargo build target — almost always `wasm32-wasip2`. Empty
    /// for projects that haven't picked a target yet.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    /// Cargo build profile — `dev`, `release`, or a custom name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub profile: String,
    /// Lowercase hex SHA-256 over the project's source bytes —
    /// see `commands/build.rs::compute_source_digest`. Empty when
    /// the build was invoked without source hashing (e.g. a
    /// build of a pre-supplied artifact).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_digest: String,
    /// `start_build`'s first call to `Instant::now`, in
    /// RFC3339 / ISO8601. Empty for legacy builds.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub build_started_on: String,
}

impl BuildMetadata {
    /// Path to the canonical on-disk location: `<project>/.edge/build_metadata.json`.
    pub fn path_in(project_dir: &Path) -> std::path::PathBuf {
        project_dir.join(".edge").join("build_metadata.json")
    }

    /// Read build metadata from `.edge/build_metadata.json`. Returns
    /// `Ok(None)` when the file is absent — the deploy path treats
    /// absent metadata as "no toolchain info available; build an
    /// envelope with empty fields" rather than refusing the
    /// deploy.
    pub fn load_opt(project_dir: &Path) -> Result<Option<Self>> {
        let path = Self::path_in(project_dir);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let parsed = serde_json::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(Some(parsed))
    }

    /// Write build metadata to `.edge/build_metadata.json`,
    /// creating the `.edge` directory if necessary. Overwrites
    /// any prior file unconditionally — a fresh build always
    /// supersedes the previous metadata.
    pub fn save(&self, project_dir: &Path) -> Result<()> {
        let dir = project_dir.join(".edge");
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let path = dir.join("build_metadata.json");
        let content =
            serde_json::to_string_pretty(self).with_context(|| "serialize BuildMetadata")?;
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

    // ─────────────────────────────────────────────────────────────────────
    // BuildMetadata — issue #307 PR2 (SLSA L1 provenance)
    // ─────────────────────────────────────────────────────────────────────

    // TestBuildMetadata_RoundTrip pins the wire format of
    // build_metadata.json — every optional toolchain field
    // round-trips cleanly. Skipping empty fields keeps the JSON
    // compact and means the control plane sees `null` / missing
    // for unset entries (handled by Go's `omitempty`).
    #[test]
    fn build_metadata_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let bm = BuildMetadata {
            toolchain_rustc: "rustc 1.82.0 (a long string)".to_string(),
            toolchain_cargo: "cargo 1.82.0".to_string(),
            toolchain_clang: String::new(),
            toolchain_rustup: String::new(),
            target: "wasm32-wasip2".to_string(),
            profile: "release".to_string(),
            source_digest: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
                .to_string(),
            build_started_on: "2026-07-08T10:00:00Z".to_string(),
        };
        bm.save(dir.path()).unwrap();
        let loaded = BuildMetadata::load_opt(dir.path())
            .unwrap()
            .expect("metadata must exist");
        assert_eq!(loaded.toolchain_rustc, "rustc 1.82.0 (a long string)");
        assert_eq!(loaded.target, "wasm32-wasip2");
        assert_eq!(loaded.profile, "release");
        assert_eq!(loaded.source_digest.len(), 64);
        assert_eq!(loaded.build_started_on, "2026-07-08T10:00:00Z");
        assert!(loaded.toolchain_clang.is_empty());
        assert!(loaded.toolchain_rustup.is_empty());
    }

    // TestBuildMetadata_LoadOpt_Absent returns None (not an error)
    // when the file does not exist — a fresh project that hasn't
    // built yet has no build_metadata.json, and `edge deploy`
    // treats this as "no toolchain info available" rather than
    // refusing to deploy.
    #[test]
    fn build_metadata_load_opt_absent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = BuildMetadata::load_opt(dir.path()).unwrap();
        assert!(loaded.is_none(), "absent file should yield None");
    }

    // TestBuildMetadata_EmptyToolchainOmittedFromJSON pins the
    // skip_serializing_if contract: the on-disk JSON omits fields
    // whose value is "", keeping the file minimal and keeping the
    // server-side `omitempty` mirror in sync.
    #[test]
    fn build_metadata_empty_toolchain_omitted_from_json() {
        let dir = tempfile::tempdir().unwrap();
        let bm = BuildMetadata {
            toolchain_rustc: "rustc 1.82.0".to_string(),
            toolchain_cargo: "cargo 1.82.0".to_string(),
            target: "wasm32-wasip2".to_string(),
            profile: "release".to_string(),
            source_digest: "abcd".to_string(),
            build_started_on: "2026-07-08T10:00:00Z".to_string(),
            ..Default::default()
        };
        bm.save(dir.path()).unwrap();
        let raw = std::fs::read_to_string(BuildMetadata::path_in(dir.path())).unwrap();
        assert!(raw.contains("toolchain_rustc"), "json: {raw}");
        assert!(
            !raw.contains("toolchain_clang"),
            "json: {raw} should omit empty clang"
        );
        assert!(
            !raw.contains("toolchain_rustup"),
            "json: {raw} should omit empty rustup"
        );
    }
}
