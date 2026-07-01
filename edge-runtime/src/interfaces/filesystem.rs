//! Per-deployment filesystem preopen policy for wasi:filesystem.

use std::io;
use std::path::PathBuf;
use thiserror::Error;

const ENV_FS_SCRATCH_PATH: &str = "EDGE_FS_SCRATCH_PATH";
const ENV_FS_MAX_MB: &str = "EDGE_FS_MAX_MB";
const DEFAULT_MAX_MB: u64 = 512;

#[derive(Debug, Error)]
pub enum FilesystemError {
    #[error("invalid tenant_id {0:?} — contains path-traversal characters")]
    InvalidTenantId(String),
    #[error("invalid deployment_id {0:?} — contains path-traversal characters")]
    InvalidDeploymentId(String),
    #[error("EDGE_FS_SCRATCH_PATH must be an absolute path, got {0:?}")]
    RelativePath(String),
    #[error("deployment scratch dir exceeds EDGE_FS_MAX_MB={0} MB limit")]
    QuotaExceeded(u64),
    #[error("failed to create deployment scratch directory: {0}")]
    Io(#[from] io::Error),
}

/// Returns the per-deployment scratch directory path, creating it if absent.
/// Returns `Ok(None)` when `EDGE_FS_SCRATCH_PATH` is not set (no-FS mode).
/// Returns `Err` for an unsafe id, a relative base path, or IO failures.
pub fn scratch_dir_for_deployment(
    tenant_id: &str,
    deployment_id: &str,
) -> Result<Option<PathBuf>, FilesystemError> {
    // Check env var first: if unset, skip all validation and return None.
    // This avoids spurious errors when EDGE_FS_SCRATCH_PATH is unset (the
    // common case in tests and single-node dev deployments without a scratch FS).
    let base_str = match std::env::var(ENV_FS_SCRATCH_PATH) {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };
    if !super::is_safe_tenant_id(tenant_id) {
        return Err(FilesystemError::InvalidTenantId(tenant_id.to_string()));
    }
    if !super::is_safe_tenant_id(deployment_id) {
        return Err(FilesystemError::InvalidDeploymentId(
            deployment_id.to_string(),
        ));
    }
    let base = PathBuf::from(&base_str);
    if !base.is_absolute() {
        return Err(FilesystemError::RelativePath(base_str));
    }
    let deploy_dir = base.join(tenant_id).join(deployment_id);
    std::fs::create_dir_all(&deploy_dir)?;

    // Pre-admission quota check. Full per-write enforcement requires OS-level
    // quotas; this catches accumulation from prior invocations of the same
    // deployment slot. EDGE_FS_MAX_MB is a per-deployment cap.
    let max_mb = std::env::var(ENV_FS_MAX_MB)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_MB);
    if dir_size_mb(&deploy_dir) > max_mb {
        return Err(FilesystemError::QuotaExceeded(max_mb));
    }

    Ok(Some(deploy_dir))
}

/// Remove the per-deployment scratch directory. Called by the supervisor on
/// app stop and on crash/hung exhaustion to prevent data accumulation.
/// Ignores `NotFound`; silently skips on unsafe IDs or misconfigured paths.
pub fn cleanup_scratch_dir_for_deployment(tenant_id: &str, deployment_id: &str) {
    let base_str = match std::env::var(ENV_FS_SCRATCH_PATH) {
        Ok(p) => p,
        Err(_) => return,
    };
    if !super::is_safe_tenant_id(tenant_id) || !super::is_safe_tenant_id(deployment_id) {
        tracing::warn!(
            tenant_id,
            deployment_id,
            "cleanup_scratch_dir: unsafe ID, skipping to prevent path traversal"
        );
        return;
    }
    let base = PathBuf::from(&base_str);
    if !base.is_absolute() {
        tracing::warn!(
            "cleanup_scratch_dir: EDGE_FS_SCRATCH_PATH {:?} is not absolute, skipping",
            base_str
        );
        return;
    }
    let path = base.join(tenant_id).join(deployment_id);
    if let Err(e) = std::fs::remove_dir_all(&path) {
        if e.kind() != io::ErrorKind::NotFound {
            tracing::warn!("failed to clean scratch dir {:?}: {}", path, e);
        }
    }
}

/// Returns the total size of `path` in MiB, following symlinks.
/// Uses a private byte-accumulating helper to avoid intermediate truncation
/// when summing across subdirectories.
fn dir_size_mb(path: &std::path::Path) -> u64 {
    dir_size_bytes(path) / (1024 * 1024)
}

fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            // Use std::fs::metadata (follows symlinks) rather than
            // entry.metadata() (lstat) so symlinked subtrees count against quota.
            if let Ok(meta) = std::fs::metadata(entry.path()) {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size_bytes(&entry.path());
                }
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;
    use std::io::Write;

    #[test]
    #[serial]
    fn returns_none_when_env_unset() {
        env::remove_var(ENV_FS_SCRATCH_PATH);
        // Empty IDs should also return None when env unset (no path to build).
        assert!(scratch_dir_for_deployment("", "").unwrap().is_none());
        assert!(scratch_dir_for_deployment("t_abc", "d_001")
            .unwrap()
            .is_none());
    }

    #[test]
    #[serial]
    fn rejects_path_traversal_tenant_id() {
        let tmp = tempfile::tempdir().expect("tempdir");
        env::set_var(ENV_FS_SCRATCH_PATH, tmp.path());
        let result = scratch_dir_for_deployment("../evil", "d_001");
        env::remove_var(ENV_FS_SCRATCH_PATH);
        assert!(matches!(result, Err(FilesystemError::InvalidTenantId(_))));
    }

    #[test]
    #[serial]
    fn rejects_path_traversal_deployment_id() {
        let tmp = tempfile::tempdir().expect("tempdir");
        env::set_var(ENV_FS_SCRATCH_PATH, tmp.path());
        let result = scratch_dir_for_deployment("t_abc", "../evil");
        env::remove_var(ENV_FS_SCRATCH_PATH);
        assert!(matches!(
            result,
            Err(FilesystemError::InvalidDeploymentId(_))
        ));
    }

    #[test]
    #[serial]
    fn rejects_windows_reserved_names() {
        let tmp = tempfile::tempdir().expect("tempdir");
        env::set_var(ENV_FS_SCRATCH_PATH, tmp.path());
        for name in ["CON", "NUL", "PRN", "COM1", "LPT9"] {
            let result = scratch_dir_for_deployment(name, "d_001");
            assert!(
                matches!(result, Err(FilesystemError::InvalidTenantId(_))),
                "{name} should be rejected"
            );
        }
        env::remove_var(ENV_FS_SCRATCH_PATH);
    }

    #[test]
    #[serial]
    fn rejects_relative_scratch_path() {
        env::set_var(ENV_FS_SCRATCH_PATH, "relative/path");
        let result = scratch_dir_for_deployment("t_abc", "d_001");
        env::remove_var(ENV_FS_SCRATCH_PATH);
        assert!(matches!(result, Err(FilesystemError::RelativePath(_))));
    }

    #[test]
    #[serial]
    fn creates_per_deployment_dir_when_env_set() {
        let tmp = tempfile::tempdir().expect("tempdir");
        env::set_var(ENV_FS_SCRATCH_PATH, tmp.path());
        let result = scratch_dir_for_deployment("t_tenant1", "d_deploy1").unwrap();
        env::remove_var(ENV_FS_SCRATCH_PATH);
        let dir = result.expect("should return Some");
        assert!(dir.exists(), "deployment dir should be created");
        assert_eq!(dir, tmp.path().join("t_tenant1").join("d_deploy1"));
    }

    #[test]
    #[serial]
    fn cleanup_removes_deployment_dir() {
        let tmp = tempfile::tempdir().expect("tempdir");
        env::set_var(ENV_FS_SCRATCH_PATH, tmp.path());
        scratch_dir_for_deployment("t_abc", "d_002").unwrap();
        let dir = tmp.path().join("t_abc").join("d_002");
        assert!(dir.exists());
        cleanup_scratch_dir_for_deployment("t_abc", "d_002");
        env::remove_var(ENV_FS_SCRATCH_PATH);
        assert!(!dir.exists(), "cleanup should remove the directory");
    }

    #[test]
    #[serial]
    fn cleanup_is_noop_when_env_unset() {
        env::remove_var(ENV_FS_SCRATCH_PATH);
        cleanup_scratch_dir_for_deployment("t_abc", "d_003");
    }

    #[test]
    #[serial]
    fn cleanup_skips_path_traversal_ids() {
        let tmp = tempfile::tempdir().expect("tempdir");
        env::set_var(ENV_FS_SCRATCH_PATH, tmp.path());
        // Should not panic, should not delete anything outside the base.
        cleanup_scratch_dir_for_deployment("../evil", "d_004");
        cleanup_scratch_dir_for_deployment("t_abc", "../evil");
        env::remove_var(ENV_FS_SCRATCH_PATH);
    }

    #[test]
    #[serial]
    fn quota_blocks_subdirectory_spreading() {
        let tmp = tempfile::tempdir().expect("tempdir");
        env::set_var(ENV_FS_SCRATCH_PATH, tmp.path());
        env::set_var(ENV_FS_MAX_MB, "1");

        let deploy_dir = tmp.path().join("t_quota").join("d_sub");
        for i in 0..3 {
            let sub = deploy_dir.join(format!("sub{}", i));
            std::fs::create_dir_all(&sub).unwrap();
            let mut f = std::fs::File::create(sub.join("data.bin")).unwrap();
            f.write_all(&vec![0u8; 900 * 1024]).unwrap();
        }
        let result = scratch_dir_for_deployment("t_quota", "d_sub");
        env::remove_var(ENV_FS_SCRATCH_PATH);
        env::remove_var(ENV_FS_MAX_MB);
        assert!(
            matches!(result, Err(FilesystemError::QuotaExceeded(_))),
            "2.7 MB across 3 subdirs must be caught by 1 MB quota, got: {:?}",
            result
        );
    }
}
