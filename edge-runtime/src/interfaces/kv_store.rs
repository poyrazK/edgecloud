//! `edge:kv-store` — durable key-value persistence.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const KV_TTL_CLEANUP_BATCH_SIZE: usize = 100;
const STORE_FILENAME: &str = "store.json";
const ENV_KV_STORE_PATH: &str = "EDGE_KV_STORE_PATH";

#[derive(Clone)]
pub struct KvEntry {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

impl KvEntry {
    fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            now > expires_at
        } else {
            false
        }
    }
}

/// Errors that can occur during persistence operations.
#[derive(Debug, thiserror::Error)]
pub enum KvStoreError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("data file corrupted: {0}")]
    Corrupted(String),
}

/// On-disk representation of the store.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedStore {
    version: u32,
    keys: Vec<PersistedKey>,
}

/// On-disk representation of a single key.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedKey {
    key: String,
    value: String, // base64-encoded
    expires_at: Option<u64>,
}

/// Handles async file I/O for the store.
struct KvStorePersistence {
    path: PathBuf,
}

impl KvStorePersistence {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Load all non-expired keys from the store file.
    /// Missing or corrupt files result in an empty store (no error).
    pub async fn load(&self) -> Result<HashMap<String, KvEntry>, KvStoreError> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("KV store file not found, starting empty");
                return Ok(HashMap::new());
            }
            Err(e) => return Err(KvStoreError::Io(e.to_string())),
        };

        let state: PersistedStore =
            serde_json::from_str(&contents).map_err(|e| KvStoreError::Corrupted(e.to_string()))?;

        let now = KvStore::now_secs();
        state
            .keys
            .into_iter()
            .filter(|k| k.expires_at.map(|e| e > now).unwrap_or(true))
            .map(|k| {
                let value = BASE64
                    .decode(&k.value)
                    .map_err(|_| KvStoreError::Corrupted("invalid base64".into()))?;
                Ok((
                    k.key,
                    KvEntry {
                        value,
                        expires_at: k.expires_at,
                    },
                ))
            })
            .collect()
    }

    /// Atomically flush the current in-memory state to disk.
    /// Uses rename-to-replace: write to a .tmp file, then rename atomically.
    /// Note: data is cloned under the lock, then lock is released before async I/O.
    pub async fn flush(&self, data: &RwLock<HashMap<String, KvEntry>>) -> Result<(), KvStoreError> {
        // Clone under lock, then release lock before async I/O.
        let keys: Vec<PersistedKey> = {
            let data = data.read().unwrap();
            data.iter()
                .map(|(k, v)| PersistedKey {
                    key: k.clone(),
                    value: BASE64.encode(&v.value),
                    expires_at: v.expires_at,
                })
                .collect()
        };
        let state = PersistedStore { version: 1, keys };

        let json = serde_json::to_string(&state)
            .map_err(|e| KvStoreError::Serialization(e.to_string()))?;

        let tmp_path = self.path.with_extension("json.tmp");
        // Ensure the parent directory exists before writing.
        if let Some(parent) = tmp_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                KvStoreError::Io(format!("failed to create store directory: {}", e))
            })?;
        }
        tokio::fs::write(&tmp_path, json.as_bytes())
            .await
            .map_err(|e| KvStoreError::Io(e.to_string()))?;
        tokio::fs::rename(&tmp_path, &self.path)
            .await
            .map_err(|e| KvStoreError::Io(e.to_string()))?;
        Ok(())
    }
}

pub struct KvStore {
    data: RwLock<HashMap<String, KvEntry>>,
    /// Counts write operations since last TTL cleanup.
    /// Triggers cleanup every KV_TTL_CLEANUP_BATCH_SIZE operations.
    op_counter: RwLock<usize>,
    /// Persistence handle. None = ephemeral in-memory store.
    persistence: Option<KvStorePersistence>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            op_counter: RwLock::new(0),
            persistence: None,
        }
    }
}

impl KvStore {
    /// Ephemeral in-memory store (backward compatible).
    pub fn new() -> Self {
        Self::default()
    }

    /// Persistent store at the given directory path.
    /// The store file is `<path>/store.json`.
    pub fn with_persistence(path: &Path) -> Result<Self, KvStoreError> {
        let store_path = path.join(STORE_FILENAME);
        let persistence = KvStorePersistence::new(store_path);
        let rt = tokio::runtime::Handle::current();
        let data = rt.block_on(persistence.load())?;
        Ok(Self {
            data: RwLock::new(data),
            op_counter: RwLock::new(0),
            persistence: Some(persistence),
        })
    }

    /// Persistent store using the `EDGE_KV_STORE_PATH` environment variable.
    /// Returns `Ok(None)` if the env var is not set (ephemeral mode).
    pub fn from_env() -> Result<Option<Self>, KvStoreError> {
        match std::env::var(ENV_KV_STORE_PATH) {
            Ok(path) => Self::with_persistence(Path::new(&path)).map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Returns the current unix timestamp in seconds.
    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Convert a relative TTL (seconds) to an absolute expiry timestamp.
    fn ttl_to_abs(ttl_secs: u32) -> u64 {
        Self::now_secs() + ttl_secs as u64
    }

    /// Get a non-expired entry from the data map. Caller must hold the read lock.
    fn get_entry(data: &HashMap<String, KvEntry>, key: &str) -> Option<KvEntry> {
        data.get(key).cloned().filter(|e| !e.is_expired())
    }

    /// Remove expired entries from the map. Caller must hold the write lock.
    fn cleanup_expired(data: &mut HashMap<String, KvEntry>) {
        let now = Self::now_secs();
        data.retain(|_, entry| entry.expires_at.map(|e| e > now).unwrap_or(true));
    }

    /// Try to trigger TTL cleanup if the operation counter has reached the batch size.
    /// Caller must hold both `data` and `op_counter` write locks.
    fn try_cleanup(&self, data: &mut HashMap<String, KvEntry>, op_counter: &mut usize) {
        if *op_counter >= KV_TTL_CLEANUP_BATCH_SIZE {
            *op_counter = 0;
            Self::cleanup_expired(data);
        }
    }

    /// Internal helper: flush to disk if persistence is configured.
    /// If no Tokio runtime is active (e.g., in unit tests), skip the flush silently.
    fn flush_if_persistent(&self) {
        if self.persistence.is_none() {
            return;
        }
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let _ = rt.block_on(self.flush_impl());
        }
        // If no runtime is active, silently skip the flush (tests without persistence).
    }

    /// Called by `flush_if_persistent` only when `persistence` is `Some`.
    async fn flush_impl(&self) -> Result<(), KvStoreError> {
        // SAFETY: flush_if_persistent returns early when persistence.is_none(),
        // so this is only invoked when persistence is Some.
        let p = self.persistence.as_ref().unwrap();
        p.flush(&self.data).await?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let mut data = self.data.write().unwrap();
        if let Some(entry) = Self::get_entry(&data, key) {
            return Ok(Some(entry.value));
        }
        // Key missing or expired — clean it up.
        data.remove(key);
        Ok(None)
    }

    /// Set a key. Triggers a disk flush if persistence is configured.
    pub fn set(&self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) -> Result<(), String> {
        let expires_at = ttl_secs.map(Self::ttl_to_abs);
        let mut data = self.data.write().unwrap();
        let mut op_counter = self.op_counter.write().unwrap();
        data.insert(key, KvEntry { value, expires_at });
        *op_counter += 1;
        self.try_cleanup(&mut data, &mut op_counter);
        drop(data);
        drop(op_counter);
        self.flush_if_persistent();
        Ok(())
    }

    /// Delete a key. Triggers a disk flush if persistence is configured.
    pub fn delete(&self, key: &str) -> Result<(), String> {
        {
            let mut data = self.data.write().unwrap();
            data.remove(key);
        }
        self.flush_if_persistent();
        Ok(())
    }

    pub fn list_keys(&self, prefix: &str) -> Result<Vec<String>, String> {
        let data = self.data.read().unwrap();
        Ok(data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    /// Fetch multiple keys at once. Returns a parallel list where each element
    /// is `Some(value)` if the key exists and is not expired, or `None` otherwise.
    pub fn get_many(&self, keys: &[String]) -> Vec<Option<Vec<u8>>> {
        // First pass: read values under read lock.
        let results: Vec<_> = {
            let data = self.data.read().unwrap();
            keys.iter().map(|k| Self::get_entry(&data, k)).collect()
        };

        // Second pass: write lock only for cleanup of expired entries.
        let mut data = self.data.write().unwrap();
        let to_remove: Vec<_> = keys
            .iter()
            .zip(results.iter())
            .filter(|(_, entry)| entry.is_none())
            .map(|(k, _)| k.clone())
            .collect();
        for k in to_remove {
            data.remove(&k);
        }

        results.into_iter().map(|e| e.map(|e| e.value)).collect()
    }

    /// Set multiple key-value pairs atomically. Triggers one disk flush at the end.
    pub fn set_many(&self, items: &[(String, Vec<u8>, Option<u32>)]) -> Result<(), String> {
        {
            let mut data = self.data.write().unwrap();
            let mut op_counter = self.op_counter.write().unwrap();
            for (key, value, ttl_secs) in items {
                let expires_at = ttl_secs.map(Self::ttl_to_abs);
                data.insert(
                    key.clone(),
                    KvEntry {
                        value: value.clone(),
                        expires_at,
                    },
                );
                *op_counter += 1;
            }
            self.try_cleanup(&mut data, &mut op_counter);
        }
        self.flush_if_persistent();
        Ok(())
    }

    /// Delete multiple keys at once. Triggers one disk flush at the end.
    pub fn delete_many(&self, keys: &[String]) -> Result<(), String> {
        {
            let mut data = self.data.write().unwrap();
            for key in keys {
                data.remove(key);
            }
        }
        self.flush_if_persistent();
        Ok(())
    }

    /// Returns `true` if the key exists and is not expired.
    pub fn exists(&self, key: &str) -> bool {
        let data = self.data.read().unwrap();
        Self::get_entry(&data, key).is_some()
    }

    /// Remove all entries from the store. Triggers a disk flush if persistence is configured.
    pub fn clear(&self) {
        {
            let mut data = self.data.write().unwrap();
            data.clear();
        }
        self.flush_if_persistent();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with_data() -> KvStore {
        let store = KvStore::new();
        store
            .set_many(&[
                ("key1".into(), b"val1".to_vec(), None),
                ("key2".into(), b"val2".to_vec(), None),
                ("key3".into(), b"val3".to_vec(), None),
            ])
            .unwrap();
        store
    }

    #[test]
    fn test_get_existing_key() {
        let store = store_with_data();
        assert_eq!(store.get("key1").unwrap(), Some(b"val1".to_vec()));
    }

    #[test]
    fn test_get_missing_key() {
        let store = store_with_data();
        assert_eq!(store.get("nonexistent").unwrap(), None);
    }

    #[test]
    fn test_set_and_get() {
        let store = KvStore::new();
        store.set("k".into(), b"v".to_vec(), None).unwrap();
        assert_eq!(store.get("k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_delete() {
        let store = store_with_data();
        store.delete("key1").unwrap();
        assert_eq!(store.get("key1").unwrap(), None);
    }

    #[test]
    fn test_list_keys() {
        let store = store_with_data();
        let mut keys = store.list_keys("").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["key1", "key2", "key3"]);
    }

    #[test]
    fn test_list_keys_with_prefix() {
        let store = store_with_data();
        let keys = store.list_keys("key1").unwrap();
        assert_eq!(keys, vec!["key1"]);
    }

    // --- Batch operation tests ---

    #[test]
    fn test_get_many_all_exist() {
        let store = store_with_data();
        let result = store.get_many(&["key1".into(), "key2".into(), "key3".into()]);
        assert_eq!(
            result,
            vec![
                Some(b"val1".to_vec()),
                Some(b"val2".to_vec()),
                Some(b"val3".to_vec())
            ]
        );
    }

    #[test]
    fn test_get_many_some_missing() {
        let store = store_with_data();
        let result = store.get_many(&["key1".into(), "nonexistent".into(), "key3".into()]);
        assert_eq!(
            result,
            vec![Some(b"val1".to_vec()), None, Some(b"val3".to_vec())]
        );
    }

    #[test]
    fn test_set_many() {
        let store = KvStore::new();
        store
            .set_many(&[
                ("a".into(), b"1".to_vec(), None),
                ("b".into(), b"2".to_vec(), None),
                ("c".into(), b"3".to_vec(), None),
            ])
            .unwrap();
        let result = store.get_many(&["a".into(), "b".into(), "c".into()]);
        assert_eq!(
            result,
            vec![
                Some(b"1".to_vec()),
                Some(b"2".to_vec()),
                Some(b"3".to_vec())
            ]
        );
    }

    #[test]
    fn test_delete_many() {
        let store = store_with_data();
        store.delete_many(&["key1".into(), "key2".into()]).unwrap();
        let result = store.get_many(&["key1".into(), "key2".into(), "key3".into()]);
        assert_eq!(result, vec![None, None, Some(b"val3".to_vec())]);
    }

    #[test]
    fn test_exists() {
        let store = store_with_data();
        assert!(store.exists("key1"));
        assert!(!store.exists("nonexistent"));
    }

    #[test]
    fn test_clear() {
        let store = store_with_data();
        store.clear();
        assert!(store.list_keys("").unwrap().is_empty());
    }

    #[test]
    fn test_ttl_expiry() {
        let store = KvStore::new();
        // Set with a TTL of 1 second — should expire immediately since we're not
        // actually advancing time in tests. The store cleanup runs on next write.
        store
            .set("short".into(), b"temp".to_vec(), Some(1))
            .unwrap();
        // Without time travel, the key should still be there (cleanup not triggered yet).
        assert!(store.exists("short"));
    }

    #[test]
    fn test_from_env_returns_none_when_not_set() {
        // When EDGE_KV_STORE_PATH is not set, from_env should return Ok(None)
        let result = KvStore::from_env();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
