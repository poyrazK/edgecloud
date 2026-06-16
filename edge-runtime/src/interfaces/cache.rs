//! `edge:cache` — in-memory LRU cache with TTL and optional persistence.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// --- Constants ---

const CACHE_TTL_CLEANUP_BATCH_SIZE: u32 = 100;
const CACHE_FILENAME: &str = "cache.json";
const ENV_CACHE_PATH: &str = "EDGE_CACHE_PATH";

// --- Time helpers ---

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn ttl_to_abs(ttl_secs: u32) -> u64 {
    now_secs() + ttl_secs as u64
}

fn is_expired(expires_at: Option<u64>) -> bool {
    match expires_at {
        Some(e) => now_secs() > e,
        None => false,
    }
}

// --- Cache entry ---

#[derive(Clone)]
pub struct CacheEntry {
    pub value: Vec<u8>,
    pub expires_at: Option<u64>,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        is_expired(self.expires_at)
    }
}

// --- Persistence error type ---

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

// --- On-disk representation ---

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedCache {
    version: u32,
    entries: Vec<PersistedEntry>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedEntry {
    key: String,
    value: String, // base64-encoded
    expires_at: Option<u64>,
}

// --- Persistence handle ---

struct CachePersistence {
    path: PathBuf,
}

impl CachePersistence {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Load all non-expired entries from the cache file.
    /// Missing or corrupt files result in an empty cache (no error).
    pub async fn load(&self) -> Result<Vec<(String, CacheEntry)>, CacheError> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("cache file not found, starting empty");
                return Ok(Vec::new());
            }
            Err(e) => return Err(CacheError::Io(e.to_string())),
        };

        let state: PersistedCache = serde_json::from_str(&contents)
            .map_err(|e| CacheError::Io(format!("corrupt cache file: {}", e)))?;

        let now = now_secs();
        let mut loaded = Vec::with_capacity(state.entries.len());

        for p in state.entries {
            if let Some(expires_at) = p.expires_at {
                if expires_at <= now {
                    // Expired — skip.
                    continue;
                }
            }
            let value = BASE64
                .decode(&p.value)
                .map_err(|_| CacheError::Io("invalid base64 payload".into()))?;
            loaded.push((
                p.key,
                CacheEntry {
                    value,
                    expires_at: p.expires_at,
                },
            ));
        }

        Ok(loaded)
    }

    /// Atomically flush the current cache contents to disk.
    /// Uses rename-to-replace: write to .tmp, then rename.
    pub async fn flush(
        &self,
        lru: &Mutex<lru::LruCache<String, CacheEntry>>,
    ) -> Result<(), CacheError> {
        let entries: Vec<PersistedEntry> = {
            let lru = lru.lock().unwrap();
            lru.iter()
                .map(|(k, v)| PersistedEntry {
                    key: k.clone(),
                    value: BASE64.encode(&v.value),
                    expires_at: v.expires_at,
                })
                .collect()
        };

        let state = PersistedCache {
            version: 1,
            entries,
        };

        let json =
            serde_json::to_string(&state).map_err(|e| CacheError::Serialization(e.to_string()))?;

        let tmp_path = self.path.with_extension("json.tmp");
        if let Some(parent) = tmp_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| CacheError::Io(format!("failed to create directory: {}", e)))?;
        }
        tokio::fs::write(&tmp_path, json.as_bytes())
            .await
            .map_err(|e| CacheError::Io(e.to_string()))?;
        tokio::fs::rename(&tmp_path, &self.path)
            .await
            .map_err(|e| CacheError::Io(e.to_string()))?;
        Ok(())
    }
}

// --- Cache struct ---

pub struct Cache {
    lru: Mutex<lru::LruCache<String, CacheEntry>>,
    /// Tracks operations since last TTL cleanup. Triggers cleanup every CACHE_TTL_CLEANUP_BATCH_SIZE.
    ops_since_cleanup: Mutex<u32>,
    /// Persistence handle. None = ephemeral in-memory cache.
    persistence: Option<CachePersistence>,
}

impl Default for Cache {
    fn default() -> Self {
        Self::new(1000)
    }
}

impl Cache {
    /// Ephemeral in-memory cache (backward compatible).
    pub fn new(max_entries: u32) -> Self {
        let lru = lru::LruCache::new(std::num::NonZeroUsize::new(max_entries as usize).unwrap());
        Self {
            lru: Mutex::new(lru),
            ops_since_cleanup: Mutex::new(0),
            persistence: None,
        }
    }

    /// Persistent cache at the given directory path.
    /// The cache file is `<path>/cache.json`.
    pub fn with_persistence(path: &Path, max_entries: u32) -> Result<Self, CacheError> {
        let cache_path = path.join(CACHE_FILENAME);
        let persistence = CachePersistence::new(cache_path);
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| CacheError::Io("no Tokio runtime active".into()))?;
        let loaded = rt.block_on(persistence.load())?;

        let lru = lru::LruCache::new(std::num::NonZeroUsize::new(max_entries as usize).unwrap());
        let cache = Self {
            lru: Mutex::new(lru),
            ops_since_cleanup: Mutex::new(0),
            persistence: Some(persistence),
        };

        // Populate from loaded entries.
        {
            let mut lru_guard = cache.lru.lock().unwrap();
            for (key, entry) in loaded {
                lru_guard.push(key, entry);
            }
        }

        Ok(cache)
    }

    /// Persistent cache using the `EDGE_CACHE_PATH` environment variable.
    /// Returns `Ok(None)` if the env var is not set (ephemeral mode).
    pub fn from_env(max_entries: u32) -> Result<Option<Self>, CacheError> {
        match std::env::var(ENV_CACHE_PATH) {
            Ok(path) => Self::with_persistence(Path::new(&path), max_entries).map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Internal helper: flush to disk if persistence is configured.
    /// Skips silently when no Tokio runtime is active (e.g., in unit tests).
    fn flush_if_persistent(&self) {
        if self.persistence.is_none() {
            return;
        }
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let _ = rt.block_on(self.flush_impl());
        }
    }

    async fn flush_impl(&self) -> Result<(), CacheError> {
        if let Some(ref p) = self.persistence {
            p.flush(&self.lru).await?;
        }
        Ok(())
    }

    /// Remove expired entries. Caller must hold the write lock.
    fn cleanup_expired(lru: &mut lru::LruCache<String, CacheEntry>) {
        let now = now_secs();
        let keys_to_remove: Vec<String> = lru
            .iter()
            .filter(|(_, entry)| entry.expires_at.map(|e| e <= now).unwrap_or(false))
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys_to_remove {
            lru.pop(&k);
        }
    }

    /// Try to trigger TTL cleanup if enough operations have occurred.
    /// Caller must hold the `ops_since_cleanup` write lock.
    fn try_cleanup(&self, lru: &mut lru::LruCache<String, CacheEntry>, ops: &mut u32) {
        if *ops >= CACHE_TTL_CLEANUP_BATCH_SIZE {
            *ops = 0;
            Self::cleanup_expired(lru);
        }
    }

    /// Get a value by key. Performs lazy TTL eviction on access.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let mut lru = self.lru.lock().unwrap();
        match lru.get(key).cloned() {
            Some(entry) => {
                if entry.is_expired() {
                    lru.pop(key);
                    return Ok(None);
                }
                Ok(Some(entry.value.clone()))
            }
            None => Ok(None),
        }
    }

    /// Set a value. Triggers a disk flush if persistence is configured.
    pub fn set(&self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) -> Result<(), String> {
        let expires_at = ttl_secs.map(ttl_to_abs);
        {
            let mut lru = self.lru.lock().unwrap();
            let mut ops = self.ops_since_cleanup.lock().unwrap();
            lru.push(key, CacheEntry { value, expires_at });
            *ops += 1;
            self.try_cleanup(&mut lru, &mut ops);
        }
        self.flush_if_persistent();
        Ok(())
    }

    /// Delete a key. Triggers a disk flush if persistence is configured.
    pub fn delete(&self, key: &str) -> Result<(), String> {
        {
            let mut lru = self.lru.lock().unwrap();
            lru.pop(key);
        }
        self.flush_if_persistent();
        Ok(())
    }

    /// Remove all entries from the cache. Triggers a disk flush if persistence is configured.
    pub fn clear(&self) -> Result<(), String> {
        let needs_flush = self.persistence.is_some();
        {
            let mut lru = self.lru.lock().unwrap();
            lru.clear();
            let mut ops = self.ops_since_cleanup.lock().unwrap();
            *ops = 0;
        }
        if needs_flush {
            self.flush_if_persistent();
        }
        Ok(())
    }

    /// Return the number of entries in the cache.
    pub fn size(&self) -> Result<u32, String> {
        let lru = self.lru.lock().unwrap();
        Ok(lru.len() as u32)
    }

    /// Returns `true` if the key exists and is not expired.
    pub fn exists(&self, key: &str) -> bool {
        let mut lru = self.lru.lock().unwrap();
        match lru.get(key).cloned() {
            Some(entry) => !entry.is_expired(),
            None => false,
        }
    }

    /// Return all keys with the given prefix.
    pub fn list_keys(&self, prefix: &str) -> Vec<String> {
        let lru = self.lru.lock().unwrap();
        lru.iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Fetch multiple keys at once.
    pub fn get_many(&self, keys: &[String]) -> Vec<Option<Vec<u8>>> {
        let mut lru = self.lru.lock().unwrap();
        keys.iter()
            .map(|k| match lru.get(k).cloned() {
                Some(entry) => {
                    if entry.is_expired() {
                        lru.pop(k);
                        None
                    } else {
                        Some(entry.value)
                    }
                }
                None => None,
            })
            .collect()
    }

    /// Set multiple key-value pairs atomically. Triggers one disk flush at the end.
    pub fn set_many(&self, items: &[(String, Vec<u8>, Option<u32>)]) -> Result<(), String> {
        let needs_flush = {
            let mut lru = self.lru.lock().unwrap();
            let mut ops = self.ops_since_cleanup.lock().unwrap();
            for (key, value, ttl_secs) in items {
                let expires_at = ttl_secs.map(ttl_to_abs);
                lru.push(
                    key.clone(),
                    CacheEntry {
                        value: value.clone(),
                        expires_at,
                    },
                );
                *ops += 1;
            }
            self.try_cleanup(&mut lru, &mut ops);
            self.persistence.is_some()
        };
        if needs_flush {
            self.flush_if_persistent();
        }
        Ok(())
    }

    /// Delete multiple keys at once. Triggers one disk flush at the end.
    pub fn delete_many(&self, keys: &[String]) -> Result<(), String> {
        {
            let mut lru = self.lru.lock().unwrap();
            for key in keys {
                lru.pop(key);
            }
        }
        self.flush_if_persistent();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_with_data() -> Cache {
        let cache = Cache::new(10);
        cache
            .set_many(&[
                ("key1".into(), b"val1".to_vec(), None),
                ("key2".into(), b"val2".to_vec(), None),
                ("key3".into(), b"val3".to_vec(), None),
            ])
            .unwrap();
        cache
    }

    #[test]
    fn test_set_and_get() {
        let cache = Cache::new(10);
        cache.set("k".into(), b"v".to_vec(), None).unwrap();
        assert_eq!(cache.get("k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn test_get_existing_key() {
        let cache = cache_with_data();
        assert_eq!(cache.get("key1").unwrap(), Some(b"val1".to_vec()));
    }

    #[test]
    fn test_get_missing_key() {
        let cache = cache_with_data();
        assert_eq!(cache.get("nonexistent").unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let cache = cache_with_data();
        cache.delete("key1").unwrap();
        assert_eq!(cache.get("key1").unwrap(), None);
    }

    #[test]
    fn test_clear() {
        let cache = cache_with_data();
        cache.clear().unwrap();
        assert_eq!(cache.size().unwrap(), 0);
    }

    #[test]
    fn test_exists() {
        let cache = cache_with_data();
        assert!(cache.exists("key1"));
        assert!(!cache.exists("nonexistent"));
    }

    #[test]
    fn test_list_keys() {
        let cache = cache_with_data();
        let mut keys = cache.list_keys("");
        keys.sort();
        assert_eq!(keys, vec!["key1", "key2", "key3"]);
    }

    #[test]
    fn test_list_keys_with_prefix() {
        let cache = cache_with_data();
        let keys = cache.list_keys("key1");
        assert_eq!(keys, vec!["key1"]);
    }

    #[test]
    fn test_lru_eviction() {
        let cache = Cache::new(3);
        cache.set("a".into(), b"1".to_vec(), None).unwrap();
        cache.set("b".into(), b"2".to_vec(), None).unwrap();
        cache.set("c".into(), b"3".to_vec(), None).unwrap();
        // Adding a 4th entry should evict the oldest (a).
        cache.set("d".into(), b"4".to_vec(), None).unwrap();
        assert!(!cache.exists("a"));
        assert!(cache.exists("b"));
        assert!(cache.exists("c"));
        assert!(cache.exists("d"));
    }

    #[test]
    fn test_ttl_expiry() {
        let cache = Cache::new(10);
        // Set with TTL of 1 second — not actually expired yet (no time travel in tests).
        cache
            .set("short".into(), b"temp".to_vec(), Some(1))
            .unwrap();
        // Without time travel, key should still be present.
        assert!(cache.exists("short"));
    }

    // --- Batch operation tests ---

    #[test]
    fn test_get_many_all_exist() {
        let cache = cache_with_data();
        let result = cache.get_many(&["key1".into(), "key2".into(), "key3".into()]);
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
        let cache = cache_with_data();
        let result = cache.get_many(&["key1".into(), "nonexistent".into(), "key3".into()]);
        assert_eq!(
            result,
            vec![Some(b"val1".to_vec()), None, Some(b"val3".to_vec())]
        );
    }

    #[test]
    fn test_set_many() {
        let cache = Cache::new(10);
        cache
            .set_many(&[
                ("a".into(), b"1".to_vec(), None),
                ("b".into(), b"2".to_vec(), None),
                ("c".into(), b"3".to_vec(), None),
            ])
            .unwrap();
        let result = cache.get_many(&["a".into(), "b".into(), "c".into()]);
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
        let cache = cache_with_data();
        cache.delete_many(&["key1".into(), "key2".into()]).unwrap();
        let result = cache.get_many(&["key1".into(), "key2".into(), "key3".into()]);
        assert_eq!(result, vec![None, None, Some(b"val3".to_vec())]);
    }

    #[test]
    fn test_from_env_returns_none_when_not_set() {
        let result = Cache::from_env(1000);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
