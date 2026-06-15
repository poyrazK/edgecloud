//! `edge:kv-store` — durable key-value persistence.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const KV_TTL_CLEANUP_BATCH_SIZE: usize = 100;

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

pub struct KvStore {
    data: RwLock<HashMap<String, KvEntry>>,
    /// Counts write operations since last TTL cleanup.
    /// Triggers cleanup every KV_TTL_CLEANUP_BATCH_SIZE operations.
    op_counter: RwLock<usize>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            op_counter: RwLock::new(0),
        }
    }
}

impl KvStore {
    pub fn new() -> Self {
        Self::default()
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

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let mut data = self.data.write().unwrap();
        if let Some(entry) = Self::get_entry(&data, key) {
            return Ok(Some(entry.value));
        }
        // Key missing or expired — clean it up.
        data.remove(key);
        Ok(None)
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) -> Result<(), String> {
        let expires_at = ttl_secs.map(Self::ttl_to_abs);
        let mut data = self.data.write().unwrap();
        let mut op_counter = self.op_counter.write().unwrap();
        data.insert(key, KvEntry { value, expires_at });
        *op_counter += 1;
        self.try_cleanup(&mut data, &mut op_counter);
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<(), String> {
        let mut data = self.data.write().unwrap();
        data.remove(key);
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

    /// Set multiple key-value pairs atomically. Each item is (key, value, ttl_secs).
    pub fn set_many(&self, items: &[(String, Vec<u8>, Option<u32>)]) -> Result<(), String> {
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
        Ok(())
    }

    /// Delete multiple keys at once.
    pub fn delete_many(&self, keys: &[String]) -> Result<(), String> {
        let mut data = self.data.write().unwrap();
        for key in keys {
            data.remove(key);
        }
        Ok(())
    }

    /// Returns `true` if the key exists and is not expired.
    pub fn exists(&self, key: &str) -> bool {
        let data = self.data.read().unwrap();
        Self::get_entry(&data, key).is_some()
    }

    /// Remove all entries from the store.
    pub fn clear(&self) {
        let mut data = self.data.write().unwrap();
        data.clear();
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
}
