//! `edge:cache` — in-memory LRU cache with TTL.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct CacheEntry {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

#[allow(clippy::new_without_default)]
pub struct Cache {
    lru: Mutex<lru::LruCache<String, CacheEntry>>,
}

impl Cache {
    pub fn new(max_entries: u32) -> Self {
        let lru = lru::LruCache::new(std::num::NonZeroUsize::new(max_entries as usize).unwrap());
        Self {
            lru: Mutex::new(lru),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let mut lru = self.lru.lock().unwrap();
        match lru.get(key).cloned() {
            Some(entry) => {
                if let Some(expires_at) = entry.expires_at {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    if now > expires_at {
                        lru.pop(key);
                        return Ok(None);
                    }
                }
                Ok(Some(entry.value.clone()))
            }
            None => Ok(None),
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) -> Result<(), String> {
        let expires_at = ttl_secs.map(|s| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + s as u64
        });
        let mut lru = self.lru.lock().unwrap();
        lru.put(key, CacheEntry { value, expires_at });
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<(), String> {
        let mut lru = self.lru.lock().unwrap();
        lru.pop(key);
        Ok(())
    }

    pub fn clear(&self) -> Result<(), String> {
        let mut lru = self.lru.lock().unwrap();
        lru.clear();
        Ok(())
    }

    pub fn size(&self) -> Result<u32, String> {
        let lru = self.lru.lock().unwrap();
        Ok(lru.len() as u32)
    }
}
