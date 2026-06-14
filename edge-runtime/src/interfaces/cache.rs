//! `edge:cache` — in-memory LRU cache with TTL.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct CacheEntry {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

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

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut lru = self.lru.lock().unwrap();
        if let Some(entry) = lru.get(key) {
            if let Some(expires_at) = entry.expires_at {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if now > expires_at {
                    lru.pop(key);
                    return None;
                }
            }
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) {
        let expires_at = ttl_secs.map(|s| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + s as u64
        });
        let mut lru = self.lru.lock().unwrap();
        lru.put(key, CacheEntry { value, expires_at });
    }

    pub fn delete(&self, key: &str) {
        let mut lru = self.lru.lock().unwrap();
        lru.pop(key);
    }

    pub fn clear(&self) {
        let mut lru = self.lru.lock().unwrap();
        lru.clear();
    }

    pub fn size(&self) -> usize {
        self.lru.lock().unwrap().len()
    }
}
