//! `edge:kv-store` — durable key-value persistence.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct KvEntry {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

pub struct KvStore {
    data: RwLock<HashMap<String, KvEntry>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) => {
                if let Some(expires_at) = entry.expires_at {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    if now > expires_at {
                        drop(data);
                        self.delete(key);
                        return None;
                    }
                }
                Some(entry.value.clone())
            }
            None => None,
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
        let mut data = self.data.write().unwrap();
        data.insert(key, KvEntry { value, expires_at });
    }

    pub fn delete(&self, key: &str) {
        let mut data = self.data.write().unwrap();
        data.remove(key);
    }

    pub fn list_keys(&self, prefix: &str) -> Vec<String> {
        let data = self.data.read().unwrap();
        data.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }
}
