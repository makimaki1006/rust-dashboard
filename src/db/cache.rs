use dashmap::DashMap;
use serde_json::Value;
use std::time::{Duration, Instant};

/// TTL付きキャッシュエントリ
#[derive(Clone)]
struct CacheEntry {
    data: Value,
    expires_at: Instant,
}

/// DashMapベースのスレッドセーフキャッシュ
#[derive(Clone)]
pub struct AppCache {
    map: DashMap<String, CacheEntry>,
    ttl: Duration,
    max_entries: usize,
}

impl AppCache {
    pub fn new(ttl_secs: u64, max_entries: usize) -> Self {
        Self {
            map: DashMap::new(),
            ttl: Duration::from_secs(ttl_secs),
            max_entries,
        }
    }

    /// キャッシュから取得（有効期限切れはNone）
    pub fn get(&self, key: &str) -> Option<Value> {
        if let Some(entry) = self.map.get(key) {
            if Instant::now() < entry.expires_at {
                return Some(entry.data.clone());
            }
            // 有効期限切れ → 削除
            drop(entry);
            self.map.remove(key);
        }
        None
    }

    /// キャッシュに格納
    pub fn set(&self, key: String, data: Value) {
        // max_entries超過時は古いエントリを削除
        if self.map.len() >= self.max_entries {
            self.evict_expired();
        }

        self.map.insert(
            key,
            CacheEntry {
                data,
                expires_at: Instant::now() + self.ttl,
            },
        );
    }

    /// 期限切れエントリを削除
    fn evict_expired(&self) {
        let now = Instant::now();
        self.map.retain(|_, entry| now < entry.expires_at);
    }

    /// キャッシュクリア
    pub fn clear(&self) {
        self.map.clear();
    }

    /// エントリ数
    pub fn len(&self) -> usize {
        self.map.len()
    }
}
