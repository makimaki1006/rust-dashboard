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

#[cfg(test)]
mod tests {
    use super::*;

    // テスト46: set/get → 同一値
    #[test]
    fn test_cache_set_get() {
        let cache = AppCache::new(60, 100);
        cache.set("key1".to_string(), Value::String("hello".to_string()));
        let val = cache.get("key1");
        assert_eq!(val, Some(Value::String("hello".to_string())));
    }

    // テスト46逆証明: 異なるキー → None
    #[test]
    fn test_cache_get_missing_key() {
        let cache = AppCache::new(60, 100);
        cache.set("key1".to_string(), Value::String("hello".to_string()));
        assert_eq!(cache.get("key2"), None);
    }

    // テスト47: TTL超過 → None
    #[test]
    fn test_cache_ttl_expired() {
        let cache = AppCache::new(0, 100); // TTL=0秒 → 即期限切れ
        cache.set("key1".to_string(), Value::String("hello".to_string()));
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(cache.get("key1"), None);
    }

    // テスト47逆証明: TTL内 → Some
    #[test]
    fn test_cache_ttl_within() {
        let cache = AppCache::new(3600, 100); // TTL=1時間
        cache.set("key1".to_string(), Value::Number(42.into()));
        assert!(cache.get("key1").is_some());
    }

    // テスト48: clear → 全消去
    #[test]
    fn test_cache_clear() {
        let cache = AppCache::new(60, 100);
        cache.set("a".to_string(), Value::Null);
        cache.set("b".to_string(), Value::Null);
        cache.set("c".to_string(), Value::Null);
        assert_eq!(cache.len(), 3);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    // テスト49: max_entries超過 → エビクション
    #[test]
    fn test_cache_eviction_on_max() {
        let cache = AppCache::new(0, 3); // TTL=0, max=3
        // 全てTTL=0なので即期限切れ
        cache.set("a".to_string(), Value::Null);
        cache.set("b".to_string(), Value::Null);
        cache.set("c".to_string(), Value::Null);
        std::thread::sleep(std::time::Duration::from_millis(10));
        // 次のset時にevict_expiredが呼ばれ、期限切れが削除される
        cache.set("d".to_string(), Value::Null);
        assert!(cache.len() <= 3);
    }

    // テスト50: 並行アクセス → データ競合なし
    #[test]
    fn test_cache_concurrent_access() {
        use std::sync::Arc;
        let cache = Arc::new(AppCache::new(60, 1000));
        let mut handles = vec![];
        for i in 0..10 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("thread_{}_key_{}", i, j);
                    c.set(key.clone(), Value::Number((i * 100 + j).into()));
                    let _ = c.get(&key);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // パニックせずに完了すればOK
        assert!(cache.len() > 0);
    }
}
