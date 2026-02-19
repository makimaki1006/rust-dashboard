use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// ログイン試行のレート制限管理
pub struct RateLimiter {
    attempts: Mutex<HashMap<String, AttemptInfo>>,
    max_attempts: u32,
    lockout_duration_secs: u64,
}

struct AttemptInfo {
    count: u32,
    lockout_until: Option<Instant>,
}

impl RateLimiter {
    pub fn new(max_attempts: u32, lockout_duration_secs: u64) -> Self {
        Self {
            attempts: Mutex::new(HashMap::new()),
            max_attempts,
            lockout_duration_secs,
        }
    }

    /// ログイン試行が許可されているか確認
    pub fn is_allowed(&self, ip: &str) -> bool {
        let attempts = self.attempts.lock().unwrap();
        if let Some(info) = attempts.get(ip) {
            if let Some(until) = info.lockout_until {
                if Instant::now() < until {
                    return false;
                }
            }
        }
        true
    }

    /// 失敗を記録
    pub fn record_failure(&self, ip: &str) {
        let mut attempts = self.attempts.lock().unwrap();
        let info = attempts.entry(ip.to_string()).or_insert(AttemptInfo {
            count: 0,
            lockout_until: None,
        });

        // ロックアウト期間を過ぎていたらリセット
        if let Some(until) = info.lockout_until {
            if Instant::now() >= until {
                info.count = 0;
                info.lockout_until = None;
            }
        }

        info.count += 1;
        if info.count >= self.max_attempts {
            info.lockout_until = Some(
                Instant::now()
                    + std::time::Duration::from_secs(self.lockout_duration_secs),
            );
        }
    }

    /// 成功時にリセット
    pub fn record_success(&self, ip: &str) {
        let mut attempts = self.attempts.lock().unwrap();
        attempts.remove(ip);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // テスト44: レート制限: 5回失敗 → ロック
    #[test]
    fn test_rate_limit_lockout_after_max() {
        let limiter = RateLimiter::new(5, 300);
        let ip = "192.168.1.1";
        for _ in 0..5 {
            limiter.record_failure(ip);
        }
        assert!(!limiter.is_allowed(ip));
    }

    // テスト44逆証明: 4回目 → まだロックなし
    #[test]
    fn test_rate_limit_no_lockout_before_max() {
        let limiter = RateLimiter::new(5, 300);
        let ip = "192.168.1.1";
        for _ in 0..4 {
            limiter.record_failure(ip);
        }
        assert!(limiter.is_allowed(ip));
    }

    // テスト45: ロックアウト期間経過後 → アンロック
    #[test]
    fn test_rate_limit_unlock_after_duration() {
        // ロックアウト1秒に設定
        let limiter = RateLimiter::new(2, 1);
        let ip = "10.0.0.1";
        limiter.record_failure(ip);
        limiter.record_failure(ip);
        // ロック中であること確認
        assert!(!limiter.is_allowed(ip));
        // 1.1秒待ってアンロック
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(limiter.is_allowed(ip));
    }

    // 成功でリセット
    #[test]
    fn test_rate_limit_reset_on_success() {
        let limiter = RateLimiter::new(3, 300);
        let ip = "10.0.0.2";
        limiter.record_failure(ip);
        limiter.record_failure(ip);
        limiter.record_success(ip);
        assert!(limiter.is_allowed(ip));
    }

    // 異なるIP → 独立管理
    #[test]
    fn test_rate_limit_independent_ips() {
        let limiter = RateLimiter::new(2, 300);
        limiter.record_failure("ip1");
        limiter.record_failure("ip1");
        assert!(!limiter.is_allowed("ip1"));
        assert!(limiter.is_allowed("ip2")); // ip2は影響なし
    }
}
