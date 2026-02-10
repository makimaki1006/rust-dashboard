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
