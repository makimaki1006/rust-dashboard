use std::env;

/// アプリケーション設定
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// サーバーポート
    pub port: u16,
    /// Turso DB URL
    pub turso_url: String,
    /// Turso 認証トークン
    pub turso_auth_token: String,
    /// ログインパスワード（平文）
    pub auth_password: String,
    /// ログインパスワード（bcryptハッシュ）
    pub auth_password_hash: String,
    /// 許可ドメインリスト
    pub allowed_domains: Vec<String>,
    /// ローカルSQLiteパス
    pub local_db_path: String,
    /// セグメント分析DBパス
    pub segment_db_path: String,
    /// キャッシュTTL（秒）
    pub cache_ttl_secs: u64,
    /// レート制限: 最大試行回数
    pub rate_limit_max_attempts: u32,
    /// レート制限: ロックアウト秒数
    pub rate_limit_lockout_secs: u64,
}

impl AppConfig {
    /// 環境変数から設定を読み込む
    pub fn from_env() -> Self {
        Self {
            port: env::var("PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(9216),
            turso_url: env::var("TURSO_DATABASE_URL").unwrap_or_default(),
            turso_auth_token: env::var("TURSO_AUTH_TOKEN").unwrap_or_default(),
            auth_password: env::var("AUTH_PASSWORD").unwrap_or_default(),
            auth_password_hash: env::var("AUTH_PASSWORD_HASH").unwrap_or_default(),
            allowed_domains: env::var("ALLOWED_DOMAINS")
                .unwrap_or_else(|_| "f-a-c.co.jp,cyxen.co.jp".to_string())
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .collect(),
            local_db_path: env::var("LOCAL_DB_PATH")
                .unwrap_or_else(|_| "data/job_postings_minimal.db".to_string()),
            segment_db_path: env::var("SEGMENT_DB_PATH")
                .unwrap_or_else(|_| "data/segment_summary.db".to_string()),
            cache_ttl_secs: env::var("CACHE_TTL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1800), // 30分
            rate_limit_max_attempts: env::var("RATE_LIMIT_MAX_ATTEMPTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            rate_limit_lockout_secs: env::var("RATE_LIMIT_LOCKOUT_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    // env varはプロセスグローバルなので、並列テストで競合する
    // Mutexで直列化して安全にする
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for key in &[
            "PORT", "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN",
            "AUTH_PASSWORD", "AUTH_PASSWORD_HASH", "ALLOWED_DOMAINS",
            "LOCAL_DB_PATH", "SEGMENT_DB_PATH", "CACHE_TTL_SECS",
            "RATE_LIMIT_MAX_ATTEMPTS", "RATE_LIMIT_LOCKOUT_SECONDS",
        ] {
            env::remove_var(key);
        }
    }

    // テスト1: PORT未設定 → デフォルト9216
    #[test]
    fn test_default_port() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = AppConfig::from_env();
        assert_eq!(config.port, 9216);
    }

    // テスト1逆証明: PORT空文字 → パニックしないこと
    #[test]
    fn test_empty_port_no_panic() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        env::set_var("PORT", "");
        let config = AppConfig::from_env();
        assert_eq!(config.port, 9216);
    }

    // テスト2: TURSO_DATABASE_URL空 → 空文字列
    #[test]
    fn test_turso_url_default_empty() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = AppConfig::from_env();
        assert!(config.turso_url.is_empty());
    }

    // テスト3: ALLOWED_DOMAINS CSV解析
    #[test]
    fn test_allowed_domains_csv_parse() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        env::set_var("ALLOWED_DOMAINS", "example.com, test.co.jp ,UPPER.COM");
        let config = AppConfig::from_env();
        assert_eq!(config.allowed_domains, vec!["example.com", "test.co.jp", "upper.com"]);
    }

    // テスト3逆証明: 空CSV → デフォルトドメイン
    #[test]
    fn test_allowed_domains_default() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = AppConfig::from_env();
        assert!(config.allowed_domains.contains(&"f-a-c.co.jp".to_string()),
            "expected f-a-c.co.jp, got {:?}", config.allowed_domains);
        assert!(config.allowed_domains.contains(&"cyxen.co.jp".to_string()));
    }

    // テスト4: LOCAL_DB_PATH未設定 → デフォルトパス
    #[test]
    fn test_local_db_path_default() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = AppConfig::from_env();
        assert_eq!(config.local_db_path, "data/job_postings_minimal.db");
    }

    // テスト5: CACHE_TTL_SECS=0 → 0秒
    #[test]
    fn test_cache_ttl_zero() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        env::set_var("CACHE_TTL_SECS", "0");
        let config = AppConfig::from_env();
        assert_eq!(config.cache_ttl_secs, 0);
    }

    // テスト5逆証明: 不正値 → デフォルト1800
    #[test]
    fn test_cache_ttl_invalid() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env();
        env::set_var("CACHE_TTL_SECS", "abc");
        let config = AppConfig::from_env();
        assert_eq!(config.cache_ttl_secs, 1800);
    }
}
