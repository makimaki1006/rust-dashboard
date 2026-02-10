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
