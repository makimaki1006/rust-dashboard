use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use rust_dashboard::{
    build_app, decompress_db_if_needed, decompress_geojson_if_needed, AppState,
};
use rust_dashboard::auth::session::RateLimiter;
use rust_dashboard::config::AppConfig;
use rust_dashboard::db::cache::AppCache;
use rust_dashboard::db::local_sqlite::LocalDb;
use rust_dashboard::db::turso::TursoClient;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = AppConfig::from_env();
    let port = config.port;
    tracing::info!("Starting rust_dashboard on port {}", port);

    // セキュリティ警告: 平文パスワードフォールバック
    if config.auth_password_hash.is_empty() && !config.auth_password.is_empty() {
        tracing::warn!(
            "⚠️ AUTH_PASSWORD_HASH未設定 — 平文パスワードで認証中。\
             本番環境では AUTH_PASSWORD_HASH (bcrypt) の使用を推奨します。"
        );
    }

    let turso = TursoClient::new(&config.turso_url, &config.turso_auth_token);

    match turso.test_connection().await {
        Ok(_) => tracing::info!("Turso connection OK"),
        Err(e) => tracing::warn!("Turso connection failed: {e} (continuing without Turso)"),
    }

    decompress_geojson_if_needed();

    // DB解凍を並列実行（コールドスタート高速化）
    let local_path_clone = config.local_db_path.clone();
    let segment_path_clone = config.segment_db_path.clone();
    let geocoded_path_clone = config.geocoded_db_path.clone();

    let handles: Vec<_> = vec![
        std::thread::spawn(move || decompress_db_if_needed(&local_path_clone)),
        std::thread::spawn(move || decompress_db_if_needed(&segment_path_clone)),
        std::thread::spawn(move || decompress_db_if_needed(&geocoded_path_clone)),
    ];
    for h in handles {
        if let Err(e) = h.join() {
            tracing::error!("DB decompression thread panic: {:?}", e);
        }
    }

    let local_db = match LocalDb::new(&config.local_db_path) {
        Ok(db) => {
            tracing::info!("Local SQLite loaded: {}", config.local_db_path);
            Some(db)
        }
        Err(e) => {
            tracing::warn!("Local SQLite not available: {e}");
            None
        }
    };

    let segment_db = match LocalDb::new(&config.segment_db_path) {
        Ok(db) => {
            tracing::info!("Segment SQLite loaded: {}", config.segment_db_path);
            Some(db)
        }
        Err(e) => {
            tracing::warn!("Segment SQLite not available: {e}");
            None
        }
    };

    let geocoded_db = match LocalDb::new(&config.geocoded_db_path) {
        Ok(db) => {
            tracing::info!("Geocoded postings SQLite loaded: {}", config.geocoded_db_path);
            // パフォーマンス向上: インデックス自動作成
            let idx_sqls = [
                // 既存インデックス
                "CREATE INDEX IF NOT EXISTS idx_postings_job_pref ON postings (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_postings_job_lat_lng ON postings (job_type, lat, lng)",
                // 追加インデックス（市区町村・雇用形態・施設種別フィルタ高速化）
                "CREATE INDEX IF NOT EXISTS idx_postings_job_pref_muni ON postings (job_type, prefecture, municipality)",
                "CREATE INDEX IF NOT EXISTS idx_postings_job_pref_emp ON postings (job_type, prefecture, employment_type)",
                "CREATE INDEX IF NOT EXISTS idx_postings_job_pref_fac ON postings (job_type, prefecture, facility_type)",
                // Layer A-C 分析テーブル用インデックス
                "CREATE INDEX IF NOT EXISTS idx_salary_job_pref ON layer_a_salary_stats (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_facility_job_pref ON layer_a_facility_concentration (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_emp_div_job_pref ON layer_a_employment_diversity (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_keywords_job_pref ON layer_b_keywords (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_cooc_job_pref ON layer_b_cooccurrence (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_quality_job_pref ON layer_b_text_quality (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_clusters_job_pref ON layer_c_clusters (job_type, prefecture)",
                "CREATE INDEX IF NOT EXISTS idx_heatmap_job_pref ON layer_c_region_heatmap (job_type, prefecture)",
            ];
            for sql in &idx_sqls {
                if let Err(e) = db.execute(sql, &[]) {
                    tracing::warn!("Index creation failed: {e}");
                }
            }
            // PRAGMA mmap_size: 256MB memory-mapped I/O（最大DBで効果大）
            if let Err(e) = db.execute("PRAGMA mmap_size=268435456", &[]) {
                tracing::warn!("PRAGMA mmap_size failed: {e}");
            }
            Some(db)
        }
        Err(e) => {
            tracing::warn!("Geocoded postings SQLite not available: {e}");
            None
        }
    };

    let cache = AppCache::new(config.cache_ttl_secs, config.cache_max_entries);
    let rate_limiter = RateLimiter::new(config.rate_limit_max_attempts, config.rate_limit_lockout_secs);

    let state = Arc::new(AppState {
        config,
        turso,
        local_db,
        segment_db,
        geocoded_db,
        cache,
        rate_limiter,
    });

    let app = build_app(state);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("Listening on http://localhost:{port}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
