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

    let turso = TursoClient::new(&config.turso_url, &config.turso_auth_token);

    match turso.test_connection().await {
        Ok(_) => tracing::info!("Turso connection OK"),
        Err(e) => tracing::warn!("Turso connection failed: {e} (continuing without Turso)"),
    }

    decompress_geojson_if_needed();
    decompress_db_if_needed(&config.local_db_path);
    decompress_db_if_needed(&config.segment_db_path);

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

    let cache = AppCache::new(config.cache_ttl_secs, 100);
    let rate_limiter = RateLimiter::new(config.rate_limit_max_attempts, config.rate_limit_lockout_secs);

    let state = Arc::new(AppState {
        config,
        turso,
        local_db,
        segment_db,
        cache,
        rate_limiter,
    });

    let app = build_app(state);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("Listening on http://localhost:{port}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
