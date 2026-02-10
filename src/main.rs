mod auth;
mod config;
mod db;
mod geo;
mod handlers;
mod models;

use axum::{
    extract::{Form, State},
    middleware,
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::services::ServeDir;
use tower_sessions::{MemoryStore, Session, SessionManagerLayer};
use tracing_subscriber::EnvFilter;

use auth::{
    require_auth, validate_email_domain, verify_password,
    SESSION_JOB_TYPE_KEY, SESSION_PREFECTURE_KEY, SESSION_MUNICIPALITY_KEY, SESSION_USER_KEY,
};
use config::AppConfig;
use db::cache::AppCache;
use db::turso::TursoClient;
use models::job_seeker::JOB_TYPES;

/// アプリケーション共有状態
pub struct AppState {
    pub config: AppConfig,
    pub turso: TursoClient,
    pub local_db: Option<db::local_sqlite::LocalDb>,
    pub cache: AppCache,
    pub rate_limiter: auth::session::RateLimiter,
}

#[tokio::main]
async fn main() {
    // .envファイル読み込み
    dotenvy::dotenv().ok();

    // ログ初期化
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = AppConfig::from_env();
    let port = config.port;
    tracing::info!("Starting rust_dashboard on port {}", port);

    // Turso HTTP APIクライアント
    let turso = TursoClient::new(&config.turso_url, &config.turso_auth_token);

    // Turso接続テスト
    match turso.test_connection().await {
        Ok(_) => tracing::info!("Turso connection OK"),
        Err(e) => tracing::warn!("Turso connection failed: {e} (continuing without Turso)"),
    }

    // ローカルSQLite（gzip圧縮ファイルから自動解凍）
    decompress_db_if_needed(&config.local_db_path);
    let local_db = match db::local_sqlite::LocalDb::new(&config.local_db_path) {
        Ok(db) => {
            tracing::info!("Local SQLite loaded: {}", config.local_db_path);
            Some(db)
        }
        Err(e) => {
            tracing::warn!("Local SQLite not available: {e}");
            None
        }
    };

    // キャッシュ
    let cache = AppCache::new(config.cache_ttl_secs, 100);

    // レート制限
    let rate_limiter = auth::session::RateLimiter::new(
        config.rate_limit_max_attempts,
        config.rate_limit_lockout_secs,
    );

    let state = Arc::new(AppState {
        config,
        turso,
        local_db,
        cache,
        rate_limiter,
    });

    // セッションストア
    let session_store = MemoryStore::default();
    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false); // 開発用: HTTPでもセッション許可

    // 認証が必要なルート
    let protected_routes = Router::new()
        .route("/", get(dashboard_page))
        .route("/tab/overview", get(handlers::overview::tab_overview))
        .route("/tab/demographics", get(handlers::demographics::tab_demographics))
        .route("/tab/mobility", get(handlers::mobility::tab_mobility))
        .route("/tab/balance", get(handlers::balance::tab_balance))
        .route("/tab/workstyle", get(handlers::workstyle::tab_workstyle))
        .route("/tab/jobmap", get(handlers::jobmap::tab_jobmap))
        .route("/tab/talentmap", get(handlers::talentmap::tab_talentmap))
        .route("/tab/competitive", get(handlers::competitive::tab_competitive))
        .route("/api/geojson/{filename}", get(handlers::api::get_geojson))
        .route("/api/markers", get(handlers::api::get_markers))
        .route("/api/set_job_type", post(set_job_type))
        .route("/api/set_prefecture", post(set_prefecture))
        .route("/api/set_municipality", post(set_municipality))
        .route("/api/prefectures", get(handlers::api::get_prefectures))
        .route("/api/municipalities_cascade", get(handlers::api::get_municipalities_cascade))
        .route("/api/competitive/filter", get(handlers::competitive::comp_filter))
        .route("/api/competitive/municipalities", get(handlers::competitive::comp_municipalities))
        .route("/api/competitive/facility_types", get(handlers::competitive::comp_facility_types))
        .route("/api/report", get(handlers::competitive::comp_report))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    // ルーティング統合
    let app = Router::new()
        // 認証不要
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", get(logout))
        // 認証必要ルートをマージ
        .merge(protected_routes)
        // 静的ファイル（認証不要）
        .nest_service("/static", ServeDir::new("static"))
        // 共有状態・セッション
        .with_state(state)
        .layer(session_layer);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("Listening on http://localhost:{port}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// 認証ミドルウェア（/login と /logout 以外に適用）
async fn auth_middleware(
    session: Session,
    State(_state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    let path = request.uri().path().to_string();

    // ログイン・ログアウト・静的ファイルはスキップ
    if path == "/login" || path == "/logout" || path.starts_with("/static") {
        return next.run(request).await;
    }

    require_auth(session, request, next).await
}

/// ログインフォームデータ
#[derive(serde::Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

/// ログインページ表示
async fn login_page(State(state): State<Arc<AppState>>) -> Html<String> {
    render_login(&state, None)
}

/// ログイン処理
async fn login_submit(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<LoginForm>,
) -> impl IntoResponse {
    let client_ip = "unknown".to_string(); // 簡略化

    // レート制限チェック
    if !state.rate_limiter.is_allowed(&client_ip) {
        return render_login(&state, Some("ログイン試行回数超過。しばらく待ってください。".to_string()))
            .into_response();
    }

    // ドメイン検証
    if !validate_email_domain(&form.email, &state.config.allowed_domains) {
        state.rate_limiter.record_failure(&client_ip);
        return render_login(&state, Some("許可されていないメールドメインです".to_string()))
            .into_response();
    }

    // パスワード検証
    if !verify_password(
        &form.password,
        &state.config.auth_password,
        &state.config.auth_password_hash,
    ) {
        state.rate_limiter.record_failure(&client_ip);
        return render_login(&state, Some("パスワードが正しくありません".to_string()))
            .into_response();
    }

    // ログイン成功
    state.rate_limiter.record_success(&client_ip);
    let _ = session.insert(SESSION_USER_KEY, &form.email).await;
    let _ = session.insert(SESSION_JOB_TYPE_KEY, "介護職").await;
    let _ = session.insert(SESSION_PREFECTURE_KEY, "").await;
    let _ = session.insert(SESSION_MUNICIPALITY_KEY, "").await;

    Redirect::to("/").into_response()
}

/// ログアウト処理
async fn logout(session: Session) -> Redirect {
    session.flush().await.ok();
    Redirect::to("/login")
}

/// ダッシュボードページ表示
async fn dashboard_page(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> impl IntoResponse {
    let user_email: String = session
        .get(SESSION_USER_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown".to_string());

    let current_job_type: String = session
        .get(SESSION_JOB_TYPE_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "介護職".to_string());

    let current_prefecture: String = session
        .get(SESSION_PREFECTURE_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let current_municipality: String = session
        .get(SESSION_MUNICIPALITY_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    let job_types: Vec<String> = JOB_TYPES.iter().map(|s| s.to_string()).collect();

    let job_options: String = job_types
        .iter()
        .map(|jt| {
            let selected = if *jt == current_job_type { " selected" } else { "" };
            format!(r#"<option value="{jt}"{selected}>{jt}</option>"#)
        })
        .collect::<Vec<_>>()
        .join("\n");

    // 都道府県一覧をTursoから取得
    let pref_list = fetch_prefecture_list(&state, &current_job_type).await;
    let pref_options: String = pref_list
        .iter()
        .map(|p| {
            let selected = if *p == current_prefecture { " selected" } else { "" };
            format!(r#"<option value="{p}"{selected}>{p}</option>"#)
        })
        .collect::<Vec<_>>()
        .join("\n");

    // 市区町村一覧（都道府県選択時のみ）
    let muni_options = if !current_prefecture.is_empty() {
        let muni_list = fetch_municipality_list(&state, &current_job_type, &current_prefecture).await;
        muni_list
            .iter()
            .map(|m| {
                let selected = if *m == current_municipality { " selected" } else { "" };
                format!(r#"<option value="{m}"{selected}>{m}</option>"#)
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        String::new()
    };

    let html = include_str!("../templates/dashboard_inline.html")
        .replace("{{JOB_OPTIONS}}", &job_options)
        .replace("{{PREF_OPTIONS}}", &pref_options)
        .replace("{{MUNI_OPTIONS}}", &muni_options)
        .replace("{{USER_EMAIL}}", &user_email);

    Html(html)
}

/// 職種切り替えフォーム
#[derive(serde::Deserialize)]
struct SetJobTypeForm {
    job_type: String,
}

/// 職種切り替えAPI（セッション更新 + キャッシュクリア）
async fn set_job_type(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<SetJobTypeForm>,
) -> impl IntoResponse {
    let _ = session.insert(SESSION_JOB_TYPE_KEY, &form.job_type).await;
    state.cache.clear();
    Html("OK".to_string())
}

/// 都道府県切り替えフォーム
#[derive(serde::Deserialize)]
struct SetPrefectureForm {
    prefecture: String,
}

/// 都道府県切り替えAPI
async fn set_prefecture(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<SetPrefectureForm>,
) -> impl IntoResponse {
    let _ = session.insert(SESSION_PREFECTURE_KEY, &form.prefecture).await;
    // 市区町村をリセット
    let _ = session.insert(SESSION_MUNICIPALITY_KEY, "").await;
    state.cache.clear();
    Html("OK".to_string())
}

/// 市区町村切り替えフォーム
#[derive(serde::Deserialize)]
struct SetMunicipalityForm {
    municipality: String,
}

/// 市区町村切り替えAPI
async fn set_municipality(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<SetMunicipalityForm>,
) -> impl IntoResponse {
    let _ = session.insert(SESSION_MUNICIPALITY_KEY, &form.municipality).await;
    state.cache.clear();
    Html("OK".to_string())
}

/// 都道府県一覧をTursoから取得
async fn fetch_prefecture_list(state: &AppState, job_type: &str) -> Vec<String> {
    let sql = "SELECT DISTINCT prefecture FROM job_seeker_data WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture != '' ORDER BY prefecture";
    let params = vec![serde_json::Value::String(job_type.to_string())];
    match state.turso.query(sql, &params).await {
        Ok(rows) => rows
            .iter()
            .filter_map(|r| r.get("prefecture").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// 市区町村一覧をTursoから取得
async fn fetch_municipality_list(state: &AppState, job_type: &str, prefecture: &str) -> Vec<String> {
    let sql = "SELECT DISTINCT municipality FROM job_seeker_data WHERE job_type = ? AND prefecture = ? AND row_type = 'SUMMARY' AND municipality != '' ORDER BY municipality";
    let params = vec![
        serde_json::Value::String(job_type.to_string()),
        serde_json::Value::String(prefecture.to_string()),
    ];
    match state.turso.query(sql, &params).await {
        Ok(rows) => rows
            .iter()
            .filter_map(|r| r.get("municipality").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// gzip圧縮DBファイルを解凍（.dbが存在しない場合のみ）
fn decompress_db_if_needed(db_path: &str) {
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::{self, Read, Write};
    use std::path::Path;

    let db_file = Path::new(db_path);
    if db_file.exists() {
        return;
    }

    let gz_path = format!("{}.gz", db_path);
    let gz_file = Path::new(&gz_path);
    if !gz_file.exists() {
        tracing::info!("No gzip DB found at {gz_path}, skipping decompression");
        return;
    }

    tracing::info!("Decompressing {gz_path} → {db_path}...");

    match (|| -> io::Result<u64> {
        let f = File::open(&gz_path)?;
        let mut decoder = GzDecoder::new(f);
        let mut out = File::create(db_path)?;
        let mut buf = vec![0u8; 1024 * 1024]; // 1MB バッファ
        let mut total: u64 = 0;
        loop {
            let n = decoder.read(&mut buf)?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n])?;
            total += n as u64;
        }
        out.flush()?;
        Ok(total)
    })() {
        Ok(bytes) => {
            tracing::info!("Decompressed {} bytes → {db_path}", bytes);
        }
        Err(e) => {
            tracing::error!("Failed to decompress {gz_path}: {e}");
            // 不完全なファイルを削除
            let _ = std::fs::remove_file(db_path);
        }
    }
}

/// ログインページのHTML生成
fn render_login(state: &AppState, error_message: Option<String>) -> Html<String> {
    let domains = state
        .config
        .allowed_domains
        .iter()
        .map(|d| format!("@{d}"))
        .collect::<Vec<_>>()
        .join(", ");

    let error_html = error_message
        .map(|msg| {
            format!(
                r#"<div class="bg-red-900/50 border border-red-700 text-red-300 px-4 py-3 rounded-lg mb-4 text-sm">{msg}</div>"#
            )
        })
        .unwrap_or_default();

    let html = include_str!("../templates/login_inline.html")
        .replace("{{ERROR_HTML}}", &error_html)
        .replace("{{DOMAINS}}", &domains);

    Html(html)
}
