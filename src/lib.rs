pub mod auth;
pub mod config;
pub mod db;
pub mod geo;
pub mod handlers;
pub mod models;

use axum::{
    extract::{Form, FromRequest, State},
    middleware,
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::services::ServeDir;
use tower_sessions::{MemoryStore, Session, SessionManagerLayer};

use auth::{
    require_auth, validate_email_domain, verify_password,
    SESSION_JOB_TYPE_KEY, SESSION_MUNICIPALITY_KEY, SESSION_PREFECTURE_KEY, SESSION_USER_KEY,
};
use config::AppConfig;
use db::cache::AppCache;
use db::turso::TursoClient;
use models::job_seeker::{JOB_TYPES, PREFECTURE_ORDER};

/// アプリケーション共有状態
pub struct AppState {
    pub config: AppConfig,
    pub turso: TursoClient,
    pub local_db: Option<db::local_sqlite::LocalDb>,
    pub segment_db: Option<db::local_sqlite::LocalDb>,
    pub cache: AppCache,
    pub rate_limiter: auth::session::RateLimiter,
}

/// アプリケーションRouter構築（統合テストでも使用）
pub fn build_app(state: Arc<AppState>) -> Router {
    let session_store = MemoryStore::default();
    let session_layer = SessionManagerLayer::new(session_store).with_secure(false);

    let protected_routes = Router::new()
        .route("/", get(dashboard_page))
        .route("/tab/overview", get(handlers::overview::tab_overview))
        .route(
            "/tab/demographics",
            get(handlers::demographics::tab_demographics),
        )
        .route("/tab/mobility", get(handlers::mobility::tab_mobility))
        .route("/tab/balance", get(handlers::balance::tab_balance))
        .route("/tab/workstyle", get(handlers::workstyle::tab_workstyle))
        .route("/tab/jobmap", get(handlers::jobmap::tab_jobmap))
        .route(
            "/tab/talentmap",
            get(handlers::talentmap::tab_talentmap),
        )
        .route(
            "/tab/competitive",
            get(handlers::competitive::tab_competitive),
        )
        .route(
            "/api/geojson/{filename}",
            get(handlers::api::get_geojson),
        )
        .route("/api/markers", get(handlers::api::get_markers))
        .route("/api/set_job_type", post(set_job_type))
        .route("/api/set_prefecture", post(set_prefecture))
        .route("/api/set_municipality", post(set_municipality))
        .route(
            "/api/prefectures",
            get(handlers::api::get_prefectures),
        )
        .route(
            "/api/municipalities_cascade",
            get(handlers::api::get_municipalities_cascade),
        )
        .route("/api/rarity", get(handlers::demographics::api_rarity))
        .route(
            "/api/talentmap/detail",
            get(handlers::talentmap::api_talentmap_detail),
        )
        .route(
            "/api/competitive/filter",
            get(handlers::competitive::comp_filter),
        )
        .route(
            "/api/competitive/municipalities",
            get(handlers::competitive::comp_municipalities),
        )
        .route(
            "/api/competitive/facility_types",
            get(handlers::competitive::comp_facility_types),
        )
        .route("/api/report", get(handlers::competitive::comp_report))
        .route(
            "/api/competitive/analysis",
            get(handlers::competitive::comp_analysis),
        )
        .route(
            "/api/competitive/analysis/filter",
            get(handlers::competitive::comp_analysis_filtered),
        )
        // セグメント分析タブ (Tab 9)
        .route(
            "/tab/segment",
            get(handlers::segment::tab_segment),
        )
        // セグメント分析API
        .route(
            "/api/segment/overview",
            get(handlers::segment::segment_overview),
        )
        .route(
            "/api/segment/tier3",
            get(handlers::segment::segment_tier3),
        )
        .route(
            "/api/segment/tags",
            get(handlers::segment::segment_tags),
        )
        .route(
            "/api/segment/text_features",
            get(handlers::segment::segment_text_features),
        )
        .route(
            "/api/segment/salary_compare",
            get(handlers::segment::segment_salary_compare),
        )
        .route(
            "/api/segment/job_desc_insights",
            get(handlers::segment::segment_job_desc_insights),
        )
        .route(
            "/api/segment/age_decade",
            get(handlers::segment::segment_age_decade),
        )
        .route(
            "/api/segment/gender_lifecycle",
            get(handlers::segment::segment_gender_lifecycle),
        )
        .route(
            "/api/segment/exp_qual",
            get(handlers::segment::segment_exp_qual),
        )
        .route(
            "/api/segment/work_schedule",
            get(handlers::segment::segment_work_schedule),
        )
        .route(
            "/api/segment/holidays",
            get(handlers::segment::segment_holidays),
        )
        .route(
            "/api/segment/salary_shift",
            get(handlers::segment::segment_salary_shift),
        )
        .route("/api/status", get(api_status))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .route("/health", get(health_check))
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", get(logout))
        .merge(protected_routes)
        .nest_service("/static", ServeDir::new("static"))
        .with_state(state)
        .layer(session_layer)
}

// --- ミドルウェア ---

async fn auth_middleware(
    session: Session,
    State(_state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    let path = request.uri().path().to_string();
    if path == "/login" || path == "/logout" || path.starts_with("/static") {
        return next.run(request).await;
    }
    require_auth(session, request, next).await
}

// --- ログイン ---

#[derive(serde::Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

async fn login_page(State(state): State<Arc<AppState>>) -> Html<String> {
    render_login(&state, None)
}

async fn login_submit(
    State(state): State<Arc<AppState>>,
    session: Session,
    req: axum::extract::Request,
) -> impl IntoResponse {
    // ConnectInfoからIPを取得（into_make_service_with_connect_info使用時に利用可能）
    let socket_ip = req.extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip().to_string());

    // X-Forwarded-Forヘッダーを優先（リバースプロキシ経由の場合）、
    // なければConnectInfoのIP、最終手段として"unknown"
    let client_ip = req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or(socket_ip)
        .unwrap_or_else(|| "unknown".to_string());

    // FormをRequestのbodyから手動で抽出
    let Form(form) = match axum::extract::Form::<LoginForm>::from_request(req, &()).await {
        Ok(f) => f,
        Err(_) => {
            return render_login(&state, Some("無効なリクエストです".to_string())).into_response();
        }
    };

    if !state.rate_limiter.is_allowed(&client_ip) {
        return render_login(
            &state,
            Some("ログイン試行回数超過。しばらく待ってください。".to_string()),
        )
        .into_response();
    }

    if !validate_email_domain(&form.email, &state.config.allowed_domains) {
        state.rate_limiter.record_failure(&client_ip);
        return render_login(
            &state,
            Some("許可されていないメールドメインです".to_string()),
        )
        .into_response();
    }

    if !verify_password(
        &form.password,
        &state.config.auth_password,
        &state.config.auth_password_hash,
    ) {
        state.rate_limiter.record_failure(&client_ip);
        return render_login(
            &state,
            Some("パスワードが正しくありません".to_string()),
        )
        .into_response();
    }

    state.rate_limiter.record_success(&client_ip);
    let _ = session.insert(SESSION_USER_KEY, &form.email).await;
    let _ = session.insert(SESSION_JOB_TYPE_KEY, "介護職").await;
    let _ = session.insert(SESSION_PREFECTURE_KEY, "").await;
    let _ = session.insert(SESSION_MUNICIPALITY_KEY, "").await;

    Redirect::to("/").into_response()
}

async fn logout(session: Session) -> Redirect {
    session.flush().await.ok();
    Redirect::to("/login")
}

// --- ダッシュボード ---

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
            let selected = if *jt == current_job_type {
                " selected"
            } else {
                ""
            };
            format!(r#"<option value="{jt}"{selected}>{jt}</option>"#)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let pref_list = fetch_prefecture_list(&state, &current_job_type).await;
    let pref_options: String = pref_list
        .iter()
        .map(|p| {
            let selected = if *p == current_prefecture {
                " selected"
            } else {
                ""
            };
            format!(r#"<option value="{p}"{selected}>{p}</option>"#)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let muni_options = if !current_prefecture.is_empty() {
        let muni_list =
            fetch_municipality_list(&state, &current_job_type, &current_prefecture).await;
        muni_list
            .iter()
            .map(|m| {
                let selected = if *m == current_municipality {
                    " selected"
                } else {
                    ""
                };
                format!(r#"<option value="{m}"{selected}>{m}</option>"#)
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        String::new()
    };

    let turso_ok = !pref_list.is_empty() || state.turso.test_connection().await.is_ok();
    let turso_warning = if turso_ok {
        String::new()
    } else {
        r#"<div id="turso-warning" class="bg-red-900/80 border border-red-600 text-red-200 px-4 py-3 text-sm flex items-center gap-2">
            <span class="text-lg">⚠️</span>
            <div>
                <strong>データベース接続エラー:</strong> Tursoデータベースに接続できません。
                環境変数 <code class="bg-red-800 px-1 rounded">TURSO_DATABASE_URL</code> を確認してください。
                <a href="/api/status" target="_blank" class="underline text-red-300 hover:text-white ml-2">詳細ステータス →</a>
            </div>
        </div>"#
            .to_string()
    };

    let html = include_str!("../templates/dashboard_inline.html")
        .replace("{{JOB_OPTIONS}}", &job_options)
        .replace("{{PREF_OPTIONS}}", &pref_options)
        .replace("{{MUNI_OPTIONS}}", &muni_options)
        .replace("{{USER_EMAIL}}", &user_email)
        .replace("{{TURSO_WARNING}}", &turso_warning);

    Html(html)
}

// --- セッション更新API ---

#[derive(serde::Deserialize)]
struct SetJobTypeForm {
    job_type: String,
}

async fn set_job_type(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<SetJobTypeForm>,
) -> impl IntoResponse {
    let _ = session.insert(SESSION_JOB_TYPE_KEY, &form.job_type).await;
    state.cache.clear();
    Html("OK".to_string())
}

#[derive(serde::Deserialize)]
struct SetPrefectureForm {
    prefecture: String,
}

async fn set_prefecture(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<SetPrefectureForm>,
) -> impl IntoResponse {
    let _ = session
        .insert(SESSION_PREFECTURE_KEY, &form.prefecture)
        .await;
    let _ = session.insert(SESSION_MUNICIPALITY_KEY, "").await;
    state.cache.clear();
    Html("OK".to_string())
}

#[derive(serde::Deserialize)]
struct SetMunicipalityForm {
    municipality: String,
}

async fn set_municipality(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<SetMunicipalityForm>,
) -> impl IntoResponse {
    let _ = session
        .insert(SESSION_MUNICIPALITY_KEY, &form.municipality)
        .await;
    state.cache.clear();
    Html("OK".to_string())
}

// --- ヘルパー ---

async fn fetch_prefecture_list(state: &AppState, job_type: &str) -> Vec<String> {
    let sql = "SELECT DISTINCT prefecture FROM job_seeker_data WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture != ''";
    let params = vec![serde_json::Value::String(job_type.to_string())];
    match state.turso.query(sql, &params).await {
        Ok(rows) => {
            let mut prefs: Vec<String> = rows
                .iter()
                .filter_map(|r| {
                    r.get("prefecture")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            prefs.sort_by_key(|p| {
                PREFECTURE_ORDER
                    .iter()
                    .position(|&o| o == p.as_str())
                    .unwrap_or(99)
            });
            prefs
        }
        Err(_) => Vec::new(),
    }
}

async fn fetch_municipality_list(
    state: &AppState,
    job_type: &str,
    prefecture: &str,
) -> Vec<String> {
    let sql = "SELECT DISTINCT municipality FROM job_seeker_data WHERE job_type = ? AND prefecture = ? AND row_type = 'SUMMARY' AND municipality != '' ORDER BY municipality";
    let params = vec![
        serde_json::Value::String(job_type.to_string()),
        serde_json::Value::String(prefecture.to_string()),
    ];
    match state.turso.query(sql, &params).await {
        Ok(rows) => rows
            .iter()
            .filter_map(|r| {
                r.get("municipality")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// ヘルスチェック（認証不要）
async fn health_check() -> &'static str {
    "OK"
}

/// ステータスAPI
async fn api_status(
    State(state): State<Arc<AppState>>,
) -> axum::response::Json<serde_json::Value> {
    let turso_ok = state.turso.test_connection().await.is_ok();
    let turso_url_masked = if state.config.turso_url.len() > 20 {
        format!(
            "{}...{}",
            &state.config.turso_url[..20],
            &state.config.turso_url[state.config.turso_url.len() - 20..]
        )
    } else {
        state.config.turso_url.clone()
    };

    let local_db_ok = state.local_db.is_some();
    let local_db_count = if let Some(db) = &state.local_db {
        db.query_scalar::<i64>("SELECT COUNT(*) FROM job_postings", &[])
            .unwrap_or(0)
    } else {
        0
    };

    let segment_db_ok = state.segment_db.is_some();

    axum::response::Json(serde_json::json!({
        "turso_connected": turso_ok,
        "turso_url": turso_url_masked,
        "local_db_loaded": local_db_ok,
        "local_db_rows": local_db_count,
        "segment_db_loaded": segment_db_ok,
        "status": if turso_ok && local_db_ok { "healthy" } else { "degraded" }
    }))
}

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

// --- ファイル解凍 ---

/// data/geojson_gz/*.json.gz → static/geojson/*.json に解凍
pub fn decompress_geojson_if_needed() {
    use std::path::Path;

    let gz_dir = Path::new("data/geojson_gz");
    let out_dir = Path::new("static/geojson");

    if !gz_dir.exists() {
        tracing::info!("No geojson_gz directory found, skipping GeoJSON decompression");
        return;
    }

    let _ = std::fs::create_dir_all(out_dir);

    let entries = match std::fs::read_dir(gz_dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("Cannot read geojson_gz dir: {e}");
            return;
        }
    };

    let mut count = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let fname = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.ends_with(".json.gz") => n.to_string(),
            _ => continue,
        };
        let json_name = fname.trim_end_matches(".gz");
        let out_path = out_dir.join(json_name);

        if out_path.exists() {
            continue;
        }

        decompress_gz_file(
            path.to_str().unwrap_or_default(),
            out_path.to_str().unwrap_or_default(),
        );
        count += 1;
    }
    if count > 0 {
        tracing::info!("Decompressed {count} GeoJSON files");
    }
}

/// gzip圧縮DBファイルを解凍（.dbが存在しない場合のみ）
pub fn decompress_db_if_needed(db_path: &str) {
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

    tracing::info!("Decompressing {gz_path} -> {db_path}...");

    match (|| -> io::Result<u64> {
        let f = File::open(&gz_path)?;
        let mut decoder = GzDecoder::new(f);
        let mut out = File::create(db_path)?;
        let mut buf = vec![0u8; 1024 * 1024];
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
            tracing::info!("Decompressed {} bytes -> {db_path}", bytes);
        }
        Err(e) => {
            tracing::error!("Failed to decompress {gz_path}: {e}");
            let _ = std::fs::remove_file(db_path);
        }
    }
}

fn decompress_gz_file(gz_path: &str, out_path: &str) {
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::{Read, Write};

    let f = match File::open(gz_path) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("Cannot open {gz_path}: {e}");
            return;
        }
    };
    let mut decoder = GzDecoder::new(f);
    let mut out = match File::create(out_path) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("Cannot create {out_path}: {e}");
            return;
        }
    };
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        match decoder.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if out.write_all(&buf[..n]).is_err() {
                    let _ = std::fs::remove_file(out_path);
                    return;
                }
            }
            Err(_) => {
                let _ = std::fs::remove_file(out_path);
                return;
            }
        }
    }
    let _ = out.flush();
}
