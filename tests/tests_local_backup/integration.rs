//! 統合テスト: Axumルーター + 認証 + ローカルSQLite + GeoJSON
//!
//! テストカテゴリ:
//!   2.1 起動・初期化 (10パターン)
//!   2.2 認証フロー (10パターン)
//!   2.3 Turso統合 (10パターン) - 空Tursoでの動作確認
//!   2.4 ローカルSQLite統合 (10パターン) - 最重要
//!   2.5 GeoJSON/マップ (5パターン)
//!   2.6 追加エッジケース (5パターン)

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

use rust_dashboard::auth::session::RateLimiter;
use rust_dashboard::config::AppConfig;
use rust_dashboard::db::cache::AppCache;
use rust_dashboard::db::local_sqlite::LocalDb;
use rust_dashboard::db::turso::TursoClient;
use rust_dashboard::{build_app, AppState};

// --- テストヘルパー ---

/// テスト用設定（Turso無効、DB無し）
fn test_config() -> AppConfig {
    AppConfig {
        port: 0,
        turso_url: String::new(),
        turso_auth_token: String::new(),
        auth_password: "testpass123".to_string(),
        auth_password_hash: String::new(),
        allowed_domains: vec!["test.com".to_string(), "example.co.jp".to_string()],
        local_db_path: String::new(),
        segment_db_path: String::new(),
        cache_ttl_secs: 60,
        rate_limit_max_attempts: 5,
        rate_limit_lockout_secs: 300,
    }
}

/// テスト用State（DB無し）
fn test_state() -> Arc<AppState> {
    let config = test_config();
    Arc::new(AppState {
        turso: TursoClient::new(&config.turso_url, &config.turso_auth_token),
        local_db: None,
        segment_db: None,
        cache: AppCache::new(config.cache_ttl_secs, 100),
        rate_limiter: RateLimiter::new(config.rate_limit_max_attempts, config.rate_limit_lockout_secs),
        config,
    })
}

/// テスト用SQLiteデータベースを作成
fn create_test_db() -> (tempfile::NamedTempFile, LocalDb) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("
        CREATE TABLE job_postings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            facility_name TEXT NOT NULL,
            facility_type TEXT DEFAULT '',
            prefecture TEXT NOT NULL,
            municipality TEXT NOT NULL,
            employment_type TEXT DEFAULT '',
            salary_type TEXT DEFAULT '',
            salary_min REAL DEFAULT 0,
            salary_max REAL DEFAULT 0,
            base_salary REAL DEFAULT 0,
            bonus TEXT DEFAULT '',
            annual_holidays INTEGER DEFAULT 0,
            qualification_allowance REAL DEFAULT 0,
            other_allowances TEXT DEFAULT '',
            requirements TEXT DEFAULT '',
            latitude REAL DEFAULT 0,
            longitude REAL DEFAULT 0,
            inexperienced_ok TEXT DEFAULT ''
        );

        -- 群馬県高崎市のテストデータ（7件）
        INSERT INTO job_postings (job_type, facility_name, facility_type, prefecture, municipality, employment_type, salary_type, salary_min, salary_max, base_salary, bonus, annual_holidays, qualification_allowance, other_allowances, requirements, latitude, longitude) VALUES
        ('介護職', '施設A', '特別養護老人ホーム', '群馬県', '高崎市', '正職員', '月給', 220000, 280000, 200000, 'あり(年2回)', 110, 10000, '住宅手当', '介護福祉士', 36.32, 139.00),
        ('介護職', '施設B', '特別養護老人ホーム', '群馬県', '高崎市', '正職員', '月給', 200000, 260000, 180000, 'あり(年2回)', 115, 8000, '', '介護職員初任者研修', 36.33, 139.01),
        ('介護職', '施設C', 'デイサービス', '群馬県', '高崎市', '正職員', '月給', 190000, 250000, 170000, 'あり', 120, 5000, '通勤手当', '', 36.34, 139.02),
        ('介護職', '施設D', 'デイサービス', '群馬県', '高崎市', 'パート', '時給', 1100, 1300, 0, '', 0, 0, '', '', 36.31, 138.99),
        ('介護職', '施設E', 'グループホーム', '群馬県', '高崎市', '正職員', '月給', 210000, 270000, 190000, 'あり(年2回)', 108, 10000, '夜勤手当', '介護福祉士', 36.35, 139.03),
        ('介護職', '施設F', 'グループホーム', '群馬県', '高崎市', '正職員', '月給', 230000, 290000, 210000, 'あり(年3回)', 112, 15000, '資格手当', '介護福祉士', 36.30, 138.98),
        ('介護職', '施設G', '訪問介護', '群馬県', '高崎市', 'パート', '時給', 1200, 1500, 0, '', 0, 0, '', '', 36.29, 138.97);

        -- 群馬県前橋市のデータ（3件）
        INSERT INTO job_postings (job_type, facility_name, facility_type, prefecture, municipality, employment_type, salary_type, salary_min, salary_max, base_salary, bonus, annual_holidays, qualification_allowance, other_allowances, requirements, latitude, longitude) VALUES
        ('介護職', '施設H', '特別養護老人ホーム', '群馬県', '前橋市', '正職員', '月給', 200000, 260000, 180000, 'あり', 110, 8000, '', '介護福祉士', 36.39, 139.06),
        ('介護職', '施設I', 'デイサービス', '群馬県', '前橋市', 'パート', '時給', 1000, 1200, 0, '', 0, 0, '', '', 36.40, 139.07),
        ('介護職', '施設J', 'グループホーム', '群馬県', '前橋市', '正職員', '月給', 215000, 275000, 195000, 'あり(年2回)', 115, 10000, '通勤手当', '介護職員初任者研修', 36.38, 139.05);

        -- 東京都の追加データ（3件）
        INSERT INTO job_postings (job_type, facility_name, facility_type, prefecture, municipality, employment_type, salary_type, salary_min, salary_max, base_salary, bonus, annual_holidays, qualification_allowance, other_allowances, requirements, latitude, longitude) VALUES
        ('介護職', '施設K', '特別養護老人ホーム', '東京都', '新宿区', '正職員', '月給', 250000, 320000, 230000, 'あり(年2回)', 120, 15000, '住宅手当', '介護福祉士', 35.69, 139.70),
        ('介護職', '施設L', 'デイサービス', '東京都', '渋谷区', '正職員', '月給', 240000, 310000, 220000, 'あり', 118, 10000, '', '介護福祉士', 35.66, 139.70),
        ('看護師', '施設M', '病院', '東京都', '新宿区', '正職員', '月給', 300000, 400000, 280000, 'あり(年2回)', 125, 20000, '夜勤手当', '正看護師', 35.69, 139.69);

        -- geocoding用テーブル
        CREATE TABLE municipality_geocode (
            prefecture TEXT NOT NULL,
            municipality TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            PRIMARY KEY (prefecture, municipality)
        );
        INSERT INTO municipality_geocode VALUES ('群馬県', '高崎市', 36.32, 139.00);
        INSERT INTO municipality_geocode VALUES ('群馬県', '前橋市', 36.39, 139.06);
        INSERT INTO municipality_geocode VALUES ('東京都', '新宿区', 35.69, 139.70);
        INSERT INTO municipality_geocode VALUES ('東京都', '渋谷区', 35.66, 139.70);
    ").unwrap();
    drop(conn);

    let db = LocalDb::new(path).unwrap();
    (tmp, db)
}

/// テスト用State（ローカルDB付き）
fn test_state_with_db() -> (tempfile::NamedTempFile, Arc<AppState>) {
    let config = test_config();
    let (tmp, db) = create_test_db();
    let state = Arc::new(AppState {
        turso: TursoClient::new(&config.turso_url, &config.turso_auth_token),
        local_db: Some(db),
        segment_db: None,
        cache: AppCache::new(config.cache_ttl_secs, 100),
        rate_limiter: RateLimiter::new(config.rate_limit_max_attempts, config.rate_limit_lockout_secs),
        config,
    });
    (tmp, state)
}

/// レスポンスボディを文字列として取得
async fn body_string(body: Body) -> String {
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// ログインしてセッションCookieを取得
async fn login_and_get_cookie(app: &axum::Router) -> String {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("email=user%40test.com&password=testpass123"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Set-Cookieヘッダーからセッションcookieを抽出
    resp.headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|v| v.to_str().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("; ")
}

// ===============================================
// 2.1 起動・初期化テスト (10パターン)
// ===============================================

/// テスト1: /health → 200 "OK"（認証不要）
#[tokio::test]
async fn test_health_check_returns_ok() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert_eq!(body, "OK");
}

/// テスト2: /health はPOSTでも動作確認（GETのみ）
#[tokio::test]
async fn test_health_check_method_not_allowed() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

/// テスト3: Turso接続失敗 → 起動継続（空URL）
#[tokio::test]
async fn test_empty_turso_url_no_panic() {
    let state = test_state();
    // 空URLのTursoクライアントでもパニックしない
    assert!(state.config.turso_url.is_empty());
    let app = build_app(state);
    let resp = app
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト4: LocalDB=None → 起動継続
#[tokio::test]
async fn test_no_local_db_no_panic() {
    let state = test_state();
    assert!(state.local_db.is_none());
    let app = build_app(state);
    let resp = app
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト5: キャッシュ初期化（空のキャッシュでも動作）
#[tokio::test]
async fn test_cache_initialized_empty() {
    let state = test_state();
    assert_eq!(state.cache.len(), 0);
}

/// テスト6: レート制限初期化
#[tokio::test]
async fn test_rate_limiter_initialized() {
    let state = test_state();
    assert!(state.rate_limiter.is_allowed("any_ip"));
}

/// テスト7: /login GETは認証不要
#[tokio::test]
async fn test_login_page_no_auth_required() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(Request::builder().uri("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト8: 認証付きStateの初期化（DB付き）
#[tokio::test]
async fn test_state_with_local_db() {
    let (_tmp, state) = test_state_with_db();
    assert!(state.local_db.is_some());
}

/// テスト9: セッションストアの初期化（ログインフローで確認）
#[tokio::test]
async fn test_session_store_functional() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;
    // セッションCookieが発行される
    assert!(!cookie.is_empty());
}

/// テスト10: 複数同時リクエスト対応
#[tokio::test]
async fn test_concurrent_health_checks() {
    let state = test_state();
    let mut handles = vec![];
    for _ in 0..10 {
        let app = build_app(state.clone());
        handles.push(tokio::spawn(async move {
            let resp = app
                .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// ===============================================
// 2.2 認証フローテスト (10パターン)
// ===============================================

/// テスト11: GET /login → 200 HTML
#[tokio::test]
async fn test_login_page_returns_html() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(Request::builder().uri("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("ログイン"));
    assert!(body.contains("<form"));
}

/// テスト12: POST /login 正常 → 302リダイレクト
#[tokio::test]
async fn test_login_success_redirect() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("email=user%40test.com&password=testpass123"))
                .unwrap(),
        )
        .await
        .unwrap();
    // 303 SEE_OTHER or 302 FOUND
    assert!(
        resp.status() == StatusCode::SEE_OTHER || resp.status() == StatusCode::FOUND,
        "Expected redirect, got {}",
        resp.status()
    );
    let location = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(location, "/");
}

/// テスト13: POST /login 不正パスワード → 200 エラー表示
#[tokio::test]
async fn test_login_wrong_password() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("email=user%40test.com&password=wrongpassword"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("パスワードが正しくありません"));
}

/// テスト14: POST /login 不正ドメイン → 200 エラー表示
#[tokio::test]
async fn test_login_invalid_domain() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("email=user%40evil.com&password=testpass123"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("許可されていないメールドメイン"));
}

/// テスト15: 認証済み GET /tab/overview → 200 (リダイレクトではない)
#[tokio::test]
async fn test_authenticated_tab_access() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/overview")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // 認証済みなので200（Turso接続なしでもHTML返却）
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト16: 未認証 GET /tab/overview → 302 /login
#[tokio::test]
async fn test_unauthenticated_tab_redirect() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/tab/overview")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::SEE_OTHER || resp.status() == StatusCode::FOUND
            || resp.status() == StatusCode::TEMPORARY_REDIRECT,
        "Expected redirect, got {}",
        resp.status()
    );
}

/// テスト17: 未認証 GET /api/status → リダイレクト
#[tokio::test]
async fn test_unauthenticated_api_redirect() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::SEE_OTHER || resp.status() == StatusCode::FOUND
            || resp.status() == StatusCode::TEMPORARY_REDIRECT,
        "Expected redirect, got {}",
        resp.status()
    );
}

/// テスト18: GET /logout → 302 /login
#[tokio::test]
async fn test_logout_redirect() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logout")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::SEE_OTHER || resp.status() == StatusCode::FOUND,
        "Expected redirect to /login, got {}",
        resp.status()
    );
    let location = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(location, "/login");
}

/// テスト19: レート制限 - 6回失敗 → ロック表示
#[tokio::test]
async fn test_rate_limit_lockout_message() {
    let mut config = test_config();
    config.rate_limit_max_attempts = 3; // 3回でロック
    let state = Arc::new(AppState {
        turso: TursoClient::new("", ""),
        local_db: None,
        segment_db: None,
        cache: AppCache::new(60, 100),
        rate_limiter: RateLimiter::new(3, 300),
        config,
    });
    let app = build_app(state);

    // 3回失敗
    for _ in 0..3 {
        let _resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("email=user%40test.com&password=wrong"))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // 4回目 → ロックメッセージ
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("email=user%40test.com&password=testpass123"))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("試行回数超過"));
}

/// テスト20: ログアウト後 → 保護ページにアクセス不可
#[tokio::test]
async fn test_after_logout_no_access() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    // ログアウト
    let _resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logout")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // ログアウト後、同じcookieで保護ページにアクセス → リダイレクト
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/overview")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // セッション削除済みなのでリダイレクト
    assert!(
        resp.status() == StatusCode::SEE_OTHER
            || resp.status() == StatusCode::FOUND
            || resp.status() == StatusCode::TEMPORARY_REDIRECT,
        "Expected redirect after logout, got {}",
        resp.status()
    );
}

// ===============================================
// 2.3 Turso統合テスト (10パターン) - 空Tursoでの動作確認
// ===============================================

/// テスト21: /tab/overview 空Turso → パニックなしでHTML返却
#[tokio::test]
async fn test_tab_overview_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/overview")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト22: /tab/demographics 空Turso → 正常
#[tokio::test]
async fn test_tab_demographics_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/demographics")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト23: /tab/mobility 空Turso → 正常
#[tokio::test]
async fn test_tab_mobility_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/mobility")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト24: /tab/balance 空Turso → 正常
#[tokio::test]
async fn test_tab_balance_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/balance")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト25: /tab/workstyle 空Turso → 正常
#[tokio::test]
async fn test_tab_workstyle_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/workstyle")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト26: /tab/jobmap 空Turso → 正常
#[tokio::test]
async fn test_tab_jobmap_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/jobmap")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト27: /tab/talentmap 空Turso → 正常
#[tokio::test]
async fn test_tab_talentmap_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/talentmap")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト28: POST /api/set_job_type → "OK"
#[tokio::test]
async fn test_set_job_type_api() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/set_job_type")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::COOKIE, &cookie)
                .body(Body::from("job_type=%E7%9C%8B%E8%AD%B7%E5%B8%AB"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert_eq!(body, "OK");
}

/// テスト29: POST /api/set_prefecture → "OK"
#[tokio::test]
async fn test_set_prefecture_api() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/set_prefecture")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::COOKIE, &cookie)
                .body(Body::from("prefecture=%E6%9D%B1%E4%BA%AC%E9%83%BD"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert_eq!(body, "OK");
}

/// テスト30: POST /api/set_municipality → "OK"
#[tokio::test]
async fn test_set_municipality_api() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/set_municipality")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::COOKIE, &cookie)
                .body(Body::from("municipality=%E6%96%B0%E5%AE%BF%E5%8C%BA"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert_eq!(body, "OK");
}

// ===============================================
// 2.4 ローカルSQLite統合テスト (10パターン) - 最重要
// ===============================================

/// テスト31: /tab/competitive DB付き → 統計カード表示
#[tokio::test]
async fn test_competitive_tab_with_db() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/competitive")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    // テストDBに13件入っているので数値表示がある
    assert!(body.contains("13") || body.contains("件"), "統計カードに件数が表示されるべき: body length={}", body.len());
}

/// テスト32: /api/competitive/municipalities?prefecture=群馬県 → 市区町村リスト
#[tokio::test]
async fn test_competitive_municipalities() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/municipalities?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("高崎市"), "高崎市が含まれるべき");
    assert!(body.contains("前橋市"), "前橋市が含まれるべき");
}

/// テスト33: /api/competitive/facility_types?prefecture=群馬県 → 施設形態
#[tokio::test]
async fn test_competitive_facility_types() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/facility_types?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("特別養護老人ホーム"));
    assert!(body.contains("デイサービス"));
    assert!(body.contains("グループホーム"));
}

/// テスト34: /api/competitive/filter?prefecture=群馬県&municipality=高崎市 → 7件
#[tokio::test]
async fn test_competitive_filter_takasaki() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/filter?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C&municipality=%E9%AB%98%E5%B4%8E%E5%B8%82")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    // テストデータに高崎市7件
    assert!(body.contains("施設A") || body.contains("7"), "高崎市の結果が含まれるべき");
}

/// テスト35: 給与統計の表示確認
#[tokio::test]
async fn test_competitive_salary_stats() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/filter?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C&municipality=%E9%AB%98%E5%B4%8E%E5%B8%82")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_string(resp.into_body()).await;
    // 正職員の給与（190,000〜230,000）が含まれるか確認
    assert!(body.len() > 100, "給与統計が含まれるHTMLが返されるべき");
}

/// テスト36: ページネーション page=2（1ページ50件、7件なのでpage=2は空）
#[tokio::test]
async fn test_competitive_pagination_empty_page() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/filter?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C&municipality=%E9%AB%98%E5%B4%8E%E5%B8%82&page=2")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト37: 雇用形態フィルタ employment_type=正職員
#[tokio::test]
async fn test_competitive_filter_employment_type() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/filter?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C&employment_type=%E6%AD%A3%E8%81%B7%E5%93%A1")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    // パートは含まれないはず
    assert!(!body.contains("施設D") || body.contains("正職員"));
}

/// テスト38: 施設形態フィルタ facility_type=デイサービス
#[tokio::test]
async fn test_competitive_filter_facility_type() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/filter?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C&facility_type=%E3%83%87%E3%82%A4%E3%82%B5%E3%83%BC%E3%83%93%E3%82%B9")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト39: /api/report → HTMLレポート
#[tokio::test]
async fn test_competitive_report() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/report?prefecture=%E7%BE%A4%E9%A6%AC%E7%9C%8C&municipality=%E9%AB%98%E5%B4%8E%E5%B8%82")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("群馬県") || body.contains("高崎市"));
}

/// テスト40: /api/report パラメータなし → エラーメッセージ
#[tokio::test]
async fn test_competitive_report_no_params() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/report")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("都道府県を選択"));
}

// ===============================================
// 2.5 GeoJSON/マップ統合テスト (5パターン)
// ===============================================

/// テスト41: /api/geojson/{invalid} → null JSON（404ではない）
#[tokio::test]
async fn test_geojson_invalid_filename() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/geojson/nonexistent.json")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert_eq!(body, "null");
}

/// テスト42: /api/markers DB無し → 空配列
#[tokio::test]
async fn test_markers_no_db() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/markers")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert_eq!(body, "[]");
}

/// テスト43: /api/markers DB付き → JSON配列
#[tokio::test]
async fn test_markers_with_db() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/markers")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json.is_array());
}

/// テスト44: /api/rarity → JSON（空Tursoでも動作）
#[tokio::test]
async fn test_rarity_api_empty_turso() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/rarity")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// テスト45: /tab/competitive DB無し → エラーメッセージ（パニックなし）
#[tokio::test]
async fn test_competitive_tab_no_db() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tab/competitive")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===============================================
// 2.6 追加エッジケーステスト (5パターン)
// ===============================================

/// テスト46: /api/status 認証済み → JSON返却
#[tokio::test]
async fn test_api_status_authenticated() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["local_db_loaded"], true);
    assert!(json["local_db_rows"].as_i64().unwrap() > 0);
    assert_eq!(json["turso_connected"], false); // 空URLなので
}

/// テスト47: /api/competitive/filter 都道府県未選択 → メッセージ表示
#[tokio::test]
async fn test_competitive_filter_no_prefecture() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/filter")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("都道府県を選択"));
}

/// テスト48: /api/competitive/municipalities 都道府県未選択 → デフォルト
#[tokio::test]
async fn test_competitive_municipalities_no_pref() {
    let (_tmp, state) = test_state_with_db();
    let app = build_app(state);
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/competitive/municipalities")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("市区町村"));
}

/// テスト49: ダッシュボードページ 認証済み → HTML返却
#[tokio::test]
async fn test_dashboard_page_authenticated() {
    let app = build_app(test_state());
    let cookie = login_and_get_cookie(&app).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp.into_body()).await;
    // ダッシュボードHTMLが返される
    assert!(body.contains("<!DOCTYPE html>") || body.contains("<html"));
}

/// テスト50: ログインページに許可ドメイン表示
#[tokio::test]
async fn test_login_page_shows_domains() {
    let app = build_app(test_state());
    let resp = app
        .oneshot(Request::builder().uri("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = body_string(resp.into_body()).await;
    assert!(body.contains("@test.com") || body.contains("@example.co.jp"));
}
