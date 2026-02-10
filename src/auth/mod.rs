pub mod session;

use axum::{
    extract::Request,
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
};
use tower_sessions::Session;

/// セッションキー
pub const SESSION_USER_KEY: &str = "user_email";
pub const SESSION_JOB_TYPE_KEY: &str = "current_job_type";
pub const SESSION_PREFECTURE_KEY: &str = "current_prefecture";
pub const SESSION_MUNICIPALITY_KEY: &str = "current_municipality";

/// 認証ミドルウェア: ログイン済みでなければ /login へリダイレクト
pub async fn require_auth(session: Session, request: Request, next: Next) -> Response {
    let user: Option<String> = session.get(SESSION_USER_KEY).await.unwrap_or(None);
    if user.is_some() {
        next.run(request).await
    } else {
        Redirect::to("/login").into_response()
    }
}

/// メールアドレスのドメインが許可リストに含まれるか検証
pub fn validate_email_domain(email: &str, allowed_domains: &[String]) -> bool {
    let email_lower = email.to_lowercase();
    if let Some(domain) = email_lower.split('@').nth(1) {
        allowed_domains.iter().any(|d| d == domain)
    } else {
        false
    }
}

/// パスワード検証（bcryptハッシュまたは平文）
pub fn verify_password(input: &str, plain: &str, hash: &str) -> bool {
    if !hash.is_empty() {
        bcrypt::verify(input, hash).unwrap_or(false)
    } else if !plain.is_empty() {
        input == plain
    } else {
        false
    }
}
