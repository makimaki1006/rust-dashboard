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

#[cfg(test)]
mod tests {
    use super::*;

    // テスト41: 正しいドメイン → 許可
    #[test]
    fn test_valid_domain_allowed() {
        let domains = vec!["example.com".to_string(), "test.co.jp".to_string()];
        assert!(validate_email_domain("user@example.com", &domains));
        assert!(validate_email_domain("user@test.co.jp", &domains));
    }

    // テスト41逆証明: 不正ドメイン → 拒否
    #[test]
    fn test_invalid_domain_rejected() {
        let domains = vec!["example.com".to_string()];
        assert!(!validate_email_domain("user@evil.com", &domains));
    }

    // ドメインなしメール → 拒否
    #[test]
    fn test_no_at_sign_rejected() {
        let domains = vec!["example.com".to_string()];
        assert!(!validate_email_domain("invalid-email", &domains));
    }

    // 大文字小文字の区別なし
    #[test]
    fn test_case_insensitive_domain() {
        let domains = vec!["example.com".to_string()];
        assert!(validate_email_domain("User@EXAMPLE.COM", &domains));
    }

    // テスト42: 平文パスワード一致 → 認証OK
    #[test]
    fn test_plain_password_match() {
        assert!(verify_password("secret", "secret", ""));
    }

    // テスト42逆証明: 不一致 → 拒否
    #[test]
    fn test_plain_password_mismatch() {
        assert!(!verify_password("wrong", "secret", ""));
    }

    // テスト43: bcryptハッシュ一致 → 認証OK
    #[test]
    fn test_bcrypt_password_match() {
        let hash = bcrypt::hash("mypassword", 4).unwrap();
        assert!(verify_password("mypassword", "", &hash));
    }

    // テスト43逆証明: bcrypt不一致 → 拒否
    #[test]
    fn test_bcrypt_password_mismatch() {
        let hash = bcrypt::hash("mypassword", 4).unwrap();
        assert!(!verify_password("wrongpassword", "", &hash));
    }

    // 両方空 → 拒否
    #[test]
    fn test_no_password_configured() {
        assert!(!verify_password("anything", "", ""));
    }
}
