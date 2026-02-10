pub mod overview;
pub mod demographics;
pub mod mobility;
pub mod balance;
pub mod workstyle;
pub mod jobmap;
pub mod talentmap;
pub mod competitive;
pub mod api;

use axum::response::Html;

/// プレースホルダーレスポンス（未実装タブ用）
pub fn placeholder_html(tab_name: &str) -> Html<String> {
    Html(format!(
        r#"<div class="p-8 text-center text-gray-400">
            <h2 class="text-2xl mb-4">{tab_name}</h2>
            <p>このタブは実装中です</p>
        </div>"#
    ))
}
