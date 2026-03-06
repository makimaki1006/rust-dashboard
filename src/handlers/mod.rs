pub mod overview;
pub mod demographics;
pub mod mobility;
pub mod balance;
pub mod workstyle;
pub mod jobmap;
pub mod talentmap;
pub mod competitive;
pub mod segment;
pub mod analysis;
pub mod job_creator;
pub mod api;

use axum::response::Html;

/// 統一カラーパレット（Okabe-Ito系、CSS変数と対応）
/// EChartsのJSON色指定にはCSS変数が使えないためRust側で定数定義
#[allow(dead_code)]
pub mod colors {
    // 性別
    pub const MALE: &str = "#0072B2";
    pub const FEMALE: &str = "#E69F00";
    // 雇用形態
    pub const EMP_REGULAR: &str = "#009E73";
    pub const EMP_PART: &str = "#CC79A7";
    pub const EMP_CONTRACT: &str = "#56B4E9";
    pub const EMP_OUTSOURCE: &str = "#8b5cf6";
    // 就業状態
    pub const EMPLOYED: &str = "#009E73";
    pub const UNEMPLOYED: &str = "#D55E00";
    pub const STUDENT: &str = "#F0E442";
    // アクセント
    pub const ACCENT_INDIGO: &str = "#6366F1";
    pub const ACCENT_GREEN: &str = "#10B981";
    pub const ACCENT_AMBER: &str = "#F59E0B";
    pub const ACCENT_RED: &str = "#EF4444";
}

/// 空データ状態の統一HTMLコンポーネント
pub fn render_empty_state(title: &str, message: &str) -> String {
    format!(
        r#"<div class="flex flex-col items-center justify-center py-16 text-center">
            <svg class="w-16 h-16 text-slate-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
                      d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"/>
            </svg>
            <h3 class="text-lg font-semibold text-slate-300 mb-2">{}</h3>
            <p class="text-slate-400 max-w-md">{}</p>
        </div>"#,
        title, message
    )
}

/// エラー状態の統一HTMLコンポーネント
pub fn render_error_state(title: &str, message: &str) -> String {
    format!(
        r#"<div class="flex flex-col items-center justify-center py-16 text-center">
            <svg class="w-16 h-16 text-red-400 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
                      d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z"/>
            </svg>
            <h3 class="text-lg font-semibold text-red-300 mb-2">{}</h3>
            <p class="text-slate-400 max-w-md">{}</p>
        </div>"#,
        title, message
    )
}

/// プレースホルダーレスポンス（未実装タブ用）
pub fn placeholder_html(tab_name: &str) -> Html<String> {
    Html(format!(
        r#"<div class="p-8 text-center text-gray-400">
            <h2 class="text-2xl mb-4">{tab_name}</h2>
            <p>このタブは実装中です</p>
        </div>"#
    ))
}
