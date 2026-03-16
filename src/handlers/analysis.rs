use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::db::analytics;
use crate::AppState;
use crate::models::job_seeker::PREFECTURE_ORDER;
use super::competitive::escape_html;
use super::overview::{get_session_filters, make_location_label, format_number};

/// 近隣都道府県を返す（品質マップのフィルタリング用）
fn adjacent_prefectures(pref: &str) -> &'static [&'static str] {
    match pref {
        "北海道" => &["青森県"],
        "青森県" => &["北海道", "岩手県", "秋田県"],
        "岩手県" => &["青森県", "秋田県", "宮城県"],
        "宮城県" => &["岩手県", "秋田県", "山形県", "福島県"],
        "秋田県" => &["青森県", "岩手県", "宮城県", "山形県"],
        "山形県" => &["秋田県", "宮城県", "福島県", "新潟県"],
        "福島県" => &["宮城県", "山形県", "新潟県", "群馬県", "栃木県", "茨城県"],
        "茨城県" => &["福島県", "栃木県", "埼玉県", "千葉県"],
        "栃木県" => &["福島県", "茨城県", "群馬県", "埼玉県"],
        "群馬県" => &["福島県", "新潟県", "長野県", "埼玉県", "栃木県"],
        "埼玉県" => &["群馬県", "栃木県", "茨城県", "千葉県", "東京都", "山梨県", "長野県"],
        "千葉県" => &["茨城県", "埼玉県", "東京都"],
        "東京都" => &["埼玉県", "千葉県", "神奈川県", "山梨県"],
        "神奈川県" => &["東京都", "山梨県", "静岡県"],
        "新潟県" => &["山形県", "福島県", "群馬県", "長野県", "富山県"],
        "富山県" => &["新潟県", "長野県", "岐阜県", "石川県"],
        "石川県" => &["富山県", "岐阜県", "福井県"],
        "福井県" => &["石川県", "岐阜県", "滋賀県", "京都府"],
        "山梨県" => &["埼玉県", "東京都", "神奈川県", "長野県", "静岡県"],
        "長野県" => &["新潟県", "群馬県", "埼玉県", "山梨県", "静岡県", "愛知県", "岐阜県", "富山県"],
        "岐阜県" => &["富山県", "石川県", "福井県", "長野県", "愛知県", "三重県", "滋賀県"],
        "静岡県" => &["神奈川県", "山梨県", "長野県", "愛知県"],
        "愛知県" => &["静岡県", "長野県", "岐阜県", "三重県"],
        "三重県" => &["愛知県", "岐阜県", "滋賀県", "京都府", "奈良県", "和歌山県"],
        "滋賀県" => &["福井県", "岐阜県", "三重県", "京都府"],
        "京都府" => &["福井県", "滋賀県", "三重県", "奈良県", "大阪府", "兵庫県"],
        "大阪府" => &["京都府", "奈良県", "和歌山県", "兵庫県"],
        "兵庫県" => &["京都府", "大阪府", "鳥取県", "岡山県", "徳島県"],
        "奈良県" => &["三重県", "京都府", "大阪府", "和歌山県"],
        "和歌山県" => &["三重県", "奈良県", "大阪府"],
        "鳥取県" => &["兵庫県", "岡山県", "島根県", "広島県"],
        "島根県" => &["鳥取県", "広島県", "山口県"],
        "岡山県" => &["兵庫県", "鳥取県", "広島県", "香川県"],
        "広島県" => &["鳥取県", "島根県", "岡山県", "山口県", "愛媛県"],
        "山口県" => &["島根県", "広島県", "福岡県"],
        "徳島県" => &["兵庫県", "香川県", "愛媛県", "高知県"],
        "香川県" => &["徳島県", "愛媛県", "岡山県"],
        "愛媛県" => &["徳島県", "香川県", "高知県", "広島県"],
        "高知県" => &["徳島県", "愛媛県"],
        "福岡県" => &["山口県", "佐賀県", "熊本県", "大分県"],
        "佐賀県" => &["福岡県", "長崎県"],
        "長崎県" => &["佐賀県"],
        "熊本県" => &["福岡県", "大分県", "宮崎県", "鹿児島県"],
        "大分県" => &["福岡県", "熊本県", "宮崎県"],
        "宮崎県" => &["大分県", "熊本県", "鹿児島県"],
        "鹿児島県" => &["熊本県", "宮崎県"],
        "沖縄県" => &[],
        _ => &[],
    }
}

/// 指定都道府県がフィルタ対象か判定（選択県 + 近隣県）
/// 分析タブ クエリパラメータ
#[derive(Deserialize)]
pub struct AnalysisParams {
    pub layer: Option<String>,
    pub min_lift: Option<f64>,
    pub cluster_id: Option<i32>,
}

/// 地域比較用パラメータ
#[derive(Deserialize)]
pub struct CompareParams {
    pub pref2: Option<String>,
}

// ---------------------------------------------------------------------------
// メインタブ: /tab/analysis
// ---------------------------------------------------------------------------

/// 市場分析タブ - メインビュー
pub async fn tab_analysis(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_tab_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let location_label = make_location_label(&prefecture, &municipality);

    // サマリー取得: 市区町村指定時はpostingsテーブル(ローカル)、それ以外はTurso
    let use_postings = !municipality.is_empty();
    let summary = if use_postings {
        // 市区町村指定時: ローカルDBから直接集計
        if let Some(db) = &state.geocoded_db {
            let db_clone = db.clone();
            let jt = job_type.clone();
            let pref = prefecture.clone();
            let muni = municipality.clone();
            tokio::task::spawn_blocking(move || {
                analytics::query_analysis_summary(&db_clone, &jt, &pref, &muni).unwrap_or_default()
            }).await.unwrap_or_default()
        } else {
            HashMap::new()
        }
    } else {
        // 都道府県/全国: Tursoからlayerテーブル集計
        match analytics::query_analysis_summary_turso(&state.turso, &job_type, &prefecture).await {
            Ok(s) => s,
            Err(_e) => {
                // フォールバック: ローカルDB
                if let Some(db) = &state.geocoded_db {
                    let db_clone = db.clone();
                    let jt = job_type.clone();
                    let pref = prefecture.clone();
                    let muni = municipality.clone();
                    tokio::task::spawn_blocking(move || {
                        analytics::query_analysis_summary(&db_clone, &jt, &pref, &muni).unwrap_or_default()
                    }).await.unwrap_or_default()
                } else {
                    HashMap::new()
                }
            }
        }
    };
    let salary_count = summary.get("salary_stat_count").and_then(|v| v.as_i64()).unwrap_or(0);
    let cluster_count = summary.get("cluster_count").and_then(|v| v.as_i64()).unwrap_or(0);
    let keyword_count = summary.get("keyword_count").and_then(|v| v.as_i64()).unwrap_or(0);
    let quality_grade = summary.get("text_quality_grade").and_then(|v| v.as_str()).unwrap_or("-");
    let total_postings = summary.get("facility_total_postings").and_then(|v| v.as_i64()).unwrap_or(0);
    let cooccurrence_count = summary.get("cooccurrence_count").and_then(|v| v.as_i64()).unwrap_or(0);

    let grade_color = match quality_grade {
        "A" => "text-emerald-400",
        "B" => "text-blue-400",
        "C" => "text-yellow-400",
        "D" => "text-red-400",
        _ => "text-gray-400",
    };

    let html = format!(r##"
<div class="p-6 space-y-6">
    <div class="flex items-center justify-between mb-2">
        <h2 class="text-2xl font-bold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z'/></svg> 市場分析 — {job_type} ({location})</h2>
    </div>

    <!-- サマリーカード -->
    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">総求人数</div>
            <div class="text-xl font-bold text-white">{total_postings}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">給与パターン</div>
            <div class="text-xl font-bold text-white">{salary_count}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">セグメント数</div>
            <div class="text-xl font-bold text-white">{cluster_count}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">キーワード数</div>
            <div class="text-xl font-bold text-white">{keyword_count}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">条件の組み合わせ</div>
            <div class="text-xl font-bold text-white">{cooccurrence_count}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">求人原稿の充実度</div>
            <div class="text-xl font-bold {grade_color}">{grade}</div>
        </div>
    </div>

    <!-- サブタブ -->
    <div class="border-b border-slate-700">
        <nav class="flex gap-1 overflow-x-auto" id="analysis-subtabs">
            <button class="analysis-sub-btn active px-4 py-2 text-sm rounded-t-lg bg-navy-700 text-white border border-slate-600 border-b-0"
                    hx-get="/api/analysis/salary" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M4.5 4.5l7.5 9 7.5-9M4.5 12h15M4.5 15h15M12 12v9'/></svg>  給与分析</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/facility" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M3.75 21h16.5M4.5 3h15M5.25 3v18m13.5-18v18M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21'/></svg>  法人の分布</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/employment" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15a2.25 2.25 0 012.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V19.5a2.25 2.25 0 002.25 2.25h.75'/></svg>  雇用多様性</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/keywords" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M15.75 5.25a3 3 0 013 3m3 0a6 6 0 01-7.029 5.912c-.563-.097-1.159.026-1.563.43L10.5 17.25H8.25v2.25H6v2.25H2.25v-2.818c0-.597.237-1.17.659-1.591l6.499-6.499c.404-.404.527-1 .43-1.563A6 6 0 1121.75 8.25z'/></svg>  キーワード</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/cooccurrence" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244'/></svg>  条件の組み合わせ</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/quality" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z'/></svg>  求人原稿の充実度</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/clusters" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M12 2.25v2.25m0 15v2.25M2.25 12h2.25m15 0h2.25M12 6a6 6 0 100 12 6 6 0 000-12z'/></svg>  求人タイプ</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/heatmap" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M9 6.75V15m6-6v8.25m.503 3.498l4.875-2.437c.381-.19.622-.58.622-1.006V4.82c0-.836-.88-1.38-1.628-1.006l-3.869 1.934c-.317.159-.69.159-1.006 0L9.503 3.252a1.125 1.125 0 00-1.006 0L3.622 5.689C3.24 5.88 3 6.27 3 6.695V19.18c0 .836.88 1.38 1.628 1.006l3.869-1.934c.317-.159.69-.159 1.006 0l4.994 2.497c.317.158.69.158 1.006 0z'/></svg>  地域分布</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/compare" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M19.5 12c0-1.232-.046-2.453-.138-3.662a4.006 4.006 0 00-3.7-3.7 48.678 48.678 0 00-7.324 0 4.006 4.006 0 00-3.7 3.7c-.017.22-.032.441-.046.662M19.5 12l3-3m-3 3l-3-3m-12 3c0 1.232.046 2.453.138 3.662a4.006 4.006 0 003.7 3.7 48.656 48.656 0 007.324 0 4.006 4.006 0 003.7-3.7c.017-.22.032-.441.046-.662M4.5 12l3 3m-3-3l-3 3'/></svg>  地域比較</button>
            <span class="mx-1 border-l border-slate-600 h-6 self-center"></span>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/text_analysis" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M7.5 8.25h9m-9 3H12m-9.75 1.51c0 1.6 1.123 2.994 2.707 3.227 1.129.166 2.27.293 3.423.379.35.026.67.21.865.501L12 21l2.755-4.133a1.14 1.14 0 01.865-.501 48.172 48.172 0 003.423-.379c1.584-.233 2.707-1.626 2.707-3.228V6.741c0-1.602-1.123-2.995-2.707-3.228A48.394 48.394 0 0012 3c-2.392 0-4.744.175-7.043.513C3.373 3.746 2.25 5.14 2.25 6.741v6.018z'/></svg>  テキスト分析</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/tone" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M15.182 15.182a4.5 4.5 0 01-6.364 0M21 12a9 9 0 11-18 0 9 9 0 0118 0zM9.75 9.75c0 .414-.168.75-.375.75S9 10.164 9 9.75 9.168 9 9.375 9s.375.336.375.75zm-.375 0h.008v.015h-.008V9.75zm5.625 0c0 .414-.168.75-.375.75s-.375-.336-.375-.75.168-.75.375-.75.375.336.375.75zm-.375 0h.008v.015h-.008V9.75z'/></svg>  トーン</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/info_score" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z'/></svg>  情報充足度</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/targeting" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z'/></svg>  ターゲティング</button>
        </nav>
    </div>

    <!-- コンテンツ領域 -->
    <div id="analysis-content">
        <div class="flex items-center justify-center h-32 text-gray-400">
            <div class="animate-pulse">読み込み中...</div>
        </div>
    </div>
</div>

<script>
// 初期サブタブを明示的にロード
(function() {{
    htmx.ajax('GET', '/api/analysis/salary', {{target: '#analysis-content', swap: 'innerHTML'}});
}})();
function setAnalysisSubTab(el) {{
    document.querySelectorAll('.analysis-sub-btn').forEach(function(btn) {{
        btn.classList.remove('active', 'bg-navy-700', 'text-white', 'border', 'border-slate-600', 'border-b-0');
        btn.classList.add('text-gray-400');
    }});
    el.classList.add('active', 'bg-navy-700', 'text-white', 'border', 'border-slate-600', 'border-b-0');
    el.classList.remove('text-gray-400');
}}
</script>
"##,
        job_type = escape_html(&job_type),
        location = escape_html(&location_label),
        total_postings = format_number(total_postings),
        salary_count = salary_count,
        cluster_count = cluster_count,
        keyword_count = keyword_count,
        cooccurrence_count = cooccurrence_count,
        grade = quality_grade,
        grade_color = grade_color,
    );

    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// A-1: 給与分析 API
// ---------------------------------------------------------------------------

pub async fn api_salary(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_salary_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    let location_label = make_location_label(&prefecture, &municipality);

    // 市区町村指定時は postings テーブルから直接計算（下限/上限分離）
    let use_postings = !municipality.is_empty();

    if use_postings {
        // postings テーブルから salary_min/salary_max の統計を取得
        let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone(); let muni = municipality.clone();
        let prows = match tokio::task::spawn_blocking(move || {
            analytics::query_salary_from_postings(&db_c, &jt, &pref, &muni)
        }).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Html(error_html(&e)),
            Err(e) => return Html(error_html(&format!("spawn_blocking: {e}"))),
        };
        if prows.is_empty() {
            return Html(empty_html("給与統計データがありません"));
        }

        let mut html = format!(
            r#"<div class="space-y-6">
            <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M4.5 4.5l7.5 9 7.5-9M4.5 12h15M4.5 15h15M12 12v9'/></svg>  給与分布分析 — {}</h3>"#,
            escape_html(&location_label)
        );

        // 月給データ
        let monthly: Vec<_> = prows.iter().filter(|r| r.get("salary_type").and_then(|v| v.as_str()) == Some("月給")).collect();
        let hourly: Vec<_> = prows.iter().filter(|r| r.get("salary_type").and_then(|v| v.as_str()) == Some("時給")).collect();

        if !monthly.is_empty() {
            html.push_str(&render_salary_minmax_chart(&monthly, "月給", "salary-monthly-minmax", true));
        }
        if !hourly.is_empty() {
            html.push_str(&render_salary_minmax_chart(&hourly, "時給", "salary-hourly-minmax", false));
        }

        // テーブル（下限/上限分離）
        html.push_str(&render_salary_minmax_table(&prows));
        html.push_str("</div>");
        state.cache.set(cache_key, Value::String(html.clone()));
        return Html(html);
    }

    // 都道府県レベル: postingsからの下限/上限(ローカル) + layerテーブル(Turso)
    let db_c = db.clone(); let jt_p = job_type.clone(); let pref_p = prefecture.clone();
    let prows = match tokio::task::spawn_blocking(move || {
        analytics::query_salary_from_postings(&db_c, &jt_p, &pref_p, "")
    }).await {
        Ok(Ok(r)) => r,
        _ => Vec::new(),
    };

    // layerテーブルはTurso経由で取得（フォールバック: ローカルDB）
    let rows = match analytics::query_salary_stats_turso(&state.turso, &job_type, &prefecture).await {
        Ok(r) => r,
        Err(_e) => {
            // フォールバック: ローカルDB
            let db_c2 = db.clone(); let jt2 = job_type.clone(); let pref2 = prefecture.clone();
            tokio::task::spawn_blocking(move || {
                analytics::query_salary_stats(&db_c2, &jt2, &pref2).unwrap_or_default()
            }).await.unwrap_or_default()
        }
    };

    // いずれかにデータがあればOK
    if rows.is_empty() && prows.is_empty() {
        return Html(empty_html("給与統計データがありません"));
    }

    let mut html = format!(
        r#"<div class="space-y-6">
        <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M4.5 4.5l7.5 9 7.5-9M4.5 12h15M4.5 15h15M12 12v9'/></svg>  給与分布分析 — {}</h3>"#,
        escape_html(&location_label)
    );

    // 下限/上限分離チャートを優先表示
    if !prows.is_empty() {
        let monthly: Vec<_> = prows.iter().filter(|r| r.get("salary_type").and_then(|v| v.as_str()) == Some("月給")).collect();
        let hourly: Vec<_> = prows.iter().filter(|r| r.get("salary_type").and_then(|v| v.as_str()) == Some("時給")).collect();

        if !monthly.is_empty() {
            html.push_str(&render_salary_minmax_chart(&monthly, "月給", "salary-monthly-minmax", true));
        }
        if !hourly.is_empty() {
            html.push_str(&render_salary_minmax_chart(&hourly, "時給", "salary-hourly-minmax", false));
        }
        html.push_str(&render_salary_minmax_table(&prows));
    } else if !rows.is_empty() {
        // フォールバック: 従来テーブルからグループ棒グラフ
        let monthly_rows: Vec<_> = rows.iter().filter(|r| {
            r.get("salary_type").and_then(|v| v.as_str()) == Some("月給")
        }).collect();
        let hourly_rows: Vec<_> = rows.iter().filter(|r| {
            r.get("salary_type").and_then(|v| v.as_str()) == Some("時給")
        }).collect();

        if !monthly_rows.is_empty() {
            html.push_str(&render_salary_grouped_chart(&monthly_rows, "月給", "salary-monthly-box", true));
        }
        if !hourly_rows.is_empty() {
            html.push_str(&render_salary_grouped_chart(&hourly_rows, "時給", "salary-hourly-box", false));
        }

        // テーブル（従来版）
        html.push_str(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-3">詳細数値</h4>
            <div class="overflow-x-auto">
            <table class="w-full text-xs">
                <thead>
                    <tr class="text-gray-400 border-b border-slate-700">
                        <th class="text-left p-1.5">種別</th>
                        <th class="text-left p-1.5">雇用形態</th>
                        <th class="text-right p-1.5">件数</th>
                        <th class="text-right p-1.5">平均</th>
                        <th class="text-right p-1.5">中央値</th>
                        <th class="text-right p-1.5" title="下位25%ライン：この金額以下が全体の25%">下位25%</th>
                        <th class="text-right p-1.5" title="上位25%ライン：この金額以上が全体の25%">上位25%</th>
                        <th class="text-right p-1.5" title="上位10%ライン：この金額以上が全体の10%">上位10%</th>
                        <th class="text-right p-1.5" title="給与の偏り度合い。0に近いほど均等、1に近いほど偏りが大きい">給与の偏り</th>
                    </tr>
                </thead>
                <tbody>"#);

        for row in &rows {
            let salary_type = row.get("salary_type").and_then(|v| v.as_str()).unwrap_or("-");
            let emp_type = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
            let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            let mean = row.get("mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let median = row.get("median").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let p25 = row.get("p25").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let p75 = row.get("p75").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let p90 = row.get("p90").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let gini = row.get("gini").and_then(|v| v.as_f64()).unwrap_or(0.0);

            html.push_str(&format!(
                r#"<tr class="border-b border-slate-800 hover:bg-navy-700/50">
                    <td class="p-1.5 text-cyan-300">{salary_type}</td>
                    <td class="p-1.5">{emp_type}</td>
                    <td class="p-1.5 text-right">{count}</td>
                    <td class="p-1.5 text-right font-mono">{mean}</td>
                    <td class="p-1.5 text-right font-mono">{median}</td>
                    <td class="p-1.5 text-right font-mono text-gray-400">{p25}</td>
                    <td class="p-1.5 text-right font-mono text-gray-400">{p75}</td>
                    <td class="p-1.5 text-right font-mono text-gray-400">{p90}</td>
                    <td class="p-1.5 text-right">{gini_cell}</td>
                </tr>"#,
                salary_type = escape_html(salary_type),
                emp_type = escape_html(emp_type),
                count = format_number(count),
                mean = format_yen(mean),
                median = format_yen(median),
                p25 = format_yen(p25),
                p75 = format_yen(p75),
                p90 = format_yen(p90),
                gini_cell = {
                    let (color, label) = if gini < 0.05 {
                        ("text-green-400", "均等")
                    } else if gini < 0.10 {
                        ("text-blue-400", "やや偏り")
                    } else {
                        ("text-amber-400", "偏りあり")
                    };
                    format!(r#"<span class="font-mono">{:.3}</span> <span class="{} text-[10px]">{}</span>"#, gini, color, label)
                },
            ));
        }

        html.push_str("</tbody></table></div></div>");
    }

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// A-2: 法人集中度 API
// ---------------------------------------------------------------------------

pub async fn api_facility(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_facility_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    // 市区町村指定時は postings テーブル(ローカル)から直接計算
    // 都道府県指定時は Turso から layer テーブル取得
    let rows = if !municipality.is_empty() {
        let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone(); let muni = municipality.clone();
        match tokio::task::spawn_blocking(move || {
            analytics::query_facility_from_postings(&db_c, &jt, &pref, &muni)
        }).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Html(error_html(&e)),
            Err(e) => return Html(error_html(&format!("spawn_blocking: {e}"))),
        }
    } else {
        match analytics::query_facility_concentration_turso(&state.turso, &job_type, &prefecture).await {
            Ok(r) => r,
            Err(_e) => {
                // フォールバック: ローカルDB
                let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone();
                tokio::task::spawn_blocking(move || {
                    analytics::query_facility_concentration(&db_c, &jt, &pref).unwrap_or_default()
                }).await.unwrap_or_default()
            }
        }
    };

    if rows.is_empty() {
        return Html(empty_html("法人集中度データがありません"));
    }

    let location_label = make_location_label(&prefecture, &municipality);
    let row = &rows[0];
    let total = row.get("total_postings").and_then(|v| v.as_i64()).unwrap_or(0);
    let unique = row.get("unique_facilities").and_then(|v| v.as_i64()).unwrap_or(0);
    let top1_name = row.get("top1_name").and_then(|v| v.as_str()).unwrap_or("-");
    let top1_pct = row.get("top1_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let top5_pct = row.get("top5_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let top10_pct = row.get("top10_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let top20_pct = row.get("top20_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let hhi = row.get("hhi").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let zipf = row.get("zipf_exponent").and_then(|v| v.as_f64()).unwrap_or(0.0);

    let hhi_label = if hhi < 0.01 { "極めて分散" } else if hhi < 0.1 { "分散的" } else if hhi < 0.25 { "やや集中" } else { "高度集中" };
    let hhi_color = if hhi < 0.01 { "text-emerald-400" } else if hhi < 0.1 { "text-blue-400" } else if hhi < 0.25 { "text-yellow-400" } else { "text-red-400" };

    let mut html = format!(r##"
<div class="space-y-4">
    <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M3.75 21h16.5M4.5 3h15M5.25 3v18m13.5-18v18M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21'/></svg>  求人を出している法人の分布 — {location}</h3>

    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">総求人数</div>
            <div class="text-xl font-bold text-white">{total}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">ユニーク法人数</div>
            <div class="text-xl font-bold text-cyan-400">{unique}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400" title="HHI: 求人が少数の法人に集中しているかを示す指標">市場の集中度</div>
            <div class="text-xl font-bold {hhi_color}">{hhi:.4}</div>
            <div class="text-xs {hhi_color}">{hhi_label}</div>
            <div class="text-xs text-gray-600 mt-0.5">小さいほど競争的</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">大手偏り度</div>
            <div class="text-xl font-bold text-purple-400">{zipf:.3}</div>
            <div class="text-xs text-gray-500">小さいほど大手に集中</div>
        </div>
    </div>

    <!-- Top 占有率 EChartsバーチャート -->
    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-semibold text-gray-300 mb-2">集中度指標</h4>
        <div class="mb-3 space-y-2">
            <div class="flex items-center justify-between">
                <span class="text-sm text-gray-400">Top1 法人</span>
                <span class="text-sm text-white">{top1_name} ({top1_pct:.2}%)</span>
            </div>
        </div>
        <div id="facility-topn-chart" style="width:100%;height:200px;"></div>
    </div>
"##,
        location = escape_html(&location_label),
        total = format_number(total),
        unique = format_number(unique),
        hhi = hhi,
        hhi_color = hhi_color,
        hhi_label = hhi_label,
        zipf = zipf,
        top1_name = escape_html(top1_name),
        top1_pct = top1_pct,
    );

    // 全都道府県比較チャート（Zipf指数）— Turso経由
    let all_prefs = match analytics::query_facility_all_prefectures_turso(&state.turso, &job_type).await {
        Ok(r) => r,
        Err(_e) => {
            let db_c = db.clone(); let jt = job_type.clone();
            tokio::task::spawn_blocking(move || {
                analytics::query_facility_all_prefectures(&db_c, &jt).unwrap_or_default()
            }).await.unwrap_or_default()
        }
    };
    let pref_rows: Vec<_> = all_prefs.iter().filter(|r| {
        r.get("prefecture").and_then(|v| v.as_str()).unwrap_or("") != "全国"
    }).collect();

    if !pref_rows.is_empty() {
        html.push_str(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">都道府県別 大手偏り度の比較（値が小さいほど大手法人に求人が集中）</h4>
            <div id="facility-zipf-pref-chart" style="width:100%;height:500px;"></div>
        </div>"#);
    }

    // Top N バーチャート JS
    html.push_str(&format!(r##"
    <script>
    (function() {{
        // Top N 占有率バーチャート
        var dom1 = document.getElementById('facility-topn-chart');
        if (dom1) {{
            var c1 = echarts.init(dom1, 'dark');
            c1.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }}, formatter: function(p) {{ return p[0].name + ': ' + p[0].value.toFixed(1) + '%'; }} }},
                grid: {{ left: 80, right: 30, top: 10, bottom: 30 }},
                xAxis: {{ type: 'value', max: 100, axisLabel: {{ color: '#94a3b8', formatter: '{{value}}%' }} }},
                yAxis: {{ type: 'category', data: ['Top20', 'Top10', 'Top5', 'Top1'], axisLabel: {{ color: '#94a3b8' }} }},
                series: [{{
                    type: 'bar',
                    data: [{top1_pct_v}, {top5_pct_v}, {top10_pct_v}, {top20_pct_v}],
                    itemStyle: {{ color: function(p) {{ var colors = ['#06b6d4','#3b82f6','#8b5cf6','#a855f7']; return colors[p.dataIndex]; }} }},
                    label: {{ show: true, position: 'right', color: '#e2e8f0', formatter: '{{c}}%' }}
                }}]
            }});
        }}
    }})();
    </script>
"##,
        top1_pct_v = format!("{:.1}", top1_pct),
        top5_pct_v = format!("{:.1}", top5_pct),
        top10_pct_v = format!("{:.1}", top10_pct),
        top20_pct_v = format!("{:.1}", top20_pct),
    ));

    // 全都道府県 Zipf チャート JS
    if !pref_rows.is_empty() {
        let mut pref_labels = Vec::new();
        let mut zipf_values = Vec::new();
        let mut colors_arr = Vec::new();
        for pr in &pref_rows {
            let pname = pr.get("prefecture").and_then(|v| v.as_str()).unwrap_or("-");
            let zval = pr.get("zipf_exponent").and_then(|v| v.as_f64()).unwrap_or(0.0);
            pref_labels.push(format!("'{}'", pname));
            zipf_values.push(format!("{:.3}", zval));
            // 現在選択中の都道府県をハイライト
            if !prefecture.is_empty() && pname == prefecture {
                colors_arr.push("'#f59e0b'".to_string());
            } else {
                colors_arr.push("'#06b6d4'".to_string());
            }
        }

        html.push_str(&format!(r##"
        <script>
        (function() {{
            var dom = document.getElementById('facility-zipf-pref-chart');
            if (dom) {{
                var c = echarts.init(dom, 'dark');
                c.setOption({{
                    backgroundColor: 'transparent',
                    tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }} }},
                    grid: {{ left: 70, right: 20, top: 20, bottom: 30 }},
                    xAxis: {{ type: 'value', name: '大手偏り度（小さいほど大手に集中）', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8', fontSize: 11 }} }},
                    yAxis: {{ type: 'category', data: [{labels}], axisLabel: {{ color: '#94a3b8', fontSize: 10 }}, inverse: true }},
                    series: [{{
                        type: 'bar',
                        data: [{values}],
                        itemStyle: {{ color: function(p) {{ var cs = [{colors}]; return cs[p.dataIndex]; }} }},
                        barMaxWidth: 12,
                        label: {{ show: false }}
                    }}]
                }});
            }}
        }})();
        </script>
"##,
            labels = pref_labels.join(","),
            values = zipf_values.join(","),
            colors = colors_arr.join(","),
        ));
    }

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// A-3: 雇用多様性 API
// ---------------------------------------------------------------------------

pub async fn api_employment(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_employment_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    // 市区町村指定時は postings テーブル(ローカル)から直接計算
    // 都道府県指定時は Turso から layer テーブル取得
    let rows = if !municipality.is_empty() {
        let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone(); let muni = municipality.clone();
        match tokio::task::spawn_blocking(move || {
            analytics::query_employment_from_postings(&db_c, &jt, &pref, &muni)
        }).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Html(error_html(&e)),
            Err(e) => return Html(error_html(&format!("spawn_blocking: {e}"))),
        }
    } else {
        match analytics::query_employment_diversity_turso(&state.turso, &job_type, &prefecture).await {
            Ok(r) => r,
            Err(_e) => {
                // フォールバック: ローカルDB
                let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone();
                tokio::task::spawn_blocking(move || {
                    analytics::query_employment_diversity(&db_c, &jt, &pref).unwrap_or_default()
                }).await.unwrap_or_default()
            }
        }
    };

    if rows.is_empty() {
        return Html(empty_html("雇用形態多様性データがありません"));
    }

    let location_label = make_location_label(&prefecture, &municipality);
    let row = &rows[0];
    let total = row.get("total_postings").and_then(|v| v.as_i64()).unwrap_or(0);
    let n_types = row.get("n_types").and_then(|v| v.as_i64()).unwrap_or(0);
    let entropy = row.get("shannon_entropy").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let max_entropy = row.get("max_entropy").and_then(|v| v.as_f64()).unwrap_or(1.0);
    let evenness = row.get("evenness").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let dominant = row.get("dominant_type").and_then(|v| v.as_str()).unwrap_or("-");
    let dominant_pct = row.get("dominant_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let dist_json = row.get("type_distribution").and_then(|v| v.as_str()).unwrap_or("{}");

    // 分布をパース（ECharts用）
    let dist: serde_json::Value = serde_json::from_str(dist_json).unwrap_or(serde_json::json!({}));
    let mut pie_data = Vec::new();
    if let Some(obj) = dist.as_object() {
        for (k, v) in obj {
            let pct = v.as_f64().unwrap_or(0.0);
            pie_data.push(format!("{{value:{:.1},name:'{}'}}", pct, k));
        }
    }

    let evenness_label = if evenness > 0.8 { "非常に均等" } else if evenness > 0.6 { "比較的均等" } else if evenness > 0.4 { "やや偏り" } else { "高度集中" };

    let mut html = format!(r##"
<div class="space-y-4">
    <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15a2.25 2.25 0 012.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V19.5a2.25 2.25 0 002.25 2.25h.75'/></svg>  雇用形態多様性 — {location}</h3>

    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">総求人数</div>
            <div class="text-xl font-bold text-white">{total}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">雇用形態数</div>
            <div class="text-xl font-bold text-cyan-400">{n_types}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400" title="雇用形態がどれだけ多様に分布しているか（シャノンエントロピー）">雇用形態の多様性</div>
            <div class="text-xl font-bold text-purple-400">{entropy:.3}</div>
            <div class="text-xs text-gray-500">最大 {max_entropy:.3}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400" title="1.0に近いほど各雇用形態が均等に分布。0に近いほど特定の形態に偏り">バランス度</div>
            <div class="text-xl font-bold text-amber-400">{evenness:.3}</div>
            <div class="text-xs text-amber-400/70">{evenness_label}</div>
        </div>
    </div>

    <!-- 雇用形態 円グラフ -->
    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-semibold text-gray-300 mb-2">雇用形態分布 (主要: {dominant} {dominant_pct:.1}%)</h4>
        <div id="emp-pie-chart" style="width:100%;height:300px;"></div>
    </div>
"##,
        location = escape_html(&location_label),
        total = format_number(total),
        n_types = n_types,
        entropy = entropy,
        max_entropy = max_entropy,
        evenness = evenness,
        evenness_label = evenness_label,
        dominant = escape_html(dominant),
        dominant_pct = dominant_pct,
    );

    // 全都道府県比較（Shannon entropy）— Turso経由
    let all_prefs = match analytics::query_employment_all_prefectures_turso(&state.turso, &job_type).await {
        Ok(r) => r,
        Err(_e) => {
            let db_c = db.clone(); let jt = job_type.clone();
            tokio::task::spawn_blocking(move || {
                analytics::query_employment_all_prefectures(&db_c, &jt).unwrap_or_default()
            }).await.unwrap_or_default()
        }
    };
    let pref_rows: Vec<_> = all_prefs.iter().filter(|r| {
        r.get("prefecture").and_then(|v| v.as_str()).unwrap_or("") != "全国"
    }).collect();

    if !pref_rows.is_empty() {
        // 積み上げ横棒グラフ + Entropy 横棒グラフ
        html.push_str(r#"<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
                <h4 class="text-sm font-semibold text-gray-300 mb-2">都道府県別 雇用形態構成</h4>
                <div id="emp-stacked-chart" style="width:100%;height:600px;"></div>
            </div>
            <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
                <h4 class="text-sm font-semibold text-gray-300 mb-2">都道府県別 雇用形態のバランス度</h4>
                <div id="emp-entropy-chart" style="width:100%;height:600px;"></div>
            </div>
        </div>"#);

        // 全雇用形態タイプを収集
        let mut all_types: Vec<String> = Vec::new();
        for pr in &pref_rows {
            let dj = pr.get("type_distribution").and_then(|v| v.as_str()).unwrap_or("{}");
            if let Ok(d) = serde_json::from_str::<serde_json::Value>(dj) {
                if let Some(obj) = d.as_object() {
                    for k in obj.keys() {
                        if !all_types.contains(k) {
                            all_types.push(k.clone());
                        }
                    }
                }
            }
        }

        let type_colors = ["#06b6d4", "#8b5cf6", "#f59e0b", "#ef4444", "#10b981", "#3b82f6", "#ec4899", "#a3e635"];

        // 積み上げ棒グラフ用データ構築
        let mut pref_names = Vec::new();
        let mut entropy_vals = Vec::new();
        let mut entropy_colors = Vec::new();
        // タイプごとのデータ配列
        let mut type_data: Vec<Vec<f64>> = vec![vec![]; all_types.len()];

        for pr in &pref_rows {
            let pname = pr.get("prefecture").and_then(|v| v.as_str()).unwrap_or("-");
            let ent = pr.get("shannon_entropy").and_then(|v| v.as_f64()).unwrap_or(0.0);
            pref_names.push(pname.to_string());
            entropy_vals.push(ent);

            if !prefecture.is_empty() && pname == prefecture {
                entropy_colors.push("'#f59e0b'".to_string());
            } else {
                entropy_colors.push("'#8b5cf6'".to_string());
            }

            let dj = pr.get("type_distribution").and_then(|v| v.as_str()).unwrap_or("{}");
            let d: serde_json::Value = serde_json::from_str(dj).unwrap_or(serde_json::json!({}));
            for (ti, tname) in all_types.iter().enumerate() {
                let val = d.get(tname).and_then(|v| v.as_f64()).unwrap_or(0.0);
                type_data[ti].push(val);
            }
        }

        // 積み上げ棒グラフ series
        let mut stacked_series = Vec::new();
        for (ti, tname) in all_types.iter().enumerate() {
            let color = type_colors[ti % type_colors.len()];
            let data_str = type_data[ti].iter().map(|v| format!("{:.1}", v)).collect::<Vec<_>>().join(",");
            stacked_series.push(format!(
                "{{name:'{}',type:'bar',stack:'total',data:[{}],itemStyle:{{color:'{}'}},barMaxWidth:14}}",
                tname, data_str, color
            ));
        }

        let pref_labels_js = pref_names.iter().map(|n| format!("'{}'", n)).collect::<Vec<_>>().join(",");
        let entropy_data_js = entropy_vals.iter().map(|v| format!("{:.3}", v)).collect::<Vec<_>>().join(",");
        let legend_data_js = all_types.iter().map(|t| format!("'{}'", t)).collect::<Vec<_>>().join(",");

        html.push_str(&format!(r##"
        <script>
        (function() {{
            // 積み上げ横棒グラフ
            var dom1 = document.getElementById('emp-stacked-chart');
            if (dom1) {{
                var c1 = echarts.init(dom1, 'dark');
                c1.setOption({{
                    backgroundColor: 'transparent',
                    tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }} }},
                    legend: {{ data: [{legend}], textStyle: {{ color: '#94a3b8', fontSize: 10 }}, bottom: 0 }},
                    grid: {{ left: 70, right: 20, top: 10, bottom: 40 }},
                    xAxis: {{ type: 'value', max: 100, axisLabel: {{ color: '#94a3b8', formatter: '{{value}}%' }} }},
                    yAxis: {{ type: 'category', data: [{prefs}], axisLabel: {{ color: '#94a3b8', fontSize: 9 }}, inverse: true }},
                    series: [{series}]
                }});

            }}
            // Entropy 横棒グラフ
            var dom2 = document.getElementById('emp-entropy-chart');
            if (dom2) {{
                var c2 = echarts.init(dom2, 'dark');
                c2.setOption({{
                    backgroundColor: 'transparent',
                    tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }} }},
                    grid: {{ left: 70, right: 30, top: 10, bottom: 20 }},
                    xAxis: {{ type: 'value', name: '多様性（高いほど形態が均等）', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }} }},
                    yAxis: {{ type: 'category', data: [{prefs}], axisLabel: {{ color: '#94a3b8', fontSize: 9 }}, inverse: true }},
                    series: [{{
                        type: 'bar',
                        data: [{entropy_data}],
                        barMaxWidth: 12,
                        itemStyle: {{ color: function(p) {{ var cs = [{entropy_colors}]; return cs[p.dataIndex]; }} }}
                    }}]
                }});

            }}
        }})();
        </script>
"##,
            prefs = pref_labels_js,
            legend = legend_data_js,
            series = stacked_series.join(","),
            entropy_data = entropy_data_js,
            entropy_colors = entropy_colors.join(","),
        ));
    }

    // 円グラフ JS
    html.push_str(&format!(r##"
    <script>
    (function() {{
        var dom = document.getElementById('emp-pie-chart');
        if (dom) {{
            var c = echarts.init(dom, 'dark');
            c.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{ trigger: 'item', formatter: '{{b}}: {{c}}% ({{d}}%)' }},
                series: [{{
                    type: 'pie',
                    radius: ['40%', '70%'],
                    center: ['50%', '50%'],
                    data: [{pie_data}],
                    label: {{ color: '#e2e8f0', formatter: '{{b}}\n{{c}}%' }},
                    emphasis: {{ itemStyle: {{ shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0,0,0,0.5)' }} }}
                }}]
            }});

        }}
    }})();
    </script>
"##,
        pie_data = pie_data.join(","),
    ));

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// B-1: キーワード API
// ---------------------------------------------------------------------------

pub async fn api_keywords(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<AnalysisParams>,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let layer = params.layer.as_deref();
    let cache_key = format!("analysis_keywords_{}_{}_{}_{}", job_type, prefecture, municipality, layer.unwrap_or("all"));
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    // フォールバック付きで取得（Turso経由、都道府県データなし→全国にフォールバック）
    let (rows, is_fallback) = analytics::query_keywords_with_fallback_turso(
        &state.turso, &job_type, &prefecture, layer, Some(50),
    ).await;

    // Turso結果が空の場合、ローカルDBにフォールバック
    let (rows, is_fallback) = if rows.is_empty() {
        if let Some(db) = &state.geocoded_db {
            let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone();
            let layer_owned = layer.map(|s| s.to_string());
            tokio::task::spawn_blocking(move || {
                analytics::query_keywords_with_fallback(&db_c, &jt, &pref, layer_owned.as_deref(), Some(50))
            }).await.unwrap_or_else(|_| (Vec::new(), false))
        } else {
            (rows, is_fallback)
        }
    } else {
        (rows, is_fallback)
    };

    let location_label = make_location_label(&prefecture, &municipality);
    let active_layer = layer.unwrap_or("all");

    let target = "#analysis-content";
    let fallback_note = if is_fallback && !prefecture.is_empty() {
        let scope = if !municipality.is_empty() {
            format!("{} {} ", escape_html(&prefecture), escape_html(&municipality))
        } else {
            format!("{} ", escape_html(&prefecture))
        };
        format!(r#"<div class="bg-amber-900/30 border border-amber-700 rounded-lg px-3 py-2 text-xs text-amber-300">
            ※ {}のキーワードデータがないため、全国データを表示しています
        </div>"#, scope)
    } else {
        String::new()
    };
    // フォールバック時は「都道府県のデータを表示」注記を出さない（全国データなので矛盾する）
    let municipality_note = if !municipality.is_empty() && !is_fallback {
        format!(r#"<div class="bg-blue-900/30 border border-blue-700 rounded-lg px-3 py-2 text-xs text-blue-300">
            ※ キーワード分析は都道府県単位のデータです。{} のデータを表示しています。
        </div>"#, escape_html(&make_location_label(&prefecture, "")))
    } else {
        String::new()
    };

    let mut html = format!(
        "<div class=\"space-y-4\">\
        <h3 class=\"text-lg font-semibold text-white\"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M15.75 5.25a3 3 0 013 3m3 0a6 6 0 01-7.029 5.912c-.563-.097-1.159.026-1.563.43L10.5 17.25H8.25v2.25H6v2.25H2.25v-2.818c0-.597.237-1.17.659-1.591l6.499-6.499c.404-.404.527-1 .43-1.563A6 6 0 1121.75 8.25z'/></svg>  キーワード分析 — {location}</h3>\
        {fallback_note}{municipality_note}\
        <div class=\"flex gap-2 mb-3\">\
            <button class=\"px-3 py-1 text-xs rounded {all_cls}\" hx-get=\"/api/analysis/keywords\" hx-target=\"{target}\" hx-swap=\"innerHTML\">全て</button>\
            <button class=\"px-3 py-1 text-xs rounded {uni_cls}\" hx-get=\"/api/analysis/keywords?layer=universal\" hx-target=\"{target}\" hx-swap=\"innerHTML\">🌐 業界共通</button>\
            <button class=\"px-3 py-1 text-xs rounded {jt_cls}\" hx-get=\"/api/analysis/keywords?layer=job_type\" hx-target=\"{target}\" hx-swap=\"innerHTML\">🏷️ 職種特有</button>\
            <button class=\"px-3 py-1 text-xs rounded {reg_cls}\" hx-get=\"/api/analysis/keywords?layer=regional\" hx-target=\"{target}\" hx-swap=\"innerHTML\"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M15 10.5a3 3 0 11-6 0 3 3 0 016 0z'/><path stroke-linecap='round' stroke-linejoin='round' d='M19.5 10.5c0 7.142-7.5 11.25-7.5 11.25S4.5 17.642 4.5 10.5a7.5 7.5 0 1115 0z'/></svg>  地域特有</button>\
        </div>",
        location = escape_html(&location_label),
        fallback_note = fallback_note,
        municipality_note = municipality_note,
        target = target,
        all_cls = if active_layer == "all" { "bg-cyan-600 text-white" } else { "bg-slate-700 text-gray-400" },
        uni_cls = if active_layer == "universal" { "bg-cyan-600 text-white" } else { "bg-slate-700 text-gray-400" },
        jt_cls = if active_layer == "job_type" { "bg-cyan-600 text-white" } else { "bg-slate-700 text-gray-400" },
        reg_cls = if active_layer == "regional" { "bg-cyan-600 text-white" } else { "bg-slate-700 text-gray-400" },
    );

    if rows.is_empty() {
        html.push_str(&empty_html("キーワードデータがありません"));
    } else {
        // ECharts 横棒グラフ（3色分け）
        let top_n = rows.len().min(30);
        let top_rows = &rows[..top_n];

        let mut kw_labels = Vec::new();
        let mut kw_values = Vec::new();
        let mut kw_colors = Vec::new();

        for row in top_rows.iter().rev() {
            let keyword = row.get("keyword").and_then(|v| v.as_str()).unwrap_or("-");
            let freq_pct = row.get("doc_freq_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let kw_layer = row.get("layer").and_then(|v| v.as_str()).unwrap_or("-");

            kw_labels.push(format!("'{}'", keyword));
            kw_values.push(format!("{:.1}", freq_pct));
            let color = match kw_layer {
                "universal" => "'#64748b'",   // グレー: 定型層
                "job_type"  => "'#3b82f6'",   // ブルー: 特徴層
                "regional"  => "'#ef4444'",   // レッド: 独自層
                _ => "'#94a3b8'",
            };
            kw_colors.push(color.to_string());
        }

        html.push_str(&format!(r##"
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-1">キーワード出現率 Top{top_n}</h4>
            <div class="flex gap-4 mb-2 text-xs">
                <span class="flex items-center gap-1"><span class="w-3 h-3 rounded" style="background:#64748b"></span> 定型層 (30%+)</span>
                <span class="flex items-center gap-1"><span class="w-3 h-3 rounded" style="background:#3b82f6"></span> 特徴層 (8-30%)</span>
                <span class="flex items-center gap-1"><span class="w-3 h-3 rounded" style="background:#ef4444"></span> 独自層 (&lt;8%)</span>
            </div>
            <div id="keyword-bar-chart" style="width:100%;height:{chart_h}px;"></div>
        </div>
        <script>
        (function() {{
            var dom = document.getElementById('keyword-bar-chart');
            if (dom) {{
                var c = echarts.init(dom, 'dark');
                c.setOption({{
                    backgroundColor: 'transparent',
                    tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }}, formatter: function(p) {{ return p[0].name + ': ' + p[0].value + '%'; }} }},
                    grid: {{ left: 120, right: 40, top: 10, bottom: 20 }},
                    xAxis: {{ type: 'value', axisLabel: {{ color: '#94a3b8', formatter: '{{value}}%' }} }},
                    yAxis: {{ type: 'category', data: [{labels}], axisLabel: {{ color: '#e2e8f0', fontSize: 10 }} }},
                    series: [{{
                        type: 'bar',
                        data: [{values}],
                        barMaxWidth: 14,
                        itemStyle: {{ color: function(p) {{ var cs = [{colors}]; return cs[p.dataIndex]; }} }},
                        label: {{ show: true, position: 'right', color: '#94a3b8', fontSize: 10, formatter: '{{c}}%' }}
                    }}]
                }});

            }}
        }})();
        </script>
"##,
            top_n = top_n,
            chart_h = (top_n * 22).max(200),
            labels = kw_labels.join(","),
            values = kw_values.join(","),
            colors = kw_colors.join(","),
        ));

        // テーブル
        html.push_str(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">詳細一覧</h4>
            <div class="overflow-x-auto"><table class="w-full text-xs">
            <thead><tr class="text-gray-400 border-b border-slate-700">
                <th class="text-left p-1.5">#</th>
                <th class="text-left p-1.5">層</th>
                <th class="text-left p-1.5">キーワード</th>
                <th class="text-right p-1.5">TF-IDF</th>
                <th class="text-right p-1.5">出現率</th>
            </tr></thead><tbody>"#);

        for row in &rows {
            let rank = row.get("rank").and_then(|v| v.as_i64()).unwrap_or(0);
            let kw_layer = row.get("layer").and_then(|v| v.as_str()).unwrap_or("-");
            let keyword = row.get("keyword").and_then(|v| v.as_str()).unwrap_or("-");
            let score = row.get("tfidf_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let freq_pct = row.get("doc_freq_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);

            let layer_badge = match kw_layer {
                "universal" => r#"<span class="px-1.5 py-0.5 text-xs rounded bg-slate-700 text-gray-300">共通</span>"#,
                "job_type" => r#"<span class="px-1.5 py-0.5 text-xs rounded bg-blue-900 text-blue-300">職種</span>"#,
                "regional" => r#"<span class="px-1.5 py-0.5 text-xs rounded bg-red-900 text-red-300">地域</span>"#,
                _ => "",
            };

            html.push_str(&format!(
                r#"<tr class="border-b border-slate-800 hover:bg-navy-700/50">
                    <td class="p-1.5 text-gray-500">{rank}</td>
                    <td class="p-1.5">{layer_badge}</td>
                    <td class="p-1.5 text-white font-medium">{keyword}</td>
                    <td class="p-1.5 text-right font-mono text-cyan-300">{score:.4}</td>
                    <td class="p-1.5 text-right text-gray-400">{freq_pct:.1}%</td>
                </tr>"#,
                rank = rank,
                layer_badge = layer_badge,
                keyword = escape_html(keyword),
                score = score,
                freq_pct = freq_pct,
            ));
        }

        html.push_str("</tbody></table></div></div>");
    }

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// B-2: 条件共起 API
// ---------------------------------------------------------------------------

pub async fn api_cooccurrence(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<AnalysisParams>,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let min_lift = params.min_lift;
    let cache_key = format!("analysis_cooccurrence_{}_{}_{}_{:.1}", job_type, prefecture, municipality, min_lift.unwrap_or(0.0));
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    // フォールバック付きで取得（Turso経由、都道府県データなし→全国にフォールバック）
    let (rows, is_fallback) = analytics::query_cooccurrence_with_fallback_turso(
        &state.turso, &job_type, &prefecture, min_lift,
    ).await;

    // Turso結果が空の場合、ローカルDBにフォールバック
    let (rows, is_fallback) = if rows.is_empty() {
        if let Some(db) = &state.geocoded_db {
            let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone();
            tokio::task::spawn_blocking(move || {
                analytics::query_cooccurrence_with_fallback(&db_c, &jt, &pref, min_lift)
            }).await.unwrap_or_else(|_| (Vec::new(), false))
        } else {
            (rows, is_fallback)
        }
    } else {
        (rows, is_fallback)
    };

    let location_label = make_location_label(&prefecture, &municipality);

    let fallback_note = if is_fallback && !prefecture.is_empty() {
        let scope = if !municipality.is_empty() {
            format!("{} {} ", escape_html(&prefecture), escape_html(&municipality))
        } else {
            format!("{} ", escape_html(&prefecture))
        };
        format!(r#"<div class="bg-amber-900/30 border border-amber-700 rounded-lg px-3 py-2 text-xs text-amber-300 mb-3">
            ※ {}の共起データがないため、全国データを表示しています
        </div>"#, scope)
    } else {
        String::new()
    };
    // フォールバック時は「都道府県のデータを表示」注記を出さない（全国データなので矛盾する）
    let municipality_note = if !municipality.is_empty() && !is_fallback {
        format!(r#"<div class="bg-blue-900/30 border border-blue-700 rounded-lg px-3 py-2 text-xs text-blue-300 mb-3">
            ※ 共起分析は都道府県単位のデータです。{} のデータを表示しています。
        </div>"#, escape_html(&make_location_label(&prefecture, "")))
    } else {
        String::new()
    };

    let mut html = format!(
        r#"<div class="space-y-4">
        <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244'/></svg>  よく一緒に提示される条件の組み合わせ — {location}</h3>
        {fallback_note}{municipality_note}
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700 text-sm text-gray-300 space-y-2">
            <p><span class="text-cyan-400 font-semibold">セット度</span>: ある条件Aを掲げる求人が、条件Bもセットで提示する傾向の強さ。値が大きいほど「セット売り」される頻度が高い。</p>
            <p><span class="text-purple-400 font-semibold">結びつき</span>: 2つの条件がどれだけ強く結びついているか（-1〜+1）。0.3以上なら明確な関連あり。</p>
            <p><span class="text-amber-400 font-semibold">出現率</span>: この組み合わせが全求人の何%に登場するか。大きいほどメジャーな組み合わせ。</p>
        </div>"#,
        location = escape_html(&location_label),
        fallback_note = fallback_note,
        municipality_note = municipality_note,
    );

    if rows.is_empty() {
        html.push_str(&empty_html("共起パターンデータがありません"));
    } else {
        // ECharts バブルチャート
        let mut scatter_data = Vec::new();
        for row in &rows {
            let flag_a = row.get("flag_a").and_then(|v| v.as_str()).unwrap_or("-").replace("has_", "");
            let flag_b = row.get("flag_b").and_then(|v| v.as_str()).unwrap_or("-").replace("has_", "");
            let lift = row.get("lift").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let phi = row.get("phi_coefficient").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let support = row.get("support_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
            scatter_data.push(format!(
                "[{:.2},{:.3},{:.1},'{}+{}']",
                lift, phi, support, flag_a, flag_b
            ));
        }

        html.push_str(&format!(r##"
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">条件の組み合わせマップ（右上ほど強い結びつき、円が大きいほど頻出）</h4>
            <div id="cooccur-bubble-chart" style="width:100%;height:400px;"></div>
        </div>
        <script>
        (function() {{
            var dom = document.getElementById('cooccur-bubble-chart');
            if (dom) {{
                var c = echarts.init(dom, 'dark');
                var rawData = [{scatter_data}];
                c.setOption({{
                    backgroundColor: 'transparent',
                    tooltip: {{
                        formatter: function(p) {{
                            var d = p.data;
                            return d[3] + '<br>セット度: ' + d[0].toFixed(2) + '倍<br>結びつき: ' + d[1].toFixed(3) + '<br>出現率: ' + d[2].toFixed(1) + '%';
                        }}
                    }},
                    grid: {{ left: 60, right: 30, top: 30, bottom: 50 }},
                    xAxis: {{
                        type: 'value', name: 'セット度（高いほど一緒に掲示されやすい）', nameLocation: 'center', nameGap: 30,
                        axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                        splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                    }},
                    yAxis: {{
                        type: 'value', name: '結びつきの強さ', nameLocation: 'center', nameGap: 40,
                        axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                        splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                    }},
                    series: [{{
                        type: 'scatter',
                        data: rawData,
                        symbolSize: function(d) {{ return Math.max(Math.sqrt(d[2]) * 6, 4); }},
                        itemStyle: {{
                            color: function(p) {{
                                var lift = p.data[0];
                                if (lift >= 5) return '#ef4444';
                                if (lift >= 2) return '#f59e0b';
                                return '#06b6d4';
                            }},
                            opacity: 0.7
                        }},
                        label: {{
                            show: true,
                            formatter: function(p) {{ return p.data[3]; }},
                            position: 'top',
                            color: '#cbd5e1',
                            fontSize: 9
                        }}
                    }}]
                }});

            }}
        }})();
        </script>
"##,
            scatter_data = scatter_data.join(","),
        ));

        // テーブル
        html.push_str(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">詳細一覧</h4>
            <div class="overflow-x-auto"><table class="w-full text-xs">
            <thead><tr class="text-gray-400 border-b border-slate-700">
                <th class="text-left p-1.5">条件A</th>
                <th class="text-left p-1.5">条件B</th>
                <th class="text-right p-1.5">同時掲載数</th>
                <th class="text-right p-1.5">セット度</th>
                <th class="text-right p-1.5">結びつき</th>
                <th class="text-right p-1.5">出現率</th>
            </tr></thead><tbody>"#);

        for row in &rows {
            let flag_a = row.get("flag_a").and_then(|v| v.as_str()).unwrap_or("-").replace("has_", "");
            let flag_b = row.get("flag_b").and_then(|v| v.as_str()).unwrap_or("-").replace("has_", "");
            let count = row.get("cooccurrence_count").and_then(|v| v.as_i64()).unwrap_or(0);
            let lift = row.get("lift").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let phi = row.get("phi_coefficient").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let support = row.get("support_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);

            let lift_color = if lift >= 5.0 { "text-red-400" } else if lift >= 2.0 { "text-amber-400" } else if lift >= 1.0 { "text-emerald-400" } else { "text-gray-400" };

            html.push_str(&format!(
                r#"<tr class="border-b border-slate-800 hover:bg-navy-700/50">
                    <td class="p-1.5 text-cyan-300">{flag_a}</td>
                    <td class="p-1.5 text-cyan-300">{flag_b}</td>
                    <td class="p-1.5 text-right">{count}</td>
                    <td class="p-1.5 text-right font-mono {lift_color}">{lift:.2}</td>
                    <td class="p-1.5 text-right font-mono">{phi:.3}</td>
                    <td class="p-1.5 text-right text-gray-400">{support:.1}%</td>
                </tr>"#,
                flag_a = escape_html(&flag_a),
                flag_b = escape_html(&flag_b),
                count = format_number(count),
                lift = lift,
                lift_color = lift_color,
                phi = phi,
                support = support,
            ));
        }

        html.push_str("</tbody></table></div></div>");
    }

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// B-3: 原稿品質 API
// ---------------------------------------------------------------------------

pub async fn api_quality(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_quality_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    // 市区町村指定時はpostingsテーブル(ローカル)から直接計算してKPIに使う
    let muni_quality = if !municipality.is_empty() {
        if let Some(db) = &state.geocoded_db {
            let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone(); let muni = municipality.clone();
            tokio::task::spawn_blocking(move || {
                analytics::query_quality_from_postings(&db_c, &jt, &pref, &muni).unwrap_or_default()
            }).await.unwrap_or_default()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // 全国品質データはTurso経由で取得
    let national = match analytics::query_text_quality_turso(&state.turso, &job_type, "").await {
        Ok(r) => r,
        Err(_e) => {
            // フォールバック: ローカルDB
            if let Some(db) = &state.geocoded_db {
                let db_c = db.clone(); let jt = job_type.clone();
                tokio::task::spawn_blocking(move || {
                    analytics::query_text_quality(&db_c, &jt, "").unwrap_or_default()
                }).await.unwrap_or_default()
            } else {
                Vec::new()
            }
        }
    };

    let location_label = make_location_label(&prefecture, &municipality);

    let mut html = format!(
        r#"<div class="space-y-4">
        <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z'/></svg>  求人原稿の充実度 — {location}</h3>"#,
        location = escape_html(&location_label),
    );

    // KPI表示用データ: 市区町村データ > 都道府県データ > 全国データ
    let kpi_row = if !muni_quality.is_empty() {
        muni_quality.first()
    } else if !prefecture.is_empty() {
        national.iter().find(|r| r.get("prefecture").and_then(|v| v.as_str()) == Some(prefecture.as_str()))
            .or_else(|| national.iter().find(|r| r.get("prefecture").and_then(|v| v.as_str()) == Some("全国")))
    } else {
        national.iter().find(|r| r.get("prefecture").and_then(|v| v.as_str()) == Some("全国"))
    };

    if let Some(nr) = kpi_row {
        let grade = nr.get("grade").and_then(|v| v.as_str()).unwrap_or("-");
        let entropy = nr.get("entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let quality = nr.get("quality_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let benefits = nr.get("benefits_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let kanji = nr.get("kanji_ratio_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let desc_len = nr.get("desc_length_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let grade_color = match grade {
            "A" => "text-emerald-400 bg-emerald-900/30",
            "B" => "text-blue-400 bg-blue-900/30",
            "C" => "text-yellow-400 bg-yellow-900/30",
            "D" => "text-red-400 bg-red-900/30",
            _ => "text-gray-400 bg-gray-900/30",
        };

        html.push_str(&format!(r##"
    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">全国グレード</div>
            <div class="text-3xl font-bold {grade_color} px-3 py-1 rounded inline-block">{grade}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">表現の豊かさ</div>
            <div class="text-xl font-bold text-white">{entropy:.3}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">品質スコア</div>
            <div class="text-xl font-bold text-cyan-400">{quality:.1}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">福利厚生スコア</div>
            <div class="text-xl font-bold text-purple-400">{benefits:.1}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">漢字比率</div>
            <div class="text-xl font-bold text-amber-400">{kanji:.1}%</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">平均文字数</div>
            <div class="text-xl font-bold text-white">{desc_len:.0}</div>
        </div>
    </div>
"##,
            grade = grade,
            grade_color = grade_color,
            entropy = entropy,
            quality = quality,
            benefits = benefits,
            kanji = kanji * 100.0,
            desc_len = desc_len,
        ));
    }

    // 市区町村別品質比較（近隣市区町村との比較が分析の本質）
    // 都道府県選択時: 選択県 + 隣接県の全市区町村を比較
    // 全国選択時: 全国の都道府県レベルのみ表示（全国時は市区町村が多すぎるため）
    let has_pref = !prefecture.is_empty();

    if has_pref {
        // 市区町村レベル比較: 選択県 + 隣接県の市区町村を集計
        let mut target_prefs: Vec<&str> = vec![prefecture.as_str()];
        for neighbor in adjacent_prefectures(&prefecture) {
            target_prefs.push(neighbor);
        }

        // 市区町村別品質はpostingsテーブル（ローカルDB）から集計
        let muni_rows = if let Some(db) = &state.geocoded_db {
            let db_c = db.clone(); let jt = job_type.clone();
            let target_prefs_owned: Vec<String> = target_prefs.iter().map(|s| s.to_string()).collect();
            tokio::task::spawn_blocking(move || {
                let refs: Vec<&str> = target_prefs_owned.iter().map(|s| s.as_str()).collect();
                analytics::query_quality_by_municipality(&db_c, &jt, &refs).unwrap_or_default()
            }).await.unwrap_or_default()
        } else {
            Vec::new()
        };

        if !muni_rows.is_empty() {
            // 選択市区町村のデータを特定
            let selected_muni_name = if !municipality.is_empty() {
                municipality.clone()
            } else {
                String::new()
            };

            // バーチャート用: Entropyの高い順に上位30市区町村
            let top_n = 30.min(muni_rows.len());
            let bar_rows = &muni_rows[..top_n];

            let mut muni_names = Vec::new();
            let mut entropy_vals = Vec::new();
            let mut entropy_colors = Vec::new();

            for mr in bar_rows {
                let mname = mr.get("municipality").and_then(|v| v.as_str()).unwrap_or("-");
                let pname = mr.get("prefecture").and_then(|v| v.as_str()).unwrap_or("");
                let ent = mr.get("entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);

                // ラベル: 同県は市区町村名のみ、他県は「県名 市区町村名」
                let label = if pname == prefecture {
                    mname.to_string()
                } else {
                    format!("{} {}", pname, mname)
                };
                muni_names.push(format!("'{}'", label.replace('\'', "\\'")));
                entropy_vals.push(format!("{:.3}", ent));

                // 色分け: 選択市区町村=アンバー、同県=シアン、隣接県=グレー
                if !selected_muni_name.is_empty() && mname == selected_muni_name && pname == prefecture {
                    entropy_colors.push("'#f59e0b'".to_string());
                } else if pname == prefecture {
                    entropy_colors.push("'#06b6d4'".to_string());
                } else {
                    entropy_colors.push("'#64748b'".to_string());
                }
            }

            // 散布図用: 市区町村をシリーズ分割
            let mut scatter_selected = Vec::new();   // 選択市区町村（アンバー、大）
            let mut scatter_same_pref = Vec::new();  // 同県の他市区町村（シアン）
            let mut scatter_neighbor = Vec::new();   // 隣接県の市区町村（グレー）

            for mr in &muni_rows {
                let mname = mr.get("municipality").and_then(|v| v.as_str()).unwrap_or("-");
                let pname = mr.get("prefecture").and_then(|v| v.as_str()).unwrap_or("");
                let ent = mr.get("entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let kj = mr.get("kanji_ratio_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let cnt = mr.get("count").and_then(|v| v.as_i64()).unwrap_or(0);

                let label = if pname == prefecture {
                    mname.to_string()
                } else {
                    format!("{} {}", pname, mname)
                };
                // [kanji_ratio, entropy, label, count] でtooltip表示用にcount追加
                let item = format!("[{:.1},{:.3},'{}',{}]", kj * 100.0, ent, label.replace('\'', "\\'"), cnt);

                if !selected_muni_name.is_empty() && mname == selected_muni_name && pname == prefecture {
                    scatter_selected.push(item);
                } else if pname == prefecture {
                    scatter_same_pref.push(item);
                } else {
                    scatter_neighbor.push(item);
                }
            }

            let all_scatter: Vec<String> = scatter_selected.iter()
                .chain(scatter_same_pref.iter())
                .chain(scatter_neighbor.iter())
                .cloned().collect();

            // チャート見出し
            let chart_heading_bar = if !selected_muni_name.is_empty() {
                format!("近隣市区町村 表現の豊かさの比較（{} {} + 周辺）", escape_html(&prefecture), escape_html(&selected_muni_name))
            } else {
                format!("近隣市区町村 表現の豊かさの比較（{} + 隣接県）", escape_html(&prefecture))
            };
            let chart_heading_scatter = if !selected_muni_name.is_empty() {
                format!("品質マップ（{} {} + 近隣市区町村）", escape_html(&prefecture), escape_html(&selected_muni_name))
            } else {
                format!("品質マップ（{} + 隣接県 市区町村比較）", escape_html(&prefecture))
            };

            // 散布図シリーズ
            let has_selected = !scatter_selected.is_empty();
            let legend_items = if has_selected {
                format!("['{}','{}内','隣接県']", escape_html(&selected_muni_name), escape_html(&prefecture))
            } else {
                format!("['{}内','隣接県']", escape_html(&prefecture))
            };

            let mut series_parts = Vec::new();
            if has_selected {
                series_parts.push(format!(
                    r#"{{
                        name: '{sel_name}',
                        type: 'scatter',
                        data: [{sel_data}],
                        symbolSize: 18,
                        itemStyle: {{ color: '#f59e0b', opacity: 1.0 }},
                        label: {{ show: true, formatter: function(p) {{ return p.data[2]; }}, position: 'top', color: '#f59e0b', fontSize: 11, fontWeight: 'bold' }}
                    }}"#,
                    sel_name = escape_html(&selected_muni_name),
                    sel_data = scatter_selected.join(","),
                ));
            }
            series_parts.push(format!(
                r#"{{
                    name: '{pref}内',
                    type: 'scatter',
                    data: [{same_data}],
                    symbolSize: function(d) {{ return Math.max(6, Math.min(16, Math.sqrt(d[3]) * 1.5)); }},
                    itemStyle: {{ color: '#06b6d4', opacity: 0.85 }},
                    label: {{ show: {show_label}, formatter: function(p) {{ return p.data[2]; }}, position: 'right', color: '#94a3b8', fontSize: 8 }}
                }}"#,
                pref = escape_html(&prefecture),
                same_data = scatter_same_pref.join(","),
                show_label = if scatter_same_pref.len() <= 15 { "true" } else { "false" },
            ));
            series_parts.push(format!(
                r#"{{
                    name: '隣接県',
                    type: 'scatter',
                    data: [{nei_data}],
                    symbolSize: function(d) {{ return Math.max(5, Math.min(12, Math.sqrt(d[3]))); }},
                    itemStyle: {{ color: '#64748b', opacity: 0.6 }},
                    label: {{ show: false }}
                }}"#,
                nei_data = scatter_neighbor.join(","),
            ));
            let scatter_series = series_parts.join(",");

            // バーチャートの高さ: 市区町村数に応じて動的計算
            let bar_height = 20.max(top_n * 18 + 40).min(800);

            html.push_str(&format!(r##"
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">{chart_heading_bar}</h4>
            <div id="quality-entropy-chart" style="width:100%;height:{bar_height}px;"></div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">{chart_heading_scatter}</h4>
            <div id="quality-scatter-chart" style="width:100%;height:600px;"></div>
        </div>
    </div>
    <script>
    (function() {{
        var dom1 = document.getElementById('quality-entropy-chart');
        if (dom1) {{
            var c1 = echarts.init(dom1, 'dark');
            c1.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }} }},
                grid: {{ left: 100, right: 20, top: 10, bottom: 20 }},
                xAxis: {{ type: 'value', name: '表現の豊かさ', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }} }},
                yAxis: {{ type: 'category', data: [{muni_names}], axisLabel: {{ color: '#94a3b8', fontSize: 9 }}, inverse: true }},
                series: [{{
                    type: 'bar',
                    data: [{entropy_data}],
                    barMaxWidth: 14,
                    itemStyle: {{ color: function(p) {{ var cs = [{entropy_colors}]; return cs[p.dataIndex]; }} }}
                }}]
            }});
        }}
        var dom2 = document.getElementById('quality-scatter-chart');
        if (dom2) {{
            var c2 = echarts.init(dom2, 'dark');
            var allScatter = [{all_scatter_data}];
            var xVals = allScatter.map(function(d){{ return d[0]; }});
            var yVals = allScatter.map(function(d){{ return d[1]; }});
            var minX = Math.min.apply(null, xVals) - 2;
            var maxX = Math.max.apply(null, xVals) + 2;
            var minY = Math.min.apply(null, yVals) - 0.2;
            var maxY = Math.max.apply(null, yVals) + 0.2;
            c2.setOption({{
                backgroundColor: 'transparent',
                legend: {{ show: true, data: [{legend_items}], textStyle: {{ color: '#94a3b8', fontSize: 10 }}, top: 0 }},
                tooltip: {{
                    formatter: function(p) {{ return p.data[2] + '<br>漢字比率: ' + p.data[0].toFixed(1) + '%<br>表現の豊かさ: ' + p.data[1].toFixed(3) + '<br>求人数: ' + p.data[3]; }}
                }},
                grid: {{ left: 50, right: 20, top: 40, bottom: 50 }},
                xAxis: {{
                    type: 'value', name: '漢字比率 (%)', nameLocation: 'center', nameGap: 30,
                    min: minX, max: maxX,
                    axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                    splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                }},
                yAxis: {{
                    type: 'value', name: '表現の豊かさ', nameLocation: 'center', nameGap: 35,
                    min: minY, max: maxY,
                    axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                    splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                }},
                series: [{scatter_series}]
            }});
        }}
    }})();
    </script>
"##,
                chart_heading_bar = chart_heading_bar,
                chart_heading_scatter = chart_heading_scatter,
                bar_height = bar_height,
                muni_names = muni_names.join(","),
                entropy_data = entropy_vals.join(","),
                entropy_colors = entropy_colors.join(","),
                all_scatter_data = all_scatter.join(","),
                legend_items = legend_items,
                scatter_series = scatter_series,
            ));

            // 市区町村別品質比較テーブル
            let table_heading = if !selected_muni_name.is_empty() {
                format!("市区町村別 品質比較（{} {} + 近隣）", escape_html(&prefecture), escape_html(&selected_muni_name))
            } else {
                format!("市区町村別 品質比較（{} + 隣接県）", escape_html(&prefecture))
            };

            html.push_str(&format!(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
                <h4 class="text-sm font-semibold text-gray-300 mb-3">{table_heading}</h4>
                <div class="bg-blue-900/30 border border-blue-700 rounded-lg px-3 py-2 text-xs text-blue-300 mb-2">
                    ※ {pref} + 隣接県の市区町村を比較（5件以上の市区町村のみ表示）。市区町村クリックで地域を切り替えできます。
                </div>
                <div class="overflow-x-auto max-h-96 overflow-y-auto"><table class="w-full text-xs">
                <thead class="sticky top-0 bg-navy-800"><tr class="text-gray-400 border-b border-slate-700">
                    <th class="text-left p-1.5">都道府県</th>
                    <th class="text-left p-1.5">市区町村</th>
                    <th class="text-center p-1.5">Grade</th>
                    <th class="text-right p-1.5">件数</th>
                    <th class="text-right p-1.5">多様性</th>
                    <th class="text-right p-1.5">品質</th>
                    <th class="text-right p-1.5">福利厚生</th>
                </tr></thead><tbody>"#,
                table_heading = table_heading,
                pref = escape_html(&prefecture),
            ));

            for mr in &muni_rows {
                let pname = mr.get("prefecture").and_then(|v| v.as_str()).unwrap_or("-");
                let mname = mr.get("municipality").and_then(|v| v.as_str()).unwrap_or("-");
                let grade = mr.get("grade").and_then(|v| v.as_str()).unwrap_or("-");
                let count = mr.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
                let ent = mr.get("entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let qual = mr.get("quality_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let ben = mr.get("benefits_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);

                let grade_cls = match grade {
                    "A" => "text-emerald-400",
                    "B" => "text-blue-400",
                    "C" => "text-yellow-400",
                    _ => "text-red-400",
                };
                let is_current = !selected_muni_name.is_empty() && mname == selected_muni_name && pname == prefecture;
                let row_cls = if is_current { "bg-amber-900/30 font-semibold" } else if pname == prefecture { "" } else { "opacity-70" };
                let name_cls = if is_current { "text-amber-300" } else if pname == prefecture { "text-cyan-300" } else { "text-gray-400" };

                html.push_str(&format!(
                    r#"<tr class="border-b border-slate-800 {row_cls} cursor-pointer hover:bg-navy-700/50"
                        onclick="switchLocation('{p_esc}','{m_esc}')">
                        <td class="p-1.5 text-gray-400 text-xs">{pname}</td>
                        <td class="p-1.5 {name_cls}">{mname}</td>
                        <td class="p-1.5 text-center font-bold {grade_cls}">{grade}</td>
                        <td class="p-1.5 text-right">{count}</td>
                        <td class="p-1.5 text-right font-mono">{ent:.3}</td>
                        <td class="p-1.5 text-right">{qual:.1}</td>
                        <td class="p-1.5 text-right">{ben:.1}</td>
                    </tr>"#,
                    row_cls = row_cls,
                    p_esc = escape_html(pname),
                    m_esc = escape_html(mname),
                    pname = escape_html(pname),
                    mname = escape_html(mname),
                    name_cls = name_cls,
                    grade = grade,
                    grade_cls = grade_cls,
                    count = format_number(count),
                    ent = ent,
                    qual = qual,
                    ben = ben,
                ));
            }

            html.push_str("</tbody></table></div></div>");
        }
    } else {
        // 全国選択時: 都道府県レベルの概要のみ表示
        let all_pref_rows: Vec<_> = national.iter().filter(|r| {
            r.get("prefecture").and_then(|v| v.as_str()).unwrap_or("") != "全国"
        }).collect();

        if !all_pref_rows.is_empty() {
            let mut pref_names = Vec::new();
            let mut entropy_vals = Vec::new();

            for pr in &all_pref_rows {
                let pname = pr.get("prefecture").and_then(|v| v.as_str()).unwrap_or("-");
                let ent = pr.get("entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
                pref_names.push(format!("'{}'", pname));
                entropy_vals.push(format!("{:.3}", ent));
            }

            html.push_str(&format!(r##"
    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-semibold text-gray-300 mb-2">都道府県別 表現の豊かさ</h4>
        <div class="bg-blue-900/30 border border-blue-700 rounded-lg px-3 py-2 text-xs text-blue-300 mb-2">
            ※ 都道府県を選択すると、近隣市区町村との詳細比較が表示されます。
        </div>
        <div id="quality-entropy-chart" style="width:100%;height:700px;"></div>
    </div>
    <script>
    (function() {{
        var dom1 = document.getElementById('quality-entropy-chart');
        if (dom1) {{
            var c1 = echarts.init(dom1, 'dark');
            c1.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }} }},
                grid: {{ left: 80, right: 20, top: 10, bottom: 20 }},
                xAxis: {{ type: 'value', name: '表現の豊かさ', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }} }},
                yAxis: {{ type: 'category', data: [{prefs}], axisLabel: {{ color: '#94a3b8', fontSize: 9 }}, inverse: true }},
                series: [{{
                    type: 'bar',
                    data: [{entropy_data}],
                    barMaxWidth: 12,
                    itemStyle: {{ color: '#06b6d4' }}
                }}]
            }});
        }}
    }})();
    </script>
"##,
                prefs = pref_names.join(","),
                entropy_data = entropy_vals.join(","),
            ));
        }
    }

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// C-1: クラスタ API
// ---------------------------------------------------------------------------

pub async fn api_clusters(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_clusters_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    // クラスタプロファイルはTurso経由で取得
    let rows = match analytics::query_cluster_profiles_turso(&state.turso, &job_type).await {
        Ok(r) => r,
        Err(_e) => {
            // フォールバック: ローカルDB
            if let Some(db) = &state.geocoded_db {
                let db_c = db.clone(); let jt = job_type.clone();
                tokio::task::spawn_blocking(move || {
                    analytics::query_cluster_profiles(&db_c, &jt).unwrap_or_default()
                }).await.unwrap_or_default()
            } else {
                return Html(error_html("Tursoエラー / ローカルDB未接続"));
            }
        }
    };

    if rows.is_empty() {
        return Html(empty_html("クラスタデータがありません"));
    }

    let location_label = make_location_label(&prefecture, &municipality);
    let scope_note = if !prefecture.is_empty() {
        format!(r#"<div class="bg-blue-900/30 border border-blue-700 rounded-lg px-3 py-2 text-xs text-blue-300">
            ※ クラスタプロファイルは職種全体（全国）の分析結果です。{} の地域別クラスタ分布は「地域ヒートマップ」タブで確認できます。
        </div>"#, escape_html(&location_label))
    } else {
        String::new()
    };

    let colors = ["#06b6d4", "#8b5cf6", "#f59e0b", "#ef4444", "#10b981", "#3b82f6", "#ec4899"];

    let mut html = format!(
        r#"<div class="space-y-4">
        <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M12 2.25v2.25m0 15v2.25M2.25 12h2.25m15 0h2.25M12 6a6 6 0 100 12 6 6 0 000-12z'/></svg>  求人クラスタ分析 — {location}</h3>
        {scope_note}"#,
        location = escape_html(&location_label),
        scope_note = scope_note,
    );

    // 円グラフ（クラスタ構成比）
    let mut pie_data = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let label = row.get("cluster_label").and_then(|v| v.as_str()).unwrap_or("-");
        let size_pct = row.get("size_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let color = colors[i % colors.len()];
        pie_data.push(format!("{{value:{:.1},name:'{}',itemStyle:{{color:'{}'}}}}", size_pct, label, color));
    }

    // レーダーチャート用データ構築
    let radar_features = ["salary_min_mean", "text_entropy_mean", "benefits_score_mean",
                          "fulltime_pct", "has_salary_range_pct", "content_richness_mean"];
    let radar_labels = ["給与水準", "情報多様性", "福利厚生", "正職員率", "給与レンジ有", "原稿充実度"];

    // 各特徴の最大値を取得（正規化用）
    let mut feature_maxes = [0.0f64; 6];
    for row in &rows {
        for (fi, feat) in radar_features.iter().enumerate() {
            let v = row.get(*feat).and_then(|v| v.as_f64()).unwrap_or(0.0);
            if v > feature_maxes[fi] {
                feature_maxes[fi] = v;
            }
        }
    }
    // 0除算回避
    for mx in feature_maxes.iter_mut() {
        if *mx == 0.0 { *mx = 1.0; }
    }

    let mut radar_series = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let label = row.get("cluster_label").and_then(|v| v.as_str()).unwrap_or("-");
        let color = colors[i % colors.len()];
        let mut vals = Vec::new();
        for (fi, feat) in radar_features.iter().enumerate() {
            let v = row.get(*feat).and_then(|vv| vv.as_f64()).unwrap_or(0.0);
            // 正規化 0-100
            let norm = (v / feature_maxes[fi]) * 100.0;
            vals.push(format!("{:.1}", norm));
        }
        radar_series.push(format!(
            "{{value:[{}],name:'{}',lineStyle:{{color:'{}'}},areaStyle:{{color:'{}',opacity:0.15}},itemStyle:{{color:'{}'}}}}",
            vals.join(","), label, color, color, color
        ));
    }

    let radar_indicator = radar_labels.iter().map(|l| format!("{{name:'{}',max:100}}", l)).collect::<Vec<_>>().join(",");
    let legend_data = rows.iter().map(|r| {
        let l = r.get("cluster_label").and_then(|v| v.as_str()).unwrap_or("-");
        format!("'{}'", l)
    }).collect::<Vec<_>>().join(",");

    html.push_str(&format!(r##"
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">クラスタ構成比</h4>
            <div id="cluster-pie-chart" style="width:100%;height:350px;"></div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">クラスタ特徴レーダー</h4>
            <div id="cluster-radar-chart" style="width:100%;height:350px;"></div>
        </div>
    </div>
    <script>
    (function() {{
        // 円グラフ
        var dom1 = document.getElementById('cluster-pie-chart');
        if (dom1) {{
            var c1 = echarts.init(dom1, 'dark');
            c1.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{ trigger: 'item', formatter: '{{b}}: {{c}}%' }},
                legend: {{ orient: 'vertical', right: 10, top: 'center', textStyle: {{ color: '#94a3b8', fontSize: 10 }} }},
                series: [{{
                    type: 'pie',
                    radius: ['35%', '65%'],
                    center: ['40%', '50%'],
                    data: [{pie_data}],
                    label: {{ color: '#e2e8f0', formatter: '{{b}}\n{{c}}%', fontSize: 10 }},
                    emphasis: {{ itemStyle: {{ shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.5)' }} }}
                }}]
            }});
        }}
        // レーダーチャート
        var dom2 = document.getElementById('cluster-radar-chart');
        if (dom2) {{
            var c2 = echarts.init(dom2, 'dark');
            c2.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{}},
                legend: {{ data: [{legend}], bottom: 0, textStyle: {{ color: '#94a3b8', fontSize: 9 }} }},
                radar: {{
                    indicator: [{indicator}],
                    shape: 'circle',
                    splitNumber: 4,
                    axisName: {{ color: '#94a3b8', fontSize: 10 }},
                    splitLine: {{ lineStyle: {{ color: '#334155' }} }},
                    splitArea: {{ areaStyle: {{ color: ['transparent'] }} }},
                    axisLine: {{ lineStyle: {{ color: '#475569' }} }}
                }},
                series: [{{
                    type: 'radar',
                    data: [{radar_data}]
                }}]
            }});
        }}
    }})();
    </script>
"##,
        pie_data = pie_data.join(","),
        legend = legend_data,
        indicator = radar_indicator,
        radar_data = radar_series.join(","),
    ));

    // クラスタ詳細カード（コンパクト化）
    for (i, row) in rows.iter().enumerate() {
        let cid = row.get("cluster_id").and_then(|v| v.as_i64()).unwrap_or(0);
        let label = row.get("cluster_label").and_then(|v| v.as_str()).unwrap_or("-");
        let size = row.get("size").and_then(|v| v.as_i64()).unwrap_or(0);
        let size_pct = row.get("size_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let salary_mean = row.get("salary_min_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let salary_median = row.get("salary_min_median").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let ent = row.get("text_entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let ben = row.get("benefits_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let fulltime = row.get("fulltime_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let description = row.get("description").and_then(|v| v.as_str()).unwrap_or("");
        let color = colors[i % colors.len()];

        let top_benefits_str = row.get("top_benefits").and_then(|v| v.as_str()).unwrap_or("[]");
        let benefits_arr: Vec<serde_json::Value> = serde_json::from_str(top_benefits_str).unwrap_or_default();
        let mut benefits_html = String::new();
        for b in benefits_arr.iter().take(5) {
            let name = b.get("name").and_then(|v| v.as_str()).unwrap_or("-");
            let pct = b.get("pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
            benefits_html.push_str(&format!(
                r#"<span class="inline-block px-1.5 py-0.5 text-xs rounded bg-slate-700 text-gray-300 mr-1 mb-1">{name} {pct:.0}%</span>"#,
                name = escape_html(name), pct = pct,
            ));
        }

        html.push_str(&format!(r##"
    <div class="bg-navy-800 rounded-lg p-3 border border-slate-700" style="border-left: 3px solid {color}">
        <div class="flex items-center justify-between mb-1">
            <div class="flex items-center gap-2">
                <span class="text-sm font-bold" style="color: {color}">C{cid}</span>
                <span class="text-white font-semibold text-sm">{label}</span>
            </div>
            <div class="text-right">
                <span class="text-lg font-bold text-white">{size}</span>
                <span class="text-xs text-gray-400 ml-1">件 ({size_pct:.1}%)</span>
            </div>
        </div>
        <p class="text-xs text-gray-400 mb-2">{description}</p>
        <div class="grid grid-cols-3 md:grid-cols-5 gap-2 text-xs">
            <div><span class="text-gray-500">平均給与:</span> <span class="text-white font-mono">{salary_mean}</span></div>
            <div><span class="text-gray-500">中央給与:</span> <span class="text-white font-mono">{salary_median}</span></div>
            <div><span class="text-gray-500">正職員率:</span> <span class="text-white">{fulltime:.1}%</span></div>
            <div><span class="text-gray-500">表現の豊かさ:</span> <span class="text-white">{ent:.3}</span></div>
            <div><span class="text-gray-500">福利厚生:</span> <span class="text-white">{ben:.1}</span></div>
        </div>
        <div class="mt-1">{benefits_html}</div>
    </div>
"##,
            color = color,
            cid = cid,
            label = escape_html(label),
            size = format_number(size),
            size_pct = size_pct,
            salary_mean = format_yen(salary_mean),
            salary_median = format_yen(salary_median),
            fulltime = fulltime,
            ent = ent,
            ben = ben,
            description = escape_html(description),
            benefits_html = benefits_html,
        ));
    }

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// C-2: 地域ヒートマップ API
// ---------------------------------------------------------------------------

pub async fn api_heatmap(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<AnalysisParams>,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cluster_id = params.cluster_id;
    let cache_key = format!("analysis_heatmap_{}_{}_{}_{}", job_type, prefecture, municipality, cluster_id.unwrap_or(-1));
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    // 全都道府県 x 全クラスタ取得（ヒートマップ用）— Turso経由
    let all_rows = match analytics::query_region_heatmap_turso(&state.turso, &job_type, "", None).await {
        Ok(r) => r,
        Err(_e) => {
            // フォールバック: ローカルDB
            if let Some(db) = &state.geocoded_db {
                let db_c = db.clone(); let jt = job_type.clone();
                tokio::task::spawn_blocking(move || {
                    analytics::query_region_heatmap(&db_c, &jt, "", None).unwrap_or_default()
                }).await.unwrap_or_default()
            } else {
                return Html(error_html("Tursoエラー / ローカルDB未接続"));
            }
        }
    };

    if all_rows.is_empty() {
        return Html(empty_html("地域分布データがありません"));
    }

    let location_label = make_location_label(&prefecture, &municipality);

    // クラスタラベル一覧
    let mut cluster_labels: Vec<(i64, String)> = Vec::new();
    for row in &all_rows {
        let cid = row.get("cluster_id").and_then(|v| v.as_i64()).unwrap_or(0);
        let label = row.get("cluster_label").and_then(|v| v.as_str()).unwrap_or("-").to_string();
        if !cluster_labels.iter().any(|(id, _)| *id == cid) {
            cluster_labels.push((cid, label));
        }
    }
    cluster_labels.sort_by_key(|(id, _)| *id);

    // 都道府県一覧
    let mut prefectures: Vec<String> = Vec::new();
    for row in &all_rows {
        let p = row.get("prefecture").and_then(|v| v.as_str()).unwrap_or("-").to_string();
        if !prefectures.contains(&p) {
            prefectures.push(p);
        }
    }

    // ヒートマップデータ: [x_index(cluster), y_index(prefecture), pct]
    let mut heatmap_data = Vec::new();
    let mut max_pct: f64 = 0.0;

    for row in &all_rows {
        let p = row.get("prefecture").and_then(|v| v.as_str()).unwrap_or("-");
        let cid = row.get("cluster_id").and_then(|v| v.as_i64()).unwrap_or(0);
        let pct = row.get("pct").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let x_idx = cluster_labels.iter().position(|(id, _)| *id == cid).unwrap_or(0);
        let y_idx = prefectures.iter().position(|pp| pp == p).unwrap_or(0);
        heatmap_data.push(format!("[{},{},{:.1}]", x_idx, y_idx, pct));
        if pct > max_pct { max_pct = pct; }
    }

    let x_labels = cluster_labels.iter().map(|(_, l)| format!("'{}'", l)).collect::<Vec<_>>().join(",");
    let y_labels = prefectures.iter().map(|p| format!("'{}'", p)).collect::<Vec<_>>().join(",");

    let chart_height = (prefectures.len() * 18).max(400);

    let municipality_note = if !municipality.is_empty() {
        format!(r#"<div class="bg-blue-900/30 border border-blue-700 rounded-lg px-3 py-2 text-xs text-blue-300">
            ※ ヒートマップは都道府県単位の解像度です。{} のデータを含む {} の行をハイライトしています。
        </div>"#, escape_html(&municipality), escape_html(&prefecture))
    } else {
        String::new()
    };

    let mut html = format!(r##"
<div class="space-y-4">
    <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M9 6.75V15m6-6v8.25m.503 3.498l4.875-2.437c.381-.19.622-.58.622-1.006V4.82c0-.836-.88-1.38-1.628-1.006l-3.869 1.934c-.317.159-.69.159-1.006 0L9.503 3.252a1.125 1.125 0 00-1.006 0L3.622 5.689C3.24 5.88 3 6.27 3 6.695V19.18c0 .836.88 1.38 1.628 1.006l3.869-1.934c.317-.159.69-.159 1.006 0l4.994 2.497c.317.158.69.158 1.006 0z'/></svg>  地域×クラスタ分布 — {location}</h3>
    <p class="text-sm text-gray-400">各都道府県内でのクラスタ構成比 (%)。色が濃いほど構成比が高い。</p>
    {municipality_note}

    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-semibold text-gray-300 mb-2">ヒートマップ</h4>
        <div id="region-heatmap-chart" style="width:100%;height:{chart_height}px;"></div>
    </div>

    <script>
    (function() {{
        var dom = document.getElementById('region-heatmap-chart');
        if (dom) {{
            var c = echarts.init(dom, 'dark');
            c.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{
                    position: 'top',
                    formatter: function(p) {{
                        var xLabels = [{x_labels}];
                        var yLabels = [{y_labels}];
                        return yLabels[p.data[1]] + ' / ' + xLabels[p.data[0]] + ': ' + p.data[2] + '%';
                    }}
                }},
                grid: {{ left: 80, right: 80, top: 40, bottom: 20 }},
                xAxis: {{
                    type: 'category',
                    data: [{x_labels}],
                    position: 'top',
                    axisLabel: {{ color: '#94a3b8', fontSize: 9, rotate: 30 }},
                    splitArea: {{ show: true, areaStyle: {{ color: ['transparent', 'rgba(30,41,59,0.3)'] }} }}
                }},
                yAxis: {{
                    type: 'category',
                    data: [{y_labels}],
                    axisLabel: {{ color: '#94a3b8', fontSize: 9 }},
                    inverse: true,
                    splitArea: {{ show: true, areaStyle: {{ color: ['transparent', 'rgba(30,41,59,0.3)'] }} }}
                }},
                visualMap: {{
                    min: 0,
                    max: {max_pct:.0},
                    calculable: true,
                    orient: 'vertical',
                    right: 0,
                    top: 'center',
                    inRange: {{
                        color: ['#1e293b', '#fbbf24', '#ef4444']
                    }},
                    textStyle: {{ color: '#94a3b8' }}
                }},
                series: [{{
                    type: 'heatmap',
                    data: [{heatmap_data}],
                    label: {{
                        show: true,
                        color: '#e2e8f0',
                        fontSize: 8,
                        formatter: function(p) {{ return p.data[2] > 0 ? p.data[2].toFixed(0) : ''; }}
                    }},
                    emphasis: {{
                        itemStyle: {{ shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.5)' }}
                    }}
                }}]
            }});

        }}
    }})();
    </script>
"##,
        location = escape_html(&location_label),
        municipality_note = municipality_note,
        chart_height = chart_height,
        x_labels = x_labels,
        y_labels = y_labels,
        max_pct = max_pct.max(10.0),
        heatmap_data = heatmap_data.join(","),
    );

    // テーブルも残す（フィルター対象のデータ）— Turso経由
    let display_rows = if !prefecture.is_empty() {
        match analytics::query_region_heatmap_turso(&state.turso, &job_type, &prefecture, params.cluster_id).await {
            Ok(r) => r,
            Err(_e) => {
                // フォールバック: ローカルDB
                if let Some(db) = &state.geocoded_db {
                    let db_c = db.clone(); let jt = job_type.clone(); let pref = prefecture.clone();
                    let cid = params.cluster_id;
                    tokio::task::spawn_blocking(move || {
                        analytics::query_region_heatmap(&db_c, &jt, &pref, cid).unwrap_or_default()
                    }).await.unwrap_or_default()
                } else {
                    Vec::new()
                }
            }
        }
    } else {
        // 全国表示の場合はheatmapで表現したので、テーブルは省略しても良いが
        // 乖離度を見たい場合のためにdeviation上位を表示
        all_rows.iter().filter(|r| {
            let dev = r.get("deviation").and_then(|v| v.as_f64()).unwrap_or(0.0);
            dev.abs() > 3.0
        }).cloned().collect::<Vec<_>>()
    };

    if !display_rows.is_empty() {
        let table_title = if !prefecture.is_empty() {
            format!("{} のクラスタ分布", escape_html(&prefecture))
        } else {
            "乖離度 |3pt|以上の地域×クラスタ".to_string()
        };

        html.push_str(&format!(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">{}</h4>
            <div class="overflow-x-auto max-h-80 overflow-y-auto"><table class="w-full text-xs">
            <thead class="sticky top-0 bg-navy-800"><tr class="text-gray-400 border-b border-slate-700">
                <th class="text-left p-1.5">都道府県</th>
                <th class="text-left p-1.5">クラスタ</th>
                <th class="text-right p-1.5">件数</th>
                <th class="text-right p-1.5">構成比</th>
                <th class="text-right p-1.5">全国比</th>
                <th class="text-right p-1.5">乖離</th>
            </tr></thead><tbody>"#, table_title));

        for row in &display_rows {
            let pref = row.get("prefecture").and_then(|v| v.as_str()).unwrap_or("-");
            let cid = row.get("cluster_id").and_then(|v| v.as_i64()).unwrap_or(0);
            let label = row.get("cluster_label").and_then(|v| v.as_str()).unwrap_or("-");
            let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            let pct = row.get("pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let national_pct = row.get("national_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let deviation = row.get("deviation").and_then(|v| v.as_f64()).unwrap_or(0.0);

            let dev_color = if deviation > 5.0 { "text-emerald-400" } else if deviation > 0.0 { "text-emerald-400/60" } else if deviation > -5.0 { "text-red-400/60" } else { "text-red-400" };
            let dev_sign = if deviation > 0.0 { "+" } else { "" };

            html.push_str(&format!(
                r#"<tr class="border-b border-slate-800 hover:bg-navy-700/50">
                    <td class="p-1.5 text-gray-300">{pref}</td>
                    <td class="p-1.5"><span class="text-cyan-300">C{cid}</span> <span class="text-gray-400 text-xs">{label}</span></td>
                    <td class="p-1.5 text-right">{count}</td>
                    <td class="p-1.5 text-right">{pct:.1}%</td>
                    <td class="p-1.5 text-right text-gray-500">{national_pct:.1}%</td>
                    <td class="p-1.5 text-right font-mono {dev_color}">{dev_sign}{deviation:.1}pt</td>
                </tr>"#,
                pref = escape_html(pref),
                cid = cid,
                label = escape_html(label),
                count = format_number(count),
                pct = pct,
                national_pct = national_pct,
                dev_color = dev_color,
                dev_sign = dev_sign,
                deviation = deviation,
            ));
        }

        html.push_str("</tbody></table></div></div>");
    }

    html.push_str("</div>");
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ---------------------------------------------------------------------------
// ヘルパー関数
// ---------------------------------------------------------------------------

fn format_yen(val: f64) -> String {
    if val == 0.0 {
        "-".to_string()
    } else if val >= 10000.0 {
        format!("¥{}", format_number(val as i64))
    } else {
        format!("¥{:.0}", val)
    }
}

/// 市区町村選択時の注記バナー（6Q分析は都道府県集計のため）
/// サンプル数が少ない場合の注意表示（N < 30）
fn low_sample_notice_html(rows: &[std::collections::HashMap<String, serde_json::Value>]) -> String {
    // 最小サンプル数を取得（全体行以外で）
    let min_count: i64 = rows.iter()
        .filter(|r: &&std::collections::HashMap<String, serde_json::Value>| {
            r.get("employment_type").and_then(|v: &serde_json::Value| v.as_str()) != Some("全体")
        })
        .filter_map(|r: &std::collections::HashMap<String, serde_json::Value>| {
            r.get("count").and_then(|v: &serde_json::Value| v.as_i64())
        })
        .min()
        .unwrap_or(0);

    if min_count >= 30 {
        return String::new();
    }

    format!(
        r#"<div class="bg-amber-900/40 border border-amber-700/50 text-amber-200 px-4 py-3 rounded-lg mb-4 text-sm flex items-center gap-2">
            <svg class="w-5 h-5 flex-shrink-0" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z"/>
            </svg>
            <div>
                データ件数が少ないため（N={min_count}）、統計値の信頼性が低い可能性があります。
            </div>
        </div>"#,
        min_count = min_count,
    )
}

fn error_html(msg: &str) -> String {
    super::render_error_state("エラー", &escape_html(msg))
}

fn empty_html(msg: &str) -> String {
    super::render_empty_state("データがありません", &escape_html(msg))
}

/// 給与グループ棒グラフ（従来のlayer_a_salary_statsデータ用）
/// P25/Median/P75/P90を個別の棒グラフとして表示（スタック差分ではない）
fn render_salary_grouped_chart(
    salary_rows: &[&std::collections::HashMap<String, serde_json::Value>],
    title: &str,
    chart_id: &str,
    is_monthly: bool,
) -> String {
    let mut labels = Vec::new();
    let mut p25_data = Vec::new();
    let mut median_data = Vec::new();
    let mut p75_data = Vec::new();
    let mut p90_data = Vec::new();
    let mut mean_data = Vec::new();

    for row in salary_rows {
        let emp = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        let p25 = row.get("p25").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let median = row.get("median").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p75 = row.get("p75").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p90 = row.get("p90").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let mean = row.get("mean").and_then(|v| v.as_f64()).unwrap_or(0.0);

        labels.push(format!("'{}'", emp));
        p25_data.push(format!("{:.0}", p25));
        median_data.push(format!("{:.0}", median));
        p75_data.push(format!("{:.0}", p75));
        p90_data.push(format!("{:.0}", p90));
        mean_data.push(format!("{:.0}", mean));
    }

    let y_formatter = if is_monthly {
        "function(v){return (v/10000).toFixed(0)+'万';}"
    } else {
        "function(v){return v.toLocaleString()+'円';}"
    };

    format!(r##"
    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-semibold text-gray-300 mb-2">{title}分布（雇用形態別: P25/中央値/P75/P90）</h4>
        <div id="{chart_id}" style="width:100%;height:350px;"></div>
    </div>
    <script>
    (function() {{
        var dom = document.getElementById('{chart_id}');
        if (dom) {{
            var c = echarts.init(dom, 'dark');
            var fmtY = {y_formatter};
            c.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{
                    trigger: 'axis',
                    formatter: function(params) {{
                        var s = params[0].axisValue;
                        params.forEach(function(p) {{
                            s += '<br>' + p.marker + p.seriesName + ': ' + fmtY(p.value);
                        }});
                        return s;
                    }}
                }},
                legend: {{ data: ['P25', '中央値', 'P75', 'P90', '平均'], textStyle: {{ color: '#94a3b8' }} }},
                grid: {{ left: 80, right: 30, top: 50, bottom: 30 }},
                xAxis: {{ type: 'category', data: [{labels}], axisLabel: {{ color: '#94a3b8' }} }},
                yAxis: {{ type: 'value', axisLabel: {{ color: '#94a3b8', formatter: fmtY }} }},
                series: [
                    {{ name: 'P25', type: 'bar', data: [{p25}], itemStyle: {{ color: '#64748b' }}, barGap: '10%' }},
                    {{ name: '中央値', type: 'bar', data: [{median}], itemStyle: {{ color: '#06b6d4' }} }},
                    {{ name: 'P75', type: 'bar', data: [{p75}], itemStyle: {{ color: '#3b82f6' }} }},
                    {{ name: 'P90', type: 'bar', data: [{p90}], itemStyle: {{ color: '#8b5cf6' }} }},
                    {{ name: '平均', type: 'scatter', data: [{mean}], symbolSize: 14, itemStyle: {{ color: '#f59e0b' }}, z: 10 }}
                ]
            }});

        }}
    }})();
    </script>
"##,
        title = title,
        chart_id = chart_id,
        y_formatter = y_formatter,
        labels = labels.join(","),
        p25 = p25_data.join(","),
        median = median_data.join(","),
        p75 = p75_data.join(","),
        p90 = p90_data.join(","),
        mean = mean_data.join(","),
    )
}

/// 給与 下限/上限分離チャート（postingsテーブルデータ用）
/// salary_min と salary_max のそれぞれで P25/Median/P75/P90 を表示
fn render_salary_minmax_chart(
    salary_rows: &[&std::collections::HashMap<String, serde_json::Value>],
    title: &str,
    chart_id: &str,
    is_monthly: bool,
) -> String {
    let mut labels = Vec::new();
    // salary_min 統計
    let mut p25_min = Vec::new();
    let mut med_min = Vec::new();
    let mut p75_min = Vec::new();
    let mut p90_min = Vec::new();
    // salary_max 統計
    let mut p25_max = Vec::new();
    let mut med_max = Vec::new();
    let mut p75_max = Vec::new();
    let mut p90_max = Vec::new();

    for row in salary_rows {
        let emp = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        labels.push(format!("'{}'", emp));

        p25_min.push(format!("{:.0}", row.get("p25_min").and_then(|v| v.as_f64()).unwrap_or(0.0)));
        med_min.push(format!("{:.0}", row.get("median_min").and_then(|v| v.as_f64()).unwrap_or(0.0)));
        p75_min.push(format!("{:.0}", row.get("p75_min").and_then(|v| v.as_f64()).unwrap_or(0.0)));
        p90_min.push(format!("{:.0}", row.get("p90_min").and_then(|v| v.as_f64()).unwrap_or(0.0)));

        p25_max.push(format!("{:.0}", row.get("p25_max").and_then(|v| v.as_f64()).unwrap_or(0.0)));
        med_max.push(format!("{:.0}", row.get("median_max").and_then(|v| v.as_f64()).unwrap_or(0.0)));
        p75_max.push(format!("{:.0}", row.get("p75_max").and_then(|v| v.as_f64()).unwrap_or(0.0)));
        p90_max.push(format!("{:.0}", row.get("p90_max").and_then(|v| v.as_f64()).unwrap_or(0.0)));
    }

    let y_formatter = if is_monthly {
        "function(v){return (v/10000).toFixed(0)+'万';}"
    } else {
        "function(v){return v.toLocaleString()+'円';}"
    };

    format!(r##"
    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-semibold text-gray-300 mb-2">{title} 下限/上限 分布（雇用形態別）</h4>
        <div id="{chart_id}" style="width:100%;height:400px;"></div>
    </div>
    <script>
    (function() {{
        var dom = document.getElementById('{chart_id}');
        if (dom) {{
            var c = echarts.init(dom, 'dark');
            var fmtY = {y_formatter};
            c.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{
                    trigger: 'axis',
                    formatter: function(params) {{
                        var s = params[0].axisValue;
                        params.forEach(function(p) {{
                            s += '<br>' + p.marker + p.seriesName + ': ' + fmtY(p.value);
                        }});
                        return s;
                    }}
                }},
                legend: {{
                    data: ['下限P25','下限 中央値','下限P75','下限P90','上限P25','上限 中央値','上限P75','上限P90'],
                    textStyle: {{ color: '#94a3b8', fontSize: 10 }},
                    top: 0
                }},
                grid: {{ left: 80, right: 30, top: 60, bottom: 30 }},
                xAxis: {{ type: 'category', data: [{labels}], axisLabel: {{ color: '#94a3b8' }} }},
                yAxis: {{ type: 'value', axisLabel: {{ color: '#94a3b8', formatter: fmtY }} }},
                series: [
                    {{ name: '下限P25', type: 'bar', data: [{p25_min}], itemStyle: {{ color: '#64748b' }}, barGap: '5%' }},
                    {{ name: '下限 中央値', type: 'bar', data: [{med_min}], itemStyle: {{ color: '#06b6d4' }} }},
                    {{ name: '下限P75', type: 'bar', data: [{p75_min}], itemStyle: {{ color: '#3b82f6' }} }},
                    {{ name: '下限P90', type: 'bar', data: [{p90_min}], itemStyle: {{ color: '#8b5cf6' }} }},
                    {{ name: '上限P25', type: 'bar', data: [{p25_max}], itemStyle: {{ color: '#64748b', opacity: 0.5 }}, barGap: '5%' }},
                    {{ name: '上限 中央値', type: 'bar', data: [{med_max}], itemStyle: {{ color: '#10b981' }} }},
                    {{ name: '上限P75', type: 'bar', data: [{p75_max}], itemStyle: {{ color: '#22c55e' }} }},
                    {{ name: '上限P90', type: 'bar', data: [{p90_max}], itemStyle: {{ color: '#a855f7' }} }}
                ]
            }});

        }}
    }})();
    </script>
"##,
        title = title,
        chart_id = chart_id,
        y_formatter = y_formatter,
        labels = labels.join(","),
        p25_min = p25_min.join(","),
        med_min = med_min.join(","),
        p75_min = p75_min.join(","),
        p90_min = p90_min.join(","),
        p25_max = p25_max.join(","),
        med_max = med_max.join(","),
        p75_max = p75_max.join(","),
        p90_max = p90_max.join(","),
    )
}

/// 下限/上限分離テーブル
fn render_salary_minmax_table(rows: &[std::collections::HashMap<String, serde_json::Value>]) -> String {
    let mut html = String::from(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-semibold text-gray-300 mb-3">詳細数値（下限/上限）</h4>
        <div class="overflow-x-auto">
        <table class="w-full text-xs">
            <thead>
                <tr class="text-gray-400 border-b border-slate-700">
                    <th class="text-left p-1.5">種別</th>
                    <th class="text-left p-1.5">雇用形態</th>
                    <th class="text-right p-1.5">件数</th>
                    <th class="text-right p-1.5">下限P25</th>
                    <th class="text-right p-1.5">下限中央値</th>
                    <th class="text-right p-1.5">下限P75</th>
                    <th class="text-right p-1.5">上限P25</th>
                    <th class="text-right p-1.5">上限中央値</th>
                    <th class="text-right p-1.5">上限P75</th>
                </tr>
            </thead>
            <tbody>"#);

    for row in rows {
        let salary_type = row.get("salary_type").and_then(|v| v.as_str()).unwrap_or("-");
        let emp_type = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let p25_min = row.get("p25_min").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let med_min = row.get("median_min").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p75_min = row.get("p75_min").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p25_max = row.get("p25_max").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let med_max = row.get("median_max").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p75_max = row.get("p75_max").and_then(|v| v.as_f64()).unwrap_or(0.0);

        html.push_str(&format!(
            r#"<tr class="border-b border-slate-800 hover:bg-navy-700/50">
                <td class="p-1.5 text-cyan-300">{salary_type}</td>
                <td class="p-1.5">{emp_type}</td>
                <td class="p-1.5 text-right">{count}</td>
                <td class="p-1.5 text-right font-mono text-gray-400">{p25_min}</td>
                <td class="p-1.5 text-right font-mono">{med_min}</td>
                <td class="p-1.5 text-right font-mono text-gray-400">{p75_min}</td>
                <td class="p-1.5 text-right font-mono text-gray-400">{p25_max}</td>
                <td class="p-1.5 text-right font-mono text-emerald-300">{med_max}</td>
                <td class="p-1.5 text-right font-mono text-gray-400">{p75_max}</td>
            </tr>"#,
            salary_type = escape_html(salary_type),
            emp_type = escape_html(emp_type),
            count = format_number(count),
            p25_min = format_yen(p25_min),
            med_min = format_yen(med_min),
            p75_min = format_yen(p75_min),
            p25_max = format_yen(p25_max),
            med_max = format_yen(med_max),
            p75_max = format_yen(p75_max),
        ));
    }

    html.push_str("</tbody></table></div></div>");
    html
}

// ---------------------------------------------------------------------------
// 地域比較 API
// ---------------------------------------------------------------------------

/// 2地域の市場データを並列比較するサブタブ
pub async fn api_compare(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<CompareParams>,
) -> Html<String> {
    let (job_type, prefecture, _municipality) = get_session_filters(&session).await;

    let pref1 = if prefecture.is_empty() { "東京都".to_string() } else { prefecture };
    let pref2 = params.pref2.unwrap_or_default();

    let cache_key = format!("analysis_compare_{}_{}_{}", job_type, pref1, pref2);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    // 都道府県ドロップダウン生成
    let pref_options: String = PREFECTURE_ORDER
        .iter()
        .filter(|p| **p != pref1)
        .map(|p| {
            let sel = if *p == pref2 { " selected" } else { "" };
            format!(r#"<option value="{p}"{sel}>{p}</option>"#)
        })
        .collect::<Vec<_>>()
        .join("\n");

    // pref2 が未選択なら選択UIのみ返す
    if pref2.is_empty() {
        let html = format!(
            r##"<div class="space-y-6">
    <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M19.5 12c0-1.232-.046-2.453-.138-3.662a4.006 4.006 0 00-3.7-3.7 48.678 48.678 0 00-7.324 0 4.006 4.006 0 00-3.7 3.7c-.017.22-.032.441-.046.662M19.5 12l3-3m-3 3l-3-3m-12 3c0 1.232.046 2.453.138 3.662a4.006 4.006 0 003.7 3.7 48.656 48.656 0 007.324 0 4.006 4.006 0 003.7-3.7c.017-.22.032-.441.046-.662M4.5 12l3 3m-3-3l-3 3'/></svg>  地域比較 — {job_type}</h3>
    <p class="text-sm text-gray-400">現在の選択: <span class="text-cyan-300">{pref1}</span> — 比較先を選んでください</p>
    <div class="flex items-center gap-4">
        <div class="bg-navy-800 rounded-lg px-4 py-3 border border-slate-700">
            <span class="text-sm text-gray-400">基準地域:</span>
            <span class="text-white font-bold ml-2">{pref1}</span>
        </div>
        <span class="text-gray-500">vs</span>
        <select id="compare-pref2" class="bg-navy-800 border border-slate-600 rounded-lg px-3 py-2 text-white text-sm"
                hx-get="/api/analysis/compare" hx-target="#analysis-content" hx-swap="innerHTML"
                hx-include="this" name="pref2">
            <option value="">比較先を選択...</option>
            {pref_options}
        </select>
    </div>
</div>"##,
            job_type = escape_html(&job_type),
            pref1 = escape_html(&pref1),
            pref_options = pref_options,
        );
        state.cache.set(cache_key, Value::String(html.clone()));
        return Html(html);
    }

    // --- データ取得（Turso経由、フォールバック: ローカルDB）---
    let salary1 = analytics::query_salary_stats_turso(&state.turso, &job_type, &pref1).await.unwrap_or_default();
    let salary2 = analytics::query_salary_stats_turso(&state.turso, &job_type, &pref2).await.unwrap_or_default();
    let emp1 = analytics::query_employment_diversity_turso(&state.turso, &job_type, &pref1).await.unwrap_or_default();
    let emp2 = analytics::query_employment_diversity_turso(&state.turso, &job_type, &pref2).await.unwrap_or_default();
    let fac1 = analytics::query_facility_concentration_turso(&state.turso, &job_type, &pref1).await.unwrap_or_default();
    let fac2 = analytics::query_facility_concentration_turso(&state.turso, &job_type, &pref2).await.unwrap_or_default();

    // Turso結果が全て空の場合、ローカルDBにフォールバック
    let (salary1, salary2, emp1, emp2, fac1, fac2) = if salary1.is_empty() && fac1.is_empty() {
        if let Some(db) = &state.geocoded_db {
            let db_c = db.clone(); let jt = job_type.clone();
            let p1 = pref1.clone(); let p2 = pref2.clone();
            tokio::task::spawn_blocking(move || {
                let s1 = analytics::query_salary_stats(&db_c, &jt, &p1).unwrap_or_default();
                let s2 = analytics::query_salary_stats(&db_c, &jt, &p2).unwrap_or_default();
                let e1 = analytics::query_employment_diversity(&db_c, &jt, &p1).unwrap_or_default();
                let e2 = analytics::query_employment_diversity(&db_c, &jt, &p2).unwrap_or_default();
                let f1 = analytics::query_facility_concentration(&db_c, &jt, &p1).unwrap_or_default();
                let f2 = analytics::query_facility_concentration(&db_c, &jt, &p2).unwrap_or_default();
                (s1, s2, e1, e2, f1, f2)
            }).await.unwrap_or_else(|_| (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()))
        } else {
            (salary1, salary2, emp1, emp2, fac1, fac2)
        }
    } else {
        (salary1, salary2, emp1, emp2, fac1, fac2)
    };

    // 給与中央値の抽出（正職員優先、なければ全体にフォールバック）
    let extract_median = |rows: &[std::collections::HashMap<String, serde_json::Value>], stype: &str| -> f64 {
        // まず正職員を探す
        let seishokuin = rows.iter().find(|r| {
            r.get("salary_type").and_then(|v| v.as_str()).unwrap_or("") == stype
                && r.get("employment_type").and_then(|v| v.as_str()).unwrap_or("") == "正職員"
        });
        // なければ「全体」にフォールバック
        let row = seishokuin.or_else(|| rows.iter().find(|r| {
            r.get("salary_type").and_then(|v| v.as_str()).unwrap_or("") == stype
                && r.get("employment_type").and_then(|v| v.as_str()).unwrap_or("") == "全体"
        }));
        row.and_then(|r| r.get("median").and_then(|v| v.as_f64()))
            .unwrap_or(0.0)
    };
    let med1_monthly = extract_median(&salary1, "月給");
    let med2_monthly = extract_median(&salary2, "月給");
    let med1_hourly = extract_median(&salary1, "時給");
    let med2_hourly = extract_median(&salary2, "時給");

    // 法人集中度
    let extract_fac = |rows: &[std::collections::HashMap<String, serde_json::Value>], key: &str| -> f64 {
        rows.first()
            .and_then(|r| r.get(key).and_then(|v| v.as_f64()))
            .unwrap_or(0.0)
    };
    let hhi1 = extract_fac(&fac1, "hhi");
    let hhi2 = extract_fac(&fac2, "hhi");
    let top10_1 = extract_fac(&fac1, "top10_pct");
    let top10_2 = extract_fac(&fac2, "top10_pct");
    let total1 = fac1.first().and_then(|r| r.get("total_postings").and_then(|v| v.as_i64())).unwrap_or(0);
    let total2 = fac2.first().and_then(|r| r.get("total_postings").and_then(|v| v.as_i64())).unwrap_or(0);

    // 雇用多様性
    let extract_emp_count = |rows: &[std::collections::HashMap<String, serde_json::Value>], etype: &str| -> i64 {
        rows.iter()
            .find(|r| r.get("employment_type").and_then(|v| v.as_str()).unwrap_or("") == etype)
            .and_then(|r| r.get("count").and_then(|v| v.as_i64()))
            .unwrap_or(0)
    };

    // 差分ハイライト色（高い方が緑、低い方が赤）
    let diff_class = |a: f64, b: f64| -> (&'static str, &'static str) {
        if a > b { ("text-emerald-400", "text-red-400") }
        else if b > a { ("text-red-400", "text-emerald-400") }
        else { ("text-white", "text-white") }
    };
    let (mc1, mc2) = diff_class(med1_monthly, med2_monthly);
    let (hc1, hc2) = diff_class(med1_hourly, med2_hourly);
    let (tc1, tc2) = diff_class(total1 as f64, total2 as f64);

    // ECharts チャート: 月給/時給の並列棒グラフ
    let chart_id = "compare-salary-chart";
    let chart_config = format!(
        r#"{{"tooltip":{{"trigger":"axis"}},"legend":{{"data":["{}","{}"],"textStyle":{{"color":"{}"}}}},"xAxis":{{"type":"category","data":["月給(中央値)","時給(中央値)"],"axisLabel":{{"color":"{}"}}}},"yAxis":{{"type":"value","axisLabel":{{"color":"{}","formatter":"{{value}}"}}}},"series":[{{"name":"{}","type":"bar","data":[{},{}],"itemStyle":{{"color":"{}"}}}},{{"name":"{}","type":"bar","data":[{},{}],"itemStyle":{{"color":"{}"}}}}]}}"#,
        pref1, pref2, "#94a3b8", "#94a3b8", "#94a3b8",
        pref1, med1_monthly, med1_hourly, "#3b82f6",
        pref2, med2_monthly, med2_hourly, "#f59e0b",
    );

    // 雇用形態比較チャート
    let emp_types = ["正職員", "パート", "契約職員", "業務委託"];
    let emp1_vals: Vec<i64> = emp_types.iter().map(|t| extract_emp_count(&emp1, t)).collect();
    let emp2_vals: Vec<i64> = emp_types.iter().map(|t| extract_emp_count(&emp2, t)).collect();
    let emp_chart_id = "compare-emp-chart";
    let e1_str = emp1_vals.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
    let e2_str = emp2_vals.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
    let emp_chart_config = format!(
        r#"{{"tooltip":{{"trigger":"axis"}},"legend":{{"data":["{}","{}"],"textStyle":{{"color":"{}"}}}},"xAxis":{{"type":"category","data":["正職員","パート","契約職員","業務委託"],"axisLabel":{{"color":"{}"}}}},"yAxis":{{"type":"value","axisLabel":{{"color":"{}"}}}},"series":[{{"name":"{}","type":"bar","data":[{}],"itemStyle":{{"color":"{}"}}}},{{"name":"{}","type":"bar","data":[{}],"itemStyle":{{"color":"{}"}}}}]}}"#,
        pref1, pref2, "#94a3b8", "#94a3b8", "#94a3b8",
        pref1, e1_str, "#3b82f6",
        pref2, e2_str, "#f59e0b",
    );

    let html = format!(
        r##"<div class="space-y-6">
    <h3 class="text-lg font-semibold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M19.5 12c0-1.232-.046-2.453-.138-3.662a4.006 4.006 0 00-3.7-3.7 48.678 48.678 0 00-7.324 0 4.006 4.006 0 00-3.7 3.7c-.017.22-.032.441-.046.662M19.5 12l3-3m-3 3l-3-3m-12 3c0 1.232.046 2.453.138 3.662a4.006 4.006 0 003.7 3.7 48.656 48.656 0 007.324 0 4.006 4.006 0 003.7-3.7c.017-.22.032-.441.046-.662M4.5 12l3 3m-3-3l-3 3'/></svg>  地域比較 — {job_type}</h3>

    <!-- 地域選択UI -->
    <div class="flex items-center gap-4 flex-wrap">
        <div class="bg-navy-800 rounded-lg px-4 py-3 border border-blue-500/30">
            <span class="text-sm text-gray-400">基準:</span>
            <span class="text-blue-400 font-bold ml-2">{pref1}</span>
        </div>
        <span class="text-gray-500 text-lg">vs</span>
        <div class="flex items-center gap-2">
            <select id="compare-pref2" class="bg-navy-800 border border-amber-500/30 rounded-lg px-3 py-2 text-amber-400 font-bold text-sm"
                    hx-get="/api/analysis/compare" hx-target="#analysis-content" hx-swap="innerHTML"
                    hx-include="this" name="pref2">
                <option value="">比較先を変更...</option>
                {pref_options}
            </select>
        </div>
    </div>

    <!-- 比較カード: 求人数 -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div class="bg-navy-800 rounded-lg p-5 border border-blue-500/20">
            <div class="text-sm text-blue-400 mb-1">{pref1}</div>
            <div class="text-2xl font-bold {tc1}">{total1} 件</div>
            <div class="text-xs text-gray-500 mt-1">総求人数</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-5 border border-amber-500/20">
            <div class="text-sm text-amber-400 mb-1">{pref2}</div>
            <div class="text-2xl font-bold {tc2}">{total2} 件</div>
            <div class="text-xs text-gray-500 mt-1">総求人数</div>
        </div>
    </div>

    <!-- 給与比較テーブル -->
    <div class="bg-navy-800 rounded-lg p-5 border border-slate-700">
        <h4 class="text-white font-semibold mb-3"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M4.5 4.5l7.5 9 7.5-9M4.5 12h15M4.5 15h15M12 12v9'/></svg>  給与比較（中央値）</h4>
        <table class="data-table w-full">
            <thead>
                <tr>
                    <th class="p-2">給与タイプ</th>
                    <th class="p-2 text-right text-blue-400">{pref1}</th>
                    <th class="p-2 text-right text-amber-400">{pref2}</th>
                    <th class="p-2 text-right">差額</th>
                </tr>
            </thead>
            <tbody>
                <tr class="border-b border-slate-800">
                    <td class="p-2 text-gray-300">月給</td>
                    <td class="p-2 text-right font-mono {mc1}">{med1_m}</td>
                    <td class="p-2 text-right font-mono {mc2}">{med2_m}</td>
                    <td class="p-2 text-right font-mono text-gray-400">{diff_m}</td>
                </tr>
                <tr>
                    <td class="p-2 text-gray-300">時給</td>
                    <td class="p-2 text-right font-mono {hc1}">{med1_h}</td>
                    <td class="p-2 text-right font-mono {hc2}">{med2_h}</td>
                    <td class="p-2 text-right font-mono text-gray-400">{diff_h}</td>
                </tr>
            </tbody>
        </table>
    </div>

    <!-- 給与チャート -->
    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <div id="{chart_id}" class="echart" style="height:320px" data-chart-config='{chart_config}'></div>
    </div>

    <!-- 法人集中度比較 -->
    <div class="bg-navy-800 rounded-lg p-5 border border-slate-700">
        <h4 class="text-white font-semibold mb-3"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M3.75 21h16.5M4.5 3h15M5.25 3v18m13.5-18v18M9 6.75h1.5m-1.5 3h1.5m-1.5 3h1.5m3-6H15m-1.5 3H15m-1.5 3H15M9 21v-3.375c0-.621.504-1.125 1.125-1.125h3.75c.621 0 1.125.504 1.125 1.125V21'/></svg>  法人集中度比較</h4>
        <table class="data-table w-full">
            <thead>
                <tr>
                    <th class="p-2">指標</th>
                    <th class="p-2 text-right text-blue-400">{pref1}</th>
                    <th class="p-2 text-right text-amber-400">{pref2}</th>
                </tr>
            </thead>
            <tbody>
                <tr class="border-b border-slate-800">
                    <td class="p-2 text-gray-300" title="HHI: 値が小さいほど多くの法人が競争している状態">市場の集中度</td>
                    <td class="p-2 text-right font-mono">{hhi1:.4}</td>
                    <td class="p-2 text-right font-mono">{hhi2:.4}</td>
                </tr>
                <tr>
                    <td class="p-2 text-gray-300">上位10法人シェア</td>
                    <td class="p-2 text-right font-mono">{top10_1:.1}%</td>
                    <td class="p-2 text-right font-mono">{top10_2:.1}%</td>
                </tr>
            </tbody>
        </table>
        <p class="text-xs text-gray-500 mt-2">集中度: 値が小さいほど多くの法人が求人を出している競争的な市場。0.15以上は大手数社に求人が集中する寡占傾向。</p>
    </div>

    <!-- 雇用形態比較チャート -->
    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-white font-semibold mb-3"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15a2.25 2.25 0 012.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V19.5a2.25 2.25 0 002.25 2.25h.75'/></svg>  雇用形態比較</h4>
        <div id="{emp_chart_id}" class="echart" style="height:320px" data-chart-config='{emp_chart_config}'></div>
    </div>
</div>

<script>
(function() {{
    // EChartsの初期化
    [{chart_id_js}, {emp_chart_id_js}].forEach(function(id) {{
        var el = document.getElementById(id);
        if (el && typeof echarts !== 'undefined') {{
            var existing = echarts.getInstanceByDom(el);
            if (existing) existing.dispose();
            var chart = echarts.init(el);
            try {{
                chart.setOption(JSON.parse(el.getAttribute('data-chart-config')));
            }} catch(e) {{ console.warn('[compare] chart error:', e); }}
        }}
    }});
}})();
</script>"##,
        job_type = escape_html(&job_type),
        pref1 = escape_html(&pref1),
        pref2 = escape_html(&pref2),
        pref_options = pref_options,
        tc1 = tc1, tc2 = tc2,
        total1 = format_number(total1),
        total2 = format_number(total2),
        mc1 = mc1, mc2 = mc2,
        hc1 = hc1, hc2 = hc2,
        med1_m = format_yen(med1_monthly),
        med2_m = format_yen(med2_monthly),
        med1_h = format_yen(med1_hourly),
        med2_h = format_yen(med2_hourly),
        diff_m = format_yen(med1_monthly - med2_monthly),
        diff_h = format_yen(med1_hourly - med2_hourly),
        hhi1 = hhi1, hhi2 = hhi2,
        top10_1 = top10_1,
        top10_2 = top10_2,
        chart_id = chart_id,
        chart_config = chart_config,
        emp_chart_id = emp_chart_id,
        emp_chart_config = emp_chart_config,
        chart_id_js = format!(r#""{}""#, chart_id),
        emp_chart_id_js = format!(r#""{}""#, emp_chart_id),
    );

    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ===========================================================================
// 6Q テキスト分析 API ハンドラー (Layer B v2)
// ===========================================================================

/// Q2+Q1: テキスト分析（定型文率 + 差別化シグナル）
pub async fn api_text_analysis(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_text_analysis_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let location_label = make_location_label(&prefecture, &municipality);

    let rows = match analytics::query_text_analysis(&state.turso, &job_type, &prefecture, &municipality).await {
        Ok(r) => r,
        Err(e) => return Html(error_html(&e)),
    };

    if rows.is_empty() {
        return Html(empty_html("テキスト分析データがありません。Tursoに6Qテーブルをインポートしてください。"));
    }

    let mut table_rows = String::new();
    let mut chart_labels = Vec::new();
    let mut template_data = Vec::new();
    let mut diff_data = Vec::new();
    let mut diff_zero_data = Vec::new();

    for row in &rows {
        let emp = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let tmpl_mean = row.get("template_ratio_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let tmpl_median = row.get("template_ratio_median").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let orig_mean = row.get("original_length_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let diff_mean = row.get("diff_total_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let diff_zero = row.get("diff_zero_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let full_mean = row.get("full_length_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);

        table_rows.push_str(&format!(
            r#"<tr class="border-b border-slate-700 hover:bg-slate-800/50">
                <td class="px-4 py-3 font-medium text-white">{emp}</td>
                <td class="px-4 py-3 text-right">{count}</td>
                <td class="px-4 py-3 text-right">{tmpl_mean:.1}%</td>
                <td class="px-4 py-3 text-right">{tmpl_median:.1}%</td>
                <td class="px-4 py-3 text-right">{orig_mean:.0}字</td>
                <td class="px-4 py-3 text-right">{full_mean:.0}字</td>
                <td class="px-4 py-3 text-right">{diff_mean:.2}</td>
                <td class="px-4 py-3 text-right">{diff_zero:.1}%</td>
            </tr>"#,
            emp = escape_html(emp), count = format_number(count),
            tmpl_mean = tmpl_mean * 100.0, tmpl_median = tmpl_median * 100.0,
            orig_mean = orig_mean, full_mean = full_mean,
            diff_mean = diff_mean, diff_zero = diff_zero,
        ));

        chart_labels.push(format!("\"{}\"", escape_html(emp)));
        template_data.push(format!("{:.1}", tmpl_mean * 100.0));
        diff_data.push(format!("{:.2}", diff_mean));
        diff_zero_data.push(format!("{:.1}", diff_zero));
    }

    let chart_id = format!("text-analysis-chart-{}", job_type.len());
    let diff_chart_id = format!("diff-chart-{}", job_type.len());

    let muni_notice = low_sample_notice_html(&rows);

    let html = format!(r##"
<div class="space-y-6">
    {muni_notice}
    <h3 class="text-lg font-semibold text-white">テキスト分析 — {job_type} ({location})</h3>
    <p class="text-sm text-gray-400">求人原稿の定型文率と差別化シグナル数を雇用形態別に分析</p>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-medium text-gray-300 mb-3">定型文率（雇用形態別）</h4>
            <div id="{chart_id}" style="height:300px;"></div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-medium text-gray-300 mb-3">差別化シグナル数（雇用形態別）</h4>
            <div id="{diff_chart_id}" style="height:300px;"></div>
        </div>
    </div>

    <div class="overflow-x-auto">
        <table class="w-full text-sm text-gray-300">
            <thead class="text-xs text-gray-400 border-b border-slate-600">
                <tr>
                    <th class="px-4 py-3 text-left">雇用形態</th>
                    <th class="px-4 py-3 text-right">件数</th>
                    <th class="px-4 py-3 text-right">定型文率(平均)</th>
                    <th class="px-4 py-3 text-right">定型文率(中央値)</th>
                    <th class="px-4 py-3 text-right">オリジナル文字数</th>
                    <th class="px-4 py-3 text-right">全体文字数</th>
                    <th class="px-4 py-3 text-right">差別化シグナル</th>
                    <th class="px-4 py-3 text-right">シグナルゼロ率</th>
                </tr>
            </thead>
            <tbody>{table_rows}</tbody>
        </table>
    </div>

    <div class="bg-slate-800/50 rounded-lg p-4 text-sm text-gray-400">
        <p><strong class="text-gray-300">読み方:</strong> 定型文率が高い＝法的記載・テンプレが多い。差別化シグナルはオリジナル文中の施設特徴・強み・働き方の独自表現数。シグナルゼロ率は差別化表現がない求人の割合。</p>
    </div>
</div>

<script>
(function() {{
    var labels = [{labels}];
    var c1 = echarts.init(document.getElementById('{chart_id}'));
    c1.setOption({{
        tooltip: {{ trigger: 'axis' }},
        xAxis: {{ type: 'category', data: labels, axisLabel: {{ color: '#9ca3af' }} }},
        yAxis: {{ type: 'value', name: '%', axisLabel: {{ color: '#9ca3af' }} }},
        series: [{{ name: '定型文率', type: 'bar', data: [{template_data}], itemStyle: {{ color: '#60a5fa' }} }}],
        grid: {{ left: '10%', right: '5%', bottom: '15%' }}
    }});
    var c2 = echarts.init(document.getElementById('{diff_chart_id}'));
    c2.setOption({{
        tooltip: {{ trigger: 'axis' }},
        legend: {{ data: ['シグナル数', 'ゼロ率%'], textStyle: {{ color: '#9ca3af' }} }},
        xAxis: {{ type: 'category', data: labels, axisLabel: {{ color: '#9ca3af' }} }},
        yAxis: [
            {{ type: 'value', name: 'シグナル数', axisLabel: {{ color: '#9ca3af' }} }},
            {{ type: 'value', name: '%', axisLabel: {{ color: '#9ca3af' }} }}
        ],
        series: [
            {{ name: 'シグナル数', type: 'bar', data: [{diff_data}], itemStyle: {{ color: '#34d399' }} }},
            {{ name: 'ゼロ率%', type: 'line', yAxisIndex: 1, data: [{diff_zero_data}], itemStyle: {{ color: '#f87171' }} }}
        ],
        grid: {{ left: '10%', right: '10%', bottom: '15%' }}
    }});
    window.addEventListener('resize', function() {{ c1.resize(); c2.resize(); }});
}})();
</script>"##,
        muni_notice = muni_notice,
        job_type = escape_html(&job_type),
        location = escape_html(&location_label),
        table_rows = table_rows,
        chart_id = chart_id,
        diff_chart_id = diff_chart_id,
        labels = chart_labels.join(","),
        template_data = template_data.join(","),
        diff_data = diff_data.join(","),
        diff_zero_data = diff_zero_data.join(","),
    );

    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

/// Q4: トーン分析
pub async fn api_tone(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_tone_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let location_label = make_location_label(&prefecture, &municipality);

    let rows = match analytics::query_tone(&state.turso, &job_type, &prefecture, &municipality).await {
        Ok(r) => r,
        Err(e) => return Html(error_html(&e)),
    };

    if rows.is_empty() {
        return Html(empty_html("トーン分析データがありません。Tursoに6Qテーブルをインポートしてください。"));
    }

    let mut table_rows = String::new();
    let mut chart_labels = Vec::new();
    let mut urgency_data = Vec::new();
    let mut enthusiasm_data = Vec::new();
    let mut casual_data = Vec::new();
    let mut selectivity_data = Vec::new();

    for row in &rows {
        let emp = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let urg = row.get("urgency_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let enth = row.get("enthusiasm_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let cas = row.get("casual_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let sel = row.get("selectivity_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let emoji = row.get("emoji_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let kaomoji = row.get("kaomoji_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let deco = row.get("decorative_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);

        table_rows.push_str(&format!(
            r#"<tr class="border-b border-slate-700 hover:bg-slate-800/50">
                <td class="px-4 py-3 font-medium text-white">{emp}</td>
                <td class="px-4 py-3 text-right">{count}</td>
                <td class="px-4 py-3 text-right">{urg:.2}</td>
                <td class="px-4 py-3 text-right">{enth:.2}</td>
                <td class="px-4 py-3 text-right">{cas:.2}</td>
                <td class="px-4 py-3 text-right">{sel:.2}</td>
                <td class="px-4 py-3 text-right">{emoji:.1}%</td>
                <td class="px-4 py-3 text-right">{kaomoji:.1}%</td>
                <td class="px-4 py-3 text-right">{deco:.1}%</td>
            </tr>"#,
            emp = escape_html(emp), count = format_number(count),
            urg = urg, enth = enth, cas = cas, sel = sel,
            emoji = emoji, kaomoji = kaomoji, deco = deco,
        ));

        chart_labels.push(format!("\"{}\"", escape_html(emp)));
        urgency_data.push(format!("{:.2}", urg));
        enthusiasm_data.push(format!("{:.2}", enth));
        casual_data.push(format!("{:.2}", cas));
        selectivity_data.push(format!("{:.2}", sel));
    }

    let chart_id = format!("tone-radar-{}", job_type.len());
    let muni_notice = low_sample_notice_html(&rows);

    let html = format!(r##"
<div class="space-y-6">
    {muni_notice}
    <h3 class="text-lg font-semibold text-white">トーン分析 — {job_type} ({location})</h3>
    <p class="text-sm text-gray-400">求人原稿の採用姿勢を4軸で定量化: 緊急度・熱意・カジュアル度・選別度</p>

    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <div id="{chart_id}" style="height:400px;"></div>
    </div>

    <div class="overflow-x-auto">
        <table class="w-full text-sm text-gray-300">
            <thead class="text-xs text-gray-400 border-b border-slate-600">
                <tr>
                    <th class="px-4 py-3 text-left">雇用形態</th>
                    <th class="px-4 py-3 text-right">件数</th>
                    <th class="px-4 py-3 text-right">緊急度</th>
                    <th class="px-4 py-3 text-right">熱意</th>
                    <th class="px-4 py-3 text-right">カジュアル</th>
                    <th class="px-4 py-3 text-right">選別度</th>
                    <th class="px-4 py-3 text-right">絵文字率</th>
                    <th class="px-4 py-3 text-right">顔文字率</th>
                    <th class="px-4 py-3 text-right">装飾文字率</th>
                </tr>
            </thead>
            <tbody>{table_rows}</tbody>
        </table>
    </div>

    <div class="bg-slate-800/50 rounded-lg p-4 text-sm text-gray-400">
        <p><strong class="text-gray-300">読み方:</strong> 緊急度=「急募」「今すぐ」等の切迫表現の密度。熱意=「一緒に」「お待ちしています」等の歓迎表現。カジュアル=絵文字・顔文字・装飾文字の使用度合い。選別度=「経験者優遇」「有資格者」等の選別表現。</p>
    </div>
</div>

<script>
(function() {{
    var labels = [{labels}];
    var chart = echarts.init(document.getElementById('{chart_id}'));
    var series = [];
    var colors = ['#60a5fa', '#34d399', '#fbbf24'];
    var urgency = [{urgency}];
    var enthusiasm = [{enthusiasm}];
    var casual = [{casual}];
    var selectivity = [{selectivity}];
    for (var i = 0; i < labels.length; i++) {{
        series.push({{
            name: labels[i], type: 'radar',
            data: [{{ value: [urgency[i], enthusiasm[i], casual[i], selectivity[i]], name: labels[i] }}],
            lineStyle: {{ color: colors[i % colors.length] }},
            itemStyle: {{ color: colors[i % colors.length] }},
            areaStyle: {{ opacity: 0.1, color: colors[i % colors.length] }}
        }});
    }}
    chart.setOption({{
        tooltip: {{}},
        legend: {{ data: labels, bottom: 0, textStyle: {{ color: '#9ca3af' }} }},
        radar: {{
            indicator: [
                {{ name: '緊急度', max: Math.max(3, Math.max.apply(null, urgency) * 1.2) }},
                {{ name: '熱意', max: Math.max(3, Math.max.apply(null, enthusiasm) * 1.2) }},
                {{ name: 'カジュアル', max: Math.max(3, Math.max.apply(null, casual) * 1.2) }},
                {{ name: '選別度', max: Math.max(3, Math.max.apply(null, selectivity) * 1.2) }}
            ],
            shape: 'circle',
            splitArea: {{ areaStyle: {{ color: ['rgba(30,41,59,0.3)', 'rgba(30,41,59,0.5)'] }} }},
            axisName: {{ color: '#9ca3af' }}
        }},
        series: series
    }});
    window.addEventListener('resize', function() {{ chart.resize(); }});
}})();
</script>"##,
        muni_notice = muni_notice,
        job_type = escape_html(&job_type),
        location = escape_html(&location_label),
        table_rows = table_rows,
        chart_id = chart_id,
        labels = chart_labels.join(","),
        urgency = urgency_data.join(","),
        enthusiasm = enthusiasm_data.join(","),
        casual = casual_data.join(","),
        selectivity = selectivity_data.join(","),
    );

    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

/// Q5+Q3: 情報充足度 + 情報ギャップ
pub async fn api_info_score(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_info_score_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let location_label = make_location_label(&prefecture, &municipality);

    let rows = match analytics::query_info_score(&state.turso, &job_type, &prefecture, &municipality).await {
        Ok(r) => r,
        Err(e) => return Html(error_html(&e)),
    };

    if rows.is_empty() {
        return Html(empty_html("情報充足度データがありません。Tursoに6Qテーブルをインポートしてください。"));
    }

    let mut table_rows = String::new();
    let mut chart_labels = Vec::new();
    let categories = [
        ("salary_detail_mean", "給与詳細"),
        ("work_hours_mean", "勤務時間"),
        ("holidays_mean", "休日"),
        ("job_detail_mean", "業務内容"),
        ("benefits_mean", "福利厚生"),
        ("transparency_mean", "情報開示"),
    ];
    let mut category_data: Vec<Vec<String>> = vec![Vec::new(); 6];

    for row in &rows {
        let emp = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let score_mean = row.get("info_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let grade = row.get("grade").and_then(|v| v.as_str()).unwrap_or("-");
        let gap = row.get("gap_vs_national").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let grade_color = match grade {
            "A" => "text-emerald-400",
            "B" => "text-blue-400",
            "C" => "text-yellow-400",
            _ => "text-red-400",
        };

        let gap_display = if prefecture.is_empty() {
            "-".to_string()
        } else {
            let sign = if gap >= 0.0 { "+" } else { "" };
            format!("{}{:.3}", sign, gap)
        };

        let mut cat_cells = String::new();
        for (i, (key, _)) in categories.iter().enumerate() {
            let val = row.get(*key).and_then(|v| v.as_f64()).unwrap_or(0.0);
            category_data[i].push(format!("{:.3}", val));
            let bar_width = (val * 100.0 * 5.0).min(100.0);
            cat_cells.push_str(&format!(
                r#"<td class="px-3 py-3">
                    <div class="flex items-center gap-2">
                        <div class="w-16 bg-slate-700 rounded-full h-2">
                            <div class="bg-blue-500 h-2 rounded-full" style="width:{bar_w:.0}%"></div>
                        </div>
                        <span class="text-xs">{val:.3}</span>
                    </div>
                </td>"#,
                bar_w = bar_width, val = val,
            ));
        }

        table_rows.push_str(&format!(
            r#"<tr class="border-b border-slate-700 hover:bg-slate-800/50">
                <td class="px-4 py-3 font-medium text-white">{emp}</td>
                <td class="px-4 py-3 text-right">{count}</td>
                <td class="px-4 py-3 text-right">{score:.3}</td>
                <td class="px-4 py-3 text-center {gc}"><span class="font-bold">{grade}</span></td>
                <td class="px-4 py-3 text-right">{gap}</td>
                {cells}
            </tr>"#,
            emp = escape_html(emp), count = format_number(count),
            score = score_mean, gc = grade_color, grade = grade,
            gap = gap_display, cells = cat_cells,
        ));

        chart_labels.push(format!("\"{}\"", escape_html(emp)));
    }

    let chart_id = format!("info-score-radar-{}", job_type.len());
    let muni_notice = low_sample_notice_html(&rows);

    let html = format!(r##"
<div class="space-y-6">
    {muni_notice}
    <h3 class="text-lg font-semibold text-white">情報充足度 — {job_type} ({location})</h3>
    <p class="text-sm text-gray-400">求人情報の6カテゴリ別充足度と全国比ギャップ（Q5+Q3）</p>

    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <div id="{chart_id}" style="height:400px;"></div>
    </div>

    <div class="overflow-x-auto">
        <table class="w-full text-sm text-gray-300">
            <thead class="text-xs text-gray-400 border-b border-slate-600">
                <tr>
                    <th class="px-4 py-3 text-left">雇用形態</th>
                    <th class="px-4 py-3 text-right">件数</th>
                    <th class="px-4 py-3 text-right">総合スコア</th>
                    <th class="px-4 py-3 text-center">グレード</th>
                    <th class="px-4 py-3 text-right">全国比</th>
                    <th class="px-3 py-3 text-left">給与詳細</th>
                    <th class="px-3 py-3 text-left">勤務時間</th>
                    <th class="px-3 py-3 text-left">休日</th>
                    <th class="px-3 py-3 text-left">業務内容</th>
                    <th class="px-3 py-3 text-left">福利厚生</th>
                    <th class="px-3 py-3 text-left">情報開示</th>
                </tr>
            </thead>
            <tbody>{table_rows}</tbody>
        </table>
    </div>

    <div class="bg-slate-800/50 rounded-lg p-4 text-sm text-gray-400">
        <p><strong class="text-gray-300">グレード基準:</strong> A (&ge;0.20) — 詳細な情報開示 / B (&ge;0.14) — 平均以上 / C (&ge;0.10) — 最低限 / D (&lt;0.10) — 情報不足。全国比は選択地域と全国平均の差分。</p>
    </div>
</div>

<script>
(function() {{
    var labels = [{labels}];
    var catNames = ['給与詳細', '勤務時間', '休日', '業務内容', '福利厚生', '情報開示'];
    var catData = [{cat_arrays}];
    var chart = echarts.init(document.getElementById('{chart_id}'));
    var colors = ['#60a5fa', '#34d399', '#fbbf24'];
    var maxVal = 0;
    for (var c = 0; c < catData.length; c++) for (var i = 0; i < catData[c].length; i++) maxVal = Math.max(maxVal, catData[c][i]);
    var indicator = catNames.map(function(n) {{ return {{ name: n, max: Math.max(0.3, maxVal * 1.3) }}; }});
    var series = [];
    for (var i = 0; i < labels.length; i++) {{
        var vals = [];
        for (var c = 0; c < catData.length; c++) vals.push(catData[c][i]);
        series.push({{
            name: labels[i], type: 'radar',
            data: [{{ value: vals, name: labels[i] }}],
            lineStyle: {{ color: colors[i % colors.length] }},
            itemStyle: {{ color: colors[i % colors.length] }},
            areaStyle: {{ opacity: 0.1, color: colors[i % colors.length] }}
        }});
    }}
    chart.setOption({{
        tooltip: {{}},
        legend: {{ data: labels, bottom: 0, textStyle: {{ color: '#9ca3af' }} }},
        radar: {{ indicator: indicator, shape: 'circle',
            splitArea: {{ areaStyle: {{ color: ['rgba(30,41,59,0.3)', 'rgba(30,41,59,0.5)'] }} }},
            axisName: {{ color: '#9ca3af' }} }},
        series: series
    }});
    window.addEventListener('resize', function() {{ chart.resize(); }});
}})();
</script>"##,
        muni_notice = muni_notice,
        job_type = escape_html(&job_type),
        location = escape_html(&location_label),
        table_rows = table_rows,
        chart_id = chart_id,
        labels = chart_labels.join(","),
        cat_arrays = category_data.iter()
            .map(|d| format!("[{}]", d.join(",")))
            .collect::<Vec<_>>()
            .join(","),
    );

    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

/// Q6: ターゲティング分析
pub async fn api_targeting(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("analysis_targeting_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let location_label = make_location_label(&prefecture, &municipality);

    let rows = match analytics::query_targeting(&state.turso, &job_type, &prefecture, &municipality).await {
        Ok(r) => r,
        Err(e) => return Html(error_html(&e)),
    };

    if rows.is_empty() {
        return Html(empty_html("ターゲティングデータがありません。Tursoに6Qテーブルをインポートしてください。"));
    }

    let psycho_dims = [
        ("psycho_growth_mean", "成長志向"),
        ("psycho_stability_mean", "安定志向"),
        ("psycho_wlb_mean", "WLB"),
        ("psycho_contribution_mean", "社会貢献"),
        ("psycho_autonomy_mean", "自律志向"),
        ("psycho_belonging_mean", "帰属意識"),
        ("psycho_income_mean", "収入志向"),
        ("psycho_convenience_mean", "利便性"),
        ("psycho_environment_mean", "環境重視"),
        ("psycho_load_aversion_mean", "負荷回避"),
        ("psycho_rationality_mean", "合理性"),
        ("psycho_vision_mean", "理念"),
    ];

    let mut demo_cards = String::new();
    let mut psycho_table = String::new();
    let mut chart_labels = Vec::new();
    let mut chart_series_data: Vec<Vec<String>> = Vec::new();

    for row in &rows {
        let emp = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);

        let age_mode = row.get("demo_age_primary_mode").and_then(|v| v.as_str()).unwrap_or("-");
        let age_pct = row.get("demo_age_primary_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let exp_mode = row.get("demo_exp_primary_mode").and_then(|v| v.as_str()).unwrap_or("-");
        let exp_pct = row.get("demo_exp_primary_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let life_mode = row.get("demo_life_primary_mode").and_then(|v| v.as_str()).unwrap_or("-");
        let life_pct = row.get("demo_life_primary_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let qual_mode = row.get("demo_qual_primary_mode").and_then(|v| v.as_str()).unwrap_or("-");
        let qual_pct = row.get("demo_qual_primary_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let div_mode = row.get("demo_div_primary_mode").and_then(|v| v.as_str()).unwrap_or("-");
        let div_pct = row.get("demo_div_primary_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let psycho_top = row.get("psycho_top1_mode").and_then(|v| v.as_str()).unwrap_or("-");
        let psycho_top_pct = row.get("psycho_top1_pct").and_then(|v| v.as_f64()).unwrap_or(0.0);

        demo_cards.push_str(&format!(
            r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
                <h4 class="text-sm font-medium text-gray-300 mb-3">{emp} ({count}件)</h4>
                <div class="space-y-2 text-sm">
                    <div class="flex justify-between"><span class="text-gray-400">年齢層</span><span class="text-white font-medium">{age} ({age_p:.0}%)</span></div>
                    <div class="flex justify-between"><span class="text-gray-400">経験レベル</span><span class="text-white font-medium">{exp} ({exp_p:.0}%)</span></div>
                    <div class="flex justify-between"><span class="text-gray-400">ライフステージ</span><span class="text-white font-medium">{life} ({life_p:.0}%)</span></div>
                    <div class="flex justify-between"><span class="text-gray-400">資格要件</span><span class="text-white font-medium">{qual} ({qual_p:.0}%)</span></div>
                    <div class="flex justify-between"><span class="text-gray-400">ダイバーシティ</span><span class="text-white font-medium">{div} ({div_p:.0}%)</span></div>
                    <div class="mt-3 pt-3 border-t border-slate-600 flex justify-between">
                        <span class="text-gray-400">心理的訴求TOP</span>
                        <span class="text-emerald-400 font-bold">{ptop} ({ptop_p:.0}%)</span>
                    </div>
                </div>
            </div>"#,
            emp = escape_html(emp), count = format_number(count),
            age = escape_html(age_mode), age_p = age_pct,
            exp = escape_html(exp_mode), exp_p = exp_pct,
            life = escape_html(life_mode), life_p = life_pct,
            qual = escape_html(qual_mode), qual_p = qual_pct,
            div = escape_html(div_mode), div_p = div_pct,
            ptop = escape_html(psycho_top), ptop_p = psycho_top_pct,
        ));

        let mut vals = Vec::new();
        let mut psycho_cells = String::new();
        for (key, _label) in &psycho_dims {
            let val = row.get(*key).and_then(|v| v.as_f64()).unwrap_or(0.0);
            vals.push(format!("{:.3}", val));
            let bar_width = (val * 100.0).min(100.0);
            let bar_color = if val >= 0.1 { "bg-emerald-500" } else if val >= 0.03 { "bg-blue-500" } else { "bg-slate-600" };
            psycho_cells.push_str(&format!(
                r#"<td class="px-2 py-2">
                    <div class="w-14 bg-slate-700 rounded-full h-1.5 inline-block mr-1">
                        <div class="{bc} h-1.5 rounded-full" style="width:{bw:.0}%"></div>
                    </div>
                    <span class="text-xs">{v:.3}</span>
                </td>"#,
                bc = bar_color, bw = bar_width, v = val,
            ));
        }
        psycho_table.push_str(&format!(
            r#"<tr class="border-b border-slate-700"><td class="px-4 py-2 font-medium text-white">{emp}</td>{cells}</tr>"#,
            emp = escape_html(emp), cells = psycho_cells,
        ));

        chart_labels.push(format!("\"{}\"", escape_html(emp)));
        chart_series_data.push(vals);
    }

    let chart_id = format!("targeting-radar-{}", job_type.len());

    let psycho_headers: String = psycho_dims.iter()
        .map(|(_, label)| format!(r#"<th class="px-2 py-3 text-center text-xs">{}</th>"#, label))
        .collect();

    let muni_notice = low_sample_notice_html(&rows);

    let html = format!(r##"
<div class="space-y-6">
    {muni_notice}
    <h3 class="text-lg font-semibold text-white">ターゲティング分析 — {job_type} ({location})</h3>
    <p class="text-sm text-gray-400">求人原稿から読み取れるターゲット層（デモグラフィック5軸 + サイコグラフィック12軸）</p>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        {demo_cards}
    </div>

    <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
        <h4 class="text-sm font-medium text-gray-300 mb-3">サイコグラフィック12軸</h4>
        <div id="{chart_id}" style="height:450px;"></div>
    </div>

    <div class="overflow-x-auto">
        <table class="w-full text-sm text-gray-300">
            <thead class="text-xs text-gray-400 border-b border-slate-600">
                <tr>
                    <th class="px-4 py-3 text-left">雇用形態</th>
                    {psycho_headers}
                </tr>
            </thead>
            <tbody>{psycho_table}</tbody>
        </table>
    </div>

    <div class="bg-slate-800/50 rounded-lg p-4 text-sm text-gray-400">
        <p><strong class="text-gray-300">読み方:</strong> デモグラフィックは求人原稿中のシグナルから推定されるターゲット層。サイコグラフィックは12の心理的動機の平均密度（値が大きいほど訴求が強い）。</p>
    </div>
</div>

<script>
(function() {{
    var labels = [{labels}];
    var dimNames = [{dim_names}];
    var seriesData = [{series_arrays}];
    var chart = echarts.init(document.getElementById('{chart_id}'));
    var colors = ['#60a5fa', '#34d399', '#fbbf24'];
    var maxVal = 0;
    for (var s = 0; s < seriesData.length; s++) for (var i = 0; i < seriesData[s].length; i++) maxVal = Math.max(maxVal, seriesData[s][i]);
    var indicator = dimNames.map(function(n) {{ return {{ name: n, max: Math.max(0.15, maxVal * 1.3) }}; }});
    var series = [];
    for (var i = 0; i < labels.length; i++) {{
        series.push({{
            name: labels[i], type: 'radar',
            data: [{{ value: seriesData[i], name: labels[i] }}],
            lineStyle: {{ color: colors[i % colors.length] }},
            itemStyle: {{ color: colors[i % colors.length] }},
            areaStyle: {{ opacity: 0.15, color: colors[i % colors.length] }}
        }});
    }}
    chart.setOption({{
        tooltip: {{}},
        legend: {{ data: labels, bottom: 0, textStyle: {{ color: '#9ca3af' }} }},
        radar: {{ indicator: indicator, shape: 'circle',
            splitArea: {{ areaStyle: {{ color: ['rgba(30,41,59,0.3)', 'rgba(30,41,59,0.5)'] }} }},
            axisName: {{ color: '#9ca3af', fontSize: 11 }} }},
        series: series
    }});
    window.addEventListener('resize', function() {{ chart.resize(); }});
}})();
</script>"##,
        job_type = escape_html(&job_type),
        muni_notice = muni_notice,
        location = escape_html(&location_label),
        demo_cards = demo_cards,
        chart_id = chart_id,
        psycho_headers = psycho_headers,
        psycho_table = psycho_table,
        labels = chart_labels.join(","),
        dim_names = psycho_dims.iter()
            .map(|(_, label)| format!("\"{}\"", label))
            .collect::<Vec<_>>()
            .join(","),
        series_arrays = chart_series_data.iter()
            .map(|d| format!("[{}]", d.join(",")))
            .collect::<Vec<_>>()
            .join(","),
    );

    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}
