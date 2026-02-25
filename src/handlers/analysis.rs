use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use std::sync::Arc;
use tower_sessions::Session;

use crate::db::analytics;
use crate::AppState;
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

// ---------------------------------------------------------------------------
// メインタブ: /tab/analysis
// ---------------------------------------------------------------------------

/// 市場分析タブ - メインビュー
pub async fn tab_analysis(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;
    let location_label = make_location_label(&prefecture, &municipality);

    let db = match &state.geocoded_db {
        Some(db) => db,
        None => {
            return Html(r#"<div class="p-8 text-center text-red-400">
                <h2 class="text-2xl mb-4">市場分析</h2>
                <p>geocoded_postings.db が読み込まれていません</p>
            </div>"#.to_string());
        }
    };

    // サマリー取得（都道府県・市区町村フィルター対応）
    let summary = analytics::query_analysis_summary(db, &job_type, &prefecture, &municipality).unwrap_or_default();
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
        <h2 class="text-2xl font-bold text-white">📊 市場分析 — {job_type} ({location})</h2>
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
            <div class="text-xs text-gray-400">共起パターン</div>
            <div class="text-xl font-bold text-white">{cooccurrence_count}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">原稿品質</div>
            <div class="text-xl font-bold {grade_color}">{grade}</div>
        </div>
    </div>

    <!-- サブタブ -->
    <div class="border-b border-slate-700">
        <nav class="flex gap-1 overflow-x-auto" id="analysis-subtabs">
            <button class="analysis-sub-btn active px-4 py-2 text-sm rounded-t-lg bg-navy-700 text-white border border-slate-600 border-b-0"
                    hx-get="/api/analysis/salary" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">💰 給与分析</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/facility" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">🏢 法人集中度</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/employment" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">📋 雇用多様性</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/keywords" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">🔑 キーワード</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/cooccurrence" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">🔗 条件共起</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/quality" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">📝 原稿品質</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/clusters" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">🎯 クラスタ</button>
            <button class="analysis-sub-btn px-4 py-2 text-sm rounded-t-lg text-gray-400 hover:text-white"
                    hx-get="/api/analysis/heatmap" hx-target="#analysis-content" hx-swap="innerHTML"
                    onclick="setAnalysisSubTab(this)">🗺️ 地域分布</button>
        </nav>
    </div>

    <!-- コンテンツ領域 -->
    <div id="analysis-content" hx-get="/api/analysis/salary" hx-trigger="load" hx-swap="innerHTML">
        <div class="flex items-center justify-center h-32 text-gray-400">
            <div class="animate-pulse">読み込み中...</div>
        </div>
    </div>
</div>

<script>
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    let location_label = make_location_label(&prefecture, &municipality);

    // 市区町村指定時は postings テーブルから直接計算（下限/上限分離）
    let use_postings = !municipality.is_empty();

    if use_postings {
        // postings テーブルから salary_min/salary_max の統計を取得
        let prows = match analytics::query_salary_from_postings(db, &job_type, &prefecture, &municipality) {
            Ok(r) => r,
            Err(e) => return Html(error_html(&e)),
        };
        if prows.is_empty() {
            return Html(empty_html("給与統計データがありません"));
        }

        let mut html = format!(
            r#"<div class="space-y-6">
            <h3 class="text-lg font-semibold text-white">💰 給与分布分析 — {}</h3>"#,
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
        return Html(html);
    }

    // 都道府県レベル: 事前計算テーブルからもpostingsからも下限/上限を取得
    let prows = analytics::query_salary_from_postings(db, &job_type, &prefecture, "").unwrap_or_default();

    // 従来テーブルも取得（フォールバック用）
    let rows = match analytics::query_salary_stats(db, &job_type, &prefecture) {
        Ok(r) => r,
        Err(e) => return Html(error_html(&e)),
    };

    // いずれかにデータがあればOK
    if rows.is_empty() && prows.is_empty() {
        return Html(empty_html("給与統計データがありません"));
    }

    let mut html = format!(
        r#"<div class="space-y-6">
        <h3 class="text-lg font-semibold text-white">💰 給与分布分析 — {}</h3>"#,
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
                        <th class="text-right p-1.5">P25</th>
                        <th class="text-right p-1.5">P75</th>
                        <th class="text-right p-1.5">P90</th>
                        <th class="text-right p-1.5">Gini</th>
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
                    <td class="p-1.5 text-right">{gini:.3}</td>
                </tr>"#,
                salary_type = escape_html(salary_type),
                emp_type = escape_html(emp_type),
                count = format_number(count),
                mean = format_yen(mean),
                median = format_yen(median),
                p25 = format_yen(p25),
                p75 = format_yen(p75),
                p90 = format_yen(p90),
                gini = gini,
            ));
        }

        html.push_str("</tbody></table></div></div>");
    }

    html.push_str("</div>");
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    // 市区町村指定時は postings テーブルから直接計算
    let rows = if !municipality.is_empty() {
        match analytics::query_facility_from_postings(db, &job_type, &prefecture, &municipality) {
            Ok(r) => r,
            Err(e) => return Html(error_html(&e)),
        }
    } else {
        match analytics::query_facility_concentration(db, &job_type, &prefecture) {
            Ok(r) => r,
            Err(e) => return Html(error_html(&e)),
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
    <h3 class="text-lg font-semibold text-white">🏢 法人集中度分析 — {location}</h3>

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
            <div class="text-xs text-gray-400">HHI指数</div>
            <div class="text-xl font-bold {hhi_color}">{hhi:.4}</div>
            <div class="text-xs {hhi_color}">{hhi_label}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">Zipf指数</div>
            <div class="text-xl font-bold text-purple-400">{zipf:.3}</div>
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

    // 全都道府県比較チャート（Zipf指数）
    let all_prefs = analytics::query_facility_all_prefectures(db, &job_type).unwrap_or_default();
    let pref_rows: Vec<_> = all_prefs.iter().filter(|r| {
        r.get("prefecture").and_then(|v| v.as_str()).unwrap_or("") != "全国"
    }).collect();

    if !pref_rows.is_empty() {
        html.push_str(r#"<div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">都道府県別 Zipf指数比較</h4>
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
            window.addEventListener('resize', function() {{ c1.resize(); }});
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
                    xAxis: {{ type: 'value', name: 'Zipf指数', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }} }},
                    yAxis: {{ type: 'category', data: [{labels}], axisLabel: {{ color: '#94a3b8', fontSize: 10 }}, inverse: true }},
                    series: [{{
                        type: 'bar',
                        data: [{values}],
                        itemStyle: {{ color: function(p) {{ var cs = [{colors}]; return cs[p.dataIndex]; }} }},
                        barMaxWidth: 12,
                        label: {{ show: false }}
                    }}]
                }});
                window.addEventListener('resize', function() {{ c.resize(); }});
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    // 市区町村指定時は postings テーブルから直接計算
    let rows = if !municipality.is_empty() {
        match analytics::query_employment_from_postings(db, &job_type, &prefecture, &municipality) {
            Ok(r) => r,
            Err(e) => return Html(error_html(&e)),
        }
    } else {
        match analytics::query_employment_diversity(db, &job_type, &prefecture) {
            Ok(r) => r,
            Err(e) => return Html(error_html(&e)),
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
    <h3 class="text-lg font-semibold text-white">📋 雇用形態多様性 — {location}</h3>

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
            <div class="text-xs text-gray-400">Shannon Entropy</div>
            <div class="text-xl font-bold text-purple-400">{entropy:.3}</div>
            <div class="text-xs text-gray-500">/ {max_entropy:.3}</div>
        </div>
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <div class="text-xs text-gray-400">均等度</div>
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

    // 全都道府県比較（Shannon entropy）
    let all_prefs = analytics::query_employment_all_prefectures(db, &job_type).unwrap_or_default();
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
                <h4 class="text-sm font-semibold text-gray-300 mb-2">都道府県別 Shannon Entropy</h4>
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
                window.addEventListener('resize', function() {{ c1.resize(); }});
            }}
            // Entropy 横棒グラフ
            var dom2 = document.getElementById('emp-entropy-chart');
            if (dom2) {{
                var c2 = echarts.init(dom2, 'dark');
                c2.setOption({{
                    backgroundColor: 'transparent',
                    tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }} }},
                    grid: {{ left: 70, right: 30, top: 10, bottom: 20 }},
                    xAxis: {{ type: 'value', name: 'Shannon Entropy', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }} }},
                    yAxis: {{ type: 'category', data: [{prefs}], axisLabel: {{ color: '#94a3b8', fontSize: 9 }}, inverse: true }},
                    series: [{{
                        type: 'bar',
                        data: [{entropy_data}],
                        barMaxWidth: 12,
                        itemStyle: {{ color: function(p) {{ var cs = [{entropy_colors}]; return cs[p.dataIndex]; }} }}
                    }}]
                }});
                window.addEventListener('resize', function() {{ c2.resize(); }});
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
            window.addEventListener('resize', function() {{ c.resize(); }});
        }}
    }})();
    </script>
"##,
        pie_data = pie_data.join(","),
    ));

    html.push_str("</div>");
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    let layer = params.layer.as_deref();
    // フォールバック付きで取得（都道府県データなし→全国にフォールバック）
    let (rows, is_fallback) = analytics::query_keywords_with_fallback(db, &job_type, &prefecture, layer, Some(50));

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
        <h3 class=\"text-lg font-semibold text-white\">🔑 キーワード分析 — {location}</h3>\
        {fallback_note}{municipality_note}\
        <div class=\"flex gap-2 mb-3\">\
            <button class=\"px-3 py-1 text-xs rounded {all_cls}\" hx-get=\"/api/analysis/keywords\" hx-target=\"{target}\" hx-swap=\"innerHTML\">全て</button>\
            <button class=\"px-3 py-1 text-xs rounded {uni_cls}\" hx-get=\"/api/analysis/keywords?layer=universal\" hx-target=\"{target}\" hx-swap=\"innerHTML\">🌐 業界共通</button>\
            <button class=\"px-3 py-1 text-xs rounded {jt_cls}\" hx-get=\"/api/analysis/keywords?layer=job_type\" hx-target=\"{target}\" hx-swap=\"innerHTML\">🏷️ 職種特有</button>\
            <button class=\"px-3 py-1 text-xs rounded {reg_cls}\" hx-get=\"/api/analysis/keywords?layer=regional\" hx-target=\"{target}\" hx-swap=\"innerHTML\">📍 地域特有</button>\
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
                window.addEventListener('resize', function() {{ c.resize(); }});
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    let min_lift = params.min_lift;
    // フォールバック付きで取得（都道府県データなし→全国にフォールバック）
    let (rows, is_fallback) = analytics::query_cooccurrence_with_fallback(db, &job_type, &prefecture, min_lift);

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
        <h3 class="text-lg font-semibold text-white">🔗 条件パッケージ共起分析 — {location}</h3>
        {fallback_note}{municipality_note}
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700 text-sm text-gray-300 space-y-2">
            <p><span class="text-cyan-400 font-semibold">関連強度 (Lift)</span>: 2つの条件が独立に出現する場合と比べて、何倍共起しやすいか。1.0=無相関、2.0以上で強い関連。</p>
            <p><span class="text-purple-400 font-semibold">相関度 (Phi)</span>: -1〜+1の統計的相関指標。0.3以上で実用的に意味のある関連性。</p>
            <p><span class="text-amber-400 font-semibold">Support%</span>: 全求人中でこの条件ペアが同時に出現する割合。</p>
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
            // バブルサイズ: support_pctを基準に可視化
            let size = (support * 3.0).max(4.0).min(40.0);
            scatter_data.push(format!(
                "[{:.2},{:.3},{:.1},'{}+{}']",
                lift, phi, size, flag_a, flag_b
            ));
        }

        html.push_str(&format!(r##"
        <div class="bg-navy-800 rounded-lg p-4 border border-slate-700">
            <h4 class="text-sm font-semibold text-gray-300 mb-2">共起バブルチャート (X=関連強度, Y=相関度, サイズ=Support)</h4>
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
                            return d[3] + '<br>Lift: ' + d[0].toFixed(2) + '<br>Phi: ' + d[1].toFixed(3) + '<br>Support: ' + (d[2]/3).toFixed(1) + '%';
                        }}
                    }},
                    grid: {{ left: 60, right: 30, top: 30, bottom: 50 }},
                    xAxis: {{
                        type: 'value', name: '関連強度 (Lift)', nameLocation: 'center', nameGap: 30,
                        axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                        splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                    }},
                    yAxis: {{
                        type: 'value', name: '相関度 (Phi)', nameLocation: 'center', nameGap: 40,
                        axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                        splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                    }},
                    series: [{{
                        type: 'scatter',
                        data: rawData,
                        symbolSize: function(d) {{ return d[2]; }},
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
                window.addEventListener('resize', function() {{ c.resize(); }});
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
                <th class="text-right p-1.5">共起数</th>
                <th class="text-right p-1.5">関連強度(Lift)</th>
                <th class="text-right p-1.5">相関度(Phi)</th>
                <th class="text-right p-1.5">Support%</th>
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    // 市区町村指定時はpostingsテーブルから直接計算してKPIに使う
    let muni_quality = if !municipality.is_empty() {
        analytics::query_quality_from_postings(db, &job_type, &prefecture, &municipality).unwrap_or_default()
    } else {
        Vec::new()
    };

    // 全国データで全体概要を取得（散布図/バーチャート用）
    let national = analytics::query_text_quality(db, &job_type, "").unwrap_or_default();

    let location_label = make_location_label(&prefecture, &municipality);

    let mut html = format!(
        r#"<div class="space-y-4">
        <h3 class="text-lg font-semibold text-white">📝 原稿品質分析 — {location}</h3>"#,
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
            <div class="text-xs text-gray-400">テキストEntropy</div>
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

        let muni_rows = analytics::query_quality_by_municipality(db, &job_type, &target_prefs)
            .unwrap_or_default();

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
                format!("近隣市区町村 Entropy比較（{} {} + 周辺）", escape_html(&prefecture), escape_html(&selected_muni_name))
            } else {
                format!("近隣市区町村 Entropy比較（{} + 隣接県）", escape_html(&prefecture))
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
                xAxis: {{ type: 'value', name: 'Entropy (mean)', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }} }},
                yAxis: {{ type: 'category', data: [{muni_names}], axisLabel: {{ color: '#94a3b8', fontSize: 9 }}, inverse: true }},
                series: [{{
                    type: 'bar',
                    data: [{entropy_data}],
                    barMaxWidth: 14,
                    itemStyle: {{ color: function(p) {{ var cs = [{entropy_colors}]; return cs[p.dataIndex]; }} }}
                }}]
            }});
            window.addEventListener('resize', function() {{ c1.resize(); }});
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
                    formatter: function(p) {{ return p.data[2] + '<br>漢字比率: ' + p.data[0].toFixed(1) + '%<br>Entropy: ' + p.data[1].toFixed(3) + '<br>求人数: ' + p.data[3]; }}
                }},
                grid: {{ left: 50, right: 20, top: 40, bottom: 50 }},
                xAxis: {{
                    type: 'value', name: '漢字比率 (%)', nameLocation: 'center', nameGap: 30,
                    min: minX, max: maxX,
                    axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                    splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                }},
                yAxis: {{
                    type: 'value', name: 'Entropy (mean)', nameLocation: 'center', nameGap: 35,
                    min: minY, max: maxY,
                    axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }},
                    splitLine: {{ lineStyle: {{ color: '#334155' }} }}
                }},
                series: [{scatter_series}]
            }});
            window.addEventListener('resize', function() {{ c2.resize(); }});
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
                    <th class="text-right p-1.5">Entropy</th>
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
        <h4 class="text-sm font-semibold text-gray-300 mb-2">都道府県別 Entropy概況</h4>
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
                xAxis: {{ type: 'value', name: 'Entropy (mean)', nameLocation: 'center', nameGap: 25, axisLabel: {{ color: '#94a3b8' }}, nameTextStyle: {{ color: '#94a3b8' }} }},
                yAxis: {{ type: 'category', data: [{prefs}], axisLabel: {{ color: '#94a3b8', fontSize: 9 }}, inverse: true }},
                series: [{{
                    type: 'bar',
                    data: [{entropy_data}],
                    barMaxWidth: 12,
                    itemStyle: {{ color: '#06b6d4' }}
                }}]
            }});
            window.addEventListener('resize', function() {{ c1.resize(); }});
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    let rows = match analytics::query_cluster_profiles(db, &job_type) {
        Ok(r) => r,
        Err(e) => return Html(error_html(&e)),
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
        <h3 class="text-lg font-semibold text-white">🎯 求人クラスタ分析 — {location}</h3>
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
            window.addEventListener('resize', function() {{ c1.resize(); }});
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
            window.addEventListener('resize', function() {{ c2.resize(); }});
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
            <div><span class="text-gray-500">Entropy:</span> <span class="text-white">{ent:.3}</span></div>
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
    let db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(error_html("DB未接続")),
    };

    // 全都道府県 x 全クラスタ取得（ヒートマップ用）
    let all_rows = match analytics::query_region_heatmap(db, &job_type, "", None) {
        Ok(r) => r,
        Err(e) => return Html(error_html(&e)),
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
    <h3 class="text-lg font-semibold text-white">🗺️ 地域×クラスタ分布 — {location}</h3>
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
            window.addEventListener('resize', function() {{ c.resize(); }});
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

    // テーブルも残す（フィルター対象のデータ）
    let display_rows = if !prefecture.is_empty() {
        match analytics::query_region_heatmap(db, &job_type, &prefecture, params.cluster_id) {
            Ok(r) => r,
            Err(_) => Vec::new(),
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

fn error_html(msg: &str) -> String {
    format!(
        r#"<div class="p-4 text-red-400 bg-red-900/20 rounded-lg border border-red-700">
            <p class="font-semibold">エラー</p>
            <p class="text-sm mt-1">{}</p>
        </div>"#,
        escape_html(msg)
    )
}

fn empty_html(msg: &str) -> String {
    format!(
        r#"<div class="p-8 text-center text-gray-500">
            <p>{}</p>
        </div>"#,
        escape_html(msg)
    )
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
            window.addEventListener('resize', function() {{ c.resize(); }});
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
            window.addEventListener('resize', function() {{ c.resize(); }});
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
