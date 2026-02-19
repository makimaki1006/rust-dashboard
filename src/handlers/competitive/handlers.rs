use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;
use crate::handlers::overview::{format_number, get_session_filters};
use super::analysis::{calc_salary_stats, fetch_analysis, fetch_analysis_filtered};
use super::fetch::{
    fetch_competitive, fetch_facility_types, fetch_facility_types_hierarchical,
    fetch_nearby_postings, fetch_postings, fetch_prefectures,
};
use super::render::{
    render_analysis_html, render_analysis_html_with_scope, render_competitive,
    render_posting_table, render_report_html,
};
use super::utils::escape_html;

/// タブ8: 競合調査（ローカルSQLiteから）
pub async fn tab_competitive(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, _prefecture, _municipality) = get_session_filters(&session).await;

    let cache_key = format!("competitive_{}", job_type);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_competitive(&state, &job_type);
    let pref_options = fetch_prefectures(&state, &job_type);
    let ftype_options = fetch_facility_types(&state, &job_type);
    let html = render_competitive(&job_type, &stats, &pref_options, &ftype_options);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

/// フィルタリクエストパラメータ
#[derive(Deserialize)]
pub struct CompFilterParams {
    pub prefecture: Option<String>,
    pub municipality: Option<String>,
    pub employment_type: Option<String>,
    pub facility_type: Option<String>,
    pub nearby: Option<bool>,
    pub radius_km: Option<f64>,
    pub page: Option<i64>,
}

/// フィルタ付き求人一覧API（HTMXパーシャル）
pub async fn comp_filter(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<CompFilterParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let db = match &state.local_db {
        Some(db) => db,
        None => return Html("<p class=\"text-red-400\">ローカルDBが利用できません</p>".to_string()),
    };

    let pref = params.prefecture.as_deref().unwrap_or("");
    let muni = params.municipality.as_deref().unwrap_or("");
    let emp = params.employment_type.as_deref().unwrap_or("");
    let ftype = params.facility_type.as_deref().unwrap_or("");
    let nearby = params.nearby.unwrap_or(false);
    let radius_km = params.radius_km.unwrap_or(10.0);
    let page = params.page.unwrap_or(1).max(1);
    let page_size: i64 = 50;

    if pref.is_empty() {
        return Html("<p class=\"text-slate-400\">都道府県を選択してください</p>".to_string());
    }

    let postings = if nearby && !muni.is_empty() {
        fetch_nearby_postings(db, &job_type, pref, muni, radius_km, emp, ftype)
    } else {
        fetch_postings(db, &job_type, pref, if muni.is_empty() { None } else { Some(muni) }, emp, ftype)
    };

    let total = postings.len() as i64;
    let total_pages = if total == 0 { 1 } else { (total - 1) / page_size + 1 };
    let start = ((page - 1) * page_size) as usize;
    let start = start.min(postings.len());
    let end = (start + page_size as usize).min(postings.len());
    let page_data = &postings[start..end];

    let salary_stats = calc_salary_stats(&postings);

    render_posting_table(
        &job_type, pref, muni, page_data, &salary_stats,
        page, total_pages, total, nearby, radius_km, emp, ftype,
    )
}

/// 市区町村一覧API
#[derive(Deserialize)]
pub struct MuniParams {
    pub prefecture: Option<String>,
}

pub async fn comp_municipalities(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<MuniParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let pref = params.prefecture.as_deref().unwrap_or("");
    if pref.is_empty() {
        return Html(r#"<option value="">市区町村</option>"#.to_string());
    }

    let db = match &state.local_db {
        Some(db) => db,
        None => return Html(r#"<option value="">市区町村</option>"#.to_string()),
    };

    let rows = db.query(
        "SELECT DISTINCT municipality FROM job_postings WHERE job_type = ? AND prefecture = ? ORDER BY municipality",
        &[&job_type as &dyn rusqlite::types::ToSql, &pref as &dyn rusqlite::types::ToSql],
    ).unwrap_or_default();

    let mut html = String::from(r#"<option value="">全て</option>"#);
    for row in &rows {
        let m = row.get("municipality")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !m.is_empty() {
            html.push_str(&format!(r#"<option value="{m}">{m}</option>"#));
        }
    }
    Html(html)
}

/// 施設形態一覧API（HTMXパーシャル: 2階層アコーディオンHTML）
/// 大カテゴリ→サブカテゴリのチェックボックスツリーを返す
pub async fn comp_facility_types(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<MuniParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;
    let pref = params.prefecture.as_deref().unwrap_or("");

    let hierarchy = fetch_facility_types_hierarchical(&state, &job_type, pref);

    if hierarchy.is_empty() {
        return Html(r#"<div class="text-sm text-slate-400 p-2">データがありません</div>"#.to_string());
    }

    let mut html = String::new();
    for (major, subs) in &hierarchy {
        let major_total: i64 = subs.iter().map(|(_, c)| c).sum();
        let color = super::utils::major_category_color(major);
        let short = super::utils::major_category_short_label(major);
        let major_escaped = escape_html(major);
        let major_id = major.replace("・", "_").replace("（", "").replace("）", "").replace("、", "");

        // 大カテゴリヘッダー（展開ボタン + チェックボックス）
        html.push_str(&format!(
            r#"<div class="ftype-group mb-1">
  <div class="flex items-center gap-1 py-1 px-1 hover:bg-slate-700 rounded cursor-pointer">
    <button type="button" onclick="toggleSubTypes('{mid}')" class="text-slate-400 hover:text-white w-4 flex-shrink-0">
      <svg id="arrow-{mid}" class="w-3 h-3 transform transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/></svg>
    </button>
    <label class="flex items-center gap-2 text-sm text-white flex-1 cursor-pointer">
      <input type="checkbox" class="ftype-major-cb rounded" value="{val}" data-group="{mid}" onchange="onMajorToggle(this)">
      <span class="inline-block w-2 h-2 rounded-full flex-shrink-0" style="background:{color}"></span>
      <span class="flex-1">{short}</span>
      <span class="text-xs text-slate-400 flex-shrink-0">{total}</span>
    </label>
  </div>
  <div id="sub-{mid}" class="hidden ml-6 border-l border-slate-700 pl-2">"#,
            mid = escape_html(&major_id),
            val = major_escaped,
            color = color,
            short = escape_html(short),
            total = format_number(major_total),
        ));

        // サブカテゴリ（上位15件 + その他）
        let max_show = 15;
        let mut shown_count = 0i64;
        for (i, (sub, cnt)) in subs.iter().enumerate() {
            if i < max_show {
                let filter_val = format!("{}::{}", major_escaped, escape_html(sub));
                html.push_str(&format!(
                    r#"<label class="flex items-center gap-2 text-xs text-slate-300 py-0.5 px-1 hover:bg-slate-700 rounded cursor-pointer">
      <input type="checkbox" class="ftype-sub-cb rounded" value="{val}" data-group="{mid}" onchange="onSubToggle(this)">
      <span class="flex-1 truncate">{label}</span>
      <span class="text-slate-500 flex-shrink-0">{cnt}</span>
    </label>"#,
                    val = filter_val,
                    mid = escape_html(&major_id),
                    label = escape_html(sub),
                    cnt = format_number(*cnt),
                ));
                shown_count += cnt;
            }
        }

        // 15件を超える場合は「その他」行を追加
        if subs.len() > max_show {
            let remaining = major_total - shown_count;
            let remaining_count = subs.len() - max_show;
            html.push_str(&format!(
                r#"<div class="text-xs text-slate-500 py-0.5 px-1 italic">…他{}種 ({})件</div>"#,
                remaining_count,
                format_number(remaining),
            ));
        }

        html.push_str("</div></div>");
    }

    Html(html)
}

/// 求人データ分析API（HTMXパーシャル）
pub async fn comp_analysis(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let db = match &state.local_db {
        Some(db) => db,
        None => return Html("<p class=\"text-red-400\">ローカルDBが利用できません</p>".to_string()),
    };

    let analysis = fetch_analysis(db, &job_type);
    Html(render_analysis_html(&job_type, &analysis))
}

/// 都道府県指定の分析API
#[derive(Deserialize)]
pub struct AnalysisParams {
    pub prefecture: Option<String>,
    pub municipality: Option<String>,
}

pub async fn comp_analysis_filtered(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<AnalysisParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let db = match &state.local_db {
        Some(db) => db,
        None => return Html("<p class=\"text-red-400\">ローカルDBが利用できません</p>".to_string()),
    };

    let pref = params.prefecture.as_deref().unwrap_or("");
    let muni = params.municipality.as_deref().unwrap_or("");
    let analysis = fetch_analysis_filtered(db, &job_type, pref, muni);
    let scope_label = if !muni.is_empty() {
        format!("{} {}", pref, muni)
    } else if !pref.is_empty() {
        pref.to_string()
    } else {
        "全国".to_string()
    };
    Html(render_analysis_html_with_scope(&job_type, &scope_label, &analysis))
}

/// HTMLレポート生成API
pub async fn comp_report(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<CompFilterParams>,
) -> Html<String> {
    let (job_type, _prefecture, _municipality) = get_session_filters(&session).await;

    let db = match &state.local_db {
        Some(db) => db,
        None => return Html("<p>ローカルDBが利用できません</p>".to_string()),
    };

    let pref = params.prefecture.as_deref().unwrap_or("");
    let muni = params.municipality.as_deref().unwrap_or("");
    let emp = params.employment_type.as_deref().unwrap_or("");
    let ftype = params.facility_type.as_deref().unwrap_or("");
    let nearby = params.nearby.unwrap_or(false);
    let radius_km = params.radius_km.unwrap_or(10.0);

    if pref.is_empty() {
        return Html("<p>都道府県を選択してください</p>".to_string());
    }

    let postings = if nearby && !muni.is_empty() {
        fetch_nearby_postings(db, &job_type, pref, muni, radius_km, emp, ftype)
    } else {
        fetch_postings(db, &job_type, pref, if muni.is_empty() { None } else { Some(muni) }, emp, ftype)
    };

    let stats = calc_salary_stats(&postings);
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    render_report_html(&job_type, pref, muni, emp, &postings, &stats, &today, nearby, radius_km)
}
