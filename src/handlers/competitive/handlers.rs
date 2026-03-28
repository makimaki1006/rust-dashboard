use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;
use crate::handlers::overview::{format_number, get_session_filters};
use crate::handlers::external::{self, ext_i64};
use super::analysis::{calc_salary_stats, fetch_analysis, fetch_analysis_filtered};
use super::fetch::{
    fetch_competitive, fetch_facility_types, fetch_facility_types_hierarchical,
    fetch_nearby_postings, fetch_postings, fetch_prefectures, fetch_service_types,
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

    let state_ref = state.clone();
    let jt = job_type.clone();
    let (stats, pref_options, ftype_options, stype_options) = match tokio::task::spawn_blocking(move || {
        let stats = fetch_competitive(&state_ref, &jt);
        let pref_options = fetch_prefectures(&state_ref, &jt);
        let ftype_options = fetch_facility_types(&state_ref, &jt);
        let stype_options = fetch_service_types(&state_ref, &jt, "");
        (stats, pref_options, ftype_options, stype_options)
    }).await {
        Ok(result) => result,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html("<p class=\"text-red-400\">データ取得エラー</p>".to_string());
        }
    };
    let html = render_competitive(&job_type, &stats, &pref_options, &ftype_options, &stype_options);
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
    pub service_type: Option<String>,
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
    let stype = params.service_type.as_deref().unwrap_or("");
    let nearby = params.nearby.unwrap_or(false);
    let radius_km = params.radius_km.unwrap_or(10.0);
    let page = params.page.unwrap_or(1).max(1);
    let page_size: i64 = 50;

    if pref.is_empty() {
        return Html("<p class=\"text-slate-400\">都道府県を選択してください</p>".to_string());
    }

    let db_clone = db.clone();
    let jt = job_type.clone();
    let pref_owned = pref.to_string();
    let muni_owned = muni.to_string();
    let emp_owned = emp.to_string();
    let ftype_owned = ftype.to_string();
    let stype_owned = stype.to_string();
    let postings = match tokio::task::spawn_blocking(move || {
        if nearby && !muni_owned.is_empty() {
            // 近隣検索: 複数市区町村の場合は最初の1つを中心座標に使用
            let first_muni: String = muni_owned.split(',')
                .map(|s| s.trim())
                .find(|s| !s.is_empty())
                .unwrap_or("")
                .to_string();
            fetch_nearby_postings(&db_clone, &jt, &pref_owned, &first_muni, radius_km, &emp_owned, &ftype_owned, &stype_owned)
        } else {
            fetch_postings(&db_clone, &jt, &pref_owned, if muni_owned.is_empty() { None } else { Some(&muni_owned) }, &emp_owned, &ftype_owned, &stype_owned)
        }
    }).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html("<p class=\"text-red-400\">データ取得エラー</p>".to_string());
        }
    };

    let total = postings.len() as i64;
    let total_pages = if total == 0 { 1 } else { (total - 1) / page_size + 1 };
    let start = ((page - 1) * page_size) as usize;
    let start = start.min(postings.len());
    let end = (start + page_size as usize).min(postings.len());
    let page_data = &postings[start..end];

    let salary_stats = calc_salary_stats(&postings);

    // C-1: HW賃金比較コンテキスト
    let hw_context = build_hw_salary_context(&state, pref, emp).await;

    let mut result = render_posting_table(
        &job_type, pref, muni, page_data, &salary_stats,
        page, total_pages, total, nearby, radius_km, emp, ftype, stype,
    );
    // HW賃金比較をテーブルの後に追加
    if !hw_context.is_empty() {
        let inner = result.0;
        result = Html(format!("{}{}", inner, hw_context));
    }
    result
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

    let rows = db.query_owned(
        "SELECT DISTINCT municipality FROM job_postings WHERE job_type = ? AND prefecture = ? ORDER BY municipality".to_string(),
        vec![job_type.clone(), pref.to_string()],
    ).await.unwrap_or_default();

    let mut html = String::from(r#"<option value="">全て</option>"#);
    for row in &rows {
        let m = row.get("municipality")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !m.is_empty() {
            html.push_str(&format!(r#"<option value="{val}">{label}</option>"#, val = super::escape_html(m), label = super::escape_html(m)));
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

    let state_ref = state.clone();
    let jt = job_type.clone();
    let pref_owned = pref.to_string();
    let hierarchy = match tokio::task::spawn_blocking(move || {
        fetch_facility_types_hierarchical(&state_ref, &jt, &pref_owned)
    }).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html(r#"<div class="text-sm text-slate-400 p-2">データ取得エラー</div>"#.to_string());
        }
    };

    if hierarchy.is_empty() {
        return Html(crate::handlers::render_empty_state(
            "施設形態データなし",
            "選択された条件の施設形態データがありません",
        ));
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

/// 事業形態一覧API（都道府県変更時にHTMXで取得）
pub async fn comp_service_types(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<MuniParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;
    let pref = params.prefecture.as_deref().unwrap_or("");

    let state_ref = state.clone();
    let jt = job_type.clone();
    let pref_owned = pref.to_string();
    let stypes = match tokio::task::spawn_blocking(move || {
        fetch_service_types(&state_ref, &jt, &pref_owned)
    }).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html(r#"<option value="">全て</option>"#.to_string());
        }
    };

    let mut html = String::from(r#"<option value="">全て</option>"#);
    for (cat, cnt) in &stypes {
        html.push_str(&format!(
            r#"<option value="{}">{} ({})</option>"#,
            escape_html(cat), escape_html(cat), format_number(*cnt)
        ));
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

    let db_clone = db.clone();
    let jt = job_type.clone();
    let analysis = match tokio::task::spawn_blocking(move || {
        fetch_analysis(&db_clone, &jt)
    }).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html("<p class=\"text-red-400\">分析データ取得エラー</p>".to_string());
        }
    };
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
    let db_clone = db.clone();
    let jt = job_type.clone();
    let pref_owned = pref.to_string();
    let muni_owned = muni.to_string();
    let analysis = match tokio::task::spawn_blocking(move || {
        fetch_analysis_filtered(&db_clone, &jt, &pref_owned, &muni_owned)
    }).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html("<p class=\"text-red-400\">分析データ取得エラー</p>".to_string());
        }
    };
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
    let stype = params.service_type.as_deref().unwrap_or("");
    let nearby = params.nearby.unwrap_or(false);
    let radius_km = params.radius_km.unwrap_or(10.0);

    if pref.is_empty() {
        return Html("<p>都道府県を選択してください</p>".to_string());
    }

    let db_clone = db.clone();
    let jt = job_type.clone();
    let pref_owned = pref.to_string();
    let muni_owned = muni.to_string();
    let emp_owned = emp.to_string();
    let ftype_owned = ftype.to_string();
    let stype_owned = stype.to_string();
    let postings = match tokio::task::spawn_blocking(move || {
        if nearby && !muni_owned.is_empty() {
            // 近隣検索: 複数市区町村の場合は最初の1つを中心座標に使用
            let first_muni: String = muni_owned.split(',')
                .map(|s| s.trim())
                .find(|s| !s.is_empty())
                .unwrap_or("")
                .to_string();
            fetch_nearby_postings(&db_clone, &jt, &pref_owned, &first_muni, radius_km, &emp_owned, &ftype_owned, &stype_owned)
        } else {
            fetch_postings(&db_clone, &jt, &pref_owned, if muni_owned.is_empty() { None } else { Some(&muni_owned) }, &emp_owned, &ftype_owned, &stype_owned)
        }
    }).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html("<p>データ取得エラー</p>".to_string());
        }
    };

    let stats = calc_salary_stats(&postings);
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    // C-1: HW賃金比較コンテキスト
    let hw_salary_context = build_hw_salary_context(&state, pref, emp).await;

    render_report_html(&job_type, pref, muni, emp, &postings, &stats, &today, nearby, radius_km, &hw_salary_context)
}

/// C-1: HW賃金比較コンテキスト生成
async fn build_hw_salary_context(state: &AppState, prefecture: &str, emp_filter: &str) -> String {
    if prefecture.is_empty() {
        return String::new();
    }

    let hw_salary = external::fetch_hw_salary_latest(state, prefecture).await;
    if hw_salary.is_empty() {
        return String::new();
    }

    let mut rows = String::new();
    let emp_order = ["正社員", "パート"];
    for target_emp in &emp_order {
        // 雇用形態フィルタがある場合はマッチするもののみ
        if !emp_filter.is_empty() && emp_filter != "全て" && !emp_filter.contains(target_emp) {
            continue;
        }
        if let Some(row) = hw_salary.iter().find(|r| external::ext_str(r, "emp_group") == *target_emp) {
            let avg_min = ext_i64(row, "avg_min");
            let avg_max = ext_i64(row, "avg_max");
            let count = ext_i64(row, "count");
            if avg_min > 0 {
                let is_hourly = *target_emp == "パート";
                let salary_text = if is_hourly {
                    format!("¥{} 〜 ¥{}/h", format_number(avg_min), format_number(avg_max))
                } else if avg_max > avg_min {
                    format!("¥{} 〜 ¥{}", format_number(avg_min), format_number(avg_max))
                } else {
                    format!("¥{}", format_number(avg_min))
                };
                rows.push_str(&format!(
                    r#"<tr class="border-b border-slate-700/50">
                        <td class="py-1.5 text-sm text-slate-300">{emp}</td>
                        <td class="py-1.5 text-sm text-right text-blue-400">{salary}</td>
                        <td class="py-1.5 text-xs text-right text-slate-500">{count}件</td>
                    </tr>"#,
                    emp = target_emp, salary = salary_text, count = format_number(count),
                ));
            }
        }
    }

    if rows.is_empty() {
        return String::new();
    }

    // C-2: 最低賃金比を計算
    let mut min_wage_note = String::new();
    if let Some(ps) = external::fetch_prefecture_macro(state, prefecture).await {
        let mw = external::ext_f64(&ps, "min_wage");
        if mw > 0.0 {
            // 正社員: 月給÷160h÷最低賃金
            if let Some(reg) = hw_salary.iter().find(|r| external::ext_str(r, "emp_group") == "正社員") {
                let avg_min = ext_i64(reg, "avg_min");
                if avg_min > 0 {
                    let hourly = avg_min as f64 / 160.0;
                    let ratio = hourly / mw;
                    let color = if ratio >= 1.5 { "#22c55e" } else if ratio >= 1.2 { "#f59e0b" } else { "#ef4444" };
                    min_wage_note.push_str(&format!(
                        r#"<p class="text-xs mt-1">正社員時給換算: <span style="color:{color}">最低賃金の{ratio:.2}倍</span>（¥{hourly:.0}/h vs 最低賃金¥{mw:.0}/h）</p>"#,
                        color=color, ratio=ratio, hourly=hourly, mw=mw,
                    ));
                }
            }
            // パート: 時給÷最低賃金
            if let Some(pt) = hw_salary.iter().find(|r| external::ext_str(r, "emp_group") == "パート") {
                let avg_min = ext_i64(pt, "avg_min");
                if avg_min > 0 {
                    let ratio = avg_min as f64 / mw;
                    let color = if ratio >= 1.5 { "#22c55e" } else if ratio >= 1.2 { "#f59e0b" } else { "#ef4444" };
                    min_wage_note.push_str(&format!(
                        r#"<p class="text-xs">パート時給: <span style="color:{color}">最低賃金の{ratio:.2}倍</span>（¥{wage} vs ¥{mw:.0}）</p>"#,
                        color=color, ratio=ratio, wage=format_number(avg_min), mw=mw,
                    ));
                }
            }
        }
    }

    // C-4: 欠員補充率
    let mut vacancy_note = String::new();
    let vacancy_data = external::fetch_hw_vacancy_trend(state, prefecture).await;
    if let Some(latest) = vacancy_data.last() {
        let vr = external::ext_f64(latest, "avg_vacancy_rate");
        if vr > 0.0 {
            let color = if vr > 50.0 { "#ef4444" } else if vr > 30.0 { "#f59e0b" } else { "#22c55e" };
            vacancy_note = format!(
                r#"<p class="text-xs mt-1">欠員補充率: <span style="color:{color}">{vr:.1}%</span>（HW求人のうち離職による欠員補充の割合）</p>"#,
                color=color, vr=vr,
            );
        }
    }

    format!(
        r#"<div class="stat-card mt-4">
    <h3 class="text-sm text-slate-400 mb-2">&#x1f4b0; HW求人の賃金水準（{pref}）</h3>
    <p class="text-xs text-slate-500 mb-2">ハローワーク掲載求人（医療福祉全体）の平均賃金。※職種固有ではなく産業全体の参考値</p>
    <table class="w-full">
        <thead>
            <tr class="border-b border-slate-600">
                <th class="py-1 text-xs text-left text-slate-500">雇用形態</th>
                <th class="py-1 text-xs text-right text-slate-500">HW平均賃金</th>
                <th class="py-1 text-xs text-right text-slate-500">HW求人数</th>
            </tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>
    {min_wage_note}
    {vacancy_note}
    <p class="text-xs text-slate-500 mt-2">※上記より高い給与を提示すれば、HW掲載求人より有利に採用できる可能性があります</p>
</div>"#,
        pref = super::escape_html(prefecture),
        rows = rows,
        min_wage_note = min_wage_note,
        vacancy_note = vacancy_note,
    )
}
