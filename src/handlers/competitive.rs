use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{format_number, get_session_filters};

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
    let html = render_competitive(&job_type, &stats, &pref_options);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

/// フィルタリクエストパラメータ
#[derive(Deserialize)]
pub struct CompFilterParams {
    pub prefecture: Option<String>,
    pub municipality: Option<String>,
    pub employment_type: Option<String>,
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
    let nearby = params.nearby.unwrap_or(false);
    let radius_km = params.radius_km.unwrap_or(10.0);
    let page = params.page.unwrap_or(1).max(1);
    let page_size: i64 = 50;

    if pref.is_empty() {
        return Html("<p class=\"text-slate-400\">都道府県を選択してください</p>".to_string());
    }

    // 近辺検索 or 通常検索
    let postings = if nearby && !muni.is_empty() {
        fetch_nearby_postings(db, &job_type, pref, muni, radius_km, emp)
    } else {
        fetch_postings(db, &job_type, pref, if muni.is_empty() { None } else { Some(muni) }, emp)
    };

    let total = postings.len() as i64;
    let total_pages = if total == 0 { 1 } else { (total - 1) / page_size + 1 };
    let start = ((page - 1) * page_size) as usize;
    let end = (start + page_size as usize).min(postings.len());
    let page_data = &postings[start..end];

    // 統計計算
    let salary_stats = calc_salary_stats(&postings);

    render_posting_table(
        &job_type, pref, muni, page_data, &salary_stats,
        page, total_pages, total, nearby, radius_km, emp,
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
    let nearby = params.nearby.unwrap_or(false);
    let radius_km = params.radius_km.unwrap_or(10.0);

    if pref.is_empty() {
        return Html("<p>都道府県を選択してください</p>".to_string());
    }

    // 全件取得（ページネーションなし）
    let postings = if nearby && !muni.is_empty() {
        fetch_nearby_postings(db, &job_type, pref, muni, radius_km, emp)
    } else {
        fetch_postings(db, &job_type, pref, if muni.is_empty() { None } else { Some(muni) }, emp)
    };

    let stats = calc_salary_stats(&postings);
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    render_report_html(&job_type, pref, muni, &postings, &stats, &today, nearby, radius_km)
}

// --- 内部データ型 ---

struct CompStats {
    total_postings: i64,
    total_facilities: i64,
    pref_ranking: Vec<(String, i64)>,
}

impl Default for CompStats {
    fn default() -> Self {
        Self {
            total_postings: 0,
            total_facilities: 0,
            pref_ranking: Vec::new(),
        }
    }
}

struct PostingRow {
    facility_name: String,
    facility_type: String,
    prefecture: String,
    municipality: String,
    employment_type: String,
    salary_type: String,
    salary_min: i64,
    salary_max: i64,
    base_salary: i64,
    bonus: String,
    annual_holidays: i64,
    distance_km: Option<f64>,
}

struct SalaryStats {
    count: i64,
    salary_min_median: String,
    salary_min_avg: String,
    salary_min_mode: String,
    salary_max_median: String,
    salary_max_avg: String,
    salary_max_mode: String,
    bonus_rate: String,
    avg_holidays: String,
    has_data: bool,
}

// --- データ取得関数 ---

fn fetch_competitive(state: &AppState, job_type: &str) -> CompStats {
    let db = match &state.local_db {
        Some(db) => db,
        None => return CompStats::default(),
    };

    let mut stats = CompStats::default();

    stats.total_postings = db
        .query_scalar::<i64>(
            "SELECT COUNT(*) FROM job_postings WHERE job_type = ?",
            &[&job_type as &dyn rusqlite::types::ToSql],
        )
        .unwrap_or(0);

    stats.total_facilities = db
        .query_scalar::<i64>(
            "SELECT COUNT(DISTINCT facility_name) FROM job_postings WHERE job_type = ?",
            &[&job_type as &dyn rusqlite::types::ToSql],
        )
        .unwrap_or(0);

    if let Ok(rows) = db.query(
        "SELECT prefecture, COUNT(*) as cnt FROM job_postings WHERE job_type = ? GROUP BY prefecture ORDER BY cnt DESC LIMIT 15",
        &[&job_type as &dyn rusqlite::types::ToSql],
    ) {
        for row in &rows {
            let pref = row.get("prefecture")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let cnt = row.get("cnt")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if !pref.is_empty() {
                stats.pref_ranking.push((pref, cnt));
            }
        }
    }

    stats
}

fn fetch_prefectures(state: &AppState, job_type: &str) -> Vec<String> {
    let db = match &state.local_db {
        Some(db) => db,
        None => return Vec::new(),
    };

    let rows = db.query(
        "SELECT DISTINCT prefecture FROM job_postings WHERE job_type = ? ORDER BY prefecture",
        &[&job_type as &dyn rusqlite::types::ToSql],
    ).unwrap_or_default();

    rows.iter()
        .filter_map(|r| r.get("prefecture").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect()
}

fn fetch_postings(
    db: &crate::db::local_sqlite::LocalDb,
    job_type: &str,
    pref: &str,
    muni: Option<&str>,
    emp: &str,
) -> Vec<PostingRow> {
    let mut sql = String::from(
        "SELECT facility_name, facility_type, prefecture, municipality, employment_type, \
         salary_type, salary_min, salary_max, base_salary, bonus, annual_holidays \
         FROM job_postings WHERE job_type = ? AND prefecture = ?"
    );
    let mut param_values: Vec<String> = vec![job_type.to_string(), pref.to_string()];

    if let Some(m) = muni {
        if !m.is_empty() {
            sql.push_str(" AND municipality = ?");
            param_values.push(m.to_string());
        }
    }
    if !emp.is_empty() && emp != "全て" {
        sql.push_str(" AND employment_type = ?");
        param_values.push(emp.to_string());
    }
    sql.push_str(" ORDER BY salary_min DESC");

    let params: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match db.query(&sql, &params) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Posting query failed: {e}");
            return Vec::new();
        }
    };

    rows.iter().map(|r| row_to_posting(r, None)).collect()
}

fn fetch_nearby_postings(
    db: &crate::db::local_sqlite::LocalDb,
    job_type: &str,
    pref: &str,
    muni: &str,
    radius_km: f64,
    emp: &str,
) -> Vec<PostingRow> {
    // 中心座標を取得
    let center = match get_geocode(db, pref, muni) {
        Some(c) => c,
        None => return Vec::new(),
    };

    // Bounding box計算
    let lat_delta = radius_km / 111.0;
    let lng_delta = radius_km / (111.0 * center.0.to_radians().cos());
    let lat_min = center.0 - lat_delta;
    let lat_max = center.0 + lat_delta;
    let lng_min = center.1 - lng_delta;
    let lng_max = center.1 + lng_delta;

    let mut sql = String::from(
        "SELECT facility_name, facility_type, prefecture, municipality, employment_type, \
         salary_type, salary_min, salary_max, base_salary, bonus, annual_holidays, \
         latitude, longitude \
         FROM job_postings WHERE job_type = ? \
         AND latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?"
    );
    let mut param_values: Vec<String> = vec![
        job_type.to_string(),
        lat_min.to_string(), lat_max.to_string(),
        lng_min.to_string(), lng_max.to_string(),
    ];

    if !emp.is_empty() && emp != "全て" {
        sql.push_str(" AND employment_type = ?");
        param_values.push(emp.to_string());
    }
    sql.push_str(" ORDER BY salary_min DESC");

    let params: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match db.query(&sql, &params) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Nearby query failed: {e}");
            return Vec::new();
        }
    };

    // Haversine距離でフィルタ
    rows.iter()
        .filter_map(|r| {
            let lat = r.get("latitude").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let lng = r.get("longitude").and_then(|v| v.as_f64()).unwrap_or(0.0);
            if lat == 0.0 || lng == 0.0 {
                return None;
            }
            let dist = haversine(center.0, center.1, lat, lng);
            if dist <= radius_km {
                Some(row_to_posting(r, Some(dist)))
            } else {
                None
            }
        })
        .collect()
}

fn get_geocode(db: &crate::db::local_sqlite::LocalDb, pref: &str, muni: &str) -> Option<(f64, f64)> {
    let rows = db.query(
        "SELECT latitude, longitude FROM municipality_geocode WHERE prefecture = ? AND municipality = ?",
        &[&pref as &dyn rusqlite::types::ToSql, &muni as &dyn rusqlite::types::ToSql],
    ).ok()?;

    let row = rows.first()?;
    let lat = row.get("latitude").and_then(|v| v.as_f64())?;
    let lng = row.get("longitude").and_then(|v| v.as_f64())?;
    Some((lat, lng))
}

/// Haversine公式で2点間の距離を計算（km）
fn haversine(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let r = 6371.0; // 地球半径(km)
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}

fn row_to_posting(r: &std::collections::HashMap<String, Value>, distance: Option<f64>) -> PostingRow {
    PostingRow {
        facility_name: r.get("facility_name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        facility_type: r.get("facility_type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        prefecture: r.get("prefecture").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        municipality: r.get("municipality").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        employment_type: r.get("employment_type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        salary_type: r.get("salary_type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        salary_min: r.get("salary_min").and_then(|v| v.as_i64()).unwrap_or(0),
        salary_max: r.get("salary_max").and_then(|v| v.as_i64()).unwrap_or(0),
        base_salary: r.get("base_salary").and_then(|v| v.as_i64()).unwrap_or(0),
        bonus: r.get("bonus").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        annual_holidays: r.get("annual_holidays").and_then(|v| v.as_i64()).unwrap_or(0),
        distance_km: distance,
    }
}

// --- 統計計算 ---

fn calc_salary_stats(postings: &[PostingRow]) -> SalaryStats {
    if postings.is_empty() {
        return SalaryStats {
            count: 0,
            salary_min_median: "-".to_string(),
            salary_min_avg: "-".to_string(),
            salary_min_mode: "-".to_string(),
            salary_max_median: "-".to_string(),
            salary_max_avg: "-".to_string(),
            salary_max_mode: "-".to_string(),
            bonus_rate: "-".to_string(),
            avg_holidays: "-".to_string(),
            has_data: false,
        };
    }

    // 5万以上の月給のみ（時給データ除外）
    let min_vals: Vec<i64> = postings.iter()
        .filter(|p| p.salary_min >= 50000)
        .map(|p| p.salary_min)
        .collect();
    let max_vals: Vec<i64> = postings.iter()
        .filter(|p| p.salary_max >= 50000)
        .map(|p| p.salary_max)
        .collect();

    // 賞与率
    let bonus_count = postings.iter().filter(|p| !p.bonus.is_empty()).count();
    let bonus_rate = if !postings.is_empty() {
        format!("{:.0}%", bonus_count as f64 / postings.len() as f64 * 100.0)
    } else {
        "-".to_string()
    };

    // 年間休日（80〜200の有効値のみ）
    let holidays: Vec<i64> = postings.iter()
        .filter(|p| p.annual_holidays >= 80 && p.annual_holidays <= 200)
        .map(|p| p.annual_holidays)
        .collect();
    let avg_holidays = if !holidays.is_empty() {
        format!("{}日", holidays.iter().sum::<i64>() / holidays.len() as i64)
    } else {
        "-".to_string()
    };

    SalaryStats {
        count: postings.len() as i64,
        salary_min_median: calc_median_str(&min_vals),
        salary_min_avg: calc_avg_str(&min_vals),
        salary_min_mode: calc_mode_str(&min_vals),
        salary_max_median: calc_median_str(&max_vals),
        salary_max_avg: calc_avg_str(&max_vals),
        salary_max_mode: calc_mode_str(&max_vals),
        bonus_rate,
        avg_holidays,
        has_data: !min_vals.is_empty(),
    }
}

fn calc_median_str(vals: &[i64]) -> String {
    if vals.is_empty() { return "-".to_string(); }
    let mut sorted = vals.to_vec();
    sorted.sort();
    let mid = sorted.len() / 2;
    let median = if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2
    } else {
        sorted[mid]
    };
    format!("{}円", format_number(median))
}

fn calc_avg_str(vals: &[i64]) -> String {
    if vals.is_empty() { return "-".to_string(); }
    let avg = vals.iter().sum::<i64>() / vals.len() as i64;
    format!("{}円", format_number(avg))
}

fn calc_mode_str(vals: &[i64]) -> String {
    if vals.is_empty() { return "-".to_string(); }
    // 1万円単位に丸めて最頻値
    let mut freq: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for v in vals {
        let rounded = (v / 10000) * 10000;
        *freq.entry(rounded).or_insert(0) += 1;
    }
    let mode = freq.into_iter().max_by_key(|(_, c)| *c).map(|(v, _)| v).unwrap_or(0);
    format!("{}円", format_number(mode))
}

// --- HTMLレンダリング ---

fn render_competitive(job_type: &str, stats: &CompStats, pref_options: &[String]) -> String {
    let pref_labels: Vec<String> = stats.pref_ranking.iter().map(|(p, _)| format!("\"{}\"", p)).collect();
    let pref_values: Vec<String> = stats.pref_ranking.iter().map(|(_, v)| v.to_string()).collect();

    let pref_rows: String = stats
        .pref_ranking
        .iter()
        .enumerate()
        .map(|(i, (name, cnt))| {
            format!(
                r#"<tr><td class="text-center">{}</td><td>{}</td><td class="text-right">{}</td></tr>"#,
                i + 1, name, format_number(*cnt)
            )
        })
        .collect();

    let pref_option_html: String = pref_options
        .iter()
        .map(|p| format!(r#"<option value="{p}">{p}</option>"#))
        .collect::<Vec<_>>()
        .join("\n");

    include_str!("../../templates/tabs/competitive.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{TOTAL_POSTINGS}}", &format_number(stats.total_postings))
        .replace("{{TOTAL_FACILITIES}}", &format_number(stats.total_facilities))
        .replace("{{PREF_LABELS}}", &format!("[{}]", pref_labels.join(",")))
        .replace("{{PREF_VALUES}}", &format!("[{}]", pref_values.join(",")))
        .replace("{{PREF_ROWS}}", &pref_rows)
        .replace("{{PREF_OPTIONS}}", &pref_option_html)
}

/// 求人一覧テーブル（HTMXパーシャル）
fn render_posting_table(
    _job_type: &str,
    pref: &str,
    muni: &str,
    postings: &[PostingRow],
    stats: &SalaryStats,
    page: i64,
    total_pages: i64,
    total: i64,
    nearby: bool,
    radius_km: f64,
    emp: &str,
) -> Html<String> {
    let show_distance = nearby && postings.iter().any(|p| p.distance_km.is_some());

    let mut html = String::new();

    // 統計サマリー
    let nearby_label = if nearby { format!("（半径{}km）", radius_km) } else { String::new() };
    if stats.has_data {
        html.push_str(&format!(
            r#"<div class="stat-card mb-4">
                <h3 class="text-sm text-slate-400 mb-2">給与統計（{} {}{} / {}件）</h3>
                <div class="overflow-x-auto">
                <table class="data-table text-xs">
                    <thead><tr><th></th><th class="text-right">月給下限</th><th class="text-right">月給上限</th></tr></thead>
                    <tbody>
                        <tr><td class="text-slate-300">最頻値（1万円単位）</td><td class="text-right">{}</td><td class="text-right">{}</td></tr>
                        <tr><td class="text-slate-300">中央値</td><td class="text-right">{}</td><td class="text-right">{}</td></tr>
                        <tr><td class="text-slate-300">平均値</td><td class="text-right">{}</td><td class="text-right">{}</td></tr>
                    </tbody>
                </table>
                </div>
                <div class="mt-2 text-xs text-slate-400">
                    賞与あり率: {} ｜ 平均年間休日: {}
                </div>
            </div>"#,
            pref, muni, &nearby_label,
            total,
            stats.salary_min_mode, stats.salary_max_mode,
            stats.salary_min_median, stats.salary_max_median,
            stats.salary_min_avg, stats.salary_max_avg,
            stats.bonus_rate, stats.avg_holidays,
        ));
    }

    // ページ情報
    html.push_str(&format!(
        r#"<div class="flex justify-between items-center mb-2">
            <span class="text-sm text-slate-400">全{}件中 {}〜{}件</span>
            <a href="/api/report?prefecture={}&municipality={}&employment_type={}&nearby={}&radius_km={}"
               target="_blank"
               class="px-3 py-1.5 bg-amber-600 hover:bg-amber-500 text-white text-sm rounded-lg transition">
               HTMLレポート出力
            </a>
        </div>"#,
        total,
        (page - 1) * 50 + 1,
        ((page - 1) * 50 + postings.len() as i64).min(total),
        urlencoding::encode(pref),
        urlencoding::encode(muni),
        urlencoding::encode(emp),
        nearby,
        radius_km,
    ));

    // テーブル
    html.push_str(r#"<div class="overflow-x-auto"><table class="data-table text-xs">"#);
    html.push_str("<thead><tr>");
    html.push_str(r#"<th class="text-center" style="width:30px">#</th>"#);
    html.push_str("<th>法人・施設名</th>");
    html.push_str("<th>施設形態</th>");
    html.push_str("<th>エリア</th>");
    html.push_str("<th>雇用形態</th>");
    html.push_str("<th>給与区分</th>");
    html.push_str(r#"<th class="text-right">月給下限</th>"#);
    html.push_str(r#"<th class="text-right">月給上限</th>"#);
    html.push_str(r#"<th class="text-right">基本給</th>"#);
    html.push_str("<th>賞与</th>");
    html.push_str(r#"<th class="text-right">年間休日</th>"#);
    if show_distance {
        html.push_str(r#"<th class="text-right">距離</th>"#);
    }
    html.push_str("</tr></thead><tbody>");

    let start_num = (page - 1) * 50;
    for (i, p) in postings.iter().enumerate() {
        let fname = truncate_str(&escape_html(&p.facility_name), 40);
        let ftype = truncate_str(&escape_html(&p.facility_type), 30);
        let area = format!("{} {}", p.prefecture, p.municipality);
        let sal_min = if p.salary_min > 0 { format_number(p.salary_min) } else { "-".to_string() };
        let sal_max = if p.salary_max > 0 { format_number(p.salary_max) } else { "-".to_string() };
        let base = if p.base_salary > 0 { format_number(p.base_salary) } else { "-".to_string() };
        let holidays = if p.annual_holidays > 0 { p.annual_holidays.to_string() } else { "-".to_string() };
        let bonus = truncate_str(&escape_html(&p.bonus), 20);

        html.push_str(&format!(
            r#"<tr><td class="text-center">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class="text-right">{}</td><td class="text-right">{}</td><td class="text-right">{}</td><td>{}</td><td class="text-right">{}</td>"#,
            start_num + i as i64 + 1, fname, ftype, area, p.employment_type, p.salary_type,
            sal_min, sal_max, base, bonus, holidays,
        ));
        if show_distance {
            let dist = p.distance_km.map(|d| format!("{:.1}km", d)).unwrap_or("-".to_string());
            html.push_str(&format!(r#"<td class="text-right">{}</td>"#, dist));
        }
        html.push_str("</tr>");
    }
    html.push_str("</tbody></table></div>");

    // ページネーション
    if total_pages > 1 {
        html.push_str(r#"<div class="flex justify-center gap-2 mt-4">"#);
        let base_url = format!(
            "/api/competitive/filter?prefecture={}&municipality={}&employment_type={}&nearby={}&radius_km={}",
            urlencoding::encode(pref),
            urlencoding::encode(muni),
            urlencoding::encode(emp),
            nearby,
            radius_km,
        );
        if page > 1 {
            html.push_str(&format!(
                r##"<button class="px-3 py-1 bg-slate-700 hover:bg-slate-600 rounded text-sm" hx-get="{}&page={}" hx-target="#comp-results" hx-swap="innerHTML">前へ</button>"##,
                base_url, page - 1
            ));
        }
        html.push_str(&format!(
            r#"<span class="px-3 py-1 text-sm text-slate-400">{} / {} ページ</span>"#,
            page, total_pages
        ));
        if page < total_pages {
            html.push_str(&format!(
                r##"<button class="px-3 py-1 bg-slate-700 hover:bg-slate-600 rounded text-sm" hx-get="{}&page={}" hx-target="#comp-results" hx-swap="innerHTML">次へ</button>"##,
                base_url, page + 1
            ));
        }
        html.push_str("</div>");
    }

    Html(html)
}

/// HTMLレポート生成（A4横向き印刷対応）
fn render_report_html(
    job_type: &str,
    pref: &str,
    muni: &str,
    postings: &[PostingRow],
    stats: &SalaryStats,
    today: &str,
    nearby: bool,
    radius_km: f64,
) -> Html<String> {
    let region = if muni.is_empty() {
        pref.to_string()
    } else if nearby {
        format!("{} {}（半径{}km）", pref, muni, radius_km)
    } else {
        format!("{} {}", pref, muni)
    };

    let show_distance = nearby && postings.iter().any(|p| p.distance_km.is_some());

    let mut table_rows = String::new();
    for (i, p) in postings.iter().enumerate() {
        let fname = truncate_str(&escape_html(&p.facility_name), 40);
        let ftype = truncate_str(&escape_html(&p.facility_type), 30);
        let area = format!("{} {}", escape_html(&p.prefecture), escape_html(&p.municipality));
        let sal_min = if p.salary_min > 0 { format!("{}", format_number(p.salary_min)) } else { "-".to_string() };
        let sal_max = if p.salary_max > 0 { format!("{}", format_number(p.salary_max)) } else { "-".to_string() };
        let base = if p.base_salary > 0 { format!("{}", format_number(p.base_salary)) } else { "-".to_string() };
        let holidays = if p.annual_holidays > 0 { p.annual_holidays.to_string() } else { "-".to_string() };
        let bonus = truncate_str(&escape_html(&p.bonus), 20);
        let dist_cell = if show_distance {
            let d = p.distance_km.map(|d| format!("{:.1}km", d)).unwrap_or("-".to_string());
            format!(r#"<td class="num">{}</td>"#, d)
        } else {
            String::new()
        };

        table_rows.push_str(&format!(
            r#"<tr><td style="text-align:center">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class="num">{}</td><td class="num">{}</td><td class="num">{}</td><td>{}</td><td class="num">{}</td>{}</tr>"#,
            i + 1, fname, ftype, area, escape_html(&p.employment_type), escape_html(&p.salary_type),
            sal_min, sal_max, base, bonus, holidays, dist_cell,
        ));
    }

    let distance_th = if show_distance { r#"<th>距離</th>"# } else { "" };

    let stats_html = if stats.has_data {
        format!(
            r#"<h2>給与統計サマリー</h2>
            <table>
                <thead><tr><th></th><th>月給下限</th><th>月給上限</th></tr></thead>
                <tbody>
                    <tr><td>最頻値（1万円単位）</td><td class="num">{}</td><td class="num">{}</td></tr>
                    <tr><td>中央値</td><td class="num">{}</td><td class="num">{}</td></tr>
                    <tr><td>平均値</td><td class="num">{}</td><td class="num">{}</td></tr>
                </tbody>
            </table>
            <p>件数: {} ｜ 賞与あり率: {} ｜ 平均年間休日: {}</p>"#,
            stats.salary_min_mode, stats.salary_max_mode,
            stats.salary_min_median, stats.salary_max_median,
            stats.salary_min_avg, stats.salary_max_avg,
            stats.count, stats.bonus_rate, stats.avg_holidays,
        )
    } else {
        String::new()
    };

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>競合調査レポート - {job_type} × {region}</title>
<style>
@page {{ size: A4 landscape; margin: 10mm; }}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: "Yu Gothic", "Meiryo", sans-serif; font-size: 11px; color: #333; background: #fff; padding: 15px; }}
h1 {{ font-size: 18px; color: #1a5276; margin-bottom: 8px; border-bottom: 2px solid #1a5276; padding-bottom: 4px; }}
h2 {{ font-size: 14px; color: #2c3e50; margin: 16px 0 8px 0; }}
.meta {{ font-size: 11px; color: #666; margin-bottom: 12px; }}
.meta span {{ margin-right: 16px; }}
table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
th {{ background-color: #2c3e50; color: #fff; font-weight: bold; text-align: center; padding: 6px 4px; font-size: 10px; white-space: nowrap; border: 1px solid #1a252f; }}
td {{ padding: 5px 4px; border: 1px solid #ddd; font-size: 10px; vertical-align: top; }}
tr:nth-child(even) {{ background-color: #f8f9fa; }}
.num {{ text-align: right; white-space: nowrap; }}
@media print {{
    body {{ padding: 0; font-size: 9px; }}
    th, td {{ font-size: 8px; padding: 3px 2px; }}
}}
</style>
</head>
<body>
<h1>競合調査レポート</h1>
<div class="meta">
    <span>職種: {job_type}</span>
    <span>地域: {region}</span>
    <span>生成日: {today}</span>
    <span>{count}件</span>
</div>

{stats_html}

<h2>求人一覧</h2>
<table>
<thead>
<tr>
    <th>#</th><th>法人・施設名</th><th>施設形態</th><th>エリア</th>
    <th>雇用形態</th><th>給与区分</th><th>月給下限</th><th>月給上限</th>
    <th>基本給</th><th>賞与</th><th>年間休日</th>{distance_th}
</tr>
</thead>
<tbody>
{table_rows}
</tbody>
</table>
</body>
</html>"#,
        job_type = escape_html(job_type),
        region = escape_html(&region),
        today = today,
        count = postings.len(),
        stats_html = stats_html,
        distance_th = distance_th,
        table_rows = table_rows,
    );

    Html(html)
}

// --- ユーティリティ ---

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn truncate_str(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars - 1).collect();
        format!("{}…", truncated)
    }
}
