use serde_json::Value;

use crate::AppState;
use super::utils::{aggregate_subtypes, haversine, value_to_i64, MAJOR_CATEGORIES};

// --- 内部データ型 ---

pub(crate) struct CompStats {
    pub(crate) total_postings: i64,
    pub(crate) total_facilities: i64,
    pub(crate) pref_ranking: Vec<(String, i64)>,
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

pub(crate) struct PostingRow {
    pub(crate) facility_name: String,
    pub(crate) facility_type: String,
    pub(crate) prefecture: String,
    pub(crate) municipality: String,
    pub(crate) employment_type: String,
    pub(crate) salary_type: String,
    pub(crate) salary_min: i64,
    pub(crate) salary_max: i64,
    pub(crate) base_salary: i64,
    pub(crate) requirements: String,
    pub(crate) bonus: String,
    pub(crate) annual_holidays: i64,
    pub(crate) qualification_allowance: i64,
    pub(crate) other_allowances: String,
    pub(crate) distance_km: Option<f64>,
    pub(crate) tier3_id: String,
    pub(crate) tier3_label_short: String,
}

pub(crate) struct SalaryStats {
    pub(crate) count: i64,
    pub(crate) salary_min_median: String,
    pub(crate) salary_min_avg: String,
    pub(crate) salary_min_mode: String,
    pub(crate) salary_max_median: String,
    pub(crate) salary_max_avg: String,
    pub(crate) salary_max_mode: String,
    pub(crate) bonus_rate: String,
    pub(crate) avg_holidays: String,
    pub(crate) has_data: bool,
}

// --- データ取得関数 ---

pub(crate) fn fetch_competitive(state: &AppState, job_type: &str) -> CompStats {
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

pub(crate) fn fetch_prefectures(state: &AppState, job_type: &str) -> Vec<String> {
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

/// 施設形態の大カテゴリ一覧を取得（初期表示用: 全都道府県対象）
pub(crate) fn fetch_facility_types(state: &AppState, job_type: &str) -> Vec<(String, i64)> {
    let db = match &state.local_db {
        Some(db) => db,
        None => return Vec::new(),
    };

    let rows = db.query(
        "SELECT CASE \
            WHEN facility_type = '' OR facility_type IS NULL THEN '未分類' \
            WHEN INSTR(facility_type, ' ') > 0 THEN SUBSTR(facility_type, 1, INSTR(facility_type, ' ') - 1) \
            ELSE facility_type \
         END as major_cat, COUNT(*) as cnt \
         FROM job_postings WHERE job_type = ? \
         GROUP BY major_cat ORDER BY cnt DESC",
        &[&job_type as &dyn rusqlite::types::ToSql],
    ).unwrap_or_default();

    rows.iter()
        .filter_map(|r| {
            let cat = r.get("major_cat").and_then(|v| v.as_str())?.to_string();
            let cnt = r.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0);
            if cat.is_empty() { None } else { Some((cat, cnt)) }
        })
        .collect()
}

/// 施設形態の2階層データを取得（大カテゴリ→サブカテゴリ）
/// 返り値: Vec<(大カテゴリ名, Vec<(サブカテゴリ名, 件数)>)>
pub(crate) fn fetch_facility_types_hierarchical(
    state: &AppState,
    job_type: &str,
    pref: &str,
) -> Vec<(String, Vec<(String, i64)>)> {
    let db = match &state.local_db {
        Some(db) => db,
        None => return Vec::new(),
    };

    let (sql, param_values) = if pref.is_empty() {
        (
            "SELECT facility_type, COUNT(*) as cnt \
             FROM job_postings WHERE job_type = ? \
             GROUP BY facility_type ORDER BY cnt DESC".to_string(),
            vec![job_type.to_string()],
        )
    } else {
        (
            "SELECT facility_type, COUNT(*) as cnt \
             FROM job_postings WHERE job_type = ? AND prefecture = ? \
             GROUP BY facility_type ORDER BY cnt DESC".to_string(),
            vec![job_type.to_string(), pref.to_string()],
        )
    };

    let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = db.query(&sql, &params_ref).unwrap_or_default();
    aggregate_subtypes(&rows)
}

pub(crate) fn fetch_postings(
    db: &crate::db::local_sqlite::LocalDb,
    job_type: &str,
    pref: &str,
    muni: Option<&str>,
    emp: &str,
    ftype: &str,
) -> Vec<PostingRow> {
    let mut sql = String::from(
        "SELECT facility_name, facility_type, prefecture, municipality, employment_type, \
         salary_type, salary_min, salary_max, base_salary, requirements, \
         bonus, annual_holidays, qualification_allowance, other_allowances, \
         COALESCE(tier3_id,'') as tier3_id, COALESCE(tier3_label_short,'') as tier3_label_short \
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
    append_facility_type_filter(&mut sql, &mut param_values, ftype);
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

/// 施設形態フィルタのSQL条件を追加（2階層対応）
/// フィルタ値の形式:
///   - "大カテゴリ" → 大カテゴリ全体（前方一致）
///   - "大カテゴリ::サブカテゴリ" → 特定サブカテゴリ（部分一致）
///   - "未分類" → 空/NULL
///   - 複数はカンマ区切り
pub(crate) fn append_facility_type_filter(sql: &mut String, param_values: &mut Vec<String>, ftype: &str) {
    if ftype.is_empty() || ftype == "全て" {
        return;
    }
    let types: Vec<&str> = ftype.split(',').filter(|s| !s.is_empty()).collect();
    if types.is_empty() {
        return;
    }

    let is_major = |t: &str| -> bool {
        MAJOR_CATEGORIES.iter().any(|&(prefix, _, _)| prefix == t)
    };

    let mut conditions: Vec<String> = Vec::new();
    for t in &types {
        if t.contains("::") {
            // サブカテゴリ指定: "大カテゴリ::サブカテゴリ"
            let parts: Vec<&str> = t.splitn(2, "::").collect();
            let major = parts[0];
            let sub = parts[1];
            if major == "未分類" {
                conditions.push("(facility_type = '' OR facility_type IS NULL)".to_string());
            } else {
                // 大カテゴリ前方一致 AND サブカテゴリ部分一致
                conditions.push("(facility_type LIKE ? AND facility_type LIKE ?)".to_string());
                param_values.push(format!("{}%", major));
                param_values.push(format!("%{}%", sub));
            }
        } else if *t == "未分類" {
            conditions.push("(facility_type = '' OR facility_type IS NULL)".to_string());
        } else if is_major(t) {
            // 大カテゴリ全体
            conditions.push("(facility_type = ? OR facility_type LIKE ?)".to_string());
            param_values.push(t.to_string());
            param_values.push(format!("{} %", t));
        } else {
            // その他（旧互換: 完全一致）
            conditions.push("facility_type = ?".to_string());
            param_values.push(t.to_string());
        }
    }
    if !conditions.is_empty() {
        sql.push_str(&format!(" AND ({})", conditions.join(" OR ")));
    }
}

pub(crate) fn fetch_nearby_postings(
    db: &crate::db::local_sqlite::LocalDb,
    job_type: &str,
    pref: &str,
    muni: &str,
    radius_km: f64,
    emp: &str,
    ftype: &str,
) -> Vec<PostingRow> {
    let center = match get_geocode(db, pref, muni) {
        Some(c) => c,
        None => return Vec::new(),
    };

    let lat_delta = radius_km / 111.0;
    let lng_delta = radius_km / (111.0 * center.0.to_radians().cos());
    let lat_min = center.0 - lat_delta;
    let lat_max = center.0 + lat_delta;
    let lng_min = center.1 - lng_delta;
    let lng_max = center.1 + lng_delta;

    let mut sql = String::from(
        "SELECT facility_name, facility_type, prefecture, municipality, employment_type, \
         salary_type, salary_min, salary_max, base_salary, requirements, \
         bonus, annual_holidays, qualification_allowance, other_allowances, \
         COALESCE(tier3_id,'') as tier3_id, COALESCE(tier3_label_short,'') as tier3_label_short, \
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
    append_facility_type_filter(&mut sql, &mut param_values, ftype);
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

pub(crate) fn get_geocode(db: &crate::db::local_sqlite::LocalDb, pref: &str, muni: &str) -> Option<(f64, f64)> {
    let rows = db.query(
        "SELECT latitude, longitude FROM municipality_geocode WHERE prefecture = ? AND municipality = ?",
        &[&pref as &dyn rusqlite::types::ToSql, &muni as &dyn rusqlite::types::ToSql],
    ).ok()?;

    let row = rows.first()?;
    let lat = row.get("latitude").and_then(|v| v.as_f64())?;
    let lng = row.get("longitude").and_then(|v| v.as_f64())?;
    Some((lat, lng))
}

fn row_to_posting(r: &std::collections::HashMap<String, Value>, distance: Option<f64>) -> PostingRow {
    PostingRow {
        facility_name: r.get("facility_name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        facility_type: r.get("facility_type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        prefecture: r.get("prefecture").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        municipality: r.get("municipality").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        employment_type: r.get("employment_type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        salary_type: r.get("salary_type").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        salary_min: r.get("salary_min").map(value_to_i64).unwrap_or(0),
        salary_max: r.get("salary_max").map(value_to_i64).unwrap_or(0),
        base_salary: r.get("base_salary").map(value_to_i64).unwrap_or(0),
        requirements: r.get("requirements").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        bonus: r.get("bonus").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        annual_holidays: r.get("annual_holidays").map(value_to_i64).unwrap_or(0),
        qualification_allowance: r.get("qualification_allowance").map(value_to_i64).unwrap_or(0),
        other_allowances: r.get("other_allowances").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        distance_km: distance,
        tier3_id: r.get("tier3_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        tier3_label_short: r.get("tier3_label_short").and_then(|v| v.as_str()).unwrap_or("").to_string(),
    }
}
