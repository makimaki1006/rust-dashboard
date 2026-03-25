//! V2外部統計データ取得ヘルパー
//! country-statistics Turso DBからの読み取り専用クエリ

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::AppState;

/// V2外部統計DBからクエリ実行（turso_ext未設定時は空Vec）
pub async fn query_ext(
    state: &AppState,
    sql: &str,
    params: &[Value],
) -> Vec<HashMap<String, Value>> {
    let client = match &state.turso_ext {
        Some(c) => c,
        None => return vec![],
    };

    // キャッシュチェック
    let cache_key = format!("ext_{}", short_hash(sql, params));
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(arr) = cached.as_array() {
            // キャッシュヒット: JSON配列をHashMapに逆変換
            return arr.iter().filter_map(|v| {
                v.as_object().map(|obj| {
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                })
            }).collect();
        }
    }

    match client.query(sql, params).await {
        Ok(rows) => {
            // キャッシュに保存（24時間TTLはAppCache側で制御）
            let json_arr: Vec<Value> = rows.iter().map(|row| {
                Value::Object(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }).collect();
            state.cache.set(cache_key, Value::Array(json_arr));
            rows
        }
        Err(e) => {
            tracing::warn!("External DB query failed: {e}");
            vec![]
        }
    }
}

/// 都道府県統計を取得（v2_external_prefecture_stats）
pub async fn fetch_prefecture_macro(
    state: &AppState,
    prefecture: &str,
) -> Option<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return None;
    }

    let rows = query_ext(
        state,
        "SELECT * FROM v2_external_prefecture_stats WHERE prefecture = ?",
        &[Value::String(prefecture.to_string())],
    ).await;

    rows.into_iter().next()
}

/// 有効求人倍率の年度推移を取得
pub async fn fetch_job_openings_ratio(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return vec![];
    }

    query_ext(
        state,
        "SELECT fiscal_year, ratio_total, ratio_excl_part \
         FROM v2_external_job_openings_ratio \
         WHERE prefecture = ? ORDER BY fiscal_year",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// 人口データを取得（v2_external_population）
pub async fn fetch_population(
    state: &AppState,
    prefecture: &str,
    municipality: &str,
) -> Option<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return None;
    }

    let (sql, params) = if municipality.is_empty() {
        // 都道府県全体: SUM集計
        (
            "SELECT ? as prefecture, '合計' as municipality, \
                    SUM(total_population) as total_population, \
                    SUM(age_65_over) as age_65_over, \
                    ROUND(CAST(SUM(age_65_over) AS REAL) / SUM(total_population) * 100, 1) as aging_rate, \
                    SUM(age_15_64) as age_15_64, \
                    ROUND(CAST(SUM(age_15_64) AS REAL) / SUM(total_population) * 100, 1) as working_age_rate \
             FROM v2_external_population WHERE prefecture = ?".to_string(),
            vec![Value::String(prefecture.to_string()), Value::String(prefecture.to_string())],
        )
    } else {
        (
            "SELECT * FROM v2_external_population WHERE prefecture = ? AND municipality = ?".to_string(),
            vec![Value::String(prefecture.to_string()), Value::String(municipality.to_string())],
        )
    };

    let rows = query_ext(state, &sql, &params).await;
    rows.into_iter().next()
}

/// 介護需要データを取得（最新年度）
pub async fn fetch_care_demand(
    state: &AppState,
    prefecture: &str,
) -> Option<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return None;
    }

    let rows = query_ext(
        state,
        "SELECT * FROM v2_external_care_demand \
         WHERE prefecture = ? ORDER BY fiscal_year DESC LIMIT 1",
        &[Value::String(prefecture.to_string())],
    ).await;

    rows.into_iter().next()
}

/// 離職率データを取得（医療福祉）
pub async fn fetch_turnover(
    state: &AppState,
    prefecture: &str,
) -> Option<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return None;
    }

    let rows = query_ext(
        state,
        "SELECT * FROM v2_external_turnover \
         WHERE prefecture = ? AND industry LIKE '%医療%福祉%' \
         ORDER BY fiscal_year DESC LIMIT 1",
        &[Value::String(prefecture.to_string())],
    ).await;

    rows.into_iter().next()
}

/// HW求人数推移（ts_turso_counts）— 都道府県別合計
pub async fn fetch_hw_posting_trend(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return vec![];
    }

    query_ext(
        state,
        "SELECT snapshot_id, SUM(posting_count) as total_postings, \
                SUM(facility_count) as total_facilities \
         FROM ts_turso_counts \
         WHERE prefecture = ? \
         GROUP BY snapshot_id ORDER BY snapshot_id",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// HW欠員率推移（ts_turso_vacancy）— 都道府県別平均
pub async fn fetch_hw_vacancy_trend(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return vec![];
    }

    query_ext(
        state,
        "SELECT snapshot_id, \
                AVG(vacancy_rate) as avg_vacancy_rate, \
                AVG(growth_rate) as avg_growth_rate, \
                SUM(total_count) as total_count \
         FROM ts_turso_vacancy \
         WHERE prefecture = ? \
         GROUP BY snapshot_id ORDER BY snapshot_id",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// HW求人賃金統計（ts_turso_salary）— 都道府県別・最新snapshot
pub async fn fetch_hw_salary(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() { return vec![]; }
    query_ext(state,
        "SELECT snapshot_id, emp_group, \
                SUM(count) as count, \
                ROUND(SUM(mean_min * count) / NULLIF(SUM(count), 0)) as avg_min, \
                ROUND(SUM(mean_max * count) / NULLIF(SUM(count), 0)) as avg_max, \
                ROUND(SUM(median_min * count) / NULLIF(SUM(count), 0)) as avg_median \
         FROM ts_turso_salary \
         WHERE prefecture = ? \
         GROUP BY snapshot_id, emp_group ORDER BY snapshot_id",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// HW求人賃金（最新snapshotの集約値のみ）
pub async fn fetch_hw_salary_latest(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() { return vec![]; }
    query_ext(state,
        "SELECT emp_group, \
                SUM(count) as count, \
                ROUND(SUM(mean_min * count) / NULLIF(SUM(count), 0)) as avg_min, \
                ROUND(SUM(mean_max * count) / NULLIF(SUM(count), 0)) as avg_max, \
                ROUND(SUM(median_min * count) / NULLIF(SUM(count), 0)) as avg_median \
         FROM ts_turso_salary \
         WHERE prefecture = ? AND snapshot_id = (SELECT MAX(snapshot_id) FROM ts_turso_salary) \
         GROUP BY emp_group",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// HW掲載日数（ts_turso_fulfillment）— 都道府県別・最新snapshot
pub async fn fetch_hw_fulfillment_latest(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() { return vec![]; }
    query_ext(state,
        "SELECT emp_group, \
                SUM(count) as count, \
                ROUND(SUM(avg_listing_days * count) / NULLIF(SUM(count), 0), 1) as avg_days, \
                ROUND(SUM(median_listing_days * count) / NULLIF(SUM(count), 0), 1) as median_days, \
                SUM(long_term_count) as long_term, \
                SUM(very_long_count) as very_long \
         FROM ts_turso_fulfillment \
         WHERE prefecture = ? AND snapshot_id = (SELECT MAX(snapshot_id) FROM ts_turso_fulfillment) \
         GROUP BY emp_group",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// HW働き方統計（ts_agg_workstyle）— 都道府県別・最新snapshot
pub async fn fetch_hw_workstyle_latest(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() { return vec![]; }
    query_ext(state,
        "SELECT emp_group, count, avg_annual_holidays, avg_overtime \
         FROM ts_agg_workstyle \
         WHERE prefecture = ? AND snapshot_id = (SELECT MAX(snapshot_id) FROM ts_agg_workstyle)",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// HW求人追跡（ts_agg_tracking）— 都道府県別時系列
pub async fn fetch_hw_tracking(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() { return vec![]; }
    query_ext(state,
        "SELECT snapshot_id, \
                SUM(new_count) as new_total, \
                SUM(continue_count) as continue_total, \
                SUM(end_count) as end_total, \
                ROUND(CAST(SUM(end_count) AS REAL) / NULLIF(SUM(continue_count) + SUM(end_count), 0) * 100, 1) as churn_rate \
         FROM ts_agg_tracking \
         WHERE prefecture = ? \
         GROUP BY snapshot_id ORDER BY snapshot_id",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// 転入出データ（v2_external_migration）
pub async fn fetch_migration(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() { return vec![]; }
    query_ext(state,
        "SELECT municipality, inflow, outflow, net_migration, net_migration_rate \
         FROM v2_external_migration WHERE prefecture = ? ORDER BY net_migration DESC",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// 昼間人口データ（v2_external_daytime_population）
pub async fn fetch_daytime_population(
    state: &AppState,
    prefecture: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() { return vec![]; }
    query_ext(state,
        "SELECT municipality, daytime_population, daytime_rate \
         FROM v2_external_daytime_population WHERE prefecture = ? ORDER BY daytime_rate DESC",
        &[Value::String(prefecture.to_string())],
    ).await
}

/// 労働力統計（v2_external_labor_stats）— 最新年度
pub async fn fetch_labor_stats_latest(
    state: &AppState,
    prefecture: &str,
) -> Option<HashMap<String, Value>> {
    if prefecture.is_empty() { return None; }
    let rows = query_ext(state,
        "SELECT * FROM v2_external_labor_stats \
         WHERE prefecture = ? ORDER BY fiscal_year DESC LIMIT 1",
        &[Value::String(prefecture.to_string())],
    ).await;
    rows.into_iter().next()
}

/// 事業所数（v2_external_establishments）— 医療福祉
pub async fn fetch_establishments_medical(
    state: &AppState,
    prefecture: &str,
) -> Option<HashMap<String, Value>> {
    if prefecture.is_empty() { return None; }
    let rows = query_ext(state,
        "SELECT establishment_count, employee_count FROM v2_external_establishments \
         WHERE prefecture = ? AND industry LIKE '%医療%福祉%'",
        &[Value::String(prefecture.to_string())],
    ).await;
    rows.into_iter().next()
}

/// 人口ピラミッド（9区分×男女）を取得
pub async fn fetch_population_pyramid(
    state: &AppState,
    prefecture: &str,
    municipality: &str,
) -> Vec<HashMap<String, Value>> {
    if prefecture.is_empty() {
        return vec![];
    }

    let (sql, params) = if municipality.is_empty() {
        // 都道府県全体: SUM集計
        (
            "SELECT age_group, SUM(male_count) as male_count, SUM(female_count) as female_count \
             FROM v2_external_population_pyramid WHERE prefecture = ? \
             GROUP BY age_group ORDER BY age_group".to_string(),
            vec![Value::String(prefecture.to_string())],
        )
    } else {
        (
            "SELECT age_group, male_count, female_count \
             FROM v2_external_population_pyramid WHERE prefecture = ? AND municipality = ? \
             ORDER BY age_group".to_string(),
            vec![Value::String(prefecture.to_string()), Value::String(municipality.to_string())],
        )
    };

    query_ext(state, &sql, &params).await
}

/// HashMap値取得ヘルパー
pub fn ext_f64(row: &HashMap<String, Value>, key: &str) -> f64 {
    row.get(key)
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0.0)
}

pub fn ext_i64(row: &HashMap<String, Value>, key: &str) -> i64 {
    row.get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)).or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0)
}

pub fn ext_str<'a>(row: &'a HashMap<String, Value>, key: &str) -> &'a str {
    row.get(key).and_then(|v| v.as_str()).unwrap_or("")
}

/// 短いハッシュ生成（キャッシュキー用）
fn short_hash(sql: &str, params: &[Value]) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    for p in params {
        p.to_string().hash(&mut hasher);
    }
    format!("{:x}", hasher.finish())
}
