use crate::handlers::overview::format_number;
use super::fetch::{PostingRow, SalaryStats};
use super::utils::{major_category_short_label, value_to_i64};

// --- 分析データ型 ---

pub(crate) struct AnalysisData {
    pub(crate) total: i64,
    pub(crate) inexperienced_ok: i64,
    pub(crate) experienced_only: i64,
    pub(crate) experience_unknown: i64,
    pub(crate) employment_dist: Vec<(String, i64)>,
    pub(crate) salary_type_dist: Vec<(String, i64)>,
    pub(crate) facility_type_top: Vec<(String, i64)>,
    pub(crate) salary_range_dist: Vec<(String, i64)>,
    pub(crate) salary_avg: i64,
    pub(crate) salary_median: i64,
    pub(crate) holidays_avg: i64,
    pub(crate) holidays_with_data: i64,
    pub(crate) bonus_count: i64,
}

pub(crate) fn fetch_analysis(db: &crate::db::local_sqlite::LocalDb, job_type: &str) -> AnalysisData {
    fetch_analysis_filtered(db, job_type, "", "")
}

pub(crate) fn fetch_analysis_filtered(
    db: &crate::db::local_sqlite::LocalDb,
    job_type: &str,
    pref: &str,
    muni: &str,
) -> AnalysisData {
    let mut data = AnalysisData {
        total: 0,
        inexperienced_ok: 0,
        experienced_only: 0,
        experience_unknown: 0,
        employment_dist: Vec::new(),
        salary_type_dist: Vec::new(),
        facility_type_top: Vec::new(),
        salary_range_dist: Vec::new(),
        salary_avg: 0,
        salary_median: 0,
        holidays_avg: 0,
        holidays_with_data: 0,
        bonus_count: 0,
    };

    let mut where_clause = "WHERE job_type = ?".to_string();
    let mut param_values: Vec<String> = vec![job_type.to_string()];
    if !pref.is_empty() {
        where_clause.push_str(" AND prefecture = ?");
        param_values.push(pref.to_string());
    }
    if !muni.is_empty() {
        where_clause.push_str(" AND municipality = ?");
        param_values.push(muni.to_string());
    }

    let params: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    // クエリ1: 基本統計（旧6クエリ → 1クエリ）
    if let Ok(rows) = db.query(
        &format!(
            "SELECT \
                COUNT(*) as total, \
                SUM(CASE WHEN inexperienced_ok = 1 THEN 1 ELSE 0 END) as inexp_ok, \
                SUM(CASE WHEN inexperienced_ok = 0 THEN 1 ELSE 0 END) as exp_only, \
                SUM(CASE WHEN annual_holidays >= 80 AND annual_holidays <= 200 THEN 1 ELSE 0 END) as hol_count, \
                AVG(CASE WHEN annual_holidays >= 80 AND annual_holidays <= 200 THEN annual_holidays ELSE NULL END) as hol_avg, \
                SUM(CASE WHEN bonus != '' AND bonus IS NOT NULL THEN 1 ELSE 0 END) as bonus_cnt \
             FROM job_postings {}",
            where_clause
        ),
        &params,
    ) {
        if let Some(row) = rows.first() {
            data.total = row.get("total").map(value_to_i64).unwrap_or(0);
            data.inexperienced_ok = row.get("inexp_ok").map(value_to_i64).unwrap_or(0);
            data.experienced_only = row.get("exp_only").map(value_to_i64).unwrap_or(0);
            data.experience_unknown = data.total - data.inexperienced_ok - data.experienced_only;
            data.holidays_with_data = row.get("hol_count").map(value_to_i64).unwrap_or(0);
            data.holidays_avg = row.get("hol_avg")
                .and_then(|v| v.as_f64())
                .map(|v| v as i64)
                .unwrap_or(0);
            data.bonus_count = row.get("bonus_cnt").map(value_to_i64).unwrap_or(0);
        }
    }

    if data.total == 0 {
        return data;
    }

    // クエリ2: 月給レンジ分布（旧8クエリ → 1クエリ）
    if let Ok(rows) = db.query(
        &format!(
            "SELECT \
                SUM(CASE WHEN salary_min >= 50000  AND salary_min < 150000  THEN 1 ELSE 0 END) as r1, \
                SUM(CASE WHEN salary_min >= 150000 AND salary_min < 200000  THEN 1 ELSE 0 END) as r2, \
                SUM(CASE WHEN salary_min >= 200000 AND salary_min < 250000  THEN 1 ELSE 0 END) as r3, \
                SUM(CASE WHEN salary_min >= 250000 AND salary_min < 300000  THEN 1 ELSE 0 END) as r4, \
                SUM(CASE WHEN salary_min >= 300000 AND salary_min < 350000  THEN 1 ELSE 0 END) as r5, \
                SUM(CASE WHEN salary_min >= 350000 AND salary_min < 400000  THEN 1 ELSE 0 END) as r6, \
                SUM(CASE WHEN salary_min >= 400000 AND salary_min < 500000  THEN 1 ELSE 0 END) as r7, \
                SUM(CASE WHEN salary_min >= 500000 AND salary_min < 1000000 THEN 1 ELSE 0 END) as r8 \
             FROM job_postings {}",
            where_clause
        ),
        &params,
    ) {
        if let Some(row) = rows.first() {
            let labels = ["〜15万", "15〜20万", "20〜25万", "25〜30万", "30〜35万", "35〜40万", "40〜50万", "50万〜"];
            let keys = ["r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"];
            for (label, key) in labels.iter().zip(keys.iter()) {
                let cnt = row.get(*key).map(value_to_i64).unwrap_or(0);
                data.salary_range_dist.push((label.to_string(), cnt));
            }
        }
    }

    // クエリ3: 雇用形態別分布
    if let Ok(rows) = db.query(
        &format!(
            "SELECT CASE WHEN employment_type = '' THEN '未記載' ELSE employment_type END as emp, COUNT(*) as cnt \
             FROM job_postings {} GROUP BY emp ORDER BY cnt DESC",
            where_clause
        ),
        &params,
    ) {
        for row in &rows {
            let name = row.get("emp").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let cnt = row.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0);
            if !name.is_empty() {
                data.employment_dist.push((name, cnt));
            }
        }
    }

    // クエリ4: 給与区分別分布
    if let Ok(rows) = db.query(
        &format!(
            "SELECT CASE WHEN salary_type = '' OR salary_type IS NULL THEN '未記載' ELSE salary_type END as st, COUNT(*) as cnt \
             FROM job_postings {} GROUP BY st ORDER BY cnt DESC",
            where_clause
        ),
        &params,
    ) {
        for row in &rows {
            let name = row.get("st").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let cnt = row.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0);
            if !name.is_empty() {
                data.salary_type_dist.push((name, cnt));
            }
        }
    }

    // クエリ5: 施設形態: 9大カテゴリ別集計（空文字列は「病院・クリニック」に分類）
    if let Ok(rows) = db.query(
        &format!(
            "SELECT CASE \
                WHEN facility_type = '' OR facility_type IS NULL THEN '未分類' \
                WHEN INSTR(facility_type, ' ') > 0 THEN SUBSTR(facility_type, 1, INSTR(facility_type, ' ') - 1) \
                ELSE facility_type \
             END as major_cat, COUNT(*) as cnt \
             FROM job_postings {} \
             GROUP BY major_cat ORDER BY cnt DESC",
            where_clause
        ),
        &params,
    ) {
        for row in &rows {
            let cat = row.get("major_cat").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let cnt = row.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0);
            if !cat.is_empty() {
                let short = major_category_short_label(&cat).to_string();
                data.facility_type_top.push((short, cnt));
            }
        }
    }

    // クエリ6: 月給統計（median計算に全件取得が必要）
    if let Ok(rows) = db.query(
        &format!(
            "SELECT salary_min FROM job_postings {} AND salary_min >= 50000 ORDER BY salary_min",
            where_clause
        ),
        &params,
    ) {
        let vals: Vec<i64> = rows.iter()
            .filter_map(|r| r.get("salary_min").map(|v| value_to_i64(v)))
            .collect();
        if !vals.is_empty() {
            data.salary_avg = vals.iter().sum::<i64>() / vals.len() as i64;
            data.salary_median = vals[vals.len() / 2];
        }
    }

    data
}

// --- 統計計算 ---

pub(crate) fn calc_salary_stats(postings: &[PostingRow]) -> SalaryStats {
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

    let min_vals: Vec<i64> = postings.iter()
        .filter(|p| p.salary_min >= 50000)
        .map(|p| p.salary_min)
        .collect();
    let max_vals: Vec<i64> = postings.iter()
        .filter(|p| p.salary_max >= 50000)
        .map(|p| p.salary_max)
        .collect();

    let bonus_count = postings.iter().filter(|p| !p.bonus.is_empty()).count();
    let bonus_rate = if !postings.is_empty() {
        format!("{:.0}%", bonus_count as f64 / postings.len() as f64 * 100.0)
    } else {
        "-".to_string()
    };

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

pub(crate) fn calc_median_str(vals: &[i64]) -> String {
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

pub(crate) fn calc_avg_str(vals: &[i64]) -> String {
    if vals.is_empty() { return "-".to_string(); }
    let avg = vals.iter().sum::<i64>() / vals.len() as i64;
    format!("{}円", format_number(avg))
}

pub(crate) fn calc_mode_str(vals: &[i64]) -> String {
    if vals.is_empty() { return "-".to_string(); }
    let mut freq: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for v in vals {
        let rounded = ((v + 5000) / 10000) * 10000;
        *freq.entry(rounded).or_insert(0) += 1;
    }
    let mode = freq.into_iter().max_by_key(|(_, c)| *c).map(|(v, _)| v).unwrap_or(0);
    format!("{}円", format_number(mode))
}
