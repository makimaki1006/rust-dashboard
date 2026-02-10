use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{get_str, get_i64, get_f64, format_number, get_session_filters, build_location_filter, make_location_label};

/// タブ2: 人口動態 - HTMXパーシャルHTML
pub async fn tab_demographics(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("demographics_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_demographics(&state, &job_type, &prefecture, &municipality).await;
    let location_label = make_location_label(&prefecture, &municipality);
    let html = render_demographics(&job_type, &stats, &location_label);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct DemoStats {
    /// 都道府県別求職者数 (name, total, male, female)
    pref_ranking: Vec<(String, i64, i64, i64)>,
    /// 年齢×性別 (age_group, male, female)
    age_gender: Vec<(String, i64, i64)>,
    /// 資格分布 (qualification, count)
    qual_dist: Vec<(String, i64)>,
    /// 緊急度×性別 (gender, count, avg_urgency_score)
    urgency_gender: Vec<(String, i64, f64)>,
    /// 転職希望時期別緊急度 (start_category, count, avg_urgency_score)
    urgency_start: Vec<(String, i64, f64)>,
    /// ペルソナ構成比 (label, count) - AGE_GENDER由来の年齢×性別ペルソナ
    persona_ratio: Vec<(String, i64)>,
}

impl Default for DemoStats {
    fn default() -> Self {
        Self {
            pref_ranking: Vec::new(),
            age_gender: Vec::new(),
            qual_dist: Vec::new(),
            urgency_gender: Vec::new(),
            urgency_start: Vec::new(),
            persona_ratio: Vec::new(),
        }
    }
}

async fn fetch_demographics(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> DemoStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    let sql = format!(
        "SELECT row_type, prefecture, municipality, \
               male_count, female_count, \
               category1, category2, count, avg_urgency_score \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type IN ('SUMMARY', 'AGE_GENDER', 'QUALIFICATION_DETAIL', \
                           'URGENCY_GENDER', 'URGENCY_START_CATEGORY') \
          AND prefecture != ''{location_filter}"
    );

    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Demographics query failed: {e}");
            return DemoStats::default();
        }
    };

    let mut stats = DemoStats::default();

    // 都道府県別集計
    let mut pref_male: HashMap<String, i64> = HashMap::new();
    let mut pref_female: HashMap<String, i64> = HashMap::new();

    let age_order = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
    let mut age_male: HashMap<String, i64> = HashMap::new();
    let mut age_female: HashMap<String, i64> = HashMap::new();

    let mut qual_map: HashMap<String, i64> = HashMap::new();

    // 緊急度×性別: gender -> (合計count, urgency_score合計, 行数) で加重平均を算出
    let mut urg_gender_map: HashMap<String, (i64, f64, i64)> = HashMap::new();
    // 転職希望時期別: start_category -> (合計count, urgency_score合計, 行数)
    let mut urg_start_map: HashMap<String, (i64, f64, i64)> = HashMap::new();
    // ペルソナ構成比: "年齢層-性別" -> count
    let mut persona_map: HashMap<String, i64> = HashMap::new();

    for row in &rows {
        let row_type = get_str(row, "row_type");
        match row_type.as_str() {
            "SUMMARY" => {
                let pref = get_str(row, "prefecture");
                if !pref.is_empty() {
                    let male = get_i64(row, "male_count");
                    let female = get_i64(row, "female_count");
                    *pref_male.entry(pref.clone()).or_insert(0) += male;
                    *pref_female.entry(pref).or_insert(0) += female;
                }
            }
            "AGE_GENDER" => {
                let age_group = get_str(row, "category1");
                let gender = get_str(row, "category2");
                let cnt = get_i64(row, "count");
                if gender.contains('男') {
                    *age_male.entry(age_group.clone()).or_insert(0) += cnt;
                } else if gender.contains('女') {
                    *age_female.entry(age_group.clone()).or_insert(0) += cnt;
                }
                // ペルソナ構成比用: "年齢層 / 性別" をラベルにする
                if !age_group.is_empty() && !gender.is_empty() {
                    let label = format!("{} / {}", age_group, gender);
                    *persona_map.entry(label).or_insert(0) += cnt;
                }
            }
            "QUALIFICATION_DETAIL" => {
                let qual = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                if !qual.is_empty() {
                    *qual_map.entry(qual).or_insert(0) += cnt;
                }
            }
            "URGENCY_GENDER" => {
                let gender = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                let score = get_f64(row, "avg_urgency_score");
                if !gender.is_empty() {
                    let entry = urg_gender_map.entry(gender).or_insert((0, 0.0, 0));
                    entry.0 += cnt;
                    entry.1 += score * cnt as f64;
                    entry.2 += 1;
                }
            }
            "URGENCY_START_CATEGORY" => {
                let start_cat = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                let score = get_f64(row, "avg_urgency_score");
                if !start_cat.is_empty() {
                    let entry = urg_start_map.entry(start_cat).or_insert((0, 0.0, 0));
                    entry.0 += cnt;
                    entry.1 += score * cnt as f64;
                    entry.2 += 1;
                }
            }
            _ => {}
        }
    }

    // 都道府県ランキング（合計降順）
    let mut pref_list: Vec<(String, i64, i64, i64)> = pref_male
        .keys()
        .map(|p| {
            let m = *pref_male.get(p).unwrap_or(&0);
            let f = *pref_female.get(p).unwrap_or(&0);
            (p.clone(), m + f, m, f)
        })
        .collect();
    pref_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.pref_ranking = pref_list.into_iter().take(15).collect();

    // 年齢×性別
    for age in &age_order {
        let m = age_male.get(*age).copied().unwrap_or(0);
        let f = age_female.get(*age).copied().unwrap_or(0);
        stats.age_gender.push((age.to_string(), m, f));
    }

    // 資格分布（上位10）
    let mut qual_list: Vec<(String, i64)> = qual_map.into_iter().collect();
    qual_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.qual_dist = qual_list.into_iter().take(10).collect();

    // 緊急度×性別: 加重平均スコアを算出し、人数降順で並べる
    let mut urg_gender_list: Vec<(String, i64, f64)> = urg_gender_map
        .into_iter()
        .map(|(gender, (total_cnt, weighted_score, _))| {
            let avg = if total_cnt > 0 { weighted_score / total_cnt as f64 } else { 0.0 };
            (gender, total_cnt, avg)
        })
        .collect();
    urg_gender_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.urgency_gender = urg_gender_list;

    // 転職希望時期別緊急度: 定義順でソート
    let start_order = ["今すぐ", "1～3ヶ月", "3～6ヶ月", "6ヶ月以上"];
    let mut urg_start_list: Vec<(String, i64, f64)> = urg_start_map
        .into_iter()
        .map(|(cat, (total_cnt, weighted_score, _))| {
            let avg = if total_cnt > 0 { weighted_score / total_cnt as f64 } else { 0.0 };
            (cat, total_cnt, avg)
        })
        .collect();
    urg_start_list.sort_by(|a, b| {
        let pos_a = start_order.iter().position(|&o| a.0.contains(o)).unwrap_or(99);
        let pos_b = start_order.iter().position(|&o| b.0.contains(o)).unwrap_or(99);
        pos_a.cmp(&pos_b)
    });
    stats.urgency_start = urg_start_list;

    // ペルソナ構成比（上位10、人数降順）
    let mut persona_list: Vec<(String, i64)> = persona_map.into_iter().collect();
    persona_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.persona_ratio = persona_list.into_iter().take(10).collect();

    stats
}

fn render_demographics(job_type: &str, stats: &DemoStats, _location_label: &str) -> String {
    // 都道府県ランキングテーブル行
    let pref_rows: String = stats
        .pref_ranking
        .iter()
        .enumerate()
        .map(|(i, (name, total, male, female))| {
            format!(
                r#"<tr><td class="text-center">{}</td><td>{}</td><td class="text-right">{}</td><td class="text-right text-cyan-400">{}</td><td class="text-right text-pink-400">{}</td></tr>"#,
                i + 1, name, format_number(*total), format_number(*male), format_number(*female)
            )
        })
        .collect();

    // 年齢×性別チャートデータ
    let age_labels: Vec<String> = stats.age_gender.iter().map(|(a, _, _)| format!("\"{}\"", a)).collect();
    let age_male_vals: Vec<String> = stats.age_gender.iter().map(|(_, m, _)| m.to_string()).collect();
    let age_female_vals: Vec<String> = stats.age_gender.iter().map(|(_, _, f)| f.to_string()).collect();

    // 資格分布チャートデータ
    let qual_labels: Vec<String> = stats.qual_dist.iter().map(|(q, _)| format!("\"{}\"", q)).collect();
    let qual_values: Vec<String> = stats.qual_dist.iter().map(|(_, v)| v.to_string()).collect();

    // 緊急度×性別チャートデータ（棒: 人数、折れ線: 平均スコア）
    let urg_gender_labels: Vec<String> = stats.urgency_gender.iter().map(|(g, _, _)| format!("\"{}\"", g)).collect();
    let urg_gender_counts: Vec<String> = stats.urgency_gender.iter().map(|(_, c, _)| c.to_string()).collect();
    let urg_gender_scores: Vec<String> = stats.urgency_gender.iter().map(|(_, _, s)| format!("{:.2}", s)).collect();

    // 転職希望時期別緊急度チャートデータ
    let urg_start_labels: Vec<String> = stats.urgency_start.iter().map(|(c, _, _)| format!("\"{}\"", c)).collect();
    let urg_start_counts: Vec<String> = stats.urgency_start.iter().map(|(_, c, _)| c.to_string()).collect();
    let urg_start_scores: Vec<String> = stats.urgency_start.iter().map(|(_, _, s)| format!("{:.2}", s)).collect();

    // ペルソナ構成比チャートデータ（横棒、上位10）
    let persona_labels: Vec<String> = stats.persona_ratio.iter().map(|(l, _)| format!("\"{}\"", l)).collect();
    let persona_values: Vec<String> = stats.persona_ratio.iter().map(|(_, v)| v.to_string()).collect();
    let persona_total: i64 = stats.persona_ratio.iter().map(|(_, v)| v).sum();
    // 構成比(%)を計算
    let persona_pct: Vec<String> = stats.persona_ratio.iter().map(|(_, v)| {
        if persona_total > 0 {
            format!("{:.1}", *v as f64 / persona_total as f64 * 100.0)
        } else {
            "0.0".to_string()
        }
    }).collect();

    include_str!("../../templates/tabs/demographics.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{PREF_ROWS}}", &pref_rows)
        .replace("{{AGE_LABELS}}", &format!("[{}]", age_labels.join(",")))
        .replace("{{AGE_MALE_VALUES}}", &format!("[{}]", age_male_vals.join(",")))
        .replace("{{AGE_FEMALE_VALUES}}", &format!("[{}]", age_female_vals.join(",")))
        .replace("{{QUAL_LABELS}}", &format!("[{}]", qual_labels.join(",")))
        .replace("{{QUAL_VALUES}}", &format!("[{}]", qual_values.join(",")))
        .replace("{{URG_GENDER_LABELS}}", &format!("[{}]", urg_gender_labels.join(",")))
        .replace("{{URG_GENDER_COUNTS}}", &format!("[{}]", urg_gender_counts.join(",")))
        .replace("{{URG_GENDER_SCORES}}", &format!("[{}]", urg_gender_scores.join(",")))
        .replace("{{URG_START_LABELS}}", &format!("[{}]", urg_start_labels.join(",")))
        .replace("{{URG_START_COUNTS}}", &format!("[{}]", urg_start_counts.join(",")))
        .replace("{{URG_START_SCORES}}", &format!("[{}]", urg_start_scores.join(",")))
        .replace("{{PERSONA_LABELS}}", &format!("[{}]", persona_labels.join(",")))
        .replace("{{PERSONA_VALUES}}", &format!("[{}]", persona_values.join(",")))
        .replace("{{PERSONA_PCT}}", &format!("[{}]", persona_pct.join(",")))
}
