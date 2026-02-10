use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{get_str, get_i64, get_session_filters, build_location_filter};

/// タブ5: 働き方
pub async fn tab_workstyle(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("workstyle_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_workstyle(&state, &job_type, &prefecture, &municipality).await;
    let html = render_workstyle(&job_type, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct WorkstyleStats {
    /// 働き方分布 (workstyle, count)
    distribution: Vec<(String, i64)>,
    /// 働き方×年齢 (workstyle, age_group, count)
    age_cross: Vec<(String, String, i64)>,
    /// 緊急度×性別 (urgency, gender, count)
    urgency_gender: Vec<(String, String, i64)>,
}

impl Default for WorkstyleStats {
    fn default() -> Self {
        Self {
            distribution: Vec::new(),
            age_cross: Vec::new(),
            urgency_gender: Vec::new(),
        }
    }
}

async fn fetch_workstyle(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> WorkstyleStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    let sql = format!(
        "SELECT row_type, category1, category2, count \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type IN ('WORKSTYLE_DISTRIBUTION', 'WORKSTYLE_AGE_CROSS', 'URGENCY_GENDER'){location_filter}"
    );

    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Workstyle query failed: {e}");
            return WorkstyleStats::default();
        }
    };

    let mut stats = WorkstyleStats::default();
    let mut ws_map: HashMap<String, i64> = HashMap::new();

    for row in &rows {
        let row_type = get_str(row, "row_type");
        let cat1 = get_str(row, "category1");
        let cat2 = get_str(row, "category2");
        let cnt = get_i64(row, "count");

        match row_type.as_str() {
            "WORKSTYLE_DISTRIBUTION" => {
                if !cat1.is_empty() {
                    *ws_map.entry(cat1).or_insert(0) += cnt;
                }
            }
            "WORKSTYLE_AGE_CROSS" => {
                if !cat1.is_empty() && !cat2.is_empty() {
                    stats.age_cross.push((cat1, cat2, cnt));
                }
            }
            "URGENCY_GENDER" => {
                if !cat1.is_empty() && !cat2.is_empty() {
                    stats.urgency_gender.push((cat1, cat2, cnt));
                }
            }
            _ => {}
        }
    }

    let mut ws_list: Vec<(String, i64)> = ws_map.into_iter().collect();
    ws_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.distribution = ws_list;

    stats
}

fn render_workstyle(job_type: &str, stats: &WorkstyleStats) -> String {
    // 働き方分布パイチャートデータ
    let ws_pie: Vec<String> = stats.distribution.iter().map(|(w, v)| {
        format!(r#"{{"value": {}, "name": "{}"}}"#, v, w)
    }).collect();

    // 緊急度×性別 - 集計
    let mut urgency_map: HashMap<String, (i64, i64)> = HashMap::new();
    for (urgency, gender, cnt) in &stats.urgency_gender {
        let entry = urgency_map.entry(urgency.clone()).or_insert((0, 0));
        if gender.contains('男') {
            entry.0 += cnt;
        } else if gender.contains('女') {
            entry.1 += cnt;
        }
    }
    let mut urgency_list: Vec<(String, i64, i64)> = urgency_map.into_iter().map(|(u, (m, f))| (u, m, f)).collect();
    urgency_list.sort_by(|a, b| (b.1 + b.2).cmp(&(a.1 + a.2)));

    let urg_labels: Vec<String> = urgency_list.iter().map(|(u, _, _)| format!("\"{}\"", u)).collect();
    let urg_male: Vec<String> = urgency_list.iter().map(|(_, m, _)| m.to_string()).collect();
    let urg_female: Vec<String> = urgency_list.iter().map(|(_, _, f)| f.to_string()).collect();

    include_str!("../../templates/tabs/workstyle.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{WS_PIE_DATA}}", &ws_pie.join(","))
        .replace("{{URG_LABELS}}", &format!("[{}]", urg_labels.join(",")))
        .replace("{{URG_MALE_VALUES}}", &format!("[{}]", urg_male.join(",")))
        .replace("{{URG_FEMALE_VALUES}}", &format!("[{}]", urg_female.join(",")))
}
