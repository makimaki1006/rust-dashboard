use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{get_str, get_i64, format_number, get_session_filters};

/// タブ7: 人材マップ（コロプレス地図 + 統計）
pub async fn tab_talentmap(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("talentmap_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_talent_density(&state, &job_type).await;
    let html = render_talentmap(&job_type, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct TalentStats {
    /// 都道府県別人材密度 (pref, total)
    pref_density: Vec<(String, i64)>,
}

impl Default for TalentStats {
    fn default() -> Self {
        Self {
            pref_density: Vec::new(),
        }
    }
}

async fn fetch_talent_density(state: &AppState, job_type: &str) -> TalentStats {
    let sql = r#"
        SELECT prefecture, SUM(male_count + female_count) as total
        FROM job_seeker_data
        WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture != ''
        GROUP BY prefecture
        ORDER BY total DESC
    "#;

    let params = vec![Value::String(job_type.to_string())];
    let rows = match state.turso.query(sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Talent density query failed: {e}");
            return TalentStats::default();
        }
    };

    let mut stats = TalentStats::default();
    for row in &rows {
        let pref = get_str(row, "prefecture");
        let total = get_i64(row, "total");
        if !pref.is_empty() {
            stats.pref_density.push((pref, total));
        }
    }

    stats
}

fn render_talentmap(job_type: &str, stats: &TalentStats) -> String {
    let pref_labels: Vec<String> = stats.pref_density.iter().take(15).map(|(p, _)| format!("\"{}\"", p)).collect();
    let pref_values: Vec<String> = stats.pref_density.iter().take(15).map(|(_, v)| v.to_string()).collect();

    let density_rows: String = stats
        .pref_density
        .iter()
        .enumerate()
        .take(20)
        .map(|(i, (name, total))| {
            format!(
                r#"<tr><td class="text-center">{}</td><td>{}</td><td class="text-right">{}</td></tr>"#,
                i + 1, name, format_number(*total)
            )
        })
        .collect();

    include_str!("../../templates/tabs/talentmap.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{PREF_LABELS}}", &format!("[{}]", pref_labels.join(",")))
        .replace("{{PREF_VALUES}}", &format!("[{}]", pref_values.join(",")))
        .replace("{{DENSITY_ROWS}}", &density_rows)
}
