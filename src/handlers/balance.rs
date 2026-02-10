use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{get_str, get_i64, format_number, get_session_filters, build_location_filter};

/// タブ4: 需給バランス
pub async fn tab_balance(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("balance_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_balance(&state, &job_type, &prefecture, &municipality).await;
    let html = render_balance(&job_type, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct BalanceStats {
    /// 都道府県別需給ギャップ (pref, supply, demand_share)
    pref_gap: Vec<(String, i64, f64)>,
    /// 競争プロファイル (category, count)
    competition: Vec<(String, i64)>,
}

impl Default for BalanceStats {
    fn default() -> Self {
        Self {
            pref_gap: Vec::new(),
            competition: Vec::new(),
        }
    }
}

async fn fetch_balance(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> BalanceStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    let sql = format!(
        "SELECT row_type, prefecture, municipality, \
               supply_count, category1, category2, count, \
               male_count, female_count \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type IN ('GAP', 'COMPETITION', 'SUMMARY'){location_filter}"
    );

    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Balance query failed: {e}");
            return BalanceStats::default();
        }
    };

    let mut stats = BalanceStats::default();
    let mut pref_supply: HashMap<String, i64> = HashMap::new();
    let mut competition_map: HashMap<String, i64> = HashMap::new();

    for row in &rows {
        let row_type = get_str(row, "row_type");
        match row_type.as_str() {
            "SUMMARY" => {
                let pref = get_str(row, "prefecture");
                if !pref.is_empty() {
                    let total = get_i64(row, "male_count") + get_i64(row, "female_count");
                    *pref_supply.entry(pref).or_insert(0) += total;
                }
            }
            "COMPETITION" => {
                let cat = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                if !cat.is_empty() {
                    *competition_map.entry(cat).or_insert(0) += cnt;
                }
            }
            _ => {}
        }
    }

    // 都道府県別供給ランキング
    let mut pref_list: Vec<(String, i64, f64)> = pref_supply
        .into_iter()
        .map(|(p, s)| (p, s, 0.0))
        .collect();
    pref_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.pref_gap = pref_list.into_iter().take(15).collect();

    // 競争プロファイル
    let mut comp_list: Vec<(String, i64)> = competition_map.into_iter().collect();
    comp_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.competition = comp_list.into_iter().take(10).collect();

    stats
}

fn render_balance(job_type: &str, stats: &BalanceStats) -> String {
    let pref_labels: Vec<String> = stats.pref_gap.iter().map(|(p, _, _)| format!("\"{}\"", p)).collect();
    let pref_values: Vec<String> = stats.pref_gap.iter().map(|(_, s, _)| s.to_string()).collect();

    let comp_labels: Vec<String> = stats.competition.iter().map(|(c, _)| format!("\"{}\"", c)).collect();
    let comp_values: Vec<String> = stats.competition.iter().map(|(_, v)| v.to_string()).collect();

    let gap_rows: String = stats
        .pref_gap
        .iter()
        .enumerate()
        .map(|(i, (name, supply, _))| {
            format!(
                r#"<tr><td class="text-center">{}</td><td>{}</td><td class="text-right">{}</td></tr>"#,
                i + 1, name, format_number(*supply)
            )
        })
        .collect();

    include_str!("../../templates/tabs/balance.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{PREF_LABELS}}", &format!("[{}]", pref_labels.join(",")))
        .replace("{{PREF_VALUES}}", &format!("[{}]", pref_values.join(",")))
        .replace("{{COMP_LABELS}}", &format!("[{}]", comp_labels.join(",")))
        .replace("{{COMP_VALUES}}", &format!("[{}]", comp_values.join(",")))
        .replace("{{GAP_ROWS}}", &gap_rows)
}
