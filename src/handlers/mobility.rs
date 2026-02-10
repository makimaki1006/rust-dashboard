use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{get_str, get_i64, get_f64, format_number, get_session_filters, build_location_filter};

/// タブ3: 人材移動 - HTMXパーシャルHTML
pub async fn tab_mobility(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("mobility_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_mobility(&state, &job_type, &prefecture, &municipality).await;
    let html = render_mobility(&job_type, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct MobilityStats {
    avg_distance: f64,
    /// 移動型分布 (mobility_type, count)
    mobility_types: Vec<(String, i64)>,
    /// 主要フロー (from_pref, to_pref, count) 上位10
    top_flows: Vec<(String, String, i64)>,
    /// 流入人数（他県→選択県）
    inflow: i64,
    /// 流出人数（選択県→他県）
    outflow: i64,
    /// 地元志向人数（選択県→選択県）
    local_count: i64,
    /// フロー合計
    total_flow: i64,
    /// 距離 25パーセンタイル
    distance_q25: f64,
    /// 距離 中央値
    distance_median: f64,
    /// 距離 75パーセンタイル
    distance_q75: f64,
    /// 都道府県が選択されているか（フローKPI表示判定用）
    has_prefecture: bool,
}

impl Default for MobilityStats {
    fn default() -> Self {
        Self {
            avg_distance: 0.0,
            mobility_types: Vec::new(),
            top_flows: Vec::new(),
            inflow: 0,
            outflow: 0,
            local_count: 0,
            total_flow: 0,
            distance_q25: 0.0,
            distance_median: 0.0,
            distance_q75: 0.0,
            has_prefecture: false,
        }
    }
}

async fn fetch_mobility(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> MobilityStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    let sql = format!(
        "SELECT row_type, prefecture, municipality, \
               desired_prefecture, desired_municipality, \
               avg_reference_distance_km, mobility_type, count \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type = 'RESIDENCE_FLOW'{location_filter}"
    );

    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Mobility query failed: {e}");
            return MobilityStats::default();
        }
    };

    let mut stats = MobilityStats::default();
    let mut dist_sum: f64 = 0.0;
    let mut dist_count: i64 = 0;
    let mut mobility_map: HashMap<String, i64> = HashMap::new();
    // 都道府県間フロー集計
    let mut flow_map: HashMap<(String, String), i64> = HashMap::new();

    // 距離値を重み付きで収集（分位数計算用）
    let mut distance_values: Vec<(f64, i64)> = Vec::new();

    // 都道府県選択フラグ
    let has_pref = !prefecture.is_empty();
    stats.has_prefecture = has_pref;

    for row in &rows {
        let dist = get_f64(row, "avg_reference_distance_km");
        let cnt = get_i64(row, "count");
        let mobility = get_str(row, "mobility_type");
        let from_pref = get_str(row, "prefecture");
        let to_pref = get_str(row, "desired_prefecture");

        if dist > 0.0 && cnt > 0 {
            dist_sum += dist * cnt as f64;
            dist_count += cnt;
            distance_values.push((dist, cnt));
        }

        if !mobility.is_empty() {
            *mobility_map.entry(mobility).or_insert(0) += cnt;
        }

        // 流入・流出・地元志向の集計（都道府県選択時のみ）
        // flow_mapへのmoveより前に実行（所有権の関係）
        if has_pref && cnt > 0 && !from_pref.is_empty() && !to_pref.is_empty() {
            stats.total_flow += cnt;
            if from_pref == prefecture && to_pref == prefecture {
                // 地元志向: 居住地も希望勤務地も選択県
                stats.local_count += cnt;
            } else if from_pref != prefecture && to_pref == prefecture {
                // 流入: 他県から選択県へ
                stats.inflow += cnt;
            } else if from_pref == prefecture && to_pref != prefecture {
                // 流出: 選択県から他県へ
                stats.outflow += cnt;
            }
        }

        if !from_pref.is_empty() && !to_pref.is_empty() && from_pref != to_pref {
            *flow_map.entry((from_pref, to_pref)).or_insert(0) += cnt;
        }
    }

    stats.avg_distance = if dist_count > 0 { dist_sum / dist_count as f64 } else { 0.0 };

    // 距離分位数の計算（重み付き）
    if !distance_values.is_empty() {
        distance_values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let total_weight: i64 = distance_values.iter().map(|(_, c)| c).sum();
        if total_weight > 0 {
            stats.distance_q25 = weighted_percentile(&distance_values, total_weight, 0.25);
            stats.distance_median = weighted_percentile(&distance_values, total_weight, 0.50);
            stats.distance_q75 = weighted_percentile(&distance_values, total_weight, 0.75);
        }
    }

    let mut mobility_list: Vec<(String, i64)> = mobility_map.into_iter().collect();
    mobility_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.mobility_types = mobility_list;

    let mut flow_list: Vec<(String, String, i64)> = flow_map.into_iter().map(|((f, t), c)| (f, t, c)).collect();
    flow_list.sort_by(|a, b| b.2.cmp(&a.2));
    stats.top_flows = flow_list.into_iter().take(10).collect();

    stats
}

/// 重み付き分位数を計算する
/// values: ソート済みの (値, 重み) ペア
/// total_weight: 重みの合計
/// p: 分位数 (0.0 ~ 1.0)
fn weighted_percentile(values: &[(f64, i64)], total_weight: i64, p: f64) -> f64 {
    let target = (total_weight as f64) * p;
    let mut cumulative: f64 = 0.0;
    for (val, weight) in values {
        cumulative += *weight as f64;
        if cumulative >= target {
            return *val;
        }
    }
    // フォールバック: 最後の値を返す
    values.last().map(|(v, _)| *v).unwrap_or(0.0)
}

fn render_mobility(job_type: &str, stats: &MobilityStats) -> String {
    let flow_rows: String = stats
        .top_flows
        .iter()
        .enumerate()
        .map(|(i, (from, to, cnt))| {
            format!(
                r#"<tr><td class="text-center">{}</td><td>{}</td><td class="text-center">→</td><td>{}</td><td class="text-right">{}</td></tr>"#,
                i + 1, from, to, format_number(*cnt)
            )
        })
        .collect();

    // 移動型パイチャートデータ
    let mobility_pie: Vec<String> = stats.mobility_types.iter().map(|(m, v)| {
        format!(r#"{{"value": {}, "name": "{}"}}"#, v, m)
    }).collect();

    // 流入出KPIの計算
    let net_flow = stats.inflow - stats.outflow;
    let local_pct = if stats.total_flow > 0 {
        (stats.local_count as f64 / stats.total_flow as f64) * 100.0
    } else {
        0.0
    };

    // 純流入の符号付き表示（+/-プレフィックス）
    let net_flow_display = if net_flow > 0 {
        format!("+{}", format_number(net_flow))
    } else if net_flow < 0 {
        // format_numberは正数用なので負数は手動処理
        format!("-{}", format_number(-net_flow))
    } else {
        "0".to_string()
    };

    // 純流入の色クラス
    let net_flow_color = if net_flow > 0 {
        "text-green-400"
    } else if net_flow < 0 {
        "text-red-400"
    } else {
        "text-slate-400"
    };

    // 流入出KPIセクション（都道府県選択時のみ表示）
    let flow_kpi_section = if stats.has_prefecture {
        format!(
            r#"<div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div class="stat-card"><div class="stat-value text-green-400">{}</div><div class="stat-label">流入人数</div></div>
        <div class="stat-card"><div class="stat-value text-orange-400">{:.1}%</div><div class="stat-label">地元志向率</div></div>
        <div class="stat-card"><div class="stat-value text-red-400">{}</div><div class="stat-label">流出人数</div></div>
        <div class="stat-card"><div class="stat-value {}">{}</div><div class="stat-label">純流入</div></div>
    </div>"#,
            format_number(stats.inflow),
            local_pct,
            format_number(stats.outflow),
            net_flow_color,
            net_flow_display,
        )
    } else {
        r#"<div class="text-slate-500 text-sm italic">※ 都道府県を選択すると流入・流出の詳細が表示されます</div>"#.to_string()
    };

    include_str!("../../templates/tabs/mobility.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{AVG_DISTANCE}}", &format!("{:.1}", stats.avg_distance))
        .replace("{{FLOW_KPI_SECTION}}", &flow_kpi_section)
        .replace("{{DISTANCE_Q25}}", &format!("{:.1}", stats.distance_q25))
        .replace("{{DISTANCE_MEDIAN}}", &format!("{:.1}", stats.distance_median))
        .replace("{{DISTANCE_Q75}}", &format!("{:.1}", stats.distance_q75))
        .replace("{{MOBILITY_PIE_DATA}}", &mobility_pie.join(","))
        .replace("{{FLOW_ROWS}}", &flow_rows)
}
