use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::sync::Arc;
use tower_sessions::Session;

use crate::models::job_seeker::{has_turso_data, render_no_turso_data};
use crate::AppState;

use super::overview::{get_str, get_i64, get_f64, format_number, get_session_filters, make_location_label};

/// タブ4: 需給バランス
pub async fn tab_balance(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    if !has_turso_data(&job_type) {
        return Html(render_no_turso_data(&job_type, "需給バランス"));
    }

    let cache_key = format!("balance_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_balance(&state, &job_type, &prefecture, &municipality).await;
    let html = render_balance(&job_type, &prefecture, &municipality, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct GapRow {
    municipality: String,
    demand_count: f64,
    supply_count: f64,
    gap: f64,
    ratio: f64,
}

struct BalanceStats {
    /// GAPデータ行（市区町村別）
    gap_rows: Vec<GapRow>,
    /// 求人数（job_openingsテーブルから）
    job_count: i64,
    /// 全国求人数
    job_total: i64,
}

impl Default for BalanceStats {
    fn default() -> Self {
        Self {
            gap_rows: Vec::new(),
            job_count: 0,
            job_total: 0,
        }
    }
}

async fn fetch_balance(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> BalanceStats {
    let mut stats = BalanceStats::default();

    // GAPデータ取得
    let mut params = vec![Value::String(job_type.to_string())];
    let mut sql = String::from(
        "SELECT municipality, demand_count, supply_count, gap, demand_supply_ratio \
        FROM job_seeker_data \
        WHERE job_type = ? AND row_type = 'GAP'"
    );

    if !prefecture.is_empty() && prefecture != "全国" {
        params.push(Value::String(prefecture.to_string()));
        sql.push_str(" AND prefecture = ?");
    }

    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Balance GAP query failed: {e}");
            return stats;
        }
    };

    for row in &rows {
        let muni = get_str(row, "municipality");
        if muni.is_empty() { continue; }
        stats.gap_rows.push(GapRow {
            municipality: muni,
            demand_count: get_f64(row, "demand_count"),
            supply_count: get_f64(row, "supply_count"),
            gap: get_f64(row, "gap"),
            ratio: get_f64(row, "demand_supply_ratio"),
        });
    }

    // 求人数取得（job_openingsテーブル）
    if !prefecture.is_empty() && prefecture != "全国" {
        let jo_sql = if !municipality.is_empty() && municipality != "すべて" {
            "SELECT COUNT(*) as cnt FROM job_openings WHERE job_type = ? AND prefecture = ? AND municipality = ?"
        } else {
            "SELECT COUNT(*) as cnt FROM job_openings WHERE job_type = ? AND prefecture = ?"
        };
        let mut jo_params = vec![
            Value::String(job_type.to_string()),
            Value::String(prefecture.to_string()),
        ];
        if !municipality.is_empty() && municipality != "すべて" {
            jo_params.push(Value::String(municipality.to_string()));
        }

        if let Ok(jo_rows) = state.turso.query(jo_sql, &jo_params).await {
            if let Some(first) = jo_rows.first() {
                stats.job_count = get_i64(first, "cnt");
            }
        }

        // 全国求人数
        let total_sql = "SELECT COUNT(*) as cnt FROM job_openings WHERE job_type = ?";
        let total_params = vec![Value::String(job_type.to_string())];
        if let Ok(total_rows) = state.turso.query(total_sql, &total_params).await {
            if let Some(first) = total_rows.first() {
                stats.job_total = get_i64(first, "cnt");
            }
        }
    }

    stats
}

fn render_balance(job_type: &str, prefecture: &str, municipality: &str, stats: &BalanceStats) -> String {
    let location_label = make_location_label(prefecture, municipality);
    let pref_label = if prefecture.is_empty() || prefecture == "全国" { "全国" } else { prefecture };

    // ===== GAP統計KPI =====
    let (total_demand, total_supply, shortage_count, surplus_count) = if !stats.gap_rows.is_empty() {
        let demand: f64 = stats.gap_rows.iter().map(|r| r.demand_count).sum();
        let supply: f64 = stats.gap_rows.iter().map(|r| r.supply_count).sum();
        let shortage = stats.gap_rows.iter().filter(|r| r.gap > 0.0).count();
        let surplus = stats.gap_rows.iter().filter(|r| r.gap < 0.0).count();
        (demand, supply, shortage, surplus)
    } else {
        (0.0, 0.0, 0, 0)
    };
    let avg_ratio = if total_supply > 0.0 { total_demand / total_supply } else { 0.0 };

    let kpi_data = [
        ("総需要", format!("{:.0}件以上", total_demand)),
        ("総供給", format!("{:.0}件", total_supply)),
        ("平均比率", format!("{:.2}倍以上", avg_ratio)),
        ("不足地域", format!("{}箇所", shortage_count)),
        ("過剰地域", format!("{}箇所", surplus_count)),
    ];

    let kpi_cards: String = kpi_data.iter().map(|(label, value)| {
        format!(
            r##"<div class="stat-card" style="flex: 1; min-width: 150px;">
                <div class="text-sm text-slate-400">{}</div>
                <div class="text-2xl font-bold text-blue-400">{}</div>
            </div>"##,
            label, value
        )
    }).collect::<Vec<String>>().join("\n");

    // ===== 選択地域表示 =====
    let location_display = if !prefecture.is_empty() && prefecture != "全国" {
        let mut html = format!(r##"<span class="text-sm font-bold" style="color: #56B4E9;">{}</span>"##, prefecture);
        if !municipality.is_empty() && municipality != "すべて" {
            html.push_str(&format!(
                r##" <span class="text-slate-400 text-sm">/</span> <span class="text-sm font-bold" style="color: #D55E00;">{}</span>"##,
                municipality
            ));
        } else {
            html.push_str(r##" <span class="text-slate-500 text-sm italic">(都道府県全体)</span>"##);
        }
        html
    } else {
        r##"<span class="text-sm text-slate-400">全国</span>"##.to_string()
    };

    // ===== 競争環境言語化カード =====
    let competition_section = build_competition_section(stats, avg_ratio, prefecture);

    // ===== シェア比較カード =====
    let share_section = build_share_section(stats, total_supply, prefecture);

    // ===== ランキングチャート =====
    let shortage_chart = build_ranking_chart(&stats.gap_rows, "shortage", "#D55E00");
    let surplus_chart = build_ranking_chart(&stats.gap_rows, "surplus", "#009E73");
    let ratio_chart = build_ranking_chart(&stats.gap_rows, "ratio", "#56B4E9");

    include_str!("../../templates/tabs/balance.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{LOCATION_LABEL}}", &location_label)
        .replace("{{PREF_LABEL}}", pref_label)
        .replace("{{COMPETITION_SECTION}}", &competition_section)
        .replace("{{LOCATION_DISPLAY}}", &location_display)
        .replace("{{GAP_KPI_CARDS}}", &kpi_cards)
        .replace("{{SHARE_SECTION}}", &share_section)
        .replace("{{SHORTAGE_CHART}}", &shortage_chart)
        .replace("{{SURPLUS_CHART}}", &surplus_chart)
        .replace("{{RATIO_CHART}}", &ratio_chart)
}

fn build_competition_section(stats: &BalanceStats, avg_ratio: f64, prefecture: &str) -> String {
    if prefecture.is_empty() || prefecture == "全国" || stats.gap_rows.is_empty() {
        return String::new();
    }

    let (_level, icon, border_color, message) = if avg_ratio > 1.5 {
        ("high", "🔥", "#EF4444",
         format!("{}の{}は競争が激しい市場です。求職者に対して求人が多く、採用活動には積極的な条件提示が必要です。",
                 prefecture, "当該職種"))
    } else if avg_ratio > 1.0 {
        ("medium", "⚡", "#F59E0B",
         format!("{}は需給がやや逼迫しています。差別化された求人条件で優秀な人材を確保する戦略が有効です。",
                 prefecture))
    } else {
        ("low", "✨", "#10B981",
         format!("{}は比較的人材が充足しています。質の高い採用条件を設定し、定着率向上に注力することが推奨されます。",
                 prefecture))
    };

    let ratio_display = format!("{:.1}倍以上", avg_ratio);
    let job_display = if stats.job_count > 0 {
        format!("{}件以上", format_number(stats.job_count))
    } else {
        "データなし".to_string()
    };

    format!(
        r##"<div class="stat-card" style="border: 2px solid {};">
            <div class="flex items-center gap-2 mb-2">
                <span class="text-xl">{}</span>
                <span class="text-lg font-bold" style="color: {};">競争環境の言語化</span>
            </div>
            <p class="text-sm text-slate-300 mb-4" style="line-height: 1.6;">{}</p>
            <div class="flex flex-wrap gap-4">
                <div class="p-3 rounded-lg" style="background-color: rgba(255,255,255,0.05);">
                    <div class="text-xs text-slate-400">競争倍率</div>
                    <div class="text-xl font-bold" style="color: {};">{}</div>
                </div>
                <div class="p-3 rounded-lg" style="background-color: rgba(255,255,255,0.05);">
                    <div class="text-xs text-slate-400">求人数</div>
                    <div class="text-lg font-bold" style="color: #6366F1;">{}</div>
                </div>
            </div>
        </div>"##,
        border_color, icon, border_color, message, border_color, ratio_display, job_display
    )
}

fn build_share_section(stats: &BalanceStats, total_supply: f64, prefecture: &str) -> String {
    if prefecture.is_empty() || prefecture == "全国" || stats.job_count == 0 {
        return String::new();
    }

    let job_share = if stats.job_total > 0 {
        (stats.job_count as f64 / stats.job_total as f64) * 100.0
    } else { 0.0 };

    // 求職者シェアは全国の求職者合計に対する比率
    // ここでは簡略化してjob_openingsと同様の計算
    let _seeker_share = job_share; // 近似値（将来の全国求職者シェア計算用）

    let competition_ratio = if total_supply > 0.0 {
        stats.job_count as f64 / total_supply
    } else { 0.0 };

    let (ratio_color, ratio_label) = if competition_ratio > 1.5 {
        ("#D55E00", "競争激化")
    } else if competition_ratio < 1.0 {
        ("#56B4E9", "人材不足")
    } else {
        ("#e2e8f0", "均衡")
    };

    format!(
        r##"<div class="stat-card">
            <div class="text-sm text-slate-400 mb-3">求人・求職者シェア比較（一部媒体データ）</div>
            <div class="flex flex-wrap gap-4">
                <div class="stat-card" style="flex: 1; min-width: 140px;">
                    <div class="text-sm text-slate-400">求人シェア</div>
                    <div class="text-2xl font-bold" style="color: #56B4E9;">{:.4}%</div>
                    <div class="text-xs text-slate-500">({} / {}件以上)</div>
                </div>
                <div class="stat-card" style="flex: 1; min-width: 140px;">
                    <div class="text-sm text-slate-400">競争倍率</div>
                    <div class="text-2xl font-bold" style="color: {};">{:.4}倍以上</div>
                    <div class="text-xs" style="color: {};">（{}）</div>
                </div>
            </div>
        </div>"##,
        job_share,
        format_number(stats.job_count),
        format_number(stats.job_total),
        ratio_color, competition_ratio,
        ratio_color, ratio_label
    )
}

fn build_ranking_chart(gap_rows: &[GapRow], mode: &str, color: &str) -> String {
    let mut items: Vec<(&str, f64)> = match mode {
        "shortage" => {
            // 需要超過: gap > 0、大きい順
            let mut v: Vec<_> = gap_rows.iter()
                .filter(|r| r.gap > 0.0)
                .map(|r| (r.municipality.as_str(), r.gap))
                .collect();
            v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            v
        }
        "surplus" => {
            // 供給超過: gap < 0、絶対値大きい順
            let mut v: Vec<_> = gap_rows.iter()
                .filter(|r| r.gap < 0.0)
                .map(|r| (r.municipality.as_str(), r.gap.abs()))
                .collect();
            v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            v
        }
        "ratio" => {
            // 需給比率: 大きい順
            let mut v: Vec<_> = gap_rows.iter()
                .filter(|r| r.ratio > 0.0)
                .map(|r| (r.municipality.as_str(), r.ratio))
                .collect();
            v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            v
        }
        _ => Vec::new(),
    };

    items.truncate(10);

    if items.is_empty() {
        return r##"<p class="text-slate-500 text-sm text-center py-12">データがありません</p>"##.to_string();
    }

    // 逆順（EChartsのyAxisは下から上）
    items.reverse();

    let labels: Vec<String> = items.iter().map(|(n, _)| format!("\"{}\"", n)).collect();
    let values: Vec<String> = items.iter().map(|(_, v)| {
        if mode == "ratio" { format!("{:.2}", v) } else { format!("{:.0}", v) }
    }).collect();

    let x_name = match mode {
        "shortage" => "需要超過（人）",
        "surplus" => "供給超過（人）",
        "ratio" => "需給比率",
        _ => "",
    };

    format!(
        r##"<div class="echart" style="height:400px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "grid": {{"left": "25%", "right": "10%", "top": "5%", "bottom": "15%"}},
            "xAxis": {{
                "type": "value",
                "name": "{}",
                "nameLocation": "middle",
                "nameGap": 30
            }},
            "yAxis": {{
                "type": "category",
                "data": [{}]
            }},
            "series": [{{
                "type": "bar",
                "data": [{}],
                "itemStyle": {{"color": "{}", "borderRadius": [0, 8, 8, 0]}},
                "barWidth": 25
            }}]
        }}'></div>"##,
        x_name,
        labels.join(","),
        values.join(","),
        color
    )
}
