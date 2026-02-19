use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{get_str, get_i64, get_session_filters, build_location_filter, make_location_label};

/// タブ5: 雇用形態分析
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
    let html = render_workstyle(&job_type, &prefecture, &municipality, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct WorkstyleStats {
    /// 雇用形態分布 (workstyle, count)
    distribution: Vec<(String, i64)>,
    /// 雇用形態×年代 (workstyle, age_group, row_pct)
    age_cross: Vec<(String, String, f64)>,
    /// 雇用形態×性別 (workstyle, gender, row_pct)
    gender_cross: Vec<(String, String, f64)>,
    /// 雇用形態×就業状態 (workstyle, employment_status, row_pct)
    employment_cross: Vec<(String, String, f64)>,
    /// 雇用形態×移動パターン (workstyle, mobility, count)
    mobility: Vec<(String, String, i64)>,
}

impl Default for WorkstyleStats {
    fn default() -> Self {
        Self {
            distribution: Vec::new(),
            age_cross: Vec::new(),
            gender_cross: Vec::new(),
            employment_cross: Vec::new(),
            mobility: Vec::new(),
        }
    }
}

async fn fetch_workstyle(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> WorkstyleStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    // [WORKAROUND] municipalityをSELECTに追加: WORKSTYLE_MOBILITYデータの重複排除に必要
    // 背景: Python側generate_mapcomplete_complete_sheets.pyのWORKSTYLE_MOBILITY生成で
    // pref_flow（ResidenceFlow）の細粒度行を直接ループしているため、
    // 同一(municipality, workstyle, mobility_type)の組み合わせがN重複してDBに格納されている。
    // また、countがmobility_typeに依存せず同一市区町村内で全mobility_typeが同じ値になるバグもある。
    // Python側の修正(groupby集約)は済んでいるがCSV再生成・DB再投入が未実施のため、
    // Rust側で暫定的に重複排除を行う。
    // → Python側修正: generate_mapcomplete_complete_sheets.py の WORKSTYLE_MOBILITY セクション
    // → 本ワークアラウンドは再投入完了後に除去可能（ただし残しても無害）
    let sql = format!(
        "SELECT row_type, category1, category2, count, percentage, municipality \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type IN ('WORKSTYLE_DISTRIBUTION', 'WORKSTYLE_AGE_CROSS', \
                           'WORKSTYLE_GENDER_CROSS', 'WORKSTYLE_EMPLOYMENT_STATUS', \
                           'WORKSTYLE_MOBILITY'){location_filter}"
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
    // クロス集計はcountから行内パーセンテージを動的計算（NiceGUI準拠）
    let mut age_cross_counts: HashMap<(String, String), i64> = HashMap::new();
    let mut gender_cross_counts: HashMap<(String, String), i64> = HashMap::new();
    let mut emp_cross_counts: HashMap<(String, String), i64> = HashMap::new();
    // [WORKAROUND] WORKSTYLE_MOBILITY重複排除用
    // (municipality, workstyle, mobility_type) → count で重複排除し、
    // その後 (workstyle, mobility_type) で市区町村横断集約する
    let mut mobility_dedup: HashMap<(String, String, String), i64> = HashMap::new();

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
                    *age_cross_counts.entry((cat1, cat2)).or_insert(0) += cnt;
                }
            }
            "WORKSTYLE_GENDER_CROSS" => {
                if !cat1.is_empty() && !cat2.is_empty() {
                    *gender_cross_counts.entry((cat1, cat2)).or_insert(0) += cnt;
                }
            }
            "WORKSTYLE_EMPLOYMENT_STATUS" => {
                if !cat1.is_empty() && !cat2.is_empty() {
                    *emp_cross_counts.entry((cat1, cat2)).or_insert(0) += cnt;
                }
            }
            "WORKSTYLE_MOBILITY" => {
                if !cat1.is_empty() && !cat2.is_empty() {
                    // [WORKAROUND] 重複排除: 同一(municipality, workstyle, mobility_type)は
                    // 最初の1件のみ保持。N重複しているが全て同じcount値なのでor_insertで十分。
                    let muni = get_str(row, "municipality");
                    mobility_dedup.entry((muni, cat1, cat2)).or_insert(cnt);
                }
            }
            _ => {}
        }
    }

    // [WORKAROUND] 重複排除後、(workstyle, mobility_type)で市区町村横断集約
    let mut mobility_agg: HashMap<(String, String), i64> = HashMap::new();
    for ((_, ws, mob), cnt) in mobility_dedup {
        *mobility_agg.entry((ws, mob)).or_insert(0) += cnt;
    }
    for ((ws, mob), cnt) in mobility_agg {
        stats.mobility.push((ws, mob, cnt));
    }

    let mut ws_list: Vec<(String, i64)> = ws_map.into_iter().collect();
    ws_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.distribution = ws_list;

    // 行内パーセンテージを動的計算（各雇用形態内での比率）
    fn compute_row_pct(counts: HashMap<(String, String), i64>) -> Vec<(String, String, f64)> {
        let mut ws_totals: HashMap<String, i64> = HashMap::new();
        for ((ws, _), cnt) in &counts {
            *ws_totals.entry(ws.clone()).or_insert(0) += cnt;
        }
        let mut result: Vec<(String, String, f64)> = counts.into_iter()
            .map(|((ws, cat), cnt)| {
                let total = ws_totals.get(&ws).copied().unwrap_or(1).max(1);
                let pct = (cnt as f64 / total as f64) * 100.0;
                (ws, cat, pct)
            })
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        result
    }

    stats.age_cross = compute_row_pct(age_cross_counts);
    stats.gender_cross = compute_row_pct(gender_cross_counts);
    stats.employment_cross = compute_row_pct(emp_cross_counts);

    stats
}

fn render_workstyle(job_type: &str, prefecture: &str, municipality: &str, stats: &WorkstyleStats) -> String {
    let location_label = make_location_label(prefecture, municipality);

    // ===== 雇用形態分布ドーナツ =====
    let ws_colors = |ws: &str| -> &str {
        match ws {
            "正職員" => "#009E73",
            "パート" => "#E69F00",
            _ => "#999999",
        }
    };

    let total: i64 = stats.distribution.iter().map(|(_, c)| c).sum();

    let ws_pie: Vec<String> = stats.distribution.iter().map(|(w, v)| {
        format!(r#"{{"value": {}, "name": "{}", "itemStyle": {{"color": "{}"}}}}"#, v, w, ws_colors(w))
    }).collect();

    // ===== KPIカード =====
    let kpi_cards: Vec<String> = stats.distribution.iter().map(|(ws, cnt)| {
        let pct = if total > 0 { (*cnt as f64 / total as f64) * 100.0 } else { 0.0 };
        let color = ws_colors(ws);
        format!(
            r#"<div class="stat-card" style="border-left: 4px solid {};">
                <div class="text-sm font-semibold text-white">{}</div>
                <div class="text-xs text-slate-400">{}人 ({:.1}%)</div>
            </div>"#,
            color, ws, format_num(*cnt), pct
        )
    }).collect();

    // ===== 雇用形態×年代 スタック棒グラフ =====
    let workstyle_order = ["正職員", "パート", "その他"];
    let age_order = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];

    // age_crossからピボットテーブルを構築
    let mut age_pivot: HashMap<(&str, &str), f64> = HashMap::new();
    for (ws, age, pct) in &stats.age_cross {
        age_pivot.insert((ws.as_str(), age.as_str()), *pct);
    }

    let age_series: Vec<String> = workstyle_order.iter().map(|ws| {
        let data: Vec<String> = age_order.iter().map(|age| {
            let val = age_pivot.get(&(*ws, *age)).copied().unwrap_or(0.0);
            format!("{:.1}", val)
        }).collect();
        let color = ws_colors(ws);
        format!(
            r##"{{"name": "{}", "type": "bar", "stack": "total", "data": [{}], "itemStyle": {{"color": "{}"}}, "label": {{"show": true, "formatter": "{{c}}%", "color": "#fff", "fontSize": 10}}}}"##,
            ws, data.join(","), color
        )
    }).collect();

    // ===== 雇用形態×性別 グループ棒グラフ =====
    let mut gender_map: HashMap<(&str, &str), f64> = HashMap::new();
    for (ws, gender, pct) in &stats.gender_cross {
        gender_map.insert((ws.as_str(), gender.as_str()), *pct);
    }

    let gender_male: Vec<String> = workstyle_order.iter().map(|ws| {
        let val = gender_map.get(&(*ws, "男性")).copied().unwrap_or(0.0);
        format!("{:.1}", val)
    }).collect();

    let gender_female: Vec<String> = workstyle_order.iter().map(|ws| {
        let val = gender_map.get(&(*ws, "女性")).copied().unwrap_or(0.0);
        format!("{:.1}", val)
    }).collect();

    // ===== 雇用形態×就業状態 スタック棒グラフ =====
    let emp_statuses = ["就業中", "離職中", "在学中"];
    let emp_colors = |e: &str| -> &str {
        match e {
            "就業中" => "#009E73",
            "離職中" => "#CC79A7",
            "在学中" => "#F0E442",
            _ => "#666666",
        }
    };

    let mut emp_map: HashMap<(&str, &str), f64> = HashMap::new();
    for (ws, emp, pct) in &stats.employment_cross {
        emp_map.insert((ws.as_str(), emp.as_str()), *pct);
    }

    let emp_series: Vec<String> = emp_statuses.iter().map(|emp| {
        let data: Vec<String> = workstyle_order.iter().map(|ws| {
            let val = emp_map.get(&(*ws, *emp)).copied().unwrap_or(0.0);
            format!("{:.1}", val)
        }).collect();
        let color = emp_colors(emp);
        format!(
            r##"{{"name": "{}", "type": "bar", "stack": "total", "data": [{}], "itemStyle": {{"color": "{}"}}, "label": {{"show": true, "formatter": "{{c}}%", "color": "#fff", "fontSize": 10}}}}"##,
            emp, data.join(","), color
        )
    }).collect();

    // ===== 雇用形態×移動パターン =====
    // [一時非表示] CSV再生成後に復活: let mobility_section = build_mobility_section(stats);
    let _mobility_section = build_mobility_section(stats);

    include_str!("../../templates/tabs/workstyle.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{LOCATION_LABEL}}", &location_label)
        .replace("{{WS_PIE_DATA}}", &ws_pie.join(","))
        .replace("{{WS_KPI_CARDS}}", &kpi_cards.join("\n"))
        .replace("{{AGE_CROSS_SERIES}}", &age_series.join(","))
        .replace("{{GENDER_MALE_DATA}}", &format!("[{}]", gender_male.join(",")))
        .replace("{{GENDER_FEMALE_DATA}}", &format!("[{}]", gender_female.join(",")))
        .replace("{{EMP_CROSS_SERIES}}", &emp_series.join(","))
        // [一時非表示] WORKSTYLE_MOBILITYデータ未修正のため空文字（CSV再生成・再インポート後に復活）
        // 復活時: .replace("{{MOBILITY_CARD}}", &format!(r#"<div class=\"stat-card\">...{{MOBILITY_SECTION}}...</div>"#))
        .replace("{{MOBILITY_CARD}}", "")
}

fn build_mobility_section(stats: &WorkstyleStats) -> String {
    if stats.mobility.is_empty() {
        return r#"<p class="text-slate-500 text-sm">WORKSTYLE_MOBILITYデータなし（Tursoへのインポートが必要です）</p>"#.to_string();
    }

    let workstyles = ["正職員", "パート", "その他"];
    let mobilities = ["地元志向", "近隣移動", "中距離移動", "遠距離移動"];

    // ヒートマップデータ構築
    let mut mob_map: HashMap<(&str, &str), i64> = HashMap::new();
    let mut ws_totals: HashMap<&str, i64> = HashMap::new();
    let mut mob_totals: HashMap<&str, i64> = HashMap::new();

    // [WORKAROUND] fetch_workstyleで重複排除済みだが、安全のため加算方式で集約
    for (ws, mob, cnt) in &stats.mobility {
        *mob_map.entry((ws.as_str(), mob.as_str())).or_insert(0) += cnt;
        *ws_totals.entry(ws.as_str()).or_insert(0) += cnt;
        *mob_totals.entry(mob.as_str()).or_insert(0) += cnt;
    }

    // ヒートマップJSON
    let mut heatmap_data: Vec<String> = Vec::new();
    let mut max_val: i64 = 0;
    for (i, ws) in workstyles.iter().enumerate() {
        for (j, mob) in mobilities.iter().enumerate() {
            let val = mob_map.get(&(*ws, *mob)).copied().unwrap_or(0);
            if val > max_val { max_val = val; }
            heatmap_data.push(format!("[{}, {}, {}]", j, i, val));
        }
    }

    let ws_labels: Vec<String> = workstyles.iter().map(|w| format!("\"{}\"", w)).collect();
    let mob_labels: Vec<String> = mobilities.iter().map(|m| format!("\"{}\"", m)).collect();

    // 移動パターン別棒グラフデータ
    let mob_colors = |m: &str| -> &str {
        match m {
            "地元志向" => "#009E73",
            "近隣移動" => "#56B4E9",
            "中距離移動" => "#E69F00",
            "遠距離移動" => "#D55E00",
            _ => "#666666",
        }
    };

    let bar_data: Vec<String> = mobilities.iter().map(|mob| {
        let val = mob_totals.get(mob).copied().unwrap_or(0);
        let color = mob_colors(mob);
        format!(r#"{{"value": {}, "itemStyle": {{"color": "{}"}}}}"#, val, color)
    }).collect();

    // KPIサマリー（雇用形態別）
    let kpi_cards: Vec<String> = workstyles.iter().map(|ws| {
        let total = ws_totals.get(ws).copied().unwrap_or(0);
        format!(
            r#"<div class="stat-card text-center" style="flex: 1;">
                <div class="text-sm font-bold text-white">{}</div>
                <div class="text-xl font-bold text-blue-400">{}</div>
            </div>"#,
            ws, format_num(total)
        )
    }).collect();

    // ヒートマップのJSON設定を構築
    let heatmap_config = format!(
        r##"{{
            "tooltip": {{"position": "top", "formatter": "{{{{c}}}}人"}},
            "grid": {{"left": "15%", "right": "10%", "bottom": "15%", "top": "5%"}},
            "xAxis": {{"type": "category", "data": [{}], "axisLabel": {{"rotate": 20, "fontSize": 10}}}},
            "yAxis": {{"type": "category", "data": [{}]}},
            "visualMap": {{
                "min": 0, "max": {}, "calculable": true,
                "orient": "horizontal", "left": "center", "bottom": "0%",
                "inRange": {{"color": ["#1a237e", "#303f9f", "#3f51b5", "#7986cb", "#c5cae9"]}}
            }},
            "series": [{{"name": "人数", "type": "heatmap", "data": [{}], "label": {{"show": true, "color": "#fff", "fontSize": 10}}}}]
        }}"##,
        mob_labels.join(","),
        ws_labels.join(","),
        max_val,
        heatmap_data.join(","),
    );

    // 棒グラフのJSON設定を構築
    let bar_config = format!(
        r##"{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "grid": {{"left": "5%", "right": "5%", "bottom": "10%", "top": "10%", "containLabel": true}},
            "xAxis": {{"type": "category", "data": [{}], "axisLabel": {{"rotate": 20, "fontSize": 10}}}},
            "yAxis": {{"type": "value"}},
            "series": [{{"type": "bar", "data": [{}], "label": {{"show": true, "position": "top", "fontSize": 10}}}}]
        }}"##,
        mob_labels.join(","),
        bar_data.join(","),
    );

    format!(
        r##"<div class="flex flex-col md:flex-row gap-4">
            <div class="flex-1">
                <div class="echart" style="height:250px;" data-chart-config='{}'></div>
            </div>
            <div class="flex-1">
                <div class="echart" style="height:250px;" data-chart-config='{}'></div>
            </div>
        </div>
        <div class="flex gap-4 mt-4">
            {}
        </div>"##,
        heatmap_config,
        bar_config,
        kpi_cards.join("\n")
    )
}

/// 数値をカンマ区切りでフォーマット
fn format_num(n: i64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut result = String::with_capacity(len + len / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}
