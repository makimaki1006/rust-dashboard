use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;
use super::competitive::escape_html;
use super::overview::{format_number, get_session_filters};

/// セグメント分析パラメータ
#[derive(Deserialize)]
pub struct SegmentParams {
    pub prefecture: Option<String>,
    pub municipality: Option<String>,
    pub employment_type: Option<String>,
    pub facility_type: Option<String>,
}

/// DB職種名 → セグメントDB職種名へのマッピング（tags/text_features用）
fn map_job_type_to_segment(job_type: &str) -> Option<&str> {
    match job_type {
        "看護師" => Some("看護師・准看護師"),
        "介護職" => Some("介護職・ヘルパー"),
        "保育士" => Some("保育士"),
        "栄養士" => Some("管理栄養士・栄養士"),
        "生活相談員" => Some("生活相談員"),
        "理学療法士" => Some("理学療法士"),
        "作業療法士" => Some("作業療法士"),
        "ケアマネジャー" => Some("ケアマネジャー"),
        "サービス管理責任者" => Some("サービス管理責任者"),
        "サービス提供責任者" => Some("サービス提供責任者"),
        "学童支援" => Some("放課後児童支援員・学童指導員"),
        "調理師、調理スタッフ" => Some("調理師・調理スタッフ"),
        "薬剤師" => Some("薬剤師"),
        "言語聴覚士" => Some("言語聴覚士"),
        "児童指導員" => Some("児童指導員"),
        "児童発達支援管理責任者" => Some("児童発達支援管理責任者"),
        "生活支援員" => Some("生活支援員"),
        _ => None,
    }
}

/// Tier2ラベル定数（segment_classifier.py:440-450 と同期）
const TIER2_LABELS: &[(&str, &str)] = &[
    ("A1", "完全未経験歓迎"),
    ("A2", "未経験可（資格あり）"),
    ("A3", "軽度経験（1-2年）"),
    ("A4", "即戦力経験者"),
    ("A5", "復職・ブランク者"),
    ("B1", "新卒・第二新卒"),
    ("B2", "若手成長層"),
    ("B3", "ミドル層"),
    ("B4", "シニア層"),
    ("B5", "年齢不問・幅広い層"),
    ("C1", "フルタイム・キャリア志向"),
    ("C2", "WLB重視"),
    ("C3", "子育て・家庭両立型"),
    ("C4", "Wワーク・副業・短時間"),
    ("C5", "安定・長期就業型"),
    ("D1", "収入アップ訴求"),
    ("D2", "安定性・規模訴求"),
    ("D3", "理念・やりがい訴求"),
    ("D4", "職場環境訴求"),
    ("D5", "利便性訴求"),
    ("D6", "成長・スキルアップ訴求"),
    ("D7", "条件・待遇訴求"),
    ("E1", "緊急大量採用"),
    ("E2", "積極採用"),
    ("E3", "通常採用"),
    ("E4", "厳選採用"),
    ("E5", "静かな募集"),
];

/// 軸名の日本語ラベル
fn axis_label(axis: &str) -> &str {
    match axis {
        "A" => "経験レベル",
        "B" => "キャリアステージ",
        "C" => "ライフスタイル",
        "D" => "訴求軸",
        "E" => "採用姿勢",
        _ => axis,
    }
}

/// Tier2カテゴリコードからラベルを取得
fn tier2_label(code: &str) -> &str {
    TIER2_LABELS
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, l)| *l)
        .unwrap_or(code)
}

/// 軸のテーマカラー
fn axis_color(axis: &str) -> &str {
    match axis {
        "A" => "#10b981",
        "B" => "#3b82f6",
        "C" => "#f59e0b",
        "D" => "#8b5cf6",
        "E" => "#ef4444",
        _ => "#64748b",
    }
}

/// job_postingsテーブル用WHERE句ビルダー
fn build_postings_where(
    job_type: &str,
    pref: &str,
    muni: &str,
    emp: &str,
    facility_types: &[String],
) -> (String, Vec<String>) {
    let mut clauses = vec!["job_type = ?".to_string()];
    let mut params = vec![job_type.to_string()];
    if !pref.is_empty() {
        clauses.push("prefecture = ?".to_string());
        params.push(pref.to_string());
    }
    if !muni.is_empty() {
        clauses.push("municipality = ?".to_string());
        params.push(muni.to_string());
    }
    if !emp.is_empty() && emp != "全て" {
        clauses.push("employment_type = ?".to_string());
        params.push(emp.to_string());
    }
    if !facility_types.is_empty() {
        // カテゴリ名→LIKEパターンに変換
        let like_patterns = facility_category_to_like_patterns(facility_types);
        if !like_patterns.is_empty() {
            let like_clauses: Vec<String> = like_patterns.iter().map(|_| "facility_type LIKE ?".to_string()).collect();
            clauses.push(format!("({})", like_clauses.join(" OR ")));
            for pat in &like_patterns {
                params.push(pat.clone());
            }
        }
    }
    let where_clause = format!("WHERE {}", clauses.join(" AND "));
    (where_clause, params)
}

/// カテゴリ名からSQLのLIKEパターンリストを生成
fn facility_category_to_like_patterns(categories: &[String]) -> Vec<String> {
    let mut patterns = Vec::new();
    for cat in categories {
        match cat.as_str() {
            "訪問系" => {
                patterns.push("%訪問%".to_string());
            }
            "通所系" => {
                patterns.push("%通所%".to_string());
                patterns.push("%デイサービス%".to_string());
            }
            "入所系" => {
                patterns.push("%特別養護%".to_string());
                patterns.push("%有料老人%".to_string());
                patterns.push("%グループホーム%".to_string());
                patterns.push("%老人保健%".to_string());
                patterns.push("%小規模多機能%".to_string());
            }
            "病院・クリニック" => {
                patterns.push("%病院%".to_string());
                patterns.push("%クリニック%".to_string());
                patterns.push("%診療所%".to_string());
            }
            "保育・教育" => {
                patterns.push("%保育%".to_string());
                patterns.push("%幼稚園%".to_string());
                patterns.push("%学校%".to_string());
            }
            "障害福祉" => {
                patterns.push("%障害%".to_string());
                patterns.push("%放課後%".to_string());
            }
            "薬局" => {
                patterns.push("%薬局%".to_string());
                patterns.push("%ドラッグ%".to_string());
            }
            "その他" => {
                patterns.push("%その他%".to_string());
            }
            _ => {
                // 未知のカテゴリはそのまま部分一致
                patterns.push(format!("%{}%", cat));
            }
        }
    }
    patterns
}

/// facility_typeパラメータをカンマ区切りでVecに変換
fn parse_facility_types(raw: &str) -> Vec<String> {
    if raw.is_empty() || raw == "全て" {
        Vec::new()
    } else {
        raw.split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
}

/// フィルタ情報を含むスコープラベル
fn build_scope_label_ext(pref: &str, muni: &str, emp: &str, ftypes: &[String]) -> String {
    let mut label = build_scope_label(pref, muni);
    if !emp.is_empty() && emp != "全て" {
        label.push_str(&format!(" × {}", emp));
    }
    if !ftypes.is_empty() {
        if ftypes.len() == 1 {
            label.push_str(&format!(" × {}", ftypes[0]));
        } else {
            label.push_str(&format!(" × 施設形態{}件", ftypes.len()));
        }
    }
    label
}

// =============================================================
// API 1: /api/segment/overview → 5軸分布（local_dbから直接集計）
// =============================================================

pub async fn segment_overview(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let db = match &state.local_db {
        Some(db) => db,
        None => return Html(r#"<p class="text-red-400 text-sm">ローカルDBが利用できません</p>"#.to_string()),
    };

    let pref = params.prefecture.as_deref().unwrap_or("");
    let muni = params.municipality.as_deref().unwrap_or("");
    let emp = params.employment_type.as_deref().unwrap_or("");
    let ftypes = parse_facility_types(params.facility_type.as_deref().unwrap_or(""));

    let (where_clause, base_params) = build_postings_where(&job_type, pref, muni, emp, &ftypes);

    // 5軸をUNION ALLで1クエリ取得
    let axis_columns = [
        ("A", "tier1_experience"),
        ("B", "tier1_career_stage"),
        ("C", "tier1_lifestyle"),
        ("D", "tier1_appeal"),
        ("E", "tier1_urgency"),
    ];

    let mut union_parts = Vec::new();
    let mut all_params: Vec<String> = Vec::new();
    for (axis_code, col) in &axis_columns {
        union_parts.push(format!(
            "SELECT '{}' as axis, {} as category, COUNT(*) as count \
             FROM job_postings {} AND {} != '' GROUP BY {}",
            axis_code, col, where_clause, col, col
        ));
        all_params.extend_from_slice(&base_params);
    }
    let sql = union_parts.join(" UNION ALL ");

    let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match db.query(&sql, &params_ref) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Segment overview query failed: {e}");
            return Html(format!(r#"<p class="text-red-400 text-sm">クエリエラー: {}</p>"#, escape_html(&e)));
        }
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    // 軸ごとにグループ化
    let mut axes: std::collections::BTreeMap<String, Vec<(String, String, i64)>> = std::collections::BTreeMap::new();
    let mut totals: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for row in &rows {
        let axis = row.get("axis").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let category = row.get("category").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        axes.entry(axis.clone()).or_default().push((category.clone(), tier2_label(&category).to_string(), count));
        *totals.entry(axis).or_insert(0) += count;
    }
    // 各軸内をcount降順でソート
    for cats in axes.values_mut() {
        cats.sort_by(|a, b| b.2.cmp(&a.2));
    }

    let scope_label = build_scope_label_ext(pref, muni, emp, &ftypes);

    // レーダーチャート: 各軸のトップカテゴリ比率
    let radar_indicators: Vec<String> = ["A", "B", "C", "D", "E"]
        .iter()
        .map(|a| format!(r#"{{"name":"{}","max":100}}"#, axis_label(a)))
        .collect();
    let radar_values: Vec<String> = ["A", "B", "C", "D", "E"]
        .iter()
        .map(|a| {
            if let Some(cats) = axes.get(*a) {
                if let Some(total) = totals.get(*a) {
                    if *total > 0 {
                        if let Some((_, _, count)) = cats.first() {
                            return format!("{:.1}", *count as f64 / *total as f64 * 100.0);
                        }
                    }
                }
            }
            "0".to_string()
        })
        .collect();

    // 軸別棒グラフHTML
    let mut axis_charts = String::new();
    for axis_code in &["A", "B", "C", "D", "E"] {
        if let Some(cats) = axes.get(*axis_code) {
            let total = totals.get(*axis_code).copied().unwrap_or(1).max(1);
            let labels: Vec<String> = cats
                .iter()
                .map(|(cat, _label, _)| format!(r#""{}""#, escape_html(tier2_label(cat))))
                .collect();
            let values: Vec<String> = cats.iter().map(|(_, _, c)| c.to_string()).collect();
            // 上位カテゴリのサマリーテキスト
            let top_cat = cats.first().map(|(c, _, cnt)| {
                let pct = *cnt as f64 / total as f64 * 100.0;
                format!("{} ({:.0}%)", tier2_label(c), pct)
            }).unwrap_or_default();

            axis_charts.push_str(&format!(
                r##"<div class="stat-card">
                    <div class="flex items-center gap-2 mb-2">
                        <span class="inline-block w-3 h-3 rounded-full" style="background:{color}"></span>
                        <h4 class="text-sm text-slate-400">軸{axis}: {axis_name}</h4>
                        <span class="text-xs text-slate-500 ml-auto">{top_cat}</span>
                    </div>
                    <div class="echart" style="height:220px;" data-chart-config='{{
                        "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
                        "xAxis": {{"type": "value", "axisLabel": {{"color": "#94a3b8", "fontSize": 10}}}},
                        "yAxis": {{"type": "category", "data": [{labels}], "inverse": true, "axisLabel": {{"color": "#94a3b8", "fontSize": 11, "width": 130, "overflow": "truncate"}}}},
                        "series": [{{"type": "bar", "data": [{values}],
                            "itemStyle": {{"color": "{color}", "borderRadius": [0,4,4,0]}}, "barWidth": "70%"
                        }}],
                        "grid": {{"left": "35%", "right": "5%", "top": "8px", "bottom": "8px", "containLabel": false}}
                    }}'></div>
                </div>"##,
                color = axis_color(axis_code),
                axis = axis_code,
                axis_name = axis_label(axis_code),
                top_cat = escape_html(&top_cat),
                labels = labels.join(","),
                values = values.join(","),
            ));
        }
    }

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">🎯 求人セグメント分析 <span class="text-sm font-normal text-slate-400">（{scope} / {job_type}）</span></h3>

    <!-- レーダーチャート -->
    <div class="stat-card">
        <h4 class="text-sm text-slate-400 mb-2">5軸バランス（各軸トップカテゴリの比率）</h4>
        <div class="echart" style="height:400px;" data-chart-config='{{
            "tooltip": {{}},
            "radar": {{
                "indicator": [{indicators}],
                "shape": "polygon",
                "splitArea": {{"areaStyle": {{"color": ["rgba(30,41,59,0.3)","rgba(30,41,59,0.5)"]}}}},
                "axisName": {{"color": "#94a3b8", "fontSize": 11}},
                "splitLine": {{"lineStyle": {{"color": "rgba(148,163,184,0.2)"}}}}
            }},
            "series": [{{
                "type": "radar",
                "data": [{{
                    "value": [{values}],
                    "name": "トップカテゴリ比率(%)",
                    "areaStyle": {{"color": "rgba(99,102,241,0.3)"}},
                    "lineStyle": {{"color": "#6366f1", "width": 2}},
                    "itemStyle": {{"color": "#6366f1"}}
                }}]
            }}]
        }}'></div>
    </div>

    <!-- 軸別詳細 -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        {axis_charts}
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        indicators = radar_indicators.join(","),
        values = radar_values.join(","),
        axis_charts = axis_charts,
    );

    Html(html)
}

// =============================================================
// API 2: /api/segment/tier3 → Tier1/Tier2/Tier3分布の統合可視化
// =============================================================

pub async fn segment_tier3(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let db = match &state.local_db {
        Some(db) => db,
        None => return Html(r#"<p class="text-red-400 text-sm">ローカルDBが利用できません</p>"#.to_string()),
    };

    let pref = params.prefecture.as_deref().unwrap_or("");
    let muni = params.municipality.as_deref().unwrap_or("");
    let emp = params.employment_type.as_deref().unwrap_or("");
    let ftypes = parse_facility_types(params.facility_type.as_deref().unwrap_or(""));

    let (where_clause, param_values) = build_postings_where(&job_type, pref, muni, emp, &ftypes);

    let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    // 総件数（tier3_idあり）を取得
    let total_sql = format!(
        "SELECT COUNT(*) FROM job_postings {} AND tier3_id != ''",
        where_clause
    );
    let total: i64 = db.query_scalar::<i64>(&total_sql, &params_ref)
        .unwrap_or(1)
        .max(1);

    // --- Tier1: 5軸の分布（tier3_idの先頭文字で集計）---
    let tier1_sql = format!(
        "SELECT SUBSTR(tier3_id, 1, 1) as axis, \
         CAST(SUBSTR(tier3_id, 2, 1) AS INTEGER) as level, \
         COUNT(*) as count \
         FROM job_postings {} AND tier3_id != '' AND LENGTH(tier3_id) >= 5 \
         GROUP BY axis, level ORDER BY axis, level",
        where_clause
    );
    let tier1_rows = db.query(&tier1_sql, &params_ref).unwrap_or_default();

    // 軸別にTier1分布を集計
    let mut tier1_data: std::collections::HashMap<String, Vec<(i64, i64)>> = std::collections::HashMap::new();
    for row in &tier1_rows {
        let axis = row.get("axis").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let level = row.get("level").and_then(|v| v.as_i64()).unwrap_or(0);
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        tier1_data.entry(axis).or_default().push((level, count));
    }

    // 5軸ドーナツチャート生成
    let axes = ["A", "B", "C", "D", "E"];
    let axis_colors = [
        ["#10b981", "#34d399", "#6ee7b7", "#a7f3d0", "#d1fae5"],
        ["#3b82f6", "#60a5fa", "#93c5fd", "#bfdbfe", "#dbeafe"],
        ["#f59e0b", "#fbbf24", "#fcd34d", "#fde68a", "#fef3c7"],
        ["#ec4899", "#f472b6", "#f9a8d4", "#fbcfe8", "#fce7f3"],
        ["#8b5cf6", "#a78bfa", "#c4b5fd", "#ddd6fe", "#ede9fe"],
    ];

    let mut tier1_charts = String::new();
    for (i, axis) in axes.iter().enumerate() {
        let ax_label = axis_label(axis);
        let data = tier1_data.get(*axis);
        let mut pie_data = String::new();
        if let Some(levels) = data {
            for (level, count) in levels {
                let code = format!("{}{}", axis, level);
                let label = tier2_label(&code);
                if !pie_data.is_empty() { pie_data.push(','); }
                pie_data.push_str(&format!(
                    r#"{{"value":{},"name":"{}"}}"#, count, escape_html(label)
                ));
            }
        }
        let colors_str = axis_colors[i].iter()
            .map(|c| format!(r#""{}""#, c))
            .collect::<Vec<_>>()
            .join(",");

        tier1_charts.push_str(&format!(
            r##"<div class="stat-card">
    <h4 class="text-sm text-slate-400 mb-2">{ax_label}（軸{axis}）</h4>
    <div class="echart" style="height:260px;" data-chart-config='{{
        "tooltip": {{"trigger": "item", "formatter": "{{b}}: {{c}}件 ({{d}}%)"}},
        "legend": {{"bottom": 0, "textStyle": {{"color": "#94a3b8", "fontSize": 9}}, "itemWidth": 10, "itemHeight": 10}},
        "color": [{colors_str}],
        "series": [{{
            "type": "pie", "radius": ["38%","68%"], "center": ["50%","44%"],
            "label": {{"formatter": "{{d}}%", "color": "#e2e8f0", "fontSize": 11, "position": "outside"}},
            "data": [{pie_data}]
        }}]
    }}'></div>
</div>"##,
            ax_label = escape_html(ax_label),
            axis = axis,
            colors_str = colors_str,
            pie_data = pie_data,
        ));
    }

    // --- Tier2: 27カテゴリの全体バーチャート（軸別色分け）---
    let tier2_sql = format!(
        "SELECT SUBSTR(tier3_id, 1, 2) as code, COUNT(*) as count \
         FROM job_postings {} AND tier3_id != '' AND LENGTH(tier3_id) >= 5 \
         GROUP BY code ORDER BY code",
        where_clause
    );
    let tier2_rows = db.query(&tier2_sql, &params_ref).unwrap_or_default();

    let mut tier2_labels = Vec::new();
    let mut tier2_values = Vec::new();
    let mut tier2_colors = Vec::new();
    for row in &tier2_rows {
        let code = row.get("code").and_then(|v| v.as_str()).unwrap_or("");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let label = tier2_label(code);
        let color = match code.chars().next() {
            Some('A') => "#10b981",
            Some('B') => "#3b82f6",
            Some('C') => "#f59e0b",
            Some('D') => "#ec4899",
            Some('E') => "#8b5cf6",
            _ => "#64748b",
        };
        tier2_labels.push(format!(r#""{}""#, escape_html(label)));
        tier2_values.push(format!(r#"{{"value":{},"itemStyle":{{"color":"{}"}}}}"#, count, color));
        tier2_colors.push(color);
    }

    // --- Tier3: TOP20 ---
    let sql = format!(
        "SELECT tier3_id, tier3_label_short, COUNT(*) as count \
         FROM job_postings {} AND tier3_id != '' \
         GROUP BY tier3_id, tier3_label_short ORDER BY count DESC LIMIT 20",
        where_clause
    );

    let rows = match db.query(&sql, &params_ref) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Segment tier3 query failed: {e}");
            return Html(format!(r#"<p class="text-red-400 text-sm">クエリエラー: {}</p>"#, escape_html(&e)));
        }
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    let scope_label = build_scope_label_ext(pref, muni, emp, &ftypes);

    // Tier3 バーチャート（TOP10）
    let t3_labels: Vec<String> = rows.iter().take(10).map(|r| {
        let label = r.get("tier3_label_short").and_then(|v| v.as_str()).unwrap_or("不明");
        format!(r#""{}""#, escape_html(label))
    }).collect();
    let t3_values: Vec<String> = rows.iter().take(10).map(|r| {
        r.get("count").and_then(|v| v.as_i64()).unwrap_or(0).to_string()
    }).collect();

    // Tier3 ドーナツ（TOP10割合）
    let mut t3_pie_data = String::new();
    let mut top10_total: i64 = 0;
    for row in rows.iter().take(10) {
        let label = row.get("tier3_label_short").and_then(|v| v.as_str()).unwrap_or("不明");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        top10_total += count;
        if !t3_pie_data.is_empty() { t3_pie_data.push(','); }
        t3_pie_data.push_str(&format!(r#"{{"value":{},"name":"{}"}}"#, count, escape_html(label)));
    }
    // 残りを「その他」として追加
    let others = total - top10_total;
    if others > 0 {
        if !t3_pie_data.is_empty() { t3_pie_data.push(','); }
        t3_pie_data.push_str(&format!(r#"{{"value":{},"name":"その他"}}"#, others));
    }

    // Tier3テーブル（TOP20）
    let mut table_rows = String::new();
    for (i, row) in rows.iter().enumerate() {
        let tier3_id = row.get("tier3_id").and_then(|v| v.as_str()).unwrap_or("");
        let label = row.get("tier3_label_short").and_then(|v| v.as_str()).unwrap_or("");
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let ratio = count as f64 / total as f64 * 100.0;
        // 比率バー
        let bar_width = (ratio * 2.0).min(100.0);
        table_rows.push_str(&format!(
            r#"<tr><td class="text-center">{rank}</td><td class="text-xs"><code class="text-indigo-300">{id}</code></td><td>{label}</td><td class="text-right">{count}</td><td class="text-right">{ratio:.1}%</td><td><div class="bg-slate-700 rounded-full h-2 w-full"><div class="bg-indigo-500 rounded-full h-2" style="width:{bar_w}%"></div></div></td></tr>"#,
            rank = i + 1,
            id = escape_html(tier3_id),
            label = escape_html(label),
            count = format_number(count),
            ratio = ratio,
            bar_w = bar_width,
        ));
    }

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">📊 Tier分布分析 <span class="text-sm font-normal text-slate-400">（{scope} / {job_type} / {total}件）</span></h3>

    <!-- Tier1: 5軸の分布ドーナツ -->
    <div class="stat-card">
        <h4 class="text-md font-semibold text-white mb-3">Tier1: 5軸の分布</h4>
        <p class="text-xs text-slate-400 mb-3">各軸（経験・キャリア・ライフスタイル・訴求・採用姿勢）のスコア分布</p>
        <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
            {tier1_charts}
        </div>
    </div>

    <!-- Tier2: 27カテゴリのバーチャート -->
    <div class="stat-card">
        <h4 class="text-md font-semibold text-white mb-3">Tier2: 27カテゴリの分布</h4>
        <p class="text-xs text-slate-400 mb-3">
            <span style="color:#10b981">■</span> 経験(A)
            <span style="color:#3b82f6">■</span> キャリア(B)
            <span style="color:#f59e0b">■</span> ライフスタイル(C)
            <span style="color:#ec4899">■</span> 訴求(D)
            <span style="color:#8b5cf6">■</span> 採用姿勢(E)
        </p>
        <div class="echart" style="height:600px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "xAxis": {{"type": "value", "axisLabel": {{"color": "#94a3b8"}}}},
            "yAxis": {{"type": "category", "data": [{tier2_labels}], "inverse": true, "axisLabel": {{"color": "#94a3b8", "fontSize": 11, "width": 180, "overflow": "truncate"}}}},
            "series": [{{
                "type": "bar", "data": [{tier2_values}],
                "barWidth": "65%",
                "label": {{"show": true, "position": "right", "color": "#94a3b8", "fontSize": 11}}
            }}],
            "grid": {{"left": "30%", "right": "10%", "top": "8px", "bottom": "8px", "containLabel": false}}
        }}'></div>
    </div>

    <!-- Tier3: TOP10 バーチャート + ドーナツ -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div class="stat-card">
            <h4 class="text-md font-semibold text-white mb-3">Tier3: TOP10 件数</h4>
            <div class="echart" style="height:400px;" data-chart-config='{{
                "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
                "xAxis": {{"type": "value", "axisLabel": {{"color": "#94a3b8"}}}},
                "yAxis": {{"type": "category", "data": [{t3_labels}], "inverse": true, "axisLabel": {{"color": "#94a3b8", "fontSize": 11, "width": 200, "overflow": "truncate"}}}},
                "series": [{{
                    "type": "bar", "data": [{t3_values}],
                    "itemStyle": {{"color": {{"type": "linear", "x": 0, "y": 0, "x2": 1, "y2": 0,
                        "colorStops": [{{"offset": 0, "color": "#6366f1"}}, {{"offset": 1, "color": "#8b5cf6"}}]
                    }}, "borderRadius": [0,4,4,0]}},
                    "barWidth": "65%"
                }}],
                "grid": {{"left": "38%", "right": "5%", "top": "8px", "bottom": "8px", "containLabel": false}}
            }}'></div>
        </div>
        <div class="stat-card">
            <h4 class="text-md font-semibold text-white mb-3">Tier3: 構成比率</h4>
            <div class="echart" style="height:400px;" data-chart-config='{{
                "tooltip": {{"trigger": "item", "formatter": "{{b}}: {{c}}件 ({{d}}%)"}},
                "legend": {{"type": "scroll", "bottom": 0, "textStyle": {{"color": "#94a3b8", "fontSize": 9}}, "pageTextStyle": {{"color": "#94a3b8"}}}},
                "color": ["#6366f1","#8b5cf6","#a78bfa","#c4b5fd","#818cf8","#6d28d9","#7c3aed","#9333ea","#a855f7","#c084fc","#64748b"],
                "series": [{{
                    "type": "pie", "radius": ["38%","68%"], "center": ["50%","44%"],
                    "label": {{"formatter": "{{d}}%", "color": "#e2e8f0", "fontSize": 11}},
                    "data": [{t3_pie_data}]
                }}]
            }}'></div>
        </div>
    </div>

    <!-- Tier3: TOP20 テーブル -->
    <div class="stat-card">
        <h4 class="text-md font-semibold text-white mb-3">Tier3パターン TOP20 詳細</h4>
        <div class="overflow-x-auto">
            <table class="data-table text-xs">
                <thead><tr>
                    <th class="text-center" style="width:40px">#</th>
                    <th>パターンID</th>
                    <th>ラベル</th>
                    <th class="text-right">件数</th>
                    <th class="text-right">比率</th>
                    <th style="width:120px">分布</th>
                </tr></thead>
                <tbody>{table_rows}</tbody>
            </table>
        </div>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        total = format_number(total),
        tier1_charts = tier1_charts,
        tier2_labels = tier2_labels.join(","),
        tier2_values = tier2_values.join(","),
        t3_labels = t3_labels.join(","),
        t3_values = t3_values.join(","),
        t3_pie_data = t3_pie_data,
        table_rows = table_rows,
    );

    Html(html)
}

// =============================================================
// API 3: /api/segment/tags → タグ出現率TOP15（segment_db継続使用）
// =============================================================

pub async fn segment_tags(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;
    let seg_job = match map_job_type_to_segment(&job_type) {
        Some(j) => j,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let db = match &state.segment_db {
        Some(db) => db,
        None => return Html(r#"<p class="text-red-400 text-sm">セグメントDBが利用できません</p>"#.to_string()),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };
    let ftype_raw = params.facility_type.as_deref().unwrap_or("");
    let has_facility_filter = !ftype_raw.is_empty() && ftype_raw != "全て";

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        db, "segment_tags", seg_job, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let (where_clause, param_values) = if !muni.is_empty() && !pref.is_empty() {
        (
            "WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ?",
            vec![seg_job.to_string(), emp_type.to_string(), pref.to_string(), muni.to_string()],
        )
    } else if !pref.is_empty() {
        (
            "WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL",
            vec![seg_job.to_string(), emp_type.to_string(), pref.to_string()],
        )
    } else {
        (
            "WHERE job_type = ? AND employment_type = ? AND municipality IS NULL",
            vec![seg_job.to_string(), emp_type.to_string()],
        )
    };

    let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let sql = format!(
        "SELECT tag, SUM(count) as count, SUM(total) as total \
         FROM segment_tags {} GROUP BY tag ORDER BY count DESC LIMIT 15",
        where_clause
    );

    let rows = match db.query(&sql, &params_ref) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Segment tags query failed: {e}");
            return Html(format!(r#"<p class="text-red-400 text-sm">クエリエラー: {}</p>"#, escape_html(&e)));
        }
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    let labels: Vec<String> = rows
        .iter()
        .map(|r| {
            let tag = r.get("tag").and_then(|v| v.as_str()).unwrap_or("");
            format!(r#""{}""#, escape_html(tag))
        })
        .collect();
    let values: Vec<String> = rows
        .iter()
        .map(|r| {
            let count = r.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            let total = r.get("total").and_then(|v| v.as_i64()).unwrap_or(1).max(1);
            format!("{:.1}", count as f64 / total as f64 * 100.0)
        })
        .collect();

    let filter_notice = if has_facility_filter {
        r#"<p class="text-xs text-amber-400 mb-3">※ 施設形態フィルタはこの分析には適用されません</p>"#
    } else {
        ""
    };

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">🏷️ タグ出現率 TOP15 <span class="text-sm font-normal text-slate-400">（{scope} / {job_type}）</span></h3>
    {filter_notice}
    <div class="stat-card">
        <div class="echart" style="height:500px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}, "formatter": "{{b}}: {{c}}%"}},
            "xAxis": {{"type": "value", "axisLabel": {{"color": "#94a3b8", "formatter": "{{value}}%"}}, "max": 100}},
            "yAxis": {{"type": "category", "data": [{labels}], "inverse": true, "axisLabel": {{"color": "#94a3b8", "fontSize": 12}}}},
            "series": [{{
                "type": "bar", "data": [{values}],
                "itemStyle": {{"color": {{"type": "linear", "x": 0, "y": 0, "x2": 1, "y2": 0,
                    "colorStops": [{{"offset": 0, "color": "#f59e0b"}}, {{"offset": 1, "color": "#ef4444"}}]
                }}, "borderRadius": [0,4,4,0]}},
                "barWidth": "65%",
                "label": {{"show": true, "position": "right", "formatter": "{{c}}%", "color": "#94a3b8", "fontSize": 11}}
            }}],
            "grid": {{"left": "22%", "right": "10%", "top": "8px", "bottom": "8px", "containLabel": false}}
        }}'></div>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        filter_notice = filter_notice,
        labels = labels.join(","),
        values = values.join(","),
    );

    Html(html)
}

// =============================================================
// API 4: /api/segment/text_features → テキスト特徴分析（segment_db継続使用）
// =============================================================

pub async fn segment_text_features(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;
    let seg_job = match map_job_type_to_segment(&job_type) {
        Some(j) => j,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let db = match &state.segment_db {
        Some(db) => db,
        None => return Html(r#"<p class="text-red-400 text-sm">セグメントDBが利用できません</p>"#.to_string()),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };
    let ftype_raw = params.facility_type.as_deref().unwrap_or("");
    let has_facility_filter = !ftype_raw.is_empty() && ftype_raw != "全て";

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        db, "segment_text_features", seg_job, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let (where_clause, param_values) = if !muni.is_empty() && !pref.is_empty() {
        (
            "WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ?",
            vec![seg_job.to_string(), emp_type.to_string(), pref.to_string(), muni.to_string()],
        )
    } else if !pref.is_empty() {
        (
            "WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL",
            vec![seg_job.to_string(), emp_type.to_string(), pref.to_string()],
        )
    } else {
        (
            "WHERE job_type = ? AND employment_type = ? AND municipality IS NULL",
            vec![seg_job.to_string(), emp_type.to_string()],
        )
    };

    let params_ref: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let sql = format!(
        "SELECT category, label, SUM(count) as count, SUM(total) as total \
         FROM segment_text_features {} GROUP BY category, label ORDER BY category, count DESC",
        where_clause
    );

    let rows = match db.query(&sql, &params_ref) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Segment text_features query failed: {e}");
            return Html(format!(r#"<p class="text-red-400 text-sm">クエリエラー: {}</p>"#, escape_html(&e)));
        }
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    // カテゴリ別にグループ化
    let mut categories: std::collections::BTreeMap<String, Vec<(String, i64, i64)>> =
        std::collections::BTreeMap::new();
    for row in &rows {
        let cat = row.get("category").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let label = row.get("label").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let total = row.get("total").and_then(|v| v.as_i64()).unwrap_or(1).max(1);
        categories.entry(cat).or_default().push((label, count, total));
    }

    // カテゴリのテーマカラー
    let cat_colors = [
        ("施設形態", "#3b82f6"),
        ("勤務形態", "#10b981"),
        ("教育研修", "#f59e0b"),
        ("福利厚生", "#8b5cf6"),
        ("訴求表現", "#ef4444"),
    ];

    let mut cat_charts = String::new();
    for (cat_name, items) in &categories {
        let color = cat_colors
            .iter()
            .find(|(n, _)| *n == cat_name.as_str())
            .map(|(_, c)| *c)
            .unwrap_or("#64748b");

        let labels: Vec<String> = items
            .iter()
            .take(10)
            .map(|(l, _, _)| format!(r#""{}""#, escape_html(l)))
            .collect();
        let values: Vec<String> = items
            .iter()
            .take(10)
            .map(|(_, c, t)| format!("{:.1}", *c as f64 / *t as f64 * 100.0))
            .collect();

        let item_count = items.len().min(10);
        let bar_h = if item_count <= 3 { 55 } else if item_count <= 5 { 45 } else { 38 };
        cat_charts.push_str(&format!(
            r##"<div class="stat-card">
                <h4 class="text-sm text-slate-400 mb-2">{cat_name}</h4>
                <div class="echart" style="height:{height}px;" data-chart-config='{{
                    "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}, "formatter": "{{b}}: {{c}}%"}},
                    "xAxis": {{"type": "value", "axisLabel": {{"color": "#94a3b8", "formatter": "{{value}}%"}}, "max": 100}},
                    "yAxis": {{"type": "category", "data": [{labels}], "inverse": true, "axisLabel": {{"color": "#94a3b8", "fontSize": 11, "width": 100, "overflow": "truncate"}}}},
                    "series": [{{
                        "type": "bar", "data": [{values}],
                        "itemStyle": {{"color": "{color}", "borderRadius": [0,4,4,0]}},
                        "barWidth": "70%",
                        "label": {{"show": true, "position": "right", "formatter": "{{c}}%", "color": "#94a3b8", "fontSize": 11}}
                    }}],
                    "grid": {{"left": "28%", "right": "12%", "top": "8px", "bottom": "8px", "containLabel": false}}
                }}'></div>
            </div>"##,
            cat_name = escape_html(cat_name),
            height = (item_count * bar_h).max(180),
            labels = labels.join(","),
            values = values.join(","),
            color = color,
        ));
    }

    let filter_notice = if has_facility_filter {
        r#"<p class="text-xs text-amber-400 mb-3">※ 施設形態フィルタはこの分析には適用されません</p>"#
    } else {
        ""
    };

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">📝 テキスト特徴分析 <span class="text-sm font-normal text-slate-400">（{scope} / {job_type}）</span></h3>
    {filter_notice}
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        {cat_charts}
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        filter_notice = filter_notice,
        cat_charts = cat_charts,
    );

    Html(html)
}

// =============================================================
// ヘルパー関数
// =============================================================

fn no_segment_data_html(job_type: &str) -> String {
    format!(
        r#"<div class="stat-card text-center py-8">
            <p class="text-slate-400 text-sm">「{}」のセグメントデータはありません</p>
            <p class="text-slate-500 text-xs mt-1">セグメント分類済みの求人データが必要です</p>
        </div>"#,
        escape_html(job_type)
    )
}

fn build_scope_label(pref: &str, muni: &str) -> String {
    if !muni.is_empty() && !pref.is_empty() {
        format!("{} {}", pref, muni)
    } else if !pref.is_empty() {
        pref.to_string()
    } else {
        "全国".to_string()
    }
}

/// municipalityレベルのデータ有無をチェック
/// データがない場合はmuniを空にしてprefectureレベルにフォールバック
fn resolve_municipality_fallback(
    db: &crate::db::local_sqlite::LocalDb,
    table: &str,
    job_type: &str,
    emp_type: &str,
    pref: &str,
    muni: &str,
) -> (String, String, bool) {
    // municipalityが未指定なら何もしない
    if muni.is_empty() || pref.is_empty() {
        return (pref.to_string(), muni.to_string(), false);
    }
    // municipalityレベルのデータ存在チェック
    let check_sql = format!(
        "SELECT COUNT(*) as cnt FROM {} WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? LIMIT 1",
        table
    );
    let params: Vec<String> = vec![job_type.to_string(), emp_type.to_string(), pref.to_string(), muni.to_string()];
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
    match db.query(&check_sql, &params_ref) {
        Ok(rows) => {
            let cnt = rows.first()
                .and_then(|r| r.get("cnt"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if cnt > 0 {
                (pref.to_string(), muni.to_string(), false)
            } else {
                // フォールバック: prefectureレベルを使用
                (pref.to_string(), String::new(), true)
            }
        }
        Err(_) => (pref.to_string(), String::new(), true),
    }
}

// =============================================================
// Tab 9: セグメント分析タブ (独立タブ)
// =============================================================

pub async fn tab_segment(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;
    let scope_label = build_scope_label(&prefecture, &municipality);

    // segment_dbから雇用形態一覧を取得
    let emp_options: Vec<String> = if let Some(db) = &state.segment_db {
        match db.query(
            "SELECT DISTINCT employment_type FROM segment_prefecture ORDER BY employment_type",
            &[] as &[&dyn rusqlite::types::ToSql],
        ) {
            Ok(rows) => rows.iter()
                .filter_map(|r| r.get("employment_type").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .filter(|s| s != "全て")
                .collect(),
            Err(_) => vec![],
        }
    } else {
        vec![]
    };

    let emp_options_html: String = emp_options.iter()
        .map(|e| format!(r#"<option value="{v}">{v}</option>"#, v = escape_html(e)))
        .collect::<Vec<_>>()
        .join("\n");

    let html = format!(
        r##"<div class="space-y-6">
    <div class="flex items-center justify-between flex-wrap gap-3">
        <h2 class="text-xl font-bold text-white">🔬 セグメント分析 <span class="text-sm font-normal text-slate-400">（{scope} / {job_type}）</span></h2>
        <!-- 雇用形態フィルタ -->
        <div class="flex items-center gap-3 flex-wrap">
            <label class="text-sm text-slate-400">雇用形態:</label>
            <select id="seg-emp-filter" class="bg-slate-700 text-white text-sm rounded-lg px-3 py-1.5 border border-slate-600 focus:border-blue-500 focus:outline-none">
                <option value="全て" selected>全て</option>
                {emp_options}
            </select>
            <label class="text-sm text-slate-400 ml-2">施設形態:</label>
            <select id="seg-facility-filter" class="bg-slate-700 text-white text-sm rounded-lg px-3 py-1.5 border border-slate-600 focus:border-blue-500 focus:outline-none">
                <option value="全て" selected>全て</option>
                <option value="訪問系">訪問系</option>
                <option value="通所系">通所系</option>
                <option value="入所系">入所系</option>
                <option value="病院・クリニック">病院・クリニック</option>
                <option value="保育・教育">保育・教育</option>
                <option value="障害福祉">障害福祉</option>
                <option value="薬局">薬局</option>
                <option value="その他">その他</option>
            </select>
        </div>
    </div>

    <!-- サブタブ -->
    <div class="flex gap-2 flex-wrap">
        <button class="seg-subtab active px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white" data-panel="axis">5軸分布</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="tier3">Tier3パターン</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="tags">タグ分析</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="textfeat">テキスト特徴</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="salary">給与比較</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="jobdesc">仕事内容分析</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="agedecade">年代分布</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="genderlife">性別・ライフステージ</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="expqual">未経験×資格</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="workschedule">勤務時間帯</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="holidays">休日分析</button>
        <button class="seg-subtab px-3 py-1.5 text-sm rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600" data-panel="salaryshift">給与×シフト</button>
    </div>

    <!-- パネル: 5軸分布 -->
    <div id="seg-panel-axis" class="seg-panel"
         hx-get="/api/segment/overview?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="load" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: Tier3パターン -->
    <div id="seg-panel-tier3" class="seg-panel hidden"
         hx-get="/api/segment/tier3?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: タグ分析 -->
    <div id="seg-panel-tags" class="seg-panel hidden"
         hx-get="/api/segment/tags?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: テキスト特徴 -->
    <div id="seg-panel-textfeat" class="seg-panel hidden"
         hx-get="/api/segment/text_features?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 給与比較 -->
    <div id="seg-panel-salary" class="seg-panel hidden"
         hx-get="/api/segment/salary_compare?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 仕事内容分析 -->
    <div id="seg-panel-jobdesc" class="seg-panel hidden"
         hx-get="/api/segment/job_desc_insights?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 年代分布 -->
    <div id="seg-panel-agedecade" class="seg-panel hidden"
         hx-get="/api/segment/age_decade?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 性別・ライフステージ -->
    <div id="seg-panel-genderlife" class="seg-panel hidden"
         hx-get="/api/segment/gender_lifecycle?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 未経験×資格 -->
    <div id="seg-panel-expqual" class="seg-panel hidden"
         hx-get="/api/segment/exp_qual?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 勤務時間帯 -->
    <div id="seg-panel-workschedule" class="seg-panel hidden"
         hx-get="/api/segment/work_schedule?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 休日分析 -->
    <div id="seg-panel-holidays" class="seg-panel hidden"
         hx-get="/api/segment/holidays?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>

    <!-- パネル: 給与×シフト -->
    <div id="seg-panel-salaryshift" class="seg-panel hidden"
         hx-get="/api/segment/salary_shift?prefecture={pref_enc}&municipality={muni_enc}&employment_type={emp_enc}"
         hx-trigger="revealed" hx-swap="innerHTML">
        <div class="text-center py-8"><span class="text-slate-400">読み込み中...</span></div>
    </div>
</div>

<script>
document.querySelectorAll('.seg-subtab').forEach(btn => {{
    btn.addEventListener('click', () => {{
        document.querySelectorAll('.seg-subtab').forEach(b => {{
            b.classList.remove('active', 'bg-blue-600', 'text-white');
            b.classList.add('bg-slate-700', 'text-slate-300');
        }});
        btn.classList.add('active', 'bg-blue-600', 'text-white');
        btn.classList.remove('bg-slate-700', 'text-slate-300');
        document.querySelectorAll('.seg-panel').forEach(p => p.classList.add('hidden'));
        const panel = document.getElementById('seg-panel-' + btn.dataset.panel);
        if (panel) {{
            panel.classList.remove('hidden');
            // hidden解除後にHTMXリクエストを手動トリガー（未ロード時のみ）
            if (panel.dataset.loaded !== 'true') {{
                htmx.trigger(panel, 'revealed');
                panel.dataset.loaded = 'true';
            }}
        }}
    }});
}});

// 雇用形態フィルタ変更
document.getElementById('seg-emp-filter').addEventListener('change', function() {{
    var emp = encodeURIComponent(this.value);
    document.querySelectorAll('.seg-panel').forEach(function(panel) {{
        var url = panel.getAttribute('hx-get');
        url = url.replace(/employment_type=[^&]*/, 'employment_type=' + emp);
        panel.setAttribute('hx-get', url);
        panel.removeAttribute('data-loaded');
    }});
    var visible = document.querySelector('.seg-panel:not(.hidden)');
    if (visible) {{
        htmx.ajax('GET', visible.getAttribute('hx-get'), {{target: visible, swap: 'innerHTML'}});
        visible.dataset.loaded = 'true';
    }}
}});

// 施設形態フィルタ変更
document.getElementById('seg-facility-filter').addEventListener('change', function() {{
    var ft = encodeURIComponent(this.value);
    document.querySelectorAll('.seg-panel').forEach(function(panel) {{
        var url = panel.getAttribute('hx-get');
        if (url.indexOf('facility_type=') !== -1) {{
            url = url.replace(/facility_type=[^&]*/, 'facility_type=' + ft);
        }} else {{
            url = url + '&facility_type=' + ft;
        }}
        panel.setAttribute('hx-get', url);
        panel.removeAttribute('data-loaded');
    }});
    var visible = document.querySelector('.seg-panel:not(.hidden)');
    if (visible) {{
        htmx.ajax('GET', visible.getAttribute('hx-get'), {{target: visible, swap: 'innerHTML'}});
        visible.dataset.loaded = 'true';
    }}
}});
</script>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        pref_enc = urlencoding::encode(&prefecture),
        muni_enc = urlencoding::encode(&municipality),
        emp_enc = urlencoding::encode("全て"),
        emp_options = emp_options_html,
    );

    Html(html)
}

// =============================================================
// API: /api/segment/salary_compare
// =============================================================

pub async fn segment_salary_compare(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_salary", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    // 3分岐クエリ: 全国 / 県 / 市区町村
    let query = if !muni.is_empty() && !pref.is_empty() {
        "SELECT axis, category, count, salary_min_avg, salary_min_med, salary_max_avg, salary_max_med, holidays_avg, benefits_avg \
         FROM segment_salary WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? AND axis IN ('A','B','C') ORDER BY axis, count DESC"
    } else if !pref.is_empty() {
        "SELECT axis, category, count, salary_min_avg, salary_min_med, salary_max_avg, salary_max_med, holidays_avg, benefits_avg \
         FROM segment_salary WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL AND axis IN ('A','B','C') ORDER BY axis, count DESC"
    } else {
        "SELECT axis, category, SUM(count) as count, \
         CAST(AVG(salary_min_avg) AS INTEGER) as salary_min_avg, \
         CAST(AVG(salary_min_med) AS INTEGER) as salary_min_med, \
         CAST(AVG(salary_max_avg) AS INTEGER) as salary_max_avg, \
         CAST(AVG(salary_max_med) AS INTEGER) as salary_max_med, \
         AVG(holidays_avg) as holidays_avg, AVG(benefits_avg) as benefits_avg \
         FROM segment_salary WHERE job_type = ? AND employment_type = ? AND municipality IS NULL AND axis IN ('A','B','C') \
         GROUP BY axis, category ORDER BY axis, SUM(count) DESC"
    };

    let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
    if !pref.is_empty() {
        all_params.push(pref.to_string());
    }
    if !muni.is_empty() && !pref.is_empty() {
        all_params.push(muni.to_string());
    }
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match seg_db.query(query, &params_ref) {
        Ok(r) => r,
        Err(_) => return Html(no_segment_data_html(&job_type)),
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    // 軸ごとにグループ化してテーブル生成
    let mut current_axis = String::new();
    let mut tables_html = String::new();
    let axis_names = [("A", "経験レベル"), ("B", "キャリアステージ"), ("C", "ライフスタイル")];

    for row in &rows {
        let axis = row.get("axis").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
        let cat = row.get("category").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
        let count = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
        let s_min_avg = row.get("salary_min_avg").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
        let s_max_avg = row.get("salary_max_avg").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
        let hol_avg = row.get("holidays_avg").and_then(|v: &serde_json::Value| v.as_f64()).unwrap_or(0.0);

        let label = tier2_label(cat);

        if axis != current_axis {
            if !current_axis.is_empty() {
                tables_html.push_str("</tbody></table></div>");
            }
            let axis_label = axis_names.iter().find(|(a, _)| *a == axis).map(|(_, l)| *l).unwrap_or(axis);
            tables_html.push_str(&format!(
                r#"<div class="stat-card"><h4 class="text-sm font-bold text-slate-300 mb-2">軸{} {}</h4>
                <table class="w-full text-sm"><thead><tr class="text-slate-400 text-xs">
                <th class="text-left py-1">セグメント</th><th class="text-right">件数</th>
                <th class="text-right">下限平均</th><th class="text-right">上限平均</th>
                <th class="text-right">休日平均</th></tr></thead><tbody>"#,
                escape_html(axis), escape_html(axis_label)
            ));
            current_axis = axis.to_string();
        }

        tables_html.push_str(&format!(
            r#"<tr class="border-t border-slate-700"><td class="py-1 text-slate-200">{}</td>
            <td class="text-right text-slate-400">{}</td>
            <td class="text-right text-emerald-400">¥{}</td>
            <td class="text-right text-blue-400">¥{}</td>
            <td class="text-right text-amber-400">{:.0}日</td></tr>"#,
            escape_html(label), format_number(count),
            format_number(s_min_avg), format_number(s_max_avg), hol_avg
        ));
    }
    if !current_axis.is_empty() {
        tables_html.push_str("</tbody></table></div>");
    }

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">💰 セグメント別給与比較 <span class="text-sm font-normal text-slate-400">（{scope} / {job_type}）</span></h3>
    <div class="grid grid-cols-1 md:grid-cols-3 gap-4">{tables}</div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        tables = tables_html,
    );

    Html(html)
}

// =============================================================
// API: /api/segment/job_desc_insights
// =============================================================

pub async fn segment_job_desc_insights(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    // 仕事内容カテゴリの分布を取得
    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_job_desc", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    let is_national = pref.is_empty();
    let query = if !muni.is_empty() && !pref.is_empty() {
        "SELECT task_label, count as cnt, total as ttl \
         FROM segment_job_desc WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? \
         ORDER BY count DESC"
    } else if !pref.is_empty() {
        "SELECT task_label, count as cnt, total as ttl \
         FROM segment_job_desc WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL \
         ORDER BY count DESC"
    } else {
        "SELECT task_label, SUM(count) as cnt \
         FROM segment_job_desc WHERE job_type = ? AND employment_type = ? AND municipality IS NULL \
         GROUP BY task_label ORDER BY cnt DESC"
    };

    let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
    if !pref.is_empty() {
        all_params.push(pref.to_string());
    }
    if !muni.is_empty() && !pref.is_empty() {
        all_params.push(muni.to_string());
    }
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match seg_db.query(query, &params_ref) {
        Ok(r) => r,
        Err(_) => return Html(no_segment_data_html(&job_type)),
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    // EChartsドーナツチャートのデータを構築
    let task_colors = [
        ("#ef4444", "直接介護・看護"), ("#3b82f6", "間接業務"), ("#f59e0b", "相談支援"),
        ("#10b981", "リハビリ"), ("#8b5cf6", "マネジメント"), ("#ec4899", "保育"),
        ("#06b6d4", "調理"),
    ];

    // 全国クエリの場合、countの合計をtotalとして使用（SUM(total)の不整合回避）
    let mut chart_data = String::new();
    let mut bar_html = String::new();

    if is_national {
        // 全国: SUM(count)の合計をtotalとして計算
        let mut entries: Vec<(&str, i64)> = Vec::new();
        let mut dim_total: i64 = 0;
        for row in &rows {
            let label = row.get("task_label").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
            let cnt = row.get("cnt").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
            dim_total += cnt;
            entries.push((label, cnt));
        }
        for (label, cnt) in &entries {
            let pct = if dim_total > 0 { *cnt as f64 / dim_total as f64 * 100.0 } else { 0.0 };
            let color = task_colors.iter().find(|(_, l)| *l == *label).map(|(c, _)| *c).unwrap_or("#64748b");

            chart_data.push_str(&format!(r#"{{value:{cnt},name:'{label}'}},"#,
                cnt = cnt, label = escape_html(label)));

            bar_html.push_str(&format!(
                r#"<div class="flex items-center gap-2 mb-2">
                    <div class="w-3 h-3 rounded-full" style="background:{color}"></div>
                    <span class="text-sm text-slate-300 flex-1">{label}</span>
                    <span class="text-sm font-mono text-slate-200">{cnt}</span>
                    <span class="text-xs text-slate-400">({pct:.1}%)</span>
                </div>"#,
                color = color, label = escape_html(label),
                cnt = format_number(*cnt), pct = pct
            ));
        }
    } else {
    for row in &rows {
        let label = row.get("task_label").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
        let cnt = row.get("cnt").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
        let ttl = row.get("ttl").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(1);
        let pct = if ttl > 0 { cnt as f64 / ttl as f64 * 100.0 } else { 0.0 };
        let color = task_colors.iter().find(|(_, l)| *l == label).map(|(c, _)| *c).unwrap_or("#64748b");

        chart_data.push_str(&format!(r#"{{value:{cnt},name:'{label}'}},"#,
            cnt = cnt, label = escape_html(label)));

        bar_html.push_str(&format!(
            r#"<div class="flex items-center gap-2 mb-2">
                <div class="w-3 h-3 rounded-full" style="background:{color}"></div>
                <span class="text-sm text-slate-300 flex-1">{label}</span>
                <span class="text-sm font-mono text-slate-200">{cnt}</span>
                <span class="text-xs text-slate-400">({pct:.1}%)</span>
            </div>"#,
            color = color, label = escape_html(label),
            cnt = format_number(cnt), pct = pct
        ));
    }
    } // else (県/市区町村)

    let chart_id = format!("jd-donut-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis());

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">📋 仕事内容分析 <span class="text-sm font-normal text-slate-400">（{scope} / {job_type}）</span></h3>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-3">業務カテゴリ分布</h4>
            <div id="{chart_id}" style="height:350px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{chart_id}'), 'dark');
                c.setOption({{
                    tooltip: {{trigger:'item',formatter:'{{b}}: {{c}}件 ({{d}}%)'}},
                    series: [{{
                        type:'pie', radius:['38%','70%'],
                        label:{{color:'#94a3b8',fontSize:12}},
                        data:[{chart_data}]
                    }}]
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{chart_id}'));
            }})();
            </script>
        </div>
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-3">カテゴリ別件数</h4>
            {bar_html}
        </div>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        chart_id = chart_id,
        chart_data = chart_data,
        bar_html = bar_html,
    );

    Html(html)
}

// =============================================================
// API: /api/segment/age_decade - 年代分布
// =============================================================

pub async fn segment_age_decade(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_age_decade", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    let is_national = pref.is_empty();
    let query = if !muni.is_empty() && !pref.is_empty() {
        "SELECT decade, count, total \
         FROM segment_age_decade WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? \
         ORDER BY decade"
    } else if !pref.is_empty() {
        "SELECT decade, count, total \
         FROM segment_age_decade WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL \
         ORDER BY decade"
    } else {
        "SELECT decade, SUM(count) as count \
         FROM segment_age_decade WHERE job_type = ? AND employment_type = ? AND municipality IS NULL \
         GROUP BY decade ORDER BY decade"
    };

    let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
    if !pref.is_empty() {
        all_params.push(pref.to_string());
    }
    if !muni.is_empty() && !pref.is_empty() {
        all_params.push(muni.to_string());
    }
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match seg_db.query(query, &params_ref) {
        Ok(r) => r,
        Err(_) => return Html(no_segment_data_html(&job_type)),
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    let decade_colors = [
        ("20代", "#3b82f6"), ("30代", "#10b981"), ("40代", "#f59e0b"),
        ("50代", "#ef4444"), ("60代", "#8b5cf6"),
    ];

    let chart_id = format!("age-chart-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis());
    let mut chart_cats = String::new();
    let mut chart_vals = String::new();
    let mut bar_html = String::new();

    if is_national {
        // 全国: SUM(count)の合計をtotalとして計算
        let mut entries: Vec<(&str, i64)> = Vec::new();
        let mut dim_total: i64 = 0;
        for row in &rows {
            let decade = row.get("decade").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
            let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
            dim_total += cnt;
            entries.push((decade, cnt));
        }
        for (decade, cnt) in &entries {
            let pct = if dim_total > 0 { *cnt as f64 / dim_total as f64 * 100.0 } else { 0.0 };
            let color = decade_colors.iter().find(|(d, _)| *d == *decade).map(|(_, c)| *c).unwrap_or("#64748b");

            chart_cats.push_str(&format!("'{}',", escape_html(decade)));
            chart_vals.push_str(&format!("{{value:{cnt},itemStyle:{{color:'{color}'}}}},"));

            bar_html.push_str(&format!(
                r#"<div class="flex items-center gap-3 mb-2">
                    <div class="w-3 h-3 rounded-full" style="background:{color}"></div>
                    <span class="text-sm text-slate-300 w-12">{decade}</span>
                    <div class="flex-1 bg-slate-700 rounded-full h-4 relative">
                        <div class="bg-blue-500 h-4 rounded-full" style="width:{pct:.1}%;background:{color}"></div>
                    </div>
                    <span class="text-sm font-mono text-slate-200 w-16 text-right">{cnt}</span>
                    <span class="text-xs text-slate-400 w-16 text-right">({pct:.1}%)</span>
                </div>"#,
                color = color,
                decade = escape_html(decade),
                pct = pct,
                cnt = cnt,
            ));
        }
    } else {
    for row in &rows {
        let decade = row.get("decade").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
        let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
        let ttl = row.get("total").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(1);
        let pct = if ttl > 0 { cnt as f64 / ttl as f64 * 100.0 } else { 0.0 };
        let color = decade_colors.iter().find(|(d, _)| *d == decade).map(|(_, c)| *c).unwrap_or("#64748b");

        chart_cats.push_str(&format!("'{}',", escape_html(decade)));
        chart_vals.push_str(&format!("{{value:{cnt},itemStyle:{{color:'{color}'}}}},"));

        bar_html.push_str(&format!(
            r#"<div class="flex items-center gap-3 mb-2">
                <div class="w-3 h-3 rounded-full" style="background:{color}"></div>
                <span class="text-sm text-slate-300 w-12">{decade}</span>
                <div class="flex-1 bg-slate-700 rounded-full h-4 relative">
                    <div class="bg-blue-500 h-4 rounded-full" style="width:{pct:.1}%;background:{color}"></div>
                </div>
                <span class="text-sm font-mono text-slate-200 w-16 text-right">{cnt}</span>
                <span class="text-xs text-slate-400 w-16 text-right">({pct:.1}%)</span>
            </div>"#,
            color = color,
            decade = escape_html(decade),
            pct = pct,
            cnt = cnt,
        ));
    }
    } // else (県/市区町村)

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">📊 年代分布 <span class="text-sm font-normal text-slate-400">({scope} / {job_type})</span></h3>
    <p class="text-xs text-slate-500">求人文中のキーワードから推定される対象年代の分布</p>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="stat-card">
            <div id="{chart_id}" style="height:350px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{chart_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'axis'}},
                    xAxis:{{type:'category',data:[{cats}],axisLabel:{{color:'#94a3b8',fontSize:12}}}},
                    yAxis:{{type:'value',axisLabel:{{color:'#94a3b8'}}}},
                    series:[{{type:'bar',data:[{vals}],barWidth:'55%'}}],
                    grid:{{left:'12%',right:'5%',top:'8px',bottom:'15%'}}
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{chart_id}'));
            }})();
            </script>
        </div>
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-3">年代別求人シグナル</h4>
            {bar_html}
            <p class="text-xs text-slate-500 mt-3">※ 1求人が複数年代にマッチすることがあります</p>
        </div>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        chart_id = chart_id,
        cats = chart_cats,
        vals = chart_vals,
        bar_html = bar_html,
    );

    Html(html)
}

// =============================================================
// API: /api/segment/gender_lifecycle - 性別・ライフステージ
// =============================================================

pub async fn segment_gender_lifecycle(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_gender_lifecycle", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    let is_national = pref.is_empty();
    let query = if !muni.is_empty() && !pref.is_empty() {
        "SELECT dimension, category, count, total \
         FROM segment_gender_lifecycle WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? \
         ORDER BY dimension, count DESC"
    } else if !pref.is_empty() {
        "SELECT dimension, category, count, total \
         FROM segment_gender_lifecycle WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL \
         ORDER BY dimension, count DESC"
    } else {
        "SELECT dimension, category, SUM(count) as count \
         FROM segment_gender_lifecycle WHERE job_type = ? AND employment_type = ? AND municipality IS NULL \
         GROUP BY dimension, category ORDER BY dimension, count DESC"
    };

    let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
    if !pref.is_empty() {
        all_params.push(pref.to_string());
    }
    if !muni.is_empty() && !pref.is_empty() {
        all_params.push(muni.to_string());
    }
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match seg_db.query(query, &params_ref) {
        Ok(r) => r,
        Err(_) => return Html(no_segment_data_html(&job_type)),
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    // 性別パネルとライフステージパネルを分離
    let mut gender_html = String::new();
    let mut lifecycle_html = String::new();
    let mut lc_chart_data = String::new();
    let mut lc_chart_cats = String::new();

    let gender_labels = [
        ("female_leaning", "女性向け", "#ec4899"),
        ("male_leaning", "男性向け", "#3b82f6"),
        ("neutral", "中立", "#64748b"),
    ];

    let lifecycle_colors = [
        ("新卒・キャリア初期", "#3b82f6"),
        ("キャリア形成期", "#10b981"),
        ("結婚・出産期", "#f472b6"),
        ("育児期", "#ec4899"),
        ("復職期", "#f59e0b"),
        ("セカンドキャリア期", "#8b5cf6"),
        ("介護離職・復帰期", "#ef4444"),
    ];

    if is_national {
        // 全国: dimension別にcountの合計をtotalとして計算
        let mut gender_entries: Vec<(&str, &str, i64)> = Vec::new();
        let mut lifecycle_entries: Vec<(&str, i64)> = Vec::new();
        let mut gender_total: i64 = 0;
        let mut lifecycle_total: i64 = 0;
        for row in &rows {
            let dim = row.get("dimension").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
            let cat = row.get("category").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
            let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
            if dim == "gender" {
                gender_total += cnt;
                gender_entries.push((dim, cat, cnt));
            } else if dim == "lifecycle" {
                lifecycle_total += cnt;
                lifecycle_entries.push((cat, cnt));
            }
        }
        for (_dim, cat, cnt) in &gender_entries {
            let pct = if gender_total > 0 { *cnt as f64 / gender_total as f64 * 100.0 } else { 0.0 };
            let (label, color) = gender_labels.iter()
                .find(|(c, _, _)| *c == *cat)
                .map(|(_, l, c)| (*l, *c))
                .unwrap_or((*cat, "#64748b"));
            gender_html.push_str(&format!(
                r#"<div class="flex items-center gap-3 mb-3">
                    <div class="w-4 h-4 rounded-full" style="background:{color}"></div>
                    <span class="text-sm text-slate-300 flex-1">{label}</span>
                    <span class="text-lg font-bold text-white">{cnt}</span>
                    <span class="text-sm text-slate-400">({pct:.1}%)</span>
                </div>"#,
                color = color, label = label, cnt = cnt, pct = pct,
            ));
        }
        for (cat, cnt) in &lifecycle_entries {
            let pct = if lifecycle_total > 0 { *cnt as f64 / lifecycle_total as f64 * 100.0 } else { 0.0 };
            let color = lifecycle_colors.iter()
                .find(|(l, _)| *l == *cat)
                .map(|(_, c)| *c)
                .unwrap_or("#64748b");

            lc_chart_cats.push_str(&format!("'{}',", escape_html(cat)));
            lc_chart_data.push_str(&format!("{{value:{cnt},itemStyle:{{color:'{color}'}}}},"));

            lifecycle_html.push_str(&format!(
                r#"<div class="flex items-center gap-2 mb-2">
                    <div class="w-3 h-3 rounded-full" style="background:{color}"></div>
                    <span class="text-sm text-slate-300 flex-1">{cat}</span>
                    <span class="text-sm font-mono text-slate-200">{cnt}</span>
                    <span class="text-xs text-slate-400">({pct:.1}%)</span>
                </div>"#,
                color = color, cat = escape_html(cat), cnt = cnt, pct = pct,
            ));
        }
    } else {
    for row in &rows {
        let dim = row.get("dimension").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
        let cat = row.get("category").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
        let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
        let ttl = row.get("total").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(1);
        let pct = if ttl > 0 { cnt as f64 / ttl as f64 * 100.0 } else { 0.0 };

        if dim == "gender" {
            let (label, color) = gender_labels.iter()
                .find(|(c, _, _)| *c == cat)
                .map(|(_, l, c)| (*l, *c))
                .unwrap_or((cat, "#64748b"));
            gender_html.push_str(&format!(
                r#"<div class="flex items-center gap-3 mb-3">
                    <div class="w-4 h-4 rounded-full" style="background:{color}"></div>
                    <span class="text-sm text-slate-300 flex-1">{label}</span>
                    <span class="text-lg font-bold text-white">{cnt}</span>
                    <span class="text-sm text-slate-400">({pct:.1}%)</span>
                </div>"#,
                color = color, label = label, cnt = cnt, pct = pct,
            ));
        } else if dim == "lifecycle" {
            let color = lifecycle_colors.iter()
                .find(|(l, _)| *l == cat)
                .map(|(_, c)| *c)
                .unwrap_or("#64748b");

            lc_chart_cats.push_str(&format!("'{}',", escape_html(cat)));
            lc_chart_data.push_str(&format!("{{value:{cnt},itemStyle:{{color:'{color}'}}}},"));

            lifecycle_html.push_str(&format!(
                r#"<div class="flex items-center gap-2 mb-2">
                    <div class="w-3 h-3 rounded-full" style="background:{color}"></div>
                    <span class="text-sm text-slate-300 flex-1">{cat}</span>
                    <span class="text-sm font-mono text-slate-200">{cnt}</span>
                    <span class="text-xs text-slate-400">({pct:.1}%)</span>
                </div>"#,
                color = color, cat = escape_html(cat), cnt = cnt, pct = pct,
            ));
        }
    }
    } // else (県/市区町村)

    let chart_id = format!("lifecycle-chart-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis());

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">👩 性別・ライフステージ <span class="text-sm font-normal text-slate-400">({scope} / {job_type})</span></h3>
    <p class="text-xs text-slate-500">求人文中のキーワードから推定される性別傾向と女性のライフステージ分布</p>
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-3">性別シグナル</h4>
            {gender_html}
        </div>
        <div class="stat-card lg:col-span-2">
            <h4 class="text-sm font-bold text-slate-300 mb-3">女性ライフステージ分布</h4>
            <div id="{chart_id}" style="height:300px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{chart_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'axis'}},
                    xAxis:{{type:'category',data:[{lc_cats}],axisLabel:{{color:'#94a3b8',rotate:20,fontSize:11}}}},
                    yAxis:{{type:'value',axisLabel:{{color:'#94a3b8'}}}},
                    series:[{{type:'bar',data:[{lc_data}],barWidth:'50%'}}],
                    grid:{{left:'12%',right:'5%',top:'8px',bottom:'25%'}}
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{chart_id}'));
            }})();
            </script>
        </div>
    </div>
    <div class="stat-card">
        <h4 class="text-sm font-bold text-slate-300 mb-3">ステージ詳細</h4>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-2">
            {lifecycle_html}
        </div>
        <p class="text-xs text-slate-500 mt-3">※ 1求人が複数ステージにマッチすることがあります</p>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        gender_html = gender_html,
        chart_id = chart_id,
        lc_cats = lc_chart_cats,
        lc_data = lc_chart_data,
        lifecycle_html = lifecycle_html,
    );

    Html(html)
}

// =============================================================
// API: /api/segment/exp_qual - 未経験×資格セグメント
// =============================================================

pub async fn segment_exp_qual(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_exp_qual", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    let is_national = pref.is_empty();
    let query = if !muni.is_empty() && !pref.is_empty() {
        "SELECT segment, count, total \
         FROM segment_exp_qual WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? \
         ORDER BY count DESC"
    } else if !pref.is_empty() {
        "SELECT segment, count, total \
         FROM segment_exp_qual WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL \
         ORDER BY count DESC"
    } else {
        "SELECT segment, SUM(count) as count \
         FROM segment_exp_qual WHERE job_type = ? AND employment_type = ? AND municipality IS NULL \
         GROUP BY segment ORDER BY count DESC"
    };

    let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
    if !pref.is_empty() {
        all_params.push(pref.to_string());
    }
    if !muni.is_empty() && !pref.is_empty() {
        all_params.push(muni.to_string());
    }
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match seg_db.query(query, &params_ref) {
        Ok(r) => r,
        Err(_) => return Html(no_segment_data_html(&job_type)),
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    let seg_colors = [
        ("未経験・無資格OK", "#10b981", "間口が最も広い求人。業界未経験・無資格でも応募可能"),
        ("未経験歓迎・資格必要", "#3b82f6", "未経験OKだが資格は必要。資格保有者の新規参入を促進"),
        ("経験者・無資格可", "#f59e0b", "実務経験があれば資格不問。経験重視の採用"),
        ("経験者・資格必須", "#ef4444", "経験も資格も必要。即戦力・専門職向け"),
        ("条件不明", "#64748b", "要件が明示されていない求人"),
    ];

    let chart_id = format!("expqual-chart-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis());
    let mut chart_data = String::new();
    let mut grid_html = String::new();

    if is_national {
        // 全国: SUM(count)の合計をtotalとして計算
        let mut entries: Vec<(&str, i64)> = Vec::new();
        let mut dim_total: i64 = 0;
        for row in &rows {
            let seg = row.get("segment").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
            let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
            dim_total += cnt;
            entries.push((seg, cnt));
        }
        for (seg, cnt) in &entries {
            let pct = if dim_total > 0 { *cnt as f64 / dim_total as f64 * 100.0 } else { 0.0 };
            let (color, desc) = seg_colors.iter()
                .find(|(s, _, _)| *s == *seg)
                .map(|(_, c, d)| (*c, *d))
                .unwrap_or(("#64748b", ""));

            chart_data.push_str(&format!(
                r#"{{value:{cnt},name:'{seg}',itemStyle:{{color:'{color}'}}}},"#,
                cnt = cnt, seg = escape_html(seg), color = color,
            ));

            grid_html.push_str(&format!(
                r#"<div class="stat-card border-l-4" style="border-color:{color}">
                    <div class="flex items-center justify-between mb-1">
                        <span class="text-sm font-bold text-white">{seg}</span>
                        <span class="text-lg font-bold text-white">{pct:.1}%</span>
                    </div>
                    <div class="text-xs text-slate-400 mb-2">{desc}</div>
                    <div class="text-sm text-slate-300">{cnt} 件</div>
                </div>"#,
                color = color, seg = escape_html(seg), pct = pct, desc = desc, cnt = cnt,
            ));
        }
    } else {
    for row in &rows {
        let seg = row.get("segment").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("");
        let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
        let ttl = row.get("total").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(1);
        let pct = if ttl > 0 { cnt as f64 / ttl as f64 * 100.0 } else { 0.0 };

        let (color, desc) = seg_colors.iter()
            .find(|(s, _, _)| *s == seg)
            .map(|(_, c, d)| (*c, *d))
            .unwrap_or(("#64748b", ""));

        chart_data.push_str(&format!(
            r#"{{value:{cnt},name:'{seg}',itemStyle:{{color:'{color}'}}}},"#,
            cnt = cnt, seg = escape_html(seg), color = color,
        ));

        grid_html.push_str(&format!(
            r#"<div class="stat-card border-l-4" style="border-color:{color}">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-sm font-bold text-white">{seg}</span>
                    <span class="text-lg font-bold text-white">{pct:.1}%</span>
                </div>
                <div class="text-xs text-slate-400 mb-2">{desc}</div>
                <div class="text-sm text-slate-300">{cnt} 件</div>
            </div>"#,
            color = color, seg = escape_html(seg), pct = pct, desc = desc, cnt = cnt,
        ));
    }
    } // else (県/市区町村)

    let html = format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white">🎓 未経験×資格セグメント <span class="text-sm font-normal text-slate-400">({scope} / {job_type})</span></h3>
    <p class="text-xs text-slate-500">未経験/経験者 × 資格要否 の4象限で求人を分類</p>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="stat-card">
            <div id="{chart_id}" style="height:360px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{chart_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'item',formatter:'{{b}}: {{c}}件 ({{d}}%)'}},
                    series:[{{
                        type:'pie', radius:['35%','68%'],
                        label:{{color:'#94a3b8',fontSize:11,formatter:'{{b}}\\n{{d}}%'}},
                        data:[{chart_data}]
                    }}]
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{chart_id}'));
            }})();
            </script>
        </div>
        <div class="grid grid-cols-1 gap-3">
            {grid_html}
        </div>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        chart_id = chart_id,
        chart_data = chart_data,
        grid_html = grid_html,
    );

    Html(html)
}

// =============================================================
// API: /api/segment/work_schedule - 勤務時間帯分析
// =============================================================

pub async fn segment_work_schedule(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_work_schedule", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let is_national = pref.is_empty();

    // 4つのdimensionを個別にクエリ
    let dimensions = ["shift_type", "start_band", "end_band", "overtime"];
    let mut dim_data: Vec<Vec<(String, i64, f64)>> = Vec::new();
    let mut total_count: i64 = 0;

    for dim in &dimensions {
        let query = if !muni.is_empty() && !pref.is_empty() {
            "SELECT value, count, total \
             FROM segment_work_schedule WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? AND dimension = ? \
             ORDER BY count DESC"
        } else if !pref.is_empty() {
            "SELECT value, count, total \
             FROM segment_work_schedule WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL AND dimension = ? \
             ORDER BY count DESC"
        } else {
            "SELECT value, SUM(count) as count \
             FROM segment_work_schedule WHERE job_type = ? AND employment_type = ? AND municipality IS NULL AND dimension = ? \
             GROUP BY value ORDER BY SUM(count) DESC"
        };

        let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
        if !pref.is_empty() {
            all_params.push(pref.to_string());
        }
        if !muni.is_empty() && !pref.is_empty() {
            all_params.push(muni.to_string());
        }
        all_params.push(dim.to_string());
        let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
            .iter()
            .map(|s| s as &dyn rusqlite::types::ToSql)
            .collect();

        let rows = match seg_db.query(query, &params_ref) {
            Ok(r) => r,
            Err(_) => {
                dim_data.push(Vec::new());
                continue;
            }
        };

        if is_national {
            // 全国: SUM(count)の合計をtotalとして計算
            let mut entries: Vec<(String, i64, f64)> = Vec::new();
            let mut dim_total: i64 = 0;
            for row in &rows {
                let value = row.get("value").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("").to_string();
                let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
                dim_total += cnt;
                entries.push((value, cnt, 0.0));
            }
            // パーセンテージを再計算
            for entry in entries.iter_mut() {
                entry.2 = if dim_total > 0 { entry.1 as f64 / dim_total as f64 * 100.0 } else { 0.0 };
            }
            if total_count == 0 && dim_total > 0 {
                total_count = dim_total;
            }
            dim_data.push(entries);
        } else {
        let mut entries: Vec<(String, i64, f64)> = Vec::new();
        for row in &rows {
            let value = row.get("value").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("").to_string();
            let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
            let ttl = row.get("total").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(1);
            let pct = if ttl > 0 { cnt as f64 / ttl as f64 * 100.0 } else { 0.0 };
            if total_count == 0 && ttl > 0 {
                total_count = ttl;
            }
            entries.push((value, cnt, pct));
        }
        dim_data.push(entries);
        } // else (県/市区町村)
    }

    // データが全くない場合
    if dim_data.iter().all(|d| d.is_empty()) {
        return Html(no_segment_data_html(&job_type));
    }

    // チャートA: シフト分類 PIE
    let pie_id = format!("work-pie-{}", millis);
    let shift_colors = [
        ("日勤のみ", "#60a5fa"),
        ("2交替", "#f59e0b"),
        ("3交替", "#ef4444"),
        ("夜勤専従", "#8b5cf6"),
        ("シフト制", "#10b981"),
        ("固定時間", "#6b7280"),
        ("不明", "#374151"),
    ];
    let mut pie_data = String::new();
    for (val, cnt, _) in &dim_data[0] {
        let color = shift_colors.iter().find(|(n, _)| *n == val.as_str()).map(|(_, c)| *c).unwrap_or("#64748b");
        pie_data.push_str(&format!(
            "{{value:{cnt},name:'{}',itemStyle:{{color:'{color}'}}}},",
            escape_html(val)
        ));
    }

    // チャートB: 始業時刻帯（横棒グラフ）
    let start_id = format!("work-start-{}", millis);
    let mut start_cats = String::new();
    let mut start_vals = String::new();
    for (val, cnt, _) in dim_data[1].iter().rev() {
        start_cats.push_str(&format!("'{}',", escape_html(val)));
        start_vals.push_str(&format!("{cnt},"));
    }

    // チャートC: 終業時刻帯（横棒グラフ）
    let end_id = format!("work-end-{}", millis);
    let mut end_cats = String::new();
    let mut end_vals = String::new();
    for (val, cnt, _) in dim_data[2].iter().rev() {
        end_cats.push_str(&format!("'{}',", escape_html(val)));
        end_vals.push_str(&format!("{cnt},"));
    }

    // チャートD: 残業状況（横棒グラフ）
    let ot_id = format!("work-ot-{}", millis);
    let ot_colors = [
        ("残業なし", "#10b981"),
        ("残業ほぼなし", "#34d399"),
        ("月20h以内", "#f59e0b"),
        ("残業あり", "#ef4444"),
        ("不明", "#6b7280"),
    ];
    let mut ot_cats = String::new();
    let mut ot_vals = String::new();
    for (val, cnt, _) in dim_data[3].iter().rev() {
        let color = ot_colors.iter().find(|(n, _)| *n == val.as_str()).map(|(_, c)| *c).unwrap_or("#64748b");
        ot_cats.push_str(&format!("'{}',", escape_html(val)));
        ot_vals.push_str(&format!("{{value:{cnt},itemStyle:{{color:'{color}'}}}},"));
    }

    let html = format!(
        r##"<div class="space-y-6">
    <h3 class="text-lg font-bold text-white">🕐 勤務時間帯分析 <span class="text-sm font-normal text-slate-400">({scope} / {job_type})</span></h3>
    <p class="text-xs text-slate-500">対象: {total}件</p>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- チャートA: シフト分類 PIE -->
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-2">シフト分類</h4>
            <div id="{pie_id}" style="height:320px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{pie_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'item',formatter:'{{b}}: {{c}}件 ({{d}}%)'}},
                    series:[{{
                        type:'pie',radius:['35%','65%'],center:['50%','55%'],
                        label:{{color:'#94a3b8',fontSize:11,formatter:'{{b}}\\n{{d}}%'}},
                        data:[{pie_data}]
                    }}]
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{pie_id}'));
            }})();
            </script>
        </div>

        <!-- チャートB: 始業時刻帯 -->
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-2">始業時刻帯</h4>
            <div id="{start_id}" style="height:280px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{start_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'axis'}},
                    xAxis:{{type:'value',axisLabel:{{color:'#94a3b8'}}}},
                    yAxis:{{type:'category',data:[{start_cats}],axisLabel:{{color:'#94a3b8',fontSize:11}}}},
                    series:[{{type:'bar',data:[{start_vals}],barWidth:'55%',itemStyle:{{color:'#60a5fa'}}}}],
                    grid:{{left:'35%',right:'8%',top:'8px',bottom:'8px'}}
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{start_id}'));
            }})();
            </script>
        </div>

        <!-- チャートC: 終業時刻帯 -->
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-2">終業時刻帯</h4>
            <div id="{end_id}" style="height:280px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{end_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'axis'}},
                    xAxis:{{type:'value',axisLabel:{{color:'#94a3b8'}}}},
                    yAxis:{{type:'category',data:[{end_cats}],axisLabel:{{color:'#94a3b8',fontSize:11}}}},
                    series:[{{type:'bar',data:[{end_vals}],barWidth:'55%',itemStyle:{{color:'#34d399'}}}}],
                    grid:{{left:'35%',right:'8%',top:'8px',bottom:'8px'}}
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{end_id}'));
            }})();
            </script>
        </div>

        <!-- チャートD: 残業状況 -->
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-2">残業状況</h4>
            <div id="{ot_id}" style="height:280px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{ot_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'axis'}},
                    xAxis:{{type:'value',axisLabel:{{color:'#94a3b8'}}}},
                    yAxis:{{type:'category',data:[{ot_cats}],axisLabel:{{color:'#94a3b8',fontSize:11}}}},
                    series:[{{type:'bar',data:[{ot_vals}],barWidth:'55%'}}],
                    grid:{{left:'35%',right:'8%',top:'8px',bottom:'8px'}}
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{ot_id}'));
            }})();
            </script>
        </div>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        total = format_number(total_count),
        pie_id = pie_id,
        pie_data = pie_data,
        start_id = start_id,
        start_cats = start_cats,
        start_vals = start_vals,
        end_id = end_id,
        end_cats = end_cats,
        end_vals = end_vals,
        ot_id = ot_id,
        ot_cats = ot_cats,
        ot_vals = ot_vals,
    );

    Html(html)
}

// =============================================================
// 施設形態グループ化
// =============================================================

#[allow(dead_code)]
fn service_type_category(service_type: &str) -> &'static str {
    if service_type.contains("訪問") { return "訪問系"; }
    if service_type.contains("通所") || service_type.contains("デイ") { return "通所系"; }
    if service_type.contains("特別養護") || service_type.contains("老健") || service_type.contains("有料")
        || service_type.contains("グループホーム") || service_type.contains("サービス付き") { return "入所系"; }
    if service_type.contains("病院") || service_type.contains("クリニック") || service_type.contains("診療所") { return "病院・クリニック"; }
    if service_type.contains("保育") || service_type.contains("幼稚園") || service_type.contains("こども園") { return "保育・教育"; }
    if service_type.contains("障害") || service_type.contains("放課後") || service_type.contains("就労") { return "障害福祉"; }
    if service_type.contains("薬局") || service_type.contains("ドラッグ") { return "薬局"; }
    "その他"
}

// =============================================================
// API: /api/segment/holidays - 休日分析
// =============================================================

pub async fn segment_holidays(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_holidays", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    // 施設形態フィルタ（暫定: 「全て」以外は非対応メッセージ）
    let facility_raw = params.facility_type.as_deref().unwrap_or("");
    if !facility_raw.is_empty() && facility_raw != "全て" {
        return Html(format!(
            r#"<div class="stat-card text-center py-8">
                <p class="text-slate-400 text-sm">施設形態フィルタ「{}」はセグメントデータに対応していません</p>
                <p class="text-slate-500 text-xs mt-1">「全て」を選択してください</p>
            </div>"#,
            escape_html(facility_raw)
        ));
    }

    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let is_national = pref.is_empty();

    // 3つのdimension: hol_pattern, weekday_off, annual_holidays_band
    let dimensions = ["hol_pattern", "weekday_off", "annual_holidays_band"];
    let mut dim_data: Vec<Vec<(String, i64, f64)>> = Vec::new();
    let mut total_count: i64 = 0;

    for dim in &dimensions {
        let query = if !muni.is_empty() && !pref.is_empty() {
            "SELECT value, count, total \
             FROM segment_holidays WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? AND dimension = ? \
             ORDER BY count DESC"
        } else if !pref.is_empty() {
            "SELECT value, count, total \
             FROM segment_holidays WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL AND dimension = ? \
             ORDER BY count DESC"
        } else {
            "SELECT value, SUM(count) as count \
             FROM segment_holidays WHERE job_type = ? AND employment_type = ? AND municipality IS NULL AND dimension = ? \
             GROUP BY value ORDER BY SUM(count) DESC"
        };

        let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
        if !pref.is_empty() {
            all_params.push(pref.to_string());
        }
        if !muni.is_empty() && !pref.is_empty() {
            all_params.push(muni.to_string());
        }
        all_params.push(dim.to_string());
        let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
            .iter()
            .map(|s| s as &dyn rusqlite::types::ToSql)
            .collect();

        let rows = match seg_db.query(query, &params_ref) {
            Ok(r) => r,
            Err(_) => {
                dim_data.push(Vec::new());
                continue;
            }
        };

        if is_national {
            // 全国: SUM(count)の合計をtotalとして計算
            let mut entries: Vec<(String, i64, f64)> = Vec::new();
            let mut dim_total: i64 = 0;
            for row in &rows {
                let value = row.get("value").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("").to_string();
                let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
                dim_total += cnt;
                entries.push((value, cnt, 0.0));
            }
            // パーセンテージを再計算
            for entry in entries.iter_mut() {
                entry.2 = if dim_total > 0 { entry.1 as f64 / dim_total as f64 * 100.0 } else { 0.0 };
            }
            if total_count == 0 && dim_total > 0 {
                total_count = dim_total;
            }
            dim_data.push(entries);
        } else {
        let mut entries: Vec<(String, i64, f64)> = Vec::new();
        for row in &rows {
            let value = row.get("value").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("").to_string();
            let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);
            let ttl = row.get("total").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(1);
            let pct = if ttl > 0 { cnt as f64 / ttl as f64 * 100.0 } else { 0.0 };
            if total_count == 0 && ttl > 0 {
                total_count = ttl;
            }
            entries.push((value, cnt, pct));
        }
        dim_data.push(entries);
        } // else (県/市区町村)
    }

    if dim_data.iter().all(|d| d.is_empty()) {
        return Html(no_segment_data_html(&job_type));
    }

    // チャートA: 休日パターン PIE
    let pie_id = format!("hol-pie-{}", millis);
    let hol_colors = [
        ("4週8休", "#60a5fa"),
        ("完全週休2日", "#34d399"),
        ("週休2日", "#f59e0b"),
        ("シフト制", "#8b5cf6"),
        ("土日祝休", "#10b981"),
    ];
    let mut pie_data = String::new();
    for (val, cnt, _) in &dim_data[0] {
        let color = hol_colors.iter().find(|(n, _)| *n == val.as_str()).map(|(_, c)| *c).unwrap_or("#64748b");
        pie_data.push_str(&format!(
            "{{value:{cnt},name:'{}',itemStyle:{{color:'{color}'}}}},",
            escape_html(val)
        ));
    }

    // チャートB: 休日曜日 PIE
    let wd_id = format!("hol-wd-{}", millis);
    let wd_colors = [
        ("土日", "#60a5fa"),
        ("日曜", "#f59e0b"),
        ("平日", "#ef4444"),
        ("不明", "#6b7280"),
    ];
    let mut wd_data = String::new();
    for (val, cnt, _) in &dim_data[1] {
        let color = wd_colors.iter().find(|(n, _)| *n == val.as_str()).map(|(_, c)| *c).unwrap_or("#64748b");
        wd_data.push_str(&format!(
            "{{value:{cnt},name:'{}',itemStyle:{{color:'{color}'}}}},",
            escape_html(val)
        ));
    }

    // チャートC: 年間休日数 BAR
    let ann_id = format!("hol-ann-{}", millis);
    let mut ann_cats = String::new();
    let mut ann_vals = String::new();
    for (val, cnt, _) in dim_data[2].iter().rev() {
        ann_cats.push_str(&format!("'{}',", escape_html(val)));
        ann_vals.push_str(&format!("{cnt},"));
    }

    let html = format!(
        r##"<div class="space-y-6">
    <h3 class="text-lg font-bold text-white">📅 休日分析 <span class="text-sm font-normal text-slate-400">({scope} / {job_type})</span></h3>
    <p class="text-xs text-slate-500">対象: {total}件</p>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- チャートA: 休日パターン PIE -->
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-2">休日パターン</h4>
            <div id="{pie_id}" style="height:320px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{pie_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'item',formatter:'{{b}}: {{c}}件 ({{d}}%)'}},
                    series:[{{
                        type:'pie',radius:['35%','65%'],center:['50%','55%'],
                        label:{{color:'#94a3b8',fontSize:11,formatter:'{{b}}\\n{{d}}%'}},
                        data:[{pie_data}]
                    }}]
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{pie_id}'));
            }})();
            </script>
        </div>

        <!-- チャートB: 休日曜日 PIE -->
        <div class="stat-card">
            <h4 class="text-sm font-bold text-slate-300 mb-2">休日曜日</h4>
            <div id="{wd_id}" style="height:320px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{wd_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'item',formatter:'{{b}}: {{c}}件 ({{d}}%)'}},
                    series:[{{
                        type:'pie',radius:['35%','65%'],center:['50%','55%'],
                        label:{{color:'#94a3b8',fontSize:11,formatter:'{{b}}\\n{{d}}%'}},
                        data:[{wd_data}]
                    }}]
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{wd_id}'));
            }})();
            </script>
        </div>

        <!-- チャートC: 年間休日数 BAR -->
        <div class="stat-card lg:col-span-2">
            <h4 class="text-sm font-bold text-slate-300 mb-2">年間休日数</h4>
            <div id="{ann_id}" style="height:280px"></div>
            <script>
            (function(){{
                var c = echarts.init(document.getElementById('{ann_id}'), 'dark');
                c.setOption({{
                    tooltip:{{trigger:'axis'}},
                    xAxis:{{type:'value',axisLabel:{{color:'#94a3b8'}}}},
                    yAxis:{{type:'category',data:[{ann_cats}],axisLabel:{{color:'#94a3b8',fontSize:11}}}},
                    series:[{{type:'bar',data:[{ann_vals}],barWidth:'55%',itemStyle:{{color:'#60a5fa'}}}}],
                    grid:{{left:'25%',right:'8%',top:'8px',bottom:'8px'}}
                }});
                new ResizeObserver(()=>c.resize()).observe(document.getElementById('{ann_id}'));
            }})();
            </script>
        </div>
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        total = format_number(total_count),
        pie_id = pie_id,
        pie_data = pie_data,
        wd_id = wd_id,
        wd_data = wd_data,
        ann_id = ann_id,
        ann_cats = ann_cats,
        ann_vals = ann_vals,
    );

    Html(html)
}

// =============================================================
// API: /api/segment/salary_shift - 給与×シフト交差分析
// =============================================================

pub async fn segment_salary_shift(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SegmentParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let seg_jt = match map_job_type_to_segment(&job_type) {
        Some(jt) => jt,
        None => return Html(no_segment_data_html(&job_type)),
    };

    let pref_raw = params.prefecture.as_deref().unwrap_or("");
    let muni_raw = params.municipality.as_deref().unwrap_or("");

    let emp = params.employment_type.as_deref().unwrap_or("");
    let emp_type = if emp.is_empty() { "全て" } else { emp };

    // municipalityフォールバックチェック
    let (pref_resolved, muni_resolved, is_fallback) = resolve_municipality_fallback(
        seg_db, "segment_salary_shift", seg_jt, emp_type, pref_raw, muni_raw,
    );
    let pref = pref_resolved.as_str();
    let muni = muni_resolved.as_str();

    let scope_label = build_scope_label(pref, muni);
    let scope_label = if is_fallback {
        format!("{} ※{}は県レベルで表示", scope_label, escape_html(muni_raw))
    } else {
        scope_label
    };

    // 施設形態フィルタ（暫定: 「全て」以外は非対応メッセージ）
    let facility_raw = params.facility_type.as_deref().unwrap_or("");
    if !facility_raw.is_empty() && facility_raw != "全て" {
        return Html(format!(
            r#"<div class="stat-card text-center py-8">
                <p class="text-slate-400 text-sm">施設形態フィルタ「{}」はセグメントデータに対応していません</p>
                <p class="text-slate-500 text-xs mt-1">「全て」を選択してください</p>
            </div>"#,
            escape_html(facility_raw)
        ));
    }

    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    // salary_type別にデータ取得
    let query = if !muni.is_empty() && !pref.is_empty() {
        "SELECT salary_type, salary_band, shift_type, SUM(count) as count \
         FROM segment_salary_shift WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality = ? \
         GROUP BY salary_type, salary_band, shift_type \
         ORDER BY salary_type, salary_band, shift_type"
    } else if !pref.is_empty() {
        "SELECT salary_type, salary_band, shift_type, SUM(count) as count \
         FROM segment_salary_shift WHERE job_type = ? AND employment_type = ? AND prefecture = ? AND municipality IS NULL \
         GROUP BY salary_type, salary_band, shift_type \
         ORDER BY salary_type, salary_band, shift_type"
    } else {
        "SELECT salary_type, salary_band, shift_type, SUM(count) as count \
         FROM segment_salary_shift WHERE job_type = ? AND employment_type = ? AND municipality IS NULL \
         GROUP BY salary_type, salary_band, shift_type \
         ORDER BY salary_type, salary_band, shift_type"
    };

    let mut all_params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
    if !pref.is_empty() {
        all_params.push(pref.to_string());
    }
    if !muni.is_empty() && !pref.is_empty() {
        all_params.push(muni.to_string());
    }
    let params_ref: Vec<&dyn rusqlite::types::ToSql> = all_params
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match seg_db.query(query, &params_ref) {
        Ok(r) => r,
        Err(_) => return Html(no_segment_data_html(&job_type)),
    };

    if rows.is_empty() {
        return Html(no_segment_data_html(&job_type));
    }

    // 月給/時給別にデータを分離
    let monthly_bands = ["~20万", "20-25万", "25-28万", "28-30万", "30-35万", "35万+"];
    let hourly_bands = ["~1200円", "1200-1400円", "1400-1600円", "1600-1800円", "1800-2000円", "2000円+"];
    let shift_types = ["日勤のみ", "2交替", "3交替", "夜勤専従", "シフト制", "固定時間", "不明"];
    let shift_colors = ["#60a5fa", "#f59e0b", "#ef4444", "#8b5cf6", "#10b981", "#6b7280", "#374151"];

    // salary_type -> salary_band -> shift_type -> count のマップ構築
    let mut monthly_map: std::collections::HashMap<(String, String), i64> = std::collections::HashMap::new();
    let mut hourly_map: std::collections::HashMap<(String, String), i64> = std::collections::HashMap::new();

    for row in &rows {
        let sal_type = row.get("salary_type").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("").to_string();
        let sal_band = row.get("salary_band").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("").to_string();
        let shift = row.get("shift_type").and_then(|v: &serde_json::Value| v.as_str()).unwrap_or("").to_string();
        let cnt = row.get("count").and_then(|v: &serde_json::Value| v.as_i64()).unwrap_or(0);

        let key = (sal_band, shift);
        if sal_type == "月給" {
            *monthly_map.entry(key).or_insert(0) += cnt;
        } else if sal_type == "時給" {
            *hourly_map.entry(key).or_insert(0) += cnt;
        }
    }

    // ヒートマップ＋スタックドバー生成
    let monthly_html = build_salary_shift_charts(
        "monthly", "月給", &monthly_bands, &shift_types, &shift_colors, &monthly_map, millis,
    );
    let hourly_html = build_salary_shift_charts(
        "hourly", "時給", &hourly_bands, &shift_types, &shift_colors, &hourly_map, millis,
    );

    let html = format!(
        r##"<div class="space-y-6">
    <h3 class="text-lg font-bold text-white">給与×シフト交差分析 <span class="text-sm font-normal text-slate-400">({scope} / {job_type})</span></h3>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {monthly_html}
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {hourly_html}
    </div>
</div>"##,
        scope = escape_html(&scope_label),
        job_type = escape_html(&job_type),
        monthly_html = monthly_html,
        hourly_html = hourly_html,
    );

    Html(html)
}

// チャート生成ヘルパー（給与×シフト用）
fn build_salary_shift_charts(
    chart_prefix: &str,
    title: &str,
    bands: &[&str],
    shift_types: &[&str],
    shift_colors: &[&str],
    data_map: &std::collections::HashMap<(String, String), i64>,
    millis: u128,
) -> String {
    let heatmap_id = format!("{}-heatmap-{}", chart_prefix, millis);
    let stack_id = format!("{}-stack-{}", chart_prefix, millis);

    // 全体の合計を算出
    let total: i64 = data_map.values().sum();

    // ヒートマップデータ [x_shift_idx, y_band_idx, count, percentage]
    let mut heatmap_data = String::new();
    let mut max_val: i64 = 1;
    for (yi, band) in bands.iter().enumerate() {
        for (xi, shift) in shift_types.iter().enumerate() {
            let cnt = data_map.get(&(band.to_string(), shift.to_string())).copied().unwrap_or(0);
            let pct = if total > 0 { cnt as f64 / total as f64 * 100.0 } else { 0.0 };
            heatmap_data.push_str(&format!("[{xi},{yi},{cnt},{pct:.1}],"));
            if cnt > max_val { max_val = cnt; }
        }
    }

    // スタックドバーデータ: 給与帯ごとの構成比
    let mut stack_series = String::new();
    for (si, shift) in shift_types.iter().enumerate() {
        let color = shift_colors.get(si).unwrap_or(&"#64748b");
        let mut vals = String::new();
        let mut cnts = String::new();
        for band in bands {
            let cnt = data_map.get(&(band.to_string(), shift.to_string())).copied().unwrap_or(0);
            let band_total: i64 = shift_types.iter()
                .map(|s| data_map.get(&(band.to_string(), s.to_string())).copied().unwrap_or(0))
                .sum();
            let pct = if band_total > 0 { cnt as f64 / band_total as f64 * 100.0 } else { 0.0 };
            vals.push_str(&format!("{:.1},", pct));
            cnts.push_str(&format!("{cnt},"));
        }
        stack_series.push_str(&format!(
            "{{name:'{}',type:'bar',stack:'total',emphasis:{{focus:'series'}},\
             data:[{vals}],_counts:[{cnts}],itemStyle:{{color:'{color}'}}}},",
            escape_html(shift)
        ));
    }

    let x_labels: String = shift_types.iter().map(|s| format!("'{}',", escape_html(s))).collect();
    let y_labels: String = bands.iter().map(|s| format!("'{}',", escape_html(s))).collect();

    format!(
        r##"<!-- {title} - 統合カード -->
    <div class="stat-card lg:col-span-2 p-0 overflow-hidden">
        <h4 class="text-sm font-bold text-slate-300 px-5 pt-4 pb-2">{title} - 給与×シフト分析</h4>
        <div class="grid grid-cols-1 lg:grid-cols-2">
            <div class="p-4 lg:border-r border-slate-700/50">
                <p class="text-xs text-slate-500 mb-1 text-center">件数分布</p>
                <div id="{heatmap_id}" style="height:360px"></div>
            </div>
            <div class="p-4">
                <p class="text-xs text-slate-500 mb-1 text-center">構成比 (%)</p>
                <div id="{stack_id}" style="height:360px"></div>
            </div>
        </div>
        <div class="mx-4 mb-3 px-2 py-1.5 bg-slate-800/50 rounded border border-slate-700/30">
            <p class="text-xs text-slate-500">
                <span class="text-slate-400 font-medium">読み方:</span>
                縦軸が給与帯、横軸がシフトタイプ。色が濃いセルほど求職者数が多いセグメントです。
            </p>
        </div>
        <script>
        (function(){{
            var c = echarts.init(document.getElementById('{heatmap_id}'), 'dark');
            c.setOption({{
                tooltip:{{
                    position:'top',
                    backgroundColor:'rgba(15,23,42,0.95)',
                    borderColor:'#334155',
                    textStyle:{{fontSize:13,color:'#e2e8f0'}},
                    formatter:function(p){{
                        var yLabels=[{y_labels}];
                        var xLabels=[{x_labels}];
                        var band=yLabels[p.data[1]];
                        var shift=xLabels[p.data[0]];
                        var cnt=p.data[2];
                        var pct=p.data[3];
                        return '<b>'+band+' × '+shift+'</b><br/>'
                            +'求職者数: <span style="color:#60a5fa;font-weight:bold">'+cnt.toLocaleString()+'件</span><br/>'
                            +'<span style="color:#94a3b8">全体の '+pct+'%</span>';
                    }}
                }},
                xAxis:{{type:'category',position:'top',data:[{x_labels}],axisLabel:{{color:'#94a3b8',fontSize:12,rotate:0}},splitArea:{{show:true}}}},
                yAxis:{{type:'category',data:[{y_labels}],axisLabel:{{color:'#94a3b8',fontSize:12}}}},
                visualMap:{{
                    type:'piecewise',
                    pieces:[
                        {{value:0,label:'0件',color:'rgba(148,163,184,0.1)'}},
                        {{gt:0,lte:Math.ceil({max_val}*0.2),label:'少',color:'#bfdbfe'}},
                        {{gt:Math.ceil({max_val}*0.2),lte:Math.ceil({max_val}*0.5),label:'中',color:'#60a5fa'}},
                        {{gt:Math.ceil({max_val}*0.5),lte:Math.ceil({max_val}*0.8),label:'多',color:'#2563eb'}},
                        {{gt:Math.ceil({max_val}*0.8),label:'最多',color:'#1e40af'}}
                    ],
                    orient:'horizontal',left:'center',bottom:0,
                    textStyle:{{color:'#cbd5e1',fontSize:11}}
                }},
                series:[{{
                    type:'heatmap',data:[{heatmap_data}],
                    label:{{
                        show:true,
                        fontSize:11,
                        fontWeight:'bold',
                        color:'#e2e8f0',
                        formatter:function(p){{
                            var cnt=p.data[2];
                            var pct=p.data[3];
                            if(cnt===0) return '';
                            if(cnt<5) return cnt+'';
                            return cnt+'\n('+pct+'%)';
                        }}
                    }},
                    emphasis:{{itemStyle:{{shadowBlur:10,shadowColor:'rgba(0,0,0,0.5)'}}}}
                }}],
                grid:{{left:'18%',right:'5%',top:'12%',bottom:'18%'}}
            }});
            new ResizeObserver(()=>c.resize()).observe(document.getElementById('{heatmap_id}'));
        }})();
        (function(){{
            var c = echarts.init(document.getElementById('{stack_id}'), 'dark');
            c.setOption({{
                tooltip:{{
                    trigger:'axis',axisPointer:{{type:'shadow'}},
                    backgroundColor:'rgba(15,23,42,0.95)',
                    borderColor:'#334155',
                    textStyle:{{fontSize:13,color:'#e2e8f0'}},
                    formatter:function(ps){{
                        var s='<b>'+ps[0].axisValue+'</b><br>';
                        ps.forEach(function(p){{
                            if(p.value>0){{
                                s+=p.marker+' '+p.seriesName+': <span style="color:#60a5fa;font-weight:bold">'+p.value.toFixed(1)+'%</span><br>';
                            }}
                        }});
                        return s;
                    }}
                }},
                legend:{{data:[{x_labels}],textStyle:{{color:'#94a3b8',fontSize:10}},top:0}},
                xAxis:{{type:'category',data:[{y_labels}],axisLabel:{{color:'#94a3b8',fontSize:11}}}},
                yAxis:{{type:'value',max:100,axisLabel:{{color:'#94a3b8',formatter:'{{value}}%'}}}},
                series:[{stack_series}],
                grid:{{left:'10%',right:'5%',top:'40px',bottom:'8%'}}
            }});
            new ResizeObserver(()=>c.resize()).observe(document.getElementById('{stack_id}'));
        }})();
        </script>
    </div>"##,
    )
}
