use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;
use crate::geo::pref_name_to_code;

use super::overview::{get_str, get_i64, get_f64, format_number, get_session_filters, make_location_label};

/// タブ7用クエリパラメータ（地図クリック等から受け取る）
#[derive(Deserialize, Default)]
pub struct TalentMapTabQuery {
    pub municipality: Option<String>,
}

/// タブ7: 人材地図（コロプレス地図 + 4モード + サイドバー）
pub async fn tab_talentmap(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<TalentMapTabQuery>,
) -> Html<String> {
    let (job_type, prefecture, session_muni) = get_session_filters(&session).await;

    // クエリパラメータのmunicipalityがあればそちらを優先（地図クリック時）
    let municipality = if let Some(ref qm) = params.municipality {
        if !qm.is_empty() { qm.clone() } else { session_muni }
    } else {
        session_muni
    };

    let cache_key = format!("talentmap_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_talentmap(&state, &job_type, &prefecture, &municipality).await;
    let html = render_talentmap(&job_type, &prefecture, &municipality, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ===== データ構造 =====

struct MarkerData {
    municipality: String,
    prefecture: String,
    lat: f64,
    lng: f64,
    count: i64,
    male_count: i64,
    female_count: i64,
}

struct FlowLine {
    from_pref: String,
    from_muni: String,
    from_lat: f64,
    from_lng: f64,
    to_pref: String,
    to_muni: String,
    to_lat: f64,
    to_lng: f64,
    count: i64,
}

struct MuniDetail {
    count: i64,
    male_count: i64,
    female_count: i64,
    age_gender: Vec<(String, i64, i64)>, // (age_group, male, female)
    workstyle_dist: Vec<(String, i64)>,  // (workstyle, count)
}

struct TalentMapStats {
    markers: Vec<MarkerData>,
    flows: Vec<FlowLine>,
    muni_detail: Option<MuniDetail>,
    total_count: i64,
}

impl Default for TalentMapStats {
    fn default() -> Self {
        Self {
            markers: Vec::new(),
            flows: Vec::new(),
            muni_detail: None,
            total_count: 0,
        }
    }
}

// ===== データ取得 =====

async fn fetch_talentmap(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> TalentMapStats {
    let mut stats = TalentMapStats::default();
    let pref = if prefecture.is_empty() || prefecture == "全国" { "" } else { prefecture };

    // マーカーデータ取得（SUMMARY + 座標あり）
    let mut sql = String::from(
        "SELECT prefecture, municipality, latitude, longitude, \
         male_count, female_count, applicant_count \
         FROM job_seeker_data \
         WHERE job_type = ? AND row_type = 'SUMMARY' \
         AND latitude IS NOT NULL AND longitude IS NOT NULL \
         AND latitude != '' AND longitude != ''"
    );
    let mut params = vec![Value::String(job_type.to_string())];

    if !pref.is_empty() {
        sql.push_str(" AND prefecture = ?");
        params.push(Value::String(pref.to_string()));
    }

    if let Ok(rows) = state.turso.query(&sql, &params).await {
        for row in &rows {
            let lat = get_f64(row, "latitude");
            let lng = get_f64(row, "longitude");
            if lat == 0.0 || lng == 0.0 { continue; }
            let count = get_i64(row, "applicant_count");
            let male = get_i64(row, "male_count");
            let female = get_i64(row, "female_count");

            stats.markers.push(MarkerData {
                municipality: get_str(row, "municipality"),
                prefecture: get_str(row, "prefecture"),
                lat,
                lng,
                count,
                male_count: male,
                female_count: female,
            });
        }
    }

    stats.total_count = stats.markers.iter().map(|m| m.count).sum();

    // フローデータ取得（都道府県選択時のみ）
    if !pref.is_empty() {
        let flow_sql = "SELECT category1 as from_pref, category2 as from_muni, \
                        category3 as to_pref, municipality as to_muni, \
                        latitude as from_lat, longitude as from_lng, \
                        target_lat as to_lat, target_lng as to_lng, \
                        count \
                        FROM job_seeker_data \
                        WHERE job_type = ? AND row_type = 'FLOW_EDGE' AND prefecture = ? \
                        ORDER BY count DESC LIMIT 50";
        let flow_params = vec![
            Value::String(job_type.to_string()),
            Value::String(pref.to_string()),
        ];

        if let Ok(rows) = state.turso.query(flow_sql, &flow_params).await {
            for row in &rows {
                let from_lat = get_f64(row, "from_lat");
                let from_lng = get_f64(row, "from_lng");
                let to_lat = get_f64(row, "to_lat");
                let to_lng = get_f64(row, "to_lng");
                if from_lat == 0.0 || to_lat == 0.0 { continue; }

                stats.flows.push(FlowLine {
                    from_pref: get_str(row, "from_pref"),
                    from_muni: get_str(row, "from_muni"),
                    from_lat,
                    from_lng,
                    to_pref: get_str(row, "to_pref"),
                    to_muni: get_str(row, "to_muni"),
                    to_lat,
                    to_lng,
                    count: get_i64(row, "count"),
                });
            }
        }

        // 市区町村詳細（選択時）
        let muni = if municipality.is_empty() || municipality == "すべて" { "" } else { municipality };
        if !muni.is_empty() {
            stats.muni_detail = fetch_muni_detail(state, job_type, pref, muni).await;
        }
    }

    stats
}

async fn fetch_muni_detail(state: &AppState, job_type: &str, pref: &str, muni: &str) -> Option<MuniDetail> {
    // 基本データ
    let sql = "SELECT male_count, female_count, applicant_count \
               FROM job_seeker_data \
               WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture = ? AND municipality = ?";
    let params = vec![
        Value::String(job_type.to_string()),
        Value::String(pref.to_string()),
        Value::String(muni.to_string()),
    ];

    let (count, male, female) = if let Ok(rows) = state.turso.query(sql, &params).await {
        if let Some(row) = rows.first() {
            (get_i64(row, "applicant_count"), get_i64(row, "male_count"), get_i64(row, "female_count"))
        } else {
            return None;
        }
    } else {
        return None;
    };

    // 年齢×性別データ
    let ag_sql = "SELECT category1, male_count, female_count \
                  FROM job_seeker_data \
                  WHERE job_type = ? AND row_type = 'AGE_GENDER' AND prefecture = ? AND municipality = ? \
                  ORDER BY category1";
    let ag_params = vec![
        Value::String(job_type.to_string()),
        Value::String(pref.to_string()),
        Value::String(muni.to_string()),
    ];
    let mut age_gender = Vec::new();
    if let Ok(rows) = state.turso.query(ag_sql, &ag_params).await {
        for row in &rows {
            let age = get_str(row, "category1");
            if !age.is_empty() {
                age_gender.push((age, get_i64(row, "male_count"), get_i64(row, "female_count")));
            }
        }
    }

    // 雇用形態分布（WORKSTYLE_DISTRIBUTIONがない場合はSUMMARYのみ）
    let ws_sql = "SELECT category1, count \
                  FROM job_seeker_data \
                  WHERE job_type = ? AND row_type = 'WORKSTYLE_DISTRIBUTION' AND prefecture = ? AND municipality = ? \
                  ORDER BY count DESC";
    let ws_params = vec![
        Value::String(job_type.to_string()),
        Value::String(pref.to_string()),
        Value::String(muni.to_string()),
    ];
    let mut workstyle_dist = Vec::new();
    if let Ok(rows) = state.turso.query(ws_sql, &ws_params).await {
        for row in &rows {
            let ws = get_str(row, "category1");
            if !ws.is_empty() {
                workstyle_dist.push((ws, get_i64(row, "count")));
            }
        }
    }

    Some(MuniDetail {
        count,
        male_count: male,
        female_count: female,
        age_gender,
        workstyle_dist,
    })
}

// ===== レンダリング =====

fn render_talentmap(job_type: &str, prefecture: &str, municipality: &str, stats: &TalentMapStats) -> String {
    let location_label = make_location_label(prefecture, municipality);
    let pref = if prefecture.is_empty() || prefecture == "全国" { "" } else { prefecture };
    let muni = if municipality.is_empty() || municipality == "すべて" { "" } else { municipality };

    // 地図の中心・ズーム
    let (map_lat, map_lng, map_zoom) = if !pref.is_empty() {
        // 都道府県選択時: 最初のマーカーの中心 or デフォルト
        let avg_lat = if stats.markers.is_empty() { 36.5 }
            else { stats.markers.iter().map(|m| m.lat).sum::<f64>() / stats.markers.len() as f64 };
        let avg_lng = if stats.markers.is_empty() { 138.0 }
            else { stats.markers.iter().map(|m| m.lng).sum::<f64>() / stats.markers.len() as f64 };
        (avg_lat, avg_lng, 9)
    } else {
        (36.5, 138.0, 5)
    };

    // GeoJSON URL
    let geojson_url = if !pref.is_empty() {
        let code_map = pref_name_to_code();
        if let Some(code) = code_map.get(pref) {
            let romaji = pref_code_to_romaji(code);
            format!("/api/geojson/{}_{}.json", code, romaji)
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // コロプレススタイル（市区町村別色分け）
    let choropleth_styles = build_choropleth_styles(&stats.markers, muni);

    // マーカーJSON
    let markers_json = build_markers_json(&stats.markers);

    // フローJSON
    let flows_json = build_flows_json(&stats.flows);

    // サイドバー
    let sidebar = build_sidebar(muni, pref, &stats.muni_detail, &stats.markers);

    // 凡例
    let legend = build_legend("basic");
    let data_summary = build_data_summary(stats);

    include_str!("../../templates/tabs/talentmap.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{LOCATION_LABEL}}", &location_label)
        .replace("{{MAP_LAT}}", &format!("{:.4}", map_lat))
        .replace("{{MAP_LNG}}", &format!("{:.4}", map_lng))
        .replace("{{MAP_ZOOM}}", &map_zoom.to_string())
        .replace("{{GEOJSON_URL}}", &geojson_url)
        .replace("{{CHOROPLETH_STYLES}}", &choropleth_styles)
        .replace("{{MARKERS_JSON}}", &markers_json)
        .replace("{{FLOWS_JSON}}", &flows_json)
        .replace("{{MAP_MODE}}", "basic")
        .replace("{{SELECTED_MUNI}}", muni)
        .replace("{{SIDEBAR_CONTENT}}", &sidebar)
        .replace("{{MODE_LABEL}}", "基本表示")
        .replace("{{LEGEND_ITEMS}}", &legend)
        .replace("{{DATA_SUMMARY}}", &data_summary)
        // フィルタ状態（初期値はすべて未選択）
        .replace("{{WS_SEL_SEI}}", "")
        .replace("{{WS_SEL_PART}}", "")
        .replace("{{WS_SEL_OTHER}}", "")
        .replace("{{AGE_SEL_20}}", "")
        .replace("{{AGE_SEL_30}}", "")
        .replace("{{AGE_SEL_40}}", "")
        .replace("{{AGE_SEL_50}}", "")
        .replace("{{GENDER_SEL_M}}", "")
        .replace("{{GENDER_SEL_F}}", "")
        .replace("{{MODE_BASIC}}", "checked")
        .replace("{{MODE_INFLOW}}", "")
        .replace("{{MODE_BALANCE}}", "")
        .replace("{{MODE_COMPETING}}", "")
}

// ===== サイドバー詳細API（HTMX用） =====

#[derive(Deserialize)]
pub struct TalentMapDetailQuery {
    pub prefecture: Option<String>,
    pub municipality: Option<String>,
}

/// 市区町村詳細サイドバー API: /api/talentmap/detail?prefecture=東京都&municipality=新宿区
pub async fn api_talentmap_detail(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<TalentMapDetailQuery>,
) -> Html<String> {
    let job_type: String = session
        .get::<String>("job_type")
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "介護職".to_string());

    let pref = params.prefecture.as_deref().unwrap_or("");
    let muni = params.municipality.as_deref().unwrap_or("");

    if pref.is_empty() || muni.is_empty() {
        return Html(build_sidebar_placeholder());
    }

    let detail = fetch_muni_detail(&state, &job_type, pref, muni).await;

    // マーカーデータも取得（基本情報表示用）
    let marker_sql = "SELECT latitude, longitude, applicant_count, male_count, female_count \
                      FROM job_seeker_data \
                      WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture = ? AND municipality = ?";
    let marker_params = vec![
        Value::String(job_type),
        Value::String(pref.to_string()),
        Value::String(muni.to_string()),
    ];

    let markers = if let Ok(rows) = state.turso.query(marker_sql, &marker_params).await {
        rows.iter().map(|row| MarkerData {
            municipality: muni.to_string(),
            prefecture: pref.to_string(),
            lat: get_f64(row, "latitude"),
            lng: get_f64(row, "longitude"),
            count: get_i64(row, "applicant_count"),
            male_count: get_i64(row, "male_count"),
            female_count: get_i64(row, "female_count"),
        }).collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    Html(build_sidebar(muni, pref, &detail, &markers))
}

// ===== ヘルパー関数 =====

fn pref_code_to_romaji(code: &str) -> &'static str {
    match code {
        "01" => "hokkaido", "02" => "aomori", "03" => "iwate", "04" => "miyagi",
        "05" => "akita", "06" => "yamagata", "07" => "fukushima", "08" => "ibaraki",
        "09" => "tochigi", "10" => "gunma", "11" => "saitama", "12" => "chiba",
        "13" => "tokyo", "14" => "kanagawa", "15" => "niigata", "16" => "toyama",
        "17" => "ishikawa", "18" => "fukui", "19" => "yamanashi", "20" => "nagano",
        "21" => "gifu", "22" => "shizuoka", "23" => "aichi", "24" => "mie",
        "25" => "shiga", "26" => "kyoto", "27" => "osaka", "28" => "hyogo",
        "29" => "nara", "30" => "wakayama", "31" => "tottori", "32" => "shimane",
        "33" => "okayama", "34" => "hiroshima", "35" => "yamaguchi", "36" => "tokushima",
        "37" => "kagawa", "38" => "ehime", "39" => "kochi", "40" => "fukuoka",
        "41" => "saga", "42" => "nagasaki", "43" => "kumamoto", "44" => "oita",
        "45" => "miyazaki", "46" => "kagoshima", "47" => "okinawa",
        _ => "unknown",
    }
}

fn build_choropleth_styles(markers: &[MarkerData], selected_muni: &str) -> String {
    if markers.is_empty() {
        return "{}".to_string();
    }

    let max_count = markers.iter().map(|m| m.count).max().unwrap_or(1).max(1);
    let mut styles: HashMap<&str, String> = HashMap::new();

    for m in markers {
        if m.municipality.is_empty() { continue; }

        let (fill_color, fill_opacity, weight, border_color) = if !selected_muni.is_empty() && m.municipality == selected_muni {
            // 選択中の市区町村: シアン強調
            ("#00d4ff".to_string(), 0.8, 3, "#ffffff")
        } else {
            // 人数に応じた色（緑→黄→赤のグラデーション）
            let ratio = m.count as f64 / max_count as f64;
            let color = count_to_color(ratio);
            (color, 0.6, 1, "#ffffff")
        };

        styles.insert(&m.municipality, format!(
            r##"{{"fillColor":"{}","weight":{},"opacity":1,"color":"{}","fillOpacity":{:.1}}}"##,
            fill_color, weight, border_color, fill_opacity
        ));
    }

    // JSONオブジェクトとして組み立て
    let entries: Vec<String> = styles.iter()
        .map(|(k, v)| format!(r#""{}": {}"#, k, v))
        .collect();
    format!("{{{}}}", entries.join(","))
}

fn count_to_color(ratio: f64) -> String {
    // 薄い黄緑(低) → 緑(中) → 濃い青緑(高) のヒートマップ配色
    let t = ratio.clamp(0.0, 1.0);
    if t < 0.5 {
        // 薄黄(#ffffcc) → 緑(#41b6c4)
        let s = t * 2.0;
        let r = (255.0 - s * 190.0) as u8;  // 255→65
        let g = (255.0 - s * 73.0) as u8;   // 255→182
        let b = (204.0 - s * 8.0) as u8;    // 204→196
        format!("#{:02x}{:02x}{:02x}", r, g, b)
    } else {
        // 緑(#41b6c4) → 濃紺(#253494)
        let s = (t - 0.5) * 2.0;
        let r = (65.0 - s * 28.0) as u8;    // 65→37
        let g = (182.0 - s * 130.0) as u8;  // 182→52
        let b = (196.0 - s * 48.0) as u8;   // 196→148
        format!("#{:02x}{:02x}{:02x}", r, g, b)
    }
}

fn build_markers_json(markers: &[MarkerData]) -> String {
    let items: Vec<String> = markers.iter().take(200).map(|m| {
        let radius = ((m.count as f64 / 50.0).max(4.0)).min(12.0);
        format!(
            r##"{{"lat":{:.6},"lng":{:.6},"count":{},"radius":{:.1},"name":"{}","male":{},"female":{}}}"##,
            m.lat, m.lng, m.count, radius, m.municipality, m.male_count, m.female_count
        )
    }).collect();
    format!("[{}]", items.join(","))
}

fn build_flows_json(flows: &[FlowLine]) -> String {
    let items: Vec<String> = flows.iter().take(50).map(|f| {
        let weight = ((f.count as f64 / 100.0).max(1.0)).min(8.0);
        format!(
            r##"{{"from":[{:.6},{:.6}],"to":[{:.6},{:.6}],"count":{},"weight":{:.1}}}"##,
            f.from_lat, f.from_lng, f.to_lat, f.to_lng, f.count, weight
        )
    }).collect();
    format!("[{}]", items.join(","))
}

fn build_sidebar(muni: &str, pref: &str, detail: &Option<MuniDetail>, markers: &[MarkerData]) -> String {
    if muni.is_empty() || pref.is_empty() {
        return build_sidebar_placeholder();
    }

    let marker = markers.iter().find(|m| m.municipality == muni);

    let mut html = String::new();

    // タイトル
    html.push_str(&format!(
        r##"<div class="text-lg font-bold mb-2" style="color: #56B4E9;">📍 {}</div>
        <div style="border-bottom: 1px solid rgba(255,255,255,0.1); margin-bottom: 8px;"></div>"##,
        muni
    ));

    if let Some(d) = detail {
        // 基本情報
        html.push_str(r##"<div class="mb-3">
            <div class="text-sm font-bold text-white mb-1">📊 基本情報</div>"##);
        html.push_str(&format!(
            r##"<div class="text-xs text-slate-400">求職者数: {}人</div>"##,
            format_number(d.count)
        ));
        html.push_str(&format!(
            r##"<div class="text-xs text-slate-400">男性: {}人 / 女性: {}人</div>"##,
            format_number(d.male_count), format_number(d.female_count)
        ));
        if d.count > 0 {
            let female_pct = d.female_count as f64 / d.count as f64 * 100.0;
            html.push_str(&format!(
                r##"<div class="text-xs text-slate-400">女性比率: {:.1}%</div>"##,
                female_pct
            ));
        }
        html.push_str("</div>");

        // 年齢×性別構成（EChartsピラミッドチャート）
        if !d.age_gender.is_empty() {
            html.push_str(r##"<div class="mb-3">
                <div class="text-sm font-bold text-white mb-1">👥 年齢×性別構成</div>"##);

            let age_order = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
            let mut labels = Vec::new();
            let mut male_vals = Vec::new();
            let mut female_vals = Vec::new();

            for age in &age_order {
                if let Some((_, male, female)) = d.age_gender.iter().find(|(a, _, _)| a == age) {
                    labels.push(format!("\"{}\"", age));
                    male_vals.push(format!("{}", -male)); // 男性は負値（左側）
                    female_vals.push(format!("{}", female));
                }
            }

            let total_m: i64 = d.age_gender.iter().map(|(_, m, _)| m).sum();
            let total_f: i64 = d.age_gender.iter().map(|(_, _, f)| f).sum();

            // EChartsピラミッド設定
            html.push_str(&format!(
                r##"<div class="echart" style="height:220px;width:100%;" data-chart-config='{{
                    "tooltip": {{
                        "trigger": "axis",
                        "axisPointer": {{"type": "shadow"}},
                        "formatter": "function(p){{var r=p[0].axisValue;var s=r;p.forEach(function(i){{s+=\"<br/>\"+i.marker+i.seriesName+\": \"+Math.abs(i.value)+\"人\"}});return s}}"
                    }},
                    "legend": {{
                        "data": ["男性","女性"],
                        "top": 0,
                        "textStyle": {{"color":"#94a3b8","fontSize":10}}
                    }},
                    "grid": {{"left":"8%","right":"8%","top":"28px","bottom":"4px","containLabel":true}},
                    "xAxis": {{
                        "type": "value",
                        "axisLabel": {{
                            "formatter": "function(v){{return Math.abs(v)}}"
                        }},
                        "splitLine": {{"lineStyle":{{"color":"rgba(255,255,255,0.05)"}}}}
                    }},
                    "yAxis": {{
                        "type": "category",
                        "data": [{}],
                        "axisTick": {{"show":false}},
                        "axisLabel": {{"fontSize":10}}
                    }},
                    "series": [
                        {{
                            "name": "男性",
                            "type": "bar",
                            "stack": "total",
                            "data": [{}],
                            "itemStyle": {{"color":"#0072B2","borderRadius":[4,0,0,4]}},
                            "barWidth": 16
                        }},
                        {{
                            "name": "女性",
                            "type": "bar",
                            "stack": "total",
                            "data": [{}],
                            "itemStyle": {{"color":"#E69F00","borderRadius":[0,4,4,0]}},
                            "barWidth": 16
                        }}
                    ]
                }}'></div>"##,
                labels.join(","),
                male_vals.join(","),
                female_vals.join(",")
            ));

            html.push_str(&format!(
                r##"<div style="display: flex; justify-content: space-between; margin-top: 4px; font-size: 0.7rem;">
                    <span style="color: #0072B2;">♂ 計 {}人</span>
                    <span style="color: #E69F00;">♀ 計 {}人</span>
                </div>"##,
                format_number(total_m), format_number(total_f)
            ));

            html.push_str("</div>");
        }

        // 雇用形態分布
        if !d.workstyle_dist.is_empty() {
            html.push_str(r##"<div class="mb-3">
                <div class="text-sm font-bold text-white mb-1">💼 希望雇用形態（上位5件）</div>"##);

            let ws_total: i64 = d.workstyle_dist.iter().map(|(_, c)| c).sum();
            for (ws, cnt) in d.workstyle_dist.iter().take(5) {
                let pct = if ws_total > 0 { *cnt as f64 / ws_total as f64 * 100.0 } else { 0.0 };
                html.push_str(&format!(
                    r##"<div class="text-xs text-slate-400">{}: {}人 ({:.0}%)</div>"##,
                    ws, format_number(*cnt), pct
                ));
            }
            html.push_str("</div>");
        }
    } else if let Some(m) = marker {
        // 詳細データなし、マーカーのみ
        html.push_str(r##"<div class="mb-3">
            <div class="text-sm font-bold text-white mb-1">📊 基本情報</div>"##);
        html.push_str(&format!(
            r##"<div class="text-xs text-slate-400">求職者数: {}人</div>"##,
            format_number(m.count)
        ));
        html.push_str("</div>");
    } else {
        html.push_str(r##"<div class="text-xs text-slate-400">この市区町村のデータがありません</div>"##);
    }

    html
}

fn build_sidebar_placeholder() -> String {
    r##"<div class="text-lg font-bold mb-2" style="color: #e2e8f0;">📍 市区町村詳細</div>
    <div style="border-bottom: 1px solid rgba(255,255,255,0.1); margin-bottom: 8px;"></div>
    <div class="text-sm text-slate-400">地図上で市区町村をクリックすると</div>
    <div class="text-sm text-slate-400">詳細情報が表示されます</div>
    <div style="margin: 16px 0;"></div>
    <div class="text-sm font-bold text-white">💡 ヒント</div>
    <div class="text-xs text-slate-400 mt-1">• ポリゴンをクリックで選択</div>
    <div class="text-xs text-slate-400">• 表示モードで分析切替</div>
    <div class="text-xs text-slate-400">• フィルタで絞り込み可能</div>"##.to_string()
}

fn build_legend(mode: &str) -> String {
    let items = match mode {
        "inflow" => vec![
            "緑●: 選択地域（流入先）",
            "━→: 流入矢印（TOP5）",
            "赤●: 主要流入元（上位10%）",
            "橙●: 重要流入元",
            "黄●: 中程度",
            "薄橙●: 少数",
        ],
        "balance" => vec![
            "濃青: 流入優位（>65%）",
            "薄青: やや流入優位",
            "緑: バランス",
            "薄赤: やや流出優位",
            "濃赤: 流出優位（<35%）",
        ],
        "competing" => vec![
            "赤: 強い競合（>20%）",
            "オレンジ: 中程度（10-20%）",
            "黄: 弱い競合（5-10%）",
            "薄橙: ほぼ競合なし",
        ],
        _ => vec![ // basic
            "🗺️ ポリゴン色: 求職者数（濃い青=多い、薄い青=少ない）",
            "⚪ マーカー: 市区町村の求職者数",
            "  小(4px): ~200人、中(8px): ~400人、大(12px): 600人~",
            "━ フロー線: 居住地→希望勤務地",
            "  細(1px): ~100人、太(8px): 800人~",
            "🖱️ クリックで市区町村を選択",
        ],
    };

    items.iter()
        .map(|item| format!(r##"<div class="text-xs text-slate-400">{}</div>"##, item))
        .collect::<Vec<_>>()
        .join("\n")
}

fn build_data_summary(stats: &TalentMapStats) -> String {
    let mut items = Vec::new();

    items.push(format!(
        r##"<div class="text-xs text-slate-400">市区町村数: {}</div>"##,
        stats.markers.len()
    ));

    let with_data = stats.markers.iter().filter(|m| m.count > 0).count();
    items.push(format!(
        r##"<div class="text-xs text-slate-400">データあり: {}</div>"##,
        with_data
    ));

    let max_count = stats.markers.iter().map(|m| m.count).max().unwrap_or(0);
    items.push(format!(
        r##"<div class="text-xs text-slate-400">最大値: {}人</div>"##,
        format_number(max_count)
    ));

    items.push(format!(
        r##"<div class="text-xs text-slate-400">総求職者数: {}人</div>"##,
        format_number(stats.total_count)
    ));

    if !stats.flows.is_empty() {
        items.push(format!(
            r##"<div class="text-xs text-slate-400">フロー数: {}本</div>"##,
            stats.flows.len()
        ));
    }

    items.join("\n")
}
