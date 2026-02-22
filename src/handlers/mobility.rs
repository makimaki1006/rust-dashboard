use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::models::job_seeker::{has_turso_data, render_no_turso_data};
use crate::AppState;

use super::overview::{get_str, get_i64, get_f64, format_number, get_session_filters, build_location_filter, make_location_label};

/// 隣接県マップ（NiceGUI版 ADJACENT_PREFECTURES 完全移植）
fn adjacent_prefectures(pref: &str) -> &'static [&'static str] {
    match pref {
        "北海道" => &["青森県"],
        "青森県" => &["北海道", "岩手県", "秋田県"],
        "岩手県" => &["青森県", "秋田県", "宮城県"],
        "宮城県" => &["岩手県", "秋田県", "山形県", "福島県"],
        "秋田県" => &["青森県", "岩手県", "宮城県", "山形県"],
        "山形県" => &["秋田県", "宮城県", "福島県", "新潟県"],
        "福島県" => &["宮城県", "山形県", "新潟県", "群馬県", "栃木県", "茨城県"],
        "茨城県" => &["福島県", "栃木県", "埼玉県", "千葉県"],
        "栃木県" => &["福島県", "茨城県", "群馬県", "埼玉県"],
        "群馬県" => &["福島県", "新潟県", "長野県", "埼玉県", "栃木県"],
        "埼玉県" => &["群馬県", "栃木県", "茨城県", "千葉県", "東京都", "山梨県", "長野県"],
        "千葉県" => &["茨城県", "埼玉県", "東京都"],
        "東京都" => &["埼玉県", "千葉県", "神奈川県", "山梨県"],
        "神奈川県" => &["東京都", "山梨県", "静岡県"],
        "新潟県" => &["山形県", "福島県", "群馬県", "長野県", "富山県"],
        "富山県" => &["新潟県", "長野県", "岐阜県", "石川県"],
        "石川県" => &["富山県", "岐阜県", "福井県"],
        "福井県" => &["石川県", "岐阜県", "滋賀県", "京都府"],
        "山梨県" => &["埼玉県", "東京都", "神奈川県", "長野県", "静岡県"],
        "長野県" => &["新潟県", "群馬県", "埼玉県", "山梨県", "静岡県", "愛知県", "岐阜県", "富山県"],
        "岐阜県" => &["富山県", "石川県", "福井県", "長野県", "愛知県", "三重県", "滋賀県"],
        "静岡県" => &["神奈川県", "山梨県", "長野県", "愛知県"],
        "愛知県" => &["静岡県", "長野県", "岐阜県", "三重県"],
        "三重県" => &["愛知県", "岐阜県", "滋賀県", "京都府", "奈良県", "和歌山県"],
        "滋賀県" => &["福井県", "岐阜県", "三重県", "京都府"],
        "京都府" => &["福井県", "滋賀県", "三重県", "奈良県", "大阪府", "兵庫県"],
        "大阪府" => &["京都府", "奈良県", "和歌山県", "兵庫県"],
        "兵庫県" => &["京都府", "大阪府", "鳥取県", "岡山県", "徳島県"],
        "奈良県" => &["三重県", "京都府", "大阪府", "和歌山県"],
        "和歌山県" => &["三重県", "奈良県", "大阪府"],
        "鳥取県" => &["兵庫県", "岡山県", "島根県", "広島県"],
        "島根県" => &["鳥取県", "広島県", "山口県"],
        "岡山県" => &["兵庫県", "鳥取県", "広島県", "香川県"],
        "広島県" => &["鳥取県", "島根県", "岡山県", "山口県", "愛媛県"],
        "山口県" => &["島根県", "広島県", "福岡県"],
        "徳島県" => &["兵庫県", "香川県", "愛媛県", "高知県"],
        "香川県" => &["徳島県", "愛媛県", "岡山県"],
        "愛媛県" => &["徳島県", "香川県", "高知県", "広島県"],
        "高知県" => &["徳島県", "愛媛県"],
        "福岡県" => &["山口県", "佐賀県", "熊本県", "大分県"],
        "佐賀県" => &["福岡県", "長崎県"],
        "長崎県" => &["佐賀県"],
        "熊本県" => &["福岡県", "大分県", "宮崎県", "鹿児島県"],
        "大分県" => &["福岡県", "熊本県", "宮崎県"],
        "宮崎県" => &["大分県", "熊本県", "鹿児島県"],
        "鹿児島県" => &["熊本県", "宮崎県"],
        "沖縄県" => &[],
        _ => &[],
    }
}

/// 大都市圏拡張（NiceGUI版 METRO_EXTENDED 完全移植）
fn metro_extended(pref: &str) -> &'static [&'static str] {
    match pref {
        "東京都" => &["茨城県", "栃木県", "群馬県", "静岡県", "長野県", "新潟県"],
        "大阪府" => &["三重県", "岡山県", "徳島県", "香川県", "福井県"],
        "愛知県" => &["滋賀県", "福井県", "石川県", "富山県"],
        "福岡県" => &["長崎県", "宮崎県", "鹿児島県"],
        "神奈川県" => &["静岡県"],
        "埼玉県" => &["茨城県", "栃木県", "群馬県"],
        "千葉県" => &["茨城県"],
        "京都府" => &["岡山県"],
        "兵庫県" => &["岡山県", "香川県"],
        "広島県" => &["香川県"],
        "宮城県" => &["岩手県", "福島県", "山形県"],
        _ => &[],
    }
}

/// 現実的なフローかを判定（同一県、隣接県、大都市圏拡張）
fn is_realistic_flow(source_pref: &str, target_pref: &str) -> bool {
    if source_pref == target_pref {
        return true;
    }
    if adjacent_prefectures(target_pref).contains(&source_pref) {
        return true;
    }
    if metro_extended(target_pref).contains(&source_pref) {
        return true;
    }
    false
}

/// タブ3: 地域・移動パターン - HTMXパーシャルHTML
pub async fn tab_mobility(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    if !has_turso_data(&job_type) {
        return Html(render_no_turso_data(&job_type, "地域・移動パターン"));
    }

    let cache_key = format!("mobility_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_mobility(&state, &job_type, &prefecture, &municipality).await;
    let html = render_mobility(&job_type, &prefecture, &municipality, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct MobilityStats {
    avg_distance: f64,
    /// 移動型分布 (mobility_type, count)
    mobility_types: Vec<(String, i64)>,
    /// 主要フロー 都道府県間 (from_pref, to_pref, count) 上位10
    pref_flows: Vec<(String, String, i64)>,
    /// 市区町村間フロー (from_muni, to_muni, count) 上位10
    muni_flows: Vec<(String, String, i64)>,
    /// 流入人数（他県→選択県）
    inflow: i64,
    /// 流出人数（選択県→他県）
    outflow: i64,
    /// 地元志向人数（選択県→選択県）
    local_count: i64,
    /// 求職者合計
    applicant_count: i64,
    /// フロー合計
    total_flow: i64,
    /// 距離 25パーセンタイル
    distance_q25: f64,
    /// 距離 中央値
    distance_median: f64,
    /// 距離 75パーセンタイル
    distance_q75: f64,
    /// 都道府県が選択されているか
    has_prefecture: bool,
    /// 採用圏カード用: 主要流入元 (地名, 人数) 上位5
    top_inflow_sources: Vec<(String, i64)>,
    /// 採用圏カード用: 主要流出先 (地名, 人数) 上位5
    top_outflow_targets: Vec<(String, i64)>,
    /// 地元志向率
    local_pct: f64,
    /// 地域サマリー: 女性比率
    female_ratio: String,
    /// 地域サマリー: 主要年齢層
    top_age: String,
    /// 地域サマリー: 主要年齢層の比率
    top_age_ratio: String,
    /// 地域サマリー: 平均資格数
    avg_qualification_count: String,
    /// 資格別定着率 (資格名, retention_rate, interpretation, count)
    retention_rates: Vec<(String, f64, String, i64)>,
}

impl Default for MobilityStats {
    fn default() -> Self {
        Self {
            avg_distance: 0.0,
            mobility_types: Vec::new(),
            pref_flows: Vec::new(),
            muni_flows: Vec::new(),
            inflow: 0,
            outflow: 0,
            local_count: 0,
            applicant_count: 0,
            total_flow: 0,
            distance_q25: 0.0,
            distance_median: 0.0,
            distance_q75: 0.0,
            has_prefecture: false,
            top_inflow_sources: Vec::new(),
            top_outflow_targets: Vec::new(),
            local_pct: 0.0,
            female_ratio: String::new(),
            top_age: String::new(),
            top_age_ratio: String::new(),
            avg_qualification_count: String::new(),
            retention_rates: Vec::new(),
        }
    }
}

async fn fetch_mobility(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> MobilityStats {
    let has_pref = !prefecture.is_empty() && prefecture != "全国";
    let has_muni = has_pref && !municipality.is_empty() && municipality != "すべて";

    // RESIDENCE_FLOW用のSQLを構築
    // 市区町村選択時は双方向フローを取得（流入元データも含める）
    let (sql, params) = if has_muni {
        // 出身地が選択市区町村のレコード（流出 + 地元）
        // + 希望地が選択市区町村のレコード（流入）
        let sql = format!(
            "SELECT row_type, prefecture, municipality, \
                   desired_prefecture, desired_municipality, \
                   avg_reference_distance_km, mobility_type, count \
            FROM job_seeker_data \
            WHERE job_type = ? \
              AND row_type = 'RESIDENCE_FLOW' \
              AND ((prefecture = ? AND municipality = ?) \
                OR (desired_prefecture = ? AND desired_municipality = ?))"
        );
        let params = vec![
            Value::String(job_type.to_string()),
            Value::String(prefecture.to_string()),
            Value::String(municipality.to_string()),
            Value::String(prefecture.to_string()),
            Value::String(municipality.to_string()),
        ];
        (sql, params)
    } else {
        // 都道府県のみ or 全国: 既存ロジック
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
        (sql, params)
    };

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
    let mut pref_flow_map: HashMap<(String, String), i64> = HashMap::new();
    let mut muni_flow_map: HashMap<(String, String), i64> = HashMap::new();
    let mut inflow_source_map: HashMap<String, i64> = HashMap::new();
    let mut outflow_target_map: HashMap<String, i64> = HashMap::new();
    let mut distance_values: Vec<(f64, i64)> = Vec::new();

    stats.has_prefecture = has_pref;

    for row in &rows {
        let dist = get_f64(row, "avg_reference_distance_km");
        let cnt = get_i64(row, "count");
        let mobility = get_str(row, "mobility_type");
        let from_pref = get_str(row, "prefecture");
        let to_pref = get_str(row, "desired_prefecture");
        let from_muni = get_str(row, "municipality");
        let to_muni = get_str(row, "desired_municipality");

        // 距離・移動パターンは居住地が選択市区町村の行のみ集計
        let is_origin_row = if has_muni {
            from_pref == prefecture && from_muni == municipality
        } else {
            true
        };

        if is_origin_row {
            if dist > 0.0 && cnt > 0 {
                dist_sum += dist * cnt as f64;
                dist_count += cnt;
                distance_values.push((dist, cnt));
            }

            if !mobility.is_empty() {
                *mobility_map.entry(mobility).or_insert(0) += cnt;
            }
        }

        stats.total_flow += cnt;

        // 隣接県フィルタ（NiceGUI版と同一ロジック）
        let realistic = is_realistic_flow(&from_pref, &to_pref);

        // 流入・流出・地元志向の集計（都道府県選択時のみ）
        if has_pref && cnt > 0 && !from_pref.is_empty() && !to_pref.is_empty() {
            if has_muni {
                // 市区町村レベル判定
                if from_muni == municipality && to_muni == municipality {
                    stats.local_count += cnt;
                } else if to_muni == municipality && from_muni != municipality && realistic {
                    stats.inflow += cnt;
                    let name = if from_muni.is_empty() { from_pref.clone() } else { from_muni.clone() };
                    *inflow_source_map.entry(name).or_insert(0) += cnt;
                } else if from_muni == municipality && to_muni != municipality && realistic {
                    stats.outflow += cnt;
                    let name = if to_muni.is_empty() { to_pref.clone() } else { to_muni.clone() };
                    *outflow_target_map.entry(name).or_insert(0) += cnt;
                }
            } else {
                // 都道府県レベル判定（従来ロジック）
                if from_pref == prefecture && to_pref == prefecture {
                    stats.local_count += cnt;
                } else if from_pref != prefecture && to_pref == prefecture && realistic {
                    stats.inflow += cnt;
                    *inflow_source_map.entry(from_pref.clone()).or_insert(0) += cnt;
                } else if from_pref == prefecture && to_pref != prefecture && realistic {
                    stats.outflow += cnt;
                    *outflow_target_map.entry(to_pref.clone()).or_insert(0) += cnt;
                }
            }
        }

        // 都道府県間フロー（隣接県フィルタ適用）
        if !from_pref.is_empty() && !to_pref.is_empty() && from_pref != to_pref && realistic {
            *pref_flow_map.entry((from_pref, to_pref)).or_insert(0) += cnt;
        }

        // 市区町村間フロー（隣接県フィルタ適用）
        if !from_muni.is_empty() && !to_muni.is_empty() && from_muni != to_muni && realistic {
            *muni_flow_map.entry((from_muni, to_muni)).or_insert(0) += cnt;
        }
    }

    stats.avg_distance = if dist_count > 0 { dist_sum / dist_count as f64 } else { 0.0 };
    stats.applicant_count = stats.inflow + stats.local_count;

    // 距離分位数の計算
    if !distance_values.is_empty() {
        distance_values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let total_weight: i64 = distance_values.iter().map(|(_, c)| c).sum();
        if total_weight > 0 {
            stats.distance_q25 = weighted_percentile(&distance_values, total_weight, 0.25);
            stats.distance_median = weighted_percentile(&distance_values, total_weight, 0.50);
            stats.distance_q75 = weighted_percentile(&distance_values, total_weight, 0.75);
        }
    }

    // 移動型分布
    let mut mobility_list: Vec<(String, i64)> = mobility_map.into_iter().collect();
    mobility_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.mobility_types = mobility_list;

    // 都道府県間フローTop10
    let mut pref_flow_list: Vec<(String, String, i64)> = pref_flow_map.into_iter().map(|((f, t), c)| (f, t, c)).collect();
    pref_flow_list.sort_by(|a, b| b.2.cmp(&a.2));
    stats.pref_flows = pref_flow_list.into_iter().take(10).collect();

    // 市区町村間フローTop10
    let mut muni_flow_list: Vec<(String, String, i64)> = muni_flow_map.into_iter().map(|((f, t), c)| (f, t, c)).collect();
    muni_flow_list.sort_by(|a, b| b.2.cmp(&a.2));
    stats.muni_flows = muni_flow_list.into_iter().take(10).collect();

    // 流入元・流出先Top3
    let mut inflow_list: Vec<(String, i64)> = inflow_source_map.into_iter().collect();
    inflow_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.top_inflow_sources = inflow_list.into_iter().take(3).collect();

    let mut outflow_list: Vec<(String, i64)> = outflow_target_map.into_iter().collect();
    outflow_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.top_outflow_targets = outflow_list.into_iter().take(3).collect();

    // 地元志向率
    stats.local_pct = if stats.applicant_count > 0 {
        (stats.local_count as f64 / stats.applicant_count as f64) * 100.0
    } else {
        0.0
    };

    // 地域サマリー + 資格別定着率を tokio::join! で並列取得
    if has_pref {
        let (region_result, retention_result) = tokio::join!(
            fetch_region_summary_data(state, job_type, prefecture, municipality),
            fetch_retention_rates_data(state, job_type, prefecture, municipality)
        );
        apply_region_summary(&mut stats, region_result);
        apply_retention_rates(&mut stats, retention_result);
    } else {
        let retention_result = fetch_retention_rates_data(state, job_type, prefecture, municipality).await;
        apply_retention_rates(&mut stats, retention_result);
    }

    stats
}

/// 地域サマリーデータのみ取得（tokio::join!用）
async fn fetch_region_summary_data(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> Vec<HashMap<String, Value>> {
    let mut sql = String::from(
        "SELECT total_applicants, female_ratio, category1, top_age_ratio, avg_qualification_count \
         FROM job_seeker_data WHERE job_type = ? AND row_type = 'COMPETITION'"
    );
    let mut params = vec![Value::String(job_type.to_string())];

    if !prefecture.is_empty() && prefecture != "全国" {
        sql.push_str(" AND prefecture = ?");
        params.push(Value::String(prefecture.to_string()));
    }
    if !municipality.is_empty() && municipality != "すべて" {
        sql.push_str(" AND municipality LIKE ?");
        params.push(Value::String(format!("{}%", municipality)));
    }
    sql.push_str(" LIMIT 1");

    state.turso.query(&sql, &params).await.unwrap_or_default()
}

/// 地域サマリーデータをstatsに適用
fn apply_region_summary(stats: &mut MobilityStats, rows: Vec<HashMap<String, Value>>) {
    if let Some(row) = rows.first() {
        let female_r = get_f64(row, "female_ratio");
        stats.female_ratio = if female_r > 0.0 { format!("{:.1}%", female_r * 100.0) } else { "-".to_string() };
        stats.top_age = get_str(row, "category1");
        let age_r = get_f64(row, "top_age_ratio");
        stats.top_age_ratio = if age_r > 0.0 { format!("{:.1}%", age_r * 100.0) } else { "-".to_string() };
        let qual = get_f64(row, "avg_qualification_count");
        stats.avg_qualification_count = if qual > 0.0 { format!("{:.1}", qual) } else { "-".to_string() };
    }
}

/// 資格別定着率データのみ取得（tokio::join!用）
async fn fetch_retention_rates_data(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> Vec<HashMap<String, Value>> {
    let mut sql = String::from(
        "SELECT category1, retention_rate, count \
         FROM job_seeker_data WHERE job_type = ? AND row_type = 'QUALIFICATION_DETAIL' \
         AND retention_rate IS NOT NULL"
    );
    let mut params = vec![Value::String(job_type.to_string())];

    if !prefecture.is_empty() && prefecture != "全国" {
        sql.push_str(" AND prefecture = ?");
        params.push(Value::String(prefecture.to_string()));
    }
    if !municipality.is_empty() && municipality != "すべて" {
        sql.push_str(" AND municipality LIKE ?");
        params.push(Value::String(format!("{}%", municipality)));
    }

    state.turso.query(&sql, &params).await.unwrap_or_default()
}

/// 資格別定着率データをstatsに適用
fn apply_retention_rates(stats: &mut MobilityStats, rows: Vec<HashMap<String, Value>>) {
    let mut qual_map: HashMap<String, (f64, i64, i64)> = HashMap::new();
    for row in &rows {
        let qual = get_str(row, "category1");
        let rate = get_f64(row, "retention_rate");
        let cnt = get_i64(row, "count");
        if !qual.is_empty() && rate > 0.0 {
            let entry = qual_map.entry(qual).or_insert((0.0, 0, 0));
            entry.0 += rate;
            entry.1 += cnt;
            entry.2 += 1;
        }
    }

    let mut retention_list: Vec<(String, f64, String, i64)> = qual_map.into_iter()
        .map(|(qual, (sum_rate, total_count, n))| {
            let avg_rate = sum_rate / n as f64;
            let interp = if avg_rate >= 1.1 {
                "地元志向強".to_string()
            } else if avg_rate >= 1.0 {
                "地元志向".to_string()
            } else if avg_rate >= 0.9 {
                "平均的".to_string()
            } else {
                "流出傾向".to_string()
            };
            (qual, avg_rate, interp, total_count)
        })
        .collect();

    retention_list.sort_by(|a, b| b.3.cmp(&a.3));
    stats.retention_rates = retention_list.into_iter().take(10).collect();
}

/// 重み付き分位数を計算する
fn weighted_percentile(values: &[(f64, i64)], total_weight: i64, p: f64) -> f64 {
    let target = (total_weight as f64) * p;
    let mut cumulative: f64 = 0.0;
    for (val, weight) in values {
        cumulative += *weight as f64;
        if cumulative >= target {
            return *val;
        }
    }
    values.last().map(|(v, _)| *v).unwrap_or(0.0)
}

fn render_mobility(job_type: &str, prefecture: &str, municipality: &str, stats: &MobilityStats) -> String {
    let location_label = make_location_label(prefecture, municipality);

    // ===== 採用圏分析カード =====
    let recruitment_area_card = build_recruitment_area_card(stats);

    // ===== 流入出KPI（NiceGUI版準拠） =====
    let flow_kpi_section = build_flow_kpi(stats);

    // ===== 都道府県フローリスト =====
    let pref_flow_list = build_flow_list(&stats.pref_flows, "フローデータがありません");

    // ===== 市区町村フローリスト =====
    let muni_flow_list = if stats.muni_flows.is_empty() {
        r#"<p class="text-sm text-slate-500">市区町村を選択するとフローを表示</p>"#.to_string()
    } else {
        build_flow_list(&stats.muni_flows, "")
    };

    // ===== 地域サマリーカード =====
    let region_summary_section = build_region_summary(stats);

    // ===== 移動パターン棒グラフ（NiceGUI版: ドーナツ→bar） =====
    let (mobility_bar_chart, mobility_pct_badges) = build_mobility_bar(stats);

    // ===== 資格別定着率カード =====
    let retention_section = build_retention_section(stats);

    include_str!("../../templates/tabs/mobility.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{LOCATION_LABEL}}", &location_label)
        .replace("{{RECRUITMENT_AREA_CARD}}", &recruitment_area_card)
        .replace("{{FLOW_KPI_SECTION}}", &flow_kpi_section)
        .replace("{{PREF_FLOW_LIST}}", &pref_flow_list)
        .replace("{{MUNI_FLOW_LIST}}", &muni_flow_list)
        .replace("{{REGION_SUMMARY_SECTION}}", &region_summary_section)
        .replace("{{MOBILITY_BAR_CHART}}", &mobility_bar_chart)
        .replace("{{MOBILITY_PCT_BADGES}}", &mobility_pct_badges)
        .replace("{{DISTANCE_Q25}}", &format!("{:.1}", stats.distance_q25))
        .replace("{{DISTANCE_MEDIAN}}", &format!("{:.1}", stats.distance_median))
        .replace("{{DISTANCE_Q75}}", &format!("{:.1}", stats.distance_q75))
        .replace("{{RETENTION_SECTION}}", &retention_section)
}

/// 採用圏分析カードのHTML生成（都道府県選択時のみ表示）
fn build_recruitment_area_card(stats: &MobilityStats) -> String {
    if !stats.has_prefecture {
        return String::new();
    }

    let local_eval = if stats.local_pct > 70.0 {
        "地元志向が非常に強い地域"
    } else if stats.local_pct > 50.0 {
        "地元志向がやや強い地域"
    } else {
        "広域から人材が集まる地域"
    };

    let inflow_html: String = if stats.top_inflow_sources.is_empty() {
        r#"<span class="text-slate-500 text-sm">データなし</span>"#.to_string()
    } else {
        stats.top_inflow_sources.iter()
            .filter(|(_, cnt)| *cnt >= 2) // ノイズ除去
            .map(|(name, cnt)| {
                let pct = if stats.inflow > 0 { *cnt as f64 / stats.inflow as f64 * 100.0 } else { 0.0 };
                format!(
                    r#"<span class="inline-flex items-center gap-1 bg-slate-700 rounded px-2 py-1 text-sm"><span class="text-green-400">&larr;</span> {} <span class="text-slate-400">({}人, {:.0}%)</span></span>"#,
                    name, format_number(*cnt), pct
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    };

    let outflow_html: String = if stats.top_outflow_targets.is_empty() {
        r#"<span class="text-slate-500 text-sm">データなし</span>"#.to_string()
    } else {
        stats.top_outflow_targets.iter()
            .filter(|(_, cnt)| *cnt >= 2) // ノイズ除去
            .map(|(name, cnt)| {
                let pct = if stats.outflow > 0 { *cnt as f64 / stats.outflow as f64 * 100.0 } else { 0.0 };
                format!(
                    r#"<span class="inline-flex items-center gap-1 bg-slate-700 rounded px-2 py-1 text-sm"><span class="text-red-400">&rarr;</span> {} <span class="text-slate-400">({}人, {:.0}%)</span></span>"#,
                    name, format_number(*cnt), pct
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    };

    // 採用圏拡大提案テキスト
    let expansion = if stats.local_pct < 50.0 && !stats.top_inflow_sources.is_empty() {
        let top_source = &stats.top_inflow_sources[0].0;
        format!("{}など近隣エリアへの求人露出強化を推奨", top_source)
    } else if stats.local_pct > 70.0 {
        "地元志向が強いため、近隣エリアへの採用圏拡大を検討".to_string()
    } else {
        String::new()
    };

    let expansion_html = if !expansion.is_empty() {
        format!(
            r#"<div class="p-3 rounded-lg flex-1" style="background-color: rgba(245, 158, 11, 0.1);">
                <div class="text-xs text-slate-400">採用圏拡大の提案</div>
                <div class="text-sm" style="color: #F59E0B;">{}</div>
            </div>"#,
            expansion
        )
    } else {
        String::new()
    };

    // 主要流入元のテキスト
    let source_text = stats.top_inflow_sources.iter().take(3)
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let source_html = if !source_text.is_empty() {
        format!(
            r#"<div class="p-3 rounded-lg" style="background-color: rgba(16, 185, 129, 0.1);">
                <div class="text-xs text-slate-400">主要流入元</div>
                <div class="text-sm font-semibold" style="color: #10B981;">{}</div>
            </div>"#,
            source_text
        )
    } else {
        String::new()
    };

    format!(
        r##"<div class="stat-card" style="border: 2px solid #06B6D4;">
    <div class="flex items-center gap-2 mb-2">
        <span class="text-xl">🌐</span>
        <span class="text-lg font-bold" style="color: #06B6D4;">採用圏の言語化</span>
    </div>
    <p class="text-sm text-slate-300 mb-3" style="line-height: 1.6;">{local_eval}（地元志向率: {local_pct:.1}%）</p>
    <div class="flex flex-wrap gap-4">
        <div class="p-3 rounded-lg" style="background-color: rgba(6, 182, 212, 0.1);">
            <div class="text-xs text-slate-400">地元志向率</div>
            <div class="text-xl font-bold" style="color: #06B6D4;">{local_pct:.1}%</div>
        </div>
        {source_html}
        {expansion_html}
    </div>
    <div class="space-y-2 mt-3">
        <div>
            <div class="text-xs text-green-400 mb-1">主要流入元（上位3）</div>
            <div class="flex flex-wrap gap-1">{inflow_html}</div>
        </div>
        <div>
            <div class="text-xs text-red-400 mb-1">主要流出先（上位3）</div>
            <div class="flex flex-wrap gap-1">{outflow_html}</div>
        </div>
    </div>
</div>"##,
        local_eval = local_eval,
        local_pct = stats.local_pct,
        source_html = source_html,
        expansion_html = expansion_html,
        inflow_html = inflow_html,
        outflow_html = outflow_html,
    )
}

/// フローKPIセクション（NiceGUI版準拠: 4つのKPI + 流入元/流出先リスト）
fn build_flow_kpi(stats: &MobilityStats) -> String {
    if !stats.has_prefecture {
        return r#"<div class="text-slate-500 text-sm italic">※ 都道府県を選択すると流入・流出の詳細が表示されます</div>"#.to_string();
    }

    if stats.applicant_count == 0 && stats.inflow == 0 {
        return r#"<p class="text-sm text-slate-500">市区町村を選択すると人材フローを表示します</p>"#.to_string();
    }

    // 人材吸引力
    let flow_ratio = if stats.outflow > 0 {
        format!("{:.2}x", stats.inflow as f64 / stats.outflow as f64)
    } else if stats.inflow > 0 {
        "∞".to_string()
    } else {
        "N/A".to_string()
    };

    // 流入元リスト（割合表示付き、少数ノイズフィルタ）
    let inflow_total: i64 = stats.top_inflow_sources.iter().map(|(_, c)| c).sum();
    let inflow_source_html: String = if stats.top_inflow_sources.is_empty() {
        r#"<p class="text-sm text-slate-500">市区町村を選択すると表示</p>"#.to_string()
    } else {
        let mut items: Vec<String> = stats.top_inflow_sources.iter().take(3)
            .filter(|(_, cnt)| *cnt >= 2) // ノイズ除去: 2人未満を除外
            .map(|(name, cnt)| {
                let pct = if inflow_total > 0 { *cnt as f64 / inflow_total as f64 * 100.0 } else { 0.0 };
                format!(
                    r#"<div class="flex items-center justify-between"><span class="text-sm text-white">{}</span><span class="text-sm text-slate-400">{}人 <span style="color:#10b981;">({:.0}%)</span></span></div>"#,
                    name, format_number(*cnt), pct
                )
            }).collect();
        if items.is_empty() {
            items.push(r#"<p class="text-sm text-slate-500">有意な流入データなし</p>"#.to_string());
        }
        items.join("\n")
    };

    // 流出先リスト（割合表示付き、少数ノイズフィルタ）
    let outflow_total: i64 = stats.top_outflow_targets.iter().map(|(_, c)| c).sum();
    let outflow_target_html: String = if stats.top_outflow_targets.is_empty() || stats.outflow == 0 {
        r#"<p class="text-sm text-slate-500">流出データなし（地元志向が高いエリアです）</p>"#.to_string()
    } else {
        let mut items: Vec<String> = stats.top_outflow_targets.iter().take(3)
            .filter(|(_, cnt)| *cnt >= 2) // ノイズ除去: 2人未満を除外
            .map(|(name, cnt)| {
                let pct = if outflow_total > 0 { *cnt as f64 / outflow_total as f64 * 100.0 } else { 0.0 };
                format!(
                    r#"<div class="flex items-center justify-between"><span class="text-sm text-white">{}</span><span class="text-sm text-slate-400">{}人 <span style="color:#ef4444;">({:.0}%)</span></span></div>"#,
                    name, format_number(*cnt), pct
                )
            }).collect();
        if items.is_empty() {
            items.push(r#"<p class="text-sm text-slate-500">有意な流出データなし</p>"#.to_string());
        }
        items.join("\n")
    };

    format!(
        r##"<div class="flex flex-wrap gap-4 mb-4">
    <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(16, 185, 129, 0.1);">
        <div class="text-xs text-slate-400">流入（就職希望）</div>
        <div class="flex items-end gap-1">
            <span class="text-2xl font-bold" style="color: #10b981;">{}</span>
            <span class="text-sm text-slate-400">人</span>
        </div>
    </div>
    <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(245, 158, 11, 0.1);">
        <div class="text-xs text-slate-400">地元志向率</div>
        <div class="flex items-end gap-1">
            <span class="text-2xl font-bold" style="color: #f59e0b;">{:.1}</span>
            <span class="text-sm text-slate-400">%</span>
        </div>
        <div class="text-xs text-slate-500">({}人)</div>
    </div>
    <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(239, 68, 68, 0.1);">
        <div class="text-xs text-slate-400">流出（他地域希望）</div>
        <div class="flex items-end gap-1">
            <span class="text-2xl font-bold" style="color: #ef4444;">{}</span>
            <span class="text-sm text-slate-400">人</span>
        </div>
    </div>
    <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(59, 130, 246, 0.1);">
        <div class="text-xs text-slate-400">人材吸引力</div>
        <span class="text-2xl font-bold" style="color: #3b82f6;">{}</span>
    </div>
</div>
<div class="flex flex-col md:flex-row gap-4">
    <div class="flex-1 p-4 rounded-lg" style="background-color: rgba(16, 185, 129, 0.08);">
        <div class="flex items-center gap-2 mb-2">
            <div class="w-3 h-3 rounded-sm" style="background-color: #10b981;"></div>
            <span class="text-sm font-semibold text-white">流入元（どこから来るか）</span>
        </div>
        {}
    </div>
    <div class="flex-1 p-4 rounded-lg" style="background-color: rgba(239, 68, 68, 0.08);">
        <div class="flex items-center gap-2 mb-2">
            <div class="w-3 h-3 rounded-sm" style="background-color: #ef4444;"></div>
            <span class="text-sm font-semibold text-white">流出先（どこへ流れるか）</span>
        </div>
        {}
    </div>
</div>
<div class="text-xs text-slate-500 mt-2 italic">※ 隣接県・広域圏フローのみ表示。広域登録ユーザーのノイズは除外済み（2人未満除外）</div>"##,
        format_number(stats.inflow),
        stats.local_pct,
        format_number(stats.local_count),
        format_number(stats.outflow),
        flow_ratio,
        inflow_source_html,
        outflow_target_html,
    )
}

/// フローリストHTML生成（都道府県/市区町村共通）
fn build_flow_list(flows: &[(String, String, i64)], empty_msg: &str) -> String {
    if flows.is_empty() {
        return format!(r#"<p class="text-sm text-slate-500">{}</p>"#, empty_msg);
    }

    flows.iter().map(|(from, to, cnt)| {
        format!(
            r#"<div class="flex items-center py-1">
                <span class="text-sm font-medium" style="color: #56B4E9;">{}</span>
                <span class="text-sm mx-1 text-slate-400">→</span>
                <span class="text-sm font-medium" style="color: #D55E00;">{}</span>
                <div class="flex-grow"></div>
                <span class="text-sm text-slate-400">{}件</span>
            </div>"#,
            from, to, format_number(*cnt)
        )
    }).collect::<Vec<_>>().join("\n")
}

/// 地域サマリーカード
fn build_region_summary(stats: &MobilityStats) -> String {
    if !stats.has_prefecture || stats.inflow == 0 {
        return String::new();
    }

    format!(
        r##"<div class="stat-card">
    <div class="flex items-center gap-2 mb-2">
        <span class="text-xl">📊</span>
        <span class="text-lg font-semibold text-white">地域サマリー</span>
    </div>
    <p class="text-sm text-slate-500 mb-4">選択地域の人材プロファイル概要</p>
    <div class="flex flex-wrap gap-4">
        <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(59, 130, 246, 0.1);">
            <div class="text-xs text-slate-400">総求職者数</div>
            <div class="text-xl font-bold text-white">{}人</div>
        </div>
        <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(230, 159, 0, 0.1);">
            <div class="text-xs text-slate-400">女性比率</div>
            <div class="text-xl font-bold" style="color: #E69F00;">{}</div>
        </div>
        <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(99, 102, 241, 0.1);">
            <div class="text-xs text-slate-400">主要年齢層</div>
            <div class="text-lg font-bold" style="color: #6366F1;">{}</div>
            <div class="text-xs text-slate-500">({})</div>
        </div>
        <div class="flex-1 p-4 rounded-lg min-w-[140px]" style="background-color: rgba(16, 185, 129, 0.1);">
            <div class="text-xs text-slate-400">平均資格数</div>
            <div class="flex items-end gap-1">
                <span class="text-xl font-bold" style="color: #009E73;">{}</span>
                <span class="text-sm text-slate-400">個</span>
            </div>
        </div>
    </div>
</div>"##,
        format_number(stats.inflow),
        stats.female_ratio,
        stats.top_age,
        stats.top_age_ratio,
        stats.avg_qualification_count,
    )
}

/// 移動パターン棒グラフ（NiceGUI版: barチャート）
fn build_mobility_bar(stats: &MobilityStats) -> (String, String) {
    if stats.mobility_types.is_empty() {
        return (
            r#"<p class="text-sm text-slate-500">移動パターンデータがありません</p>"#.to_string(),
            String::new(),
        );
    }

    let total: i64 = stats.mobility_types.iter().map(|(_, c)| c).sum();

    let labels: Vec<String> = stats.mobility_types.iter()
        .map(|(m, _)| format!("\"{}\"", m))
        .collect();
    let values: Vec<String> = stats.mobility_types.iter()
        .map(|(_, v)| v.to_string())
        .collect();

    let bar_chart = format!(
        r##"<div class="echart" style="height:320px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis"}},
            "xAxis": {{"type": "category", "data": [{}]}},
            "yAxis": {{"type": "value"}},
            "series": [{{"data": [{}], "type": "bar", "itemStyle": {{"color": "#56B4E9", "borderRadius": [8, 8, 0, 0]}}}}]
        }}'></div>"##,
        labels.join(","),
        values.join(","),
    );

    // パーセンテージバッジ
    let badges: String = stats.mobility_types.iter().map(|(m, c)| {
        let pct = if total > 0 { (*c as f64 / total as f64) * 100.0 } else { 0.0 };
        format!(
            r#"<div class="flex-1 p-2 rounded-md text-center" style="background-color: rgba(255,255,255,0.05);">
                <div class="text-xs text-slate-400">{}</div>
                <div class="text-sm font-semibold text-white">{:.1}%</div>
            </div>"#,
            m, pct
        )
    }).collect::<Vec<_>>().join("\n");

    let badges_html = format!(r#"<div class="flex gap-2 mt-2">{}</div>"#, badges);

    (bar_chart, badges_html)
}

/// 資格別定着率カード
fn build_retention_section(stats: &MobilityStats) -> String {
    if stats.retention_rates.is_empty() {
        return r##"<div class="stat-card">
    <div class="flex items-center gap-2 mb-2">
        <span class="text-xl">🏠</span>
        <span class="text-lg font-semibold text-white">資格別定着率</span>
    </div>
    <p class="text-sm text-slate-500">定着率データがありません</p>
</div>"##.to_string();
    }

    let rows: String = stats.retention_rates.iter().map(|(qual, rate, interp, cnt)| {
        let (rate_color, badge_color) = match interp.as_str() {
            "地元志向強" => ("#009E73", "#065f46"),
            "地元志向" => ("#56B4E9", "#1e3a5f"),
            "平均的" => ("#94a3b8", "#374151"),
            _ => ("#f59e0b", "#7c2d12"),
        };
        format!(
            r##"<div class="flex items-center py-1">
                <span class="text-sm font-semibold text-white" style="min-width: 120px;">{}</span>
                <div class="flex-grow"></div>
                <span class="text-sm font-semibold" style="color: {}; min-width: 50px;">{:.2}</span>
                <span class="text-xs px-2 py-0.5 rounded mx-2" style="background-color: {}; color: {};">{}</span>
                <span class="text-xs text-slate-500" style="min-width: 60px;">({}人)</span>
            </div>"##,
            qual, rate_color, rate, badge_color, rate_color, interp, format_number(*cnt)
        )
    }).collect::<Vec<_>>().join("\n");

    format!(
        r##"<div class="stat-card">
    <div class="flex items-center gap-2 mb-2">
        <span class="text-xl">🏠</span>
        <span class="text-lg font-semibold text-white">資格別定着率</span>
    </div>
    <p class="text-sm text-slate-500 mb-4">資格保有者の地元定着傾向（1.0以上＝地元志向）</p>
    <div style="max-height: 350px; overflow-y: auto;">
        {}
    </div>
    <div class="flex flex-wrap gap-2 mt-4">
        <span class="text-xs px-2 py-0.5 rounded" style="background-color: #065f46; color: #009E73;">≥1.1 地元志向強</span>
        <span class="text-xs px-2 py-0.5 rounded" style="background-color: #1e3a5f; color: #56B4E9;">≥1.0 地元志向</span>
        <span class="text-xs px-2 py-0.5 rounded" style="background-color: #374151; color: #94a3b8;">≥0.9 平均的</span>
        <span class="text-xs px-2 py-0.5 rounded" style="background-color: #7c2d12; color: #f59e0b;">&lt;0.9 流出傾向</span>
    </div>
</div>"##,
        rows
    )
}
