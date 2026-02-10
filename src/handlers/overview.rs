use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::auth::{SESSION_JOB_TYPE_KEY, SESSION_PREFECTURE_KEY, SESSION_MUNICIPALITY_KEY};
use crate::AppState;

/// セッションから共通フィルタ値を取得するヘルパー
pub async fn get_session_filters(session: &Session) -> (String, String, String) {
    let job_type: String = session
        .get(SESSION_JOB_TYPE_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "介護職".to_string());
    let prefecture: String = session
        .get(SESSION_PREFECTURE_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let municipality: String = session
        .get(SESSION_MUNICIPALITY_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    (job_type, prefecture, municipality)
}

/// SQLのWHERE句とパラメータを構築するヘルパー
pub fn build_location_filter(prefecture: &str, municipality: &str, params: &mut Vec<Value>) -> String {
    let mut clause = String::new();
    if !prefecture.is_empty() {
        clause.push_str(" AND prefecture = ?");
        params.push(Value::String(prefecture.to_string()));
    }
    if !municipality.is_empty() {
        clause.push_str(" AND municipality = ?");
        params.push(Value::String(municipality.to_string()));
    }
    clause
}

/// タブ1: 市場概況 - HTMXパーシャルHTML
pub async fn tab_overview(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("overview_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_national_stats(&state, &job_type, &prefecture, &municipality).await;
    let location_label = make_location_label(&prefecture, &municipality);
    let html = render_overview(&job_type, &stats, &location_label, &prefecture);

    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

/// 地域ラベル生成
pub fn make_location_label(pref: &str, muni: &str) -> String {
    if pref.is_empty() {
        "全国".to_string()
    } else if muni.is_empty() {
        pref.to_string()
    } else {
        format!("{} {}", pref, muni)
    }
}

/// 全国統計データ
struct NatStats {
    male_count: i64,
    female_count: i64,
    avg_age: f64,
    avg_desired_areas: f64,
    avg_qualifications: f64,
    avg_distance_km: f64,
    age_distribution: Vec<(String, i64)>,
    age_gender: Vec<(String, i64, i64)>, // (age_group, male, female)
    supply_count: i64,
    demand_count: i64,
    inflow: f64,
    outflow: f64,

    // 3層比較用（全国データ。都道府県選択時のみ値が入る）
    national_total: i64,
    national_avg_age: f64,
    national_avg_desired_areas: f64,
    national_avg_qualifications: f64,
    national_avg_distance_km: f64,
    national_male_count: i64,
    national_female_count: i64,
}

impl Default for NatStats {
    fn default() -> Self {
        Self {
            male_count: 0,
            female_count: 0,
            avg_age: 0.0,
            avg_desired_areas: 0.0,
            avg_qualifications: 0.0,
            avg_distance_km: 0.0,
            age_distribution: Vec::new(),
            age_gender: Vec::new(),
            supply_count: 0,
            demand_count: 0,
            inflow: 0.0,
            outflow: 0.0,

            national_total: 0,
            national_avg_age: 0.0,
            national_avg_desired_areas: 0.0,
            national_avg_qualifications: 0.0,
            national_avg_distance_km: 0.0,
            national_male_count: 0,
            national_female_count: 0,
        }
    }
}

/// Tursoから統計データを取得（都道府県/市区町村フィルタ対応）
async fn fetch_national_stats(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> NatStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    let sql = format!(
        "SELECT row_type, prefecture, municipality, \
               avg_desired_areas, avg_qualifications, male_count, female_count, \
               avg_reference_distance_km, category1, category2, count, \
               applicant_count, avg_age, \
               supply_count, demand_count, inflow, outflow \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type IN ('SUMMARY', 'AGE_GENDER', 'GAP', 'RESIDENCE_FLOW'){location_filter}"
    );
    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Turso query failed: {e}");
            return NatStats::default();
        }
    };

    let mut stats = NatStats::default();
    let mut total_male: i64 = 0;
    let mut total_female: i64 = 0;
    let mut age_sum: f64 = 0.0;
    let mut age_count: f64 = 0.0;
    let mut desired_sum: f64 = 0.0;
    let mut qual_sum: f64 = 0.0;
    let mut dist_values: Vec<f64> = Vec::new();
    let mut summary_count: i64 = 0;

    // 年齢層集計用
    let age_order = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
    let mut age_dist: HashMap<String, i64> = HashMap::new();
    let mut age_male: HashMap<String, i64> = HashMap::new();
    let mut age_female: HashMap<String, i64> = HashMap::new();

    // 需給・フロー集計用
    let mut total_supply: i64 = 0;
    let mut total_demand: i64 = 0;
    let mut total_inflow: f64 = 0.0;
    let mut total_outflow: f64 = 0.0;

    for row in &rows {
        let row_type = get_str(row, "row_type");

        match row_type.as_str() {
            "SUMMARY" => {
                let male = get_i64(row, "male_count");
                let female = get_i64(row, "female_count");
                total_male += male;
                total_female += female;

                let avg_age_val = get_f64(row, "avg_age");
                let total = male + female;
                if avg_age_val > 0.0 && total > 0 {
                    age_sum += avg_age_val * total as f64;
                    age_count += total as f64;
                }

                desired_sum += get_f64(row, "avg_desired_areas") * total as f64;
                qual_sum += get_f64(row, "avg_qualifications") * total as f64;
                summary_count += total;

                let dist = get_f64(row, "avg_reference_distance_km");
                if dist > 0.0 {
                    dist_values.push(dist);
                }
            }
            "AGE_GENDER" => {
                let age_group = get_str(row, "category1");
                let gender = get_str(row, "category2");
                let cnt = get_i64(row, "count");

                *age_dist.entry(age_group.clone()).or_insert(0) += cnt;

                if gender.contains('男') {
                    *age_male.entry(age_group).or_insert(0) += cnt;
                } else if gender.contains('女') {
                    *age_female.entry(age_group).or_insert(0) += cnt;
                }
            }
            "GAP" => {
                total_supply += get_i64(row, "supply_count");
                total_demand += get_i64(row, "demand_count");
            }
            "RESIDENCE_FLOW" => {
                total_inflow += get_f64(row, "inflow");
                total_outflow += get_f64(row, "outflow");
            }
            _ => {}
        }
    }

    stats.male_count = total_male;
    stats.female_count = total_female;
    stats.avg_age = if age_count > 0.0 {
        age_sum / age_count
    } else {
        0.0
    };
    stats.avg_desired_areas = if summary_count > 0 {
        desired_sum / summary_count as f64
    } else {
        0.0
    };
    stats.avg_qualifications = if summary_count > 0 {
        qual_sum / summary_count as f64
    } else {
        0.0
    };
    stats.avg_distance_km = if !dist_values.is_empty() {
        dist_values.iter().sum::<f64>() / dist_values.len() as f64
    } else {
        0.0
    };

    // 需給・フロー統計を格納
    stats.supply_count = total_supply;
    stats.demand_count = total_demand;
    stats.inflow = total_inflow;
    stats.outflow = total_outflow;

    // 年齢層分布を順序付き配列に変換
    for age in &age_order {
        let total = age_dist.get(*age).copied().unwrap_or(0);
        stats.age_distribution.push((age.to_string(), total));

        let male = age_male.get(*age).copied().unwrap_or(0);
        let female = age_female.get(*age).copied().unwrap_or(0);
        stats.age_gender.push((age.to_string(), male, female));
    }

    // --- 3層比較用: 都道府県が選択されている場合、全国SUMMARYも取得 ---
    if !prefecture.is_empty() {
        let nat_params = vec![Value::String(job_type.to_string())];
        let nat_sql = "SELECT row_type, \
                              avg_desired_areas, avg_qualifications, male_count, female_count, \
                              avg_reference_distance_km, avg_age \
                       FROM job_seeker_data \
                       WHERE job_type = ? \
                         AND row_type = 'SUMMARY' \
                         AND prefecture != ''";
        match state.turso.query(nat_sql, &nat_params).await {
            Ok(nat_rows) => {
                let mut n_male: i64 = 0;
                let mut n_female: i64 = 0;
                let mut n_age_sum: f64 = 0.0;
                let mut n_age_count: f64 = 0.0;
                let mut n_desired_sum: f64 = 0.0;
                let mut n_qual_sum: f64 = 0.0;
                let mut n_dist_values: Vec<f64> = Vec::new();
                let mut n_summary_count: i64 = 0;

                for row in &nat_rows {
                    let male = get_i64(row, "male_count");
                    let female = get_i64(row, "female_count");
                    n_male += male;
                    n_female += female;

                    let avg_age_val = get_f64(row, "avg_age");
                    let row_total = male + female;
                    if avg_age_val > 0.0 && row_total > 0 {
                        n_age_sum += avg_age_val * row_total as f64;
                        n_age_count += row_total as f64;
                    }

                    n_desired_sum += get_f64(row, "avg_desired_areas") * row_total as f64;
                    n_qual_sum += get_f64(row, "avg_qualifications") * row_total as f64;
                    n_summary_count += row_total;

                    let dist = get_f64(row, "avg_reference_distance_km");
                    if dist > 0.0 {
                        n_dist_values.push(dist);
                    }
                }

                stats.national_male_count = n_male;
                stats.national_female_count = n_female;
                stats.national_total = n_male + n_female;
                stats.national_avg_age = if n_age_count > 0.0 {
                    n_age_sum / n_age_count
                } else {
                    0.0
                };
                stats.national_avg_desired_areas = if n_summary_count > 0 {
                    n_desired_sum / n_summary_count as f64
                } else {
                    0.0
                };
                stats.national_avg_qualifications = if n_summary_count > 0 {
                    n_qual_sum / n_summary_count as f64
                } else {
                    0.0
                };
                stats.national_avg_distance_km = if !n_dist_values.is_empty() {
                    n_dist_values.iter().sum::<f64>() / n_dist_values.len() as f64
                } else {
                    0.0
                };
            }
            Err(e) => {
                tracing::warn!("全国比較データ取得失敗（3層比較パネル無効化）: {e}");
            }
        }
    }

    stats
}

/// 3層比較パネルのHTML生成
/// 都道府県が選択されている場合のみ表示する。
/// 全国 vs 地域（都道府県 or 市区町村）を横棒バーで比較。
fn build_comparison_section(stats: &NatStats, prefecture: &str, location_label: &str) -> String {
    // 都道府県未選択（全国モード）では比較不可 → 空
    if prefecture.is_empty() {
        return String::new();
    }

    // 全国データが取得できていない場合も空
    if stats.national_total == 0 {
        return String::new();
    }

    // 比較対象ラベル（市区町村選択時はそれ、都道府県のみならその県名）
    let region_label = location_label;

    // --- ヘルパー: 横棒比較バーを1指標分生成 ---
    // max_val を基準に %幅 を計算する
    fn bar_row(
        label: &str,
        nat_val: f64,
        region_val: f64,
        region_label: &str,
        unit: &str,
    ) -> String {
        // バー幅算出: 2つのうち大きい方を100%とする
        let max_val = nat_val.max(region_val).max(0.001);
        let nat_pct = (nat_val / max_val * 100.0).round();
        let reg_pct = (region_val / max_val * 100.0).round();

        // 差分（地域 - 全国）
        let diff = region_val - nat_val;
        let diff_sign = if diff > 0.0 { "+" } else { "" };
        let diff_color = if diff > 0.0 {
            "text-emerald-400"
        } else if diff < 0.0 {
            "text-rose-400"
        } else {
            "text-slate-400"
        };

        format!(
            r#"<div>
    <div class="text-xs text-slate-500 mb-1">{label}</div>
    <div class="flex items-center gap-2 text-sm">
        <span class="w-16 text-slate-400 shrink-0">全国</span>
        <div class="flex-1 bg-slate-700 rounded h-5 overflow-hidden">
            <div class="bg-blue-500/70 h-full rounded" style="width: {nat_pct}%"></div>
        </div>
        <span class="w-16 text-right text-slate-300">{nat_val:.1}{unit}</span>
    </div>
    <div class="flex items-center gap-2 text-sm mt-1">
        <span class="w-16 text-cyan-400 shrink-0 truncate" title="{region_label}">{region_label_short}</span>
        <div class="flex-1 bg-slate-700 rounded h-5 overflow-hidden">
            <div class="bg-cyan-500 h-full rounded" style="width: {reg_pct}%"></div>
        </div>
        <span class="w-16 text-right text-slate-300">{region_val:.1}{unit}</span>
    </div>
    <div class="text-right text-xs {diff_color} mt-0.5">差: {diff_sign}{diff:.2}{unit}</div>
</div>"#,
            label = label,
            nat_pct = nat_pct,
            nat_val = nat_val,
            unit = unit,
            region_label = region_label,
            region_label_short = if region_label.chars().count() > 5 {
                region_label.chars().take(5).collect::<String>() + "..."
            } else {
                region_label.to_string()
            },
            reg_pct = reg_pct,
            region_val = region_val,
            diff_color = diff_color,
            diff_sign = diff_sign,
            diff = diff.abs(),
        )
    }

    // --- 男女比の積み上げバー ---
    let nat_total = stats.national_male_count + stats.national_female_count;
    let reg_total = stats.male_count + stats.female_count;
    let nat_male_pct = if nat_total > 0 {
        (stats.national_male_count as f64 / nat_total as f64 * 100.0).round()
    } else {
        0.0
    };
    let reg_male_pct = if reg_total > 0 {
        (stats.male_count as f64 / reg_total as f64 * 100.0).round()
    } else {
        0.0
    };

    let gender_html = format!(
        r#"<div>
    <div class="text-xs text-slate-500 mb-1">男女比</div>
    <div class="flex items-center gap-2 text-sm">
        <span class="w-16 text-slate-400 shrink-0">全国</span>
        <div class="flex-1 bg-slate-700 rounded h-5 overflow-hidden flex">
            <div class="bg-sky-500 h-full" style="width: {nat_male_pct}%"></div>
            <div class="bg-pink-500 h-full" style="width: {nat_female_pct}%"></div>
        </div>
        <span class="w-16 text-right text-slate-300 text-xs">&#9794;{nat_male_pct:.0}%</span>
    </div>
    <div class="flex items-center gap-2 text-sm mt-1">
        <span class="w-16 text-cyan-400 shrink-0 truncate" title="{region_label}">{region_label_short}</span>
        <div class="flex-1 bg-slate-700 rounded h-5 overflow-hidden flex">
            <div class="bg-sky-500 h-full" style="width: {reg_male_pct}%"></div>
            <div class="bg-pink-500 h-full" style="width: {reg_female_pct}%"></div>
        </div>
        <span class="w-16 text-right text-slate-300 text-xs">&#9794;{reg_male_pct:.0}%</span>
    </div>
</div>"#,
        nat_male_pct = nat_male_pct,
        nat_female_pct = 100.0 - nat_male_pct,
        region_label = region_label,
        region_label_short = if region_label.chars().count() > 5 {
            region_label.chars().take(5).collect::<String>() + "..."
        } else {
            region_label.to_string()
        },
        reg_male_pct = reg_male_pct,
        reg_female_pct = 100.0 - reg_male_pct,
    );

    // 各指標の横棒バー生成
    let desired_bar = bar_row(
        "平均希望勤務地数",
        stats.national_avg_desired_areas,
        stats.avg_desired_areas,
        region_label,
        "",
    );
    let distance_bar = bar_row(
        "平均移動距離",
        stats.national_avg_distance_km,
        stats.avg_distance_km,
        region_label,
        "km",
    );
    let qual_bar = bar_row(
        "平均保有資格数",
        stats.national_avg_qualifications,
        stats.avg_qualifications,
        region_label,
        "",
    );
    let age_bar = bar_row(
        "平均年齢",
        stats.national_avg_age,
        stats.avg_age,
        region_label,
        "歳",
    );

    // カード全体を組み立て
    format!(
        r#"<div class="stat-card border-l-4 border-cyan-600">
    <h3 class="text-sm text-slate-400 mb-4">&#x1f4ca; 3層比較 <span class="text-cyan-400 text-xs">全国 vs {region_label}</span></h3>
    <div class="space-y-5">
        {desired_bar}
        {distance_bar}
        {qual_bar}
        {age_bar}
        {gender_html}
    </div>
</div>"#,
        region_label = region_label,
        desired_bar = desired_bar,
        distance_bar = distance_bar,
        qual_bar = qual_bar,
        age_bar = age_bar,
        gender_html = gender_html,
    )
}

/// 採用課題診断メッセージを生成
fn build_diagnosis_section(stats: &NatStats, prefecture: &str) -> String {
    // 都道府県未選択（全国モード）の場合は空
    if prefecture.is_empty() {
        return String::new();
    }

    let total_people = stats.male_count + stats.female_count;

    // 診断ロジック
    let (message, diag_type) = if stats.supply_count > 0 {
        let demand_supply_ratio = stats.demand_count as f64 / stats.supply_count as f64;
        if demand_supply_ratio > 2.0 {
            (
                "競争過多: 求職者に対して求人が少ない地域です".to_string(),
                "warning",
            )
        } else if stats.outflow > stats.inflow {
            (
                "人材流出: 他地域への流出が多い地域です".to_string(),
                "info",
            )
        } else {
            (
                "バランス型: 需給が比較的安定した地域です".to_string(),
                "info",
            )
        }
    } else if stats.outflow > stats.inflow && total_people > 0 {
        // GAPデータがない場合でもフローで判定
        (
            "人材流出: 他地域への流出が多い地域です".to_string(),
            "info",
        )
    } else {
        (
            "バランス型: 需給が比較的安定した地域です".to_string(),
            "info",
        )
    };

    // カードの左ボーダー色を診断タイプで分岐
    let border_color = if diag_type == "warning" {
        "border-amber-500"
    } else {
        "border-sky-500"
    };
    let label_color = if diag_type == "warning" {
        "text-amber-400"
    } else {
        "text-sky-400"
    };

    format!(
        r#"<div class="stat-card border-l-4 {border_color}">
    <h3 class="text-sm {label_color} mb-1">&#x1f4cb; 採用課題診断</h3>
    <p class="text-sm text-slate-300">{message}</p>
</div>"#,
        border_color = border_color,
        label_color = label_color,
        message = message,
    )
}

/// HTMLレンダリング
fn render_overview(job_type: &str, stats: &NatStats, location_label: &str, prefecture: &str) -> String {
    let total = stats.male_count + stats.female_count;
    let male_pct = if total > 0 {
        (stats.male_count as f64 / total as f64 * 100.0).round()
    } else {
        0.0
    };
    let female_pct = 100.0 - male_pct;

    // 年齢帯別のデータ（JSON配列）
    let age_labels: Vec<String> = stats.age_distribution.iter().map(|(a, _)| format!("\"{}\"", a)).collect();
    let age_values: Vec<String> = stats.age_distribution.iter().map(|(_, v)| v.to_string()).collect();

    // 性別×年齢
    let age_male_vals: Vec<String> = stats.age_gender.iter().map(|(_, m, _)| m.to_string()).collect();
    let age_female_vals: Vec<String> = stats.age_gender.iter().map(|(_, _, f)| f.to_string()).collect();

    // 採用課題診断セクション
    let diagnosis_section = build_diagnosis_section(stats, prefecture);
    // 3層比較セクション（都道府県選択時のみ生成）
    let comparison_section = build_comparison_section(stats, prefecture, location_label);

    include_str!("../../templates/tabs/overview.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{LOCATION_LABEL}}", location_label)
        .replace("{{DIAGNOSIS_SECTION}}", &diagnosis_section)
        .replace("{{COMPARISON_SECTION}}", &comparison_section)
        .replace("{{TOTAL_COUNT}}", &format_number(total))
        .replace("{{AVG_AGE}}", &format!("{:.1}", stats.avg_age))
        .replace("{{MALE_COUNT}}", &format_number(stats.male_count))
        .replace("{{FEMALE_COUNT}}", &format_number(stats.female_count))
        .replace("{{MALE_PCT}}", &format!("{:.0}", male_pct))
        .replace("{{FEMALE_PCT}}", &format!("{:.0}", female_pct))
        .replace("{{AVG_DESIRED_AREAS}}", &format!("{:.1}", stats.avg_desired_areas))
        .replace("{{AVG_QUALIFICATIONS}}", &format!("{:.1}", stats.avg_qualifications))
        .replace("{{AVG_DISTANCE_KM}}", &format!("{:.1}", stats.avg_distance_km))
        .replace("{{MALE_COUNT_RAW}}", &stats.male_count.to_string())
        .replace("{{FEMALE_COUNT_RAW}}", &stats.female_count.to_string())
        .replace("{{AGE_LABELS}}", &format!("[{}]", age_labels.join(",")))
        .replace("{{AGE_VALUES}}", &format!("[{}]", age_values.join(",")))
        .replace("{{AGE_MALE_VALUES}}", &format!("[{}]", age_male_vals.join(",")))
        .replace("{{AGE_FEMALE_VALUES}}", &format!("[{}]", age_female_vals.join(",")))
}

/// 数値を3桁区切りフォーマット
pub fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// HashMap からString値を取得
pub fn get_str(row: &HashMap<String, Value>, key: &str) -> String {
    row.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// HashMap からi64値を取得
pub fn get_i64(row: &HashMap<String, Value>, key: &str) -> i64 {
    row.get(key)
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

/// HashMap からf64値を取得
pub fn get_f64(row: &HashMap<String, Value>, key: &str) -> f64 {
    row.get(key)
        .and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_i64().map(|i| i as f64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0.0)
}
