use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::auth::{SESSION_JOB_TYPE_KEY, SESSION_PREFECTURE_KEY, SESSION_MUNICIPALITY_KEY};
use super::competitive::escape_html;
use super::external::{self, ext_f64, ext_i64};
use crate::models::job_seeker::{has_turso_data, render_no_turso_data};
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

/// カンマ区切り市区町村文字列をVecにパースするヘルパー
pub fn parse_municipalities(municipality: &str) -> Vec<String> {
    if municipality.is_empty() {
        return vec![];
    }
    municipality.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

/// SQLのWHERE句とパラメータを構築するヘルパー（市区町村マルチセレクト対応）
pub fn build_location_filter(prefecture: &str, municipality: &str, params: &mut Vec<Value>) -> String {
    let mut clause = String::new();
    if !prefecture.is_empty() {
        clause.push_str(" AND prefecture = ?");
        params.push(Value::String(prefecture.to_string()));
    }
    let munis = parse_municipalities(municipality);
    if munis.len() == 1 {
        clause.push_str(" AND municipality = ?");
        params.push(Value::String(munis[0].clone()));
    } else if munis.len() > 1 {
        let placeholders: Vec<&str> = munis.iter().map(|_| "?").collect();
        clause.push_str(&format!(" AND municipality IN ({})", placeholders.join(", ")));
        for m in &munis {
            params.push(Value::String(m.clone()));
        }
    }
    clause
}

/// タブ1: 市場概況 - HTMXパーシャルHTML
pub async fn tab_overview(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    if !has_turso_data(&job_type) {
        return Html(render_no_turso_data(&job_type, "市場概況"));
    }

    let cache_key = format!("overview_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let mut stats = fetch_national_stats(&state, &job_type, &prefecture, &municipality).await;
    let location_label = make_location_label(&prefecture, &municipality);

    // segment_dbから需要側年代データを取得
    stats.demand_age = fetch_demand_age_decade(&state, &job_type, &prefecture, &municipality).await;

    // V2外部統計データからマクロ指標を取得
    let macro_section = build_macro_indicators_section(&state, &prefecture).await;

    // V2外部統計: 人口ピラミッド（概況タブで3面比較）
    let pop_pyramid_rows = external::fetch_population_pyramid(&state, &prefecture, &municipality).await;

    // Turso接続失敗チェック: 全データが0の場合、エラーバナーを追加
    let total = stats.male_count + stats.female_count;
    let turso_error_banner = if total == 0 {
        // Turso接続テスト
        match state.turso.test_connection().await {
            Ok(_) => String::new(), // 接続はOKだがデータなし
            Err(e) => format!(
                r#"<div class="bg-red-900/60 border border-red-700 text-red-200 px-4 py-3 rounded-lg mb-4">
                    <p class="font-bold">⚠️ データベース接続エラー</p>
                    <p class="text-sm mt-1">Tursoへの接続に失敗しました。環境変数 TURSO_DATABASE_URL を確認してください。</p>
                    <p class="text-xs text-red-300 mt-1">エラー: {}</p>
                </div>"#,
                e.replace('<', "&lt;").replace('>', "&gt;")
            ),
        }
    } else {
        String::new()
    };

    let html = format!("{}{}", turso_error_banner, render_overview(&job_type, &stats, &location_label, &prefecture, &macro_section, &pop_pyramid_rows));

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

    // 需給年代ピラミッド用: segment_age_decadeから取得
    demand_age: Vec<(String, i64)>, // (decade, count) 求人対象年代
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

            demand_age: Vec::new(),
        }
    }
}

/// Tursoから統計データを取得（都道府県/市区町村フィルタ対応）
/// 都道府県選択時はメインクエリ+全国比較クエリをpipeline batchで1 HTTPリクエストにまとめる
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

    // 都道府県選択時は全国比較クエリもバッチに含める
    let nat_sql = "SELECT row_type, \
                          avg_desired_areas, avg_qualifications, male_count, female_count, \
                          avg_reference_distance_km, avg_age \
                   FROM job_seeker_data \
                   WHERE job_type = ? \
                     AND row_type = 'SUMMARY' \
                     AND prefecture != '' \
                     AND municipality != ''";
    let nat_params = vec![Value::String(job_type.to_string())];

    let batch_results = if !prefecture.is_empty() {
        // 2クエリを1 HTTPリクエストでバッチ実行
        match state.turso.query_batch(&[
            (&sql, &params),
            (nat_sql, &nat_params),
        ]).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Turso batch query failed: {e}");
                return NatStats::default();
            }
        }
    } else {
        // 全国モード: メインクエリのみ
        match state.turso.query(&sql, &params).await {
            Ok(r) => {
                if r.is_empty() {
                    tracing::warn!("Turso query returned 0 rows for job_type={}, pref={}, muni={}", job_type, prefecture, municipality);
                }
                vec![r]
            }
            Err(e) => {
                tracing::error!("Turso query failed: {e}");
                return NatStats::default();
            }
        }
    };

    let rows = &batch_results[0];
    if rows.is_empty() {
        tracing::warn!("Turso query returned 0 rows for job_type={}, pref={}, muni={}", job_type, prefecture, municipality);
    }

    let mut stats = NatStats::default();
    let mut total_male: i64 = 0;
    let mut total_female: i64 = 0;
    let mut age_sum: f64 = 0.0;
    let mut age_count: f64 = 0.0;
    let mut desired_sum: f64 = 0.0;
    let mut qual_sum: f64 = 0.0;
    let mut dist_weighted_sum: f64 = 0.0;
    let mut dist_population: i64 = 0;
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

    for row in rows {
        let row_type = get_str(row, "row_type");

        match row_type.as_str() {
            "SUMMARY" => {
                // 都道府県レベル（municipality空）は市区町村の集約なので除外して二重カウントを防ぐ
                let muni = get_str(row, "municipality");
                if muni.is_empty() {
                    continue;
                }

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
                if dist > 0.0 && total > 0 {
                    dist_weighted_sum += dist * total as f64;
                    dist_population += total;
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
    stats.avg_distance_km = if dist_population > 0 {
        dist_weighted_sum / dist_population as f64
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

    // --- 3層比較用: バッチ結果の2番目から全国データを処理 ---
    if !prefecture.is_empty() && batch_results.len() > 1 {
        let nat_rows = &batch_results[1];
        let mut n_male: i64 = 0;
        let mut n_female: i64 = 0;
        let mut n_age_sum: f64 = 0.0;
        let mut n_age_count: f64 = 0.0;
        let mut n_desired_sum: f64 = 0.0;
        let mut n_qual_sum: f64 = 0.0;
        let mut n_dist_weighted_sum: f64 = 0.0;
        let mut n_dist_population: i64 = 0;
        let mut n_summary_count: i64 = 0;

        for row in nat_rows {
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
            if dist > 0.0 && row_total > 0 {
                n_dist_weighted_sum += dist * row_total as f64;
                n_dist_population += row_total;
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
        stats.national_avg_distance_km = if n_dist_population > 0 {
            n_dist_weighted_sum / n_dist_population as f64
        } else {
            0.0
        };
    }

    stats
}

/// セッション職種名 → segment_summary.db 職種名へのマッピング
fn map_job_type_for_segment(job_type: &str) -> Option<&str> {
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
        "幼稚園教諭" => Some("幼稚園教諭"),
        _ => None,
    }
}

/// segment_dbから需要側（求人票）の年代分布を取得
async fn fetch_demand_age_decade(
    state: &AppState,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Vec<(String, i64)> {
    let seg_db = match &state.segment_db {
        Some(db) => db,
        None => return Vec::new(),
    };

    let seg_jt = match map_job_type_for_segment(job_type) {
        Some(jt) => jt,
        None => return Vec::new(),
    };

    // 全雇用形態で集計
    let emp_type = "全て";

    let mut params: Vec<String> = vec![seg_jt.to_string(), emp_type.to_string()];
    let mut location_clause = String::new();
    if !prefecture.is_empty() {
        location_clause.push_str(" AND prefecture = ?");
        params.push(prefecture.to_string());
        if !municipality.is_empty() {
            location_clause.push_str(" AND municipality = ?");
            params.push(municipality.to_string());
        }
    }

    let query = format!(
        "SELECT decade, SUM(count) as count \
         FROM segment_age_decade WHERE job_type = ? AND employment_type = ?{} \
         GROUP BY decade ORDER BY decade",
        location_clause
    );

    let rows = match seg_db.query_owned(query, params).await {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let age_order = ["20代", "30代", "40代", "50代", "60代"];
    let mut result: Vec<(String, i64)> = Vec::new();
    for age in &age_order {
        let cnt = rows.iter()
            .find(|r| r.get("decade").and_then(|v| v.as_str()) == Some(age))
            .and_then(|r| r.get("count").and_then(|v| v.as_i64()))
            .unwrap_or(0);
        result.push((age.to_string(), cnt));
    }

    result
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
            <div style="background:#0072B2;width:{nat_male_pct}%" class="h-full"></div>
            <div style="background:#E69F00;width:{nat_female_pct}%" class="h-full"></div>
        </div>
        <span class="w-16 text-right text-slate-300 text-xs">&#9794;{nat_male_pct:.0}%</span>
    </div>
    <div class="flex items-center gap-2 text-sm mt-1">
        <span class="w-16 text-cyan-400 shrink-0 truncate" title="{region_label}">{region_label_short}</span>
        <div class="flex-1 bg-slate-700 rounded h-5 overflow-hidden flex">
            <div style="background:#0072B2;width:{reg_male_pct}%" class="h-full"></div>
            <div style="background:#E69F00;width:{reg_female_pct}%" class="h-full"></div>
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

    // NiceGUI版に合わせた3指標のみ
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

    // カード全体を組み立て
    format!(
        r#"<div class="stat-card border-l-4 border-cyan-600">
    <h3 class="text-sm text-slate-400 mb-4">&#x1f4ca; 全国 vs {region_label} 比較</h3>
    <div class="space-y-5">
        {desired_bar}
        {distance_bar}
        {qual_bar}
        {gender_html}
    </div>
</div>"#,
        region_label = region_label,
        desired_bar = desired_bar,
        distance_bar = distance_bar,
        qual_bar = qual_bar,
        gender_html = gender_html,
    )
}

/// 採用課題診断セクション（無効化: 求人票データのみでは正確な診断ができないため非表示）
fn build_diagnosis_section(_stats: &NatStats, _prefecture: &str) -> String {
    String::new()
}

/// 需給年代ピラミッド（バタフライチャート）を生成
/// 左=求職者年代割合（供給）、右=求人対象年代割合（需要）
fn build_pyramid_section(stats: &NatStats, pop_pyramid_rows: &[HashMap<String, Value>]) -> String {
    // 供給側（求職者）: AGE_GENDERから。60代と70歳以上を「60代以上」に統合
    let age_labels = ["20代", "30代", "40代", "50代", "60代以上"];
    let mut supply_counts: Vec<i64> = Vec::new();
    for label in &age_labels {
        let cnt = match *label {
            "60代以上" => {
                // 60代 + 70歳以上を統合
                let c60 = stats.age_distribution.iter()
                    .find(|(a, _)| a == "60代").map(|(_, c)| *c).unwrap_or(0);
                let c70 = stats.age_distribution.iter()
                    .find(|(a, _)| a == "70歳以上").map(|(_, c)| *c).unwrap_or(0);
                c60 + c70
            }
            _ => stats.age_distribution.iter()
                .find(|(a, _)| a == *label).map(|(_, c)| *c).unwrap_or(0),
        };
        supply_counts.push(cnt);
    }

    // 需要側（求人票）: segment_age_decadeから。60代を「60代以上」に対応
    let mut demand_counts: Vec<i64> = Vec::new();
    for label in &age_labels {
        let cnt = match *label {
            "60代以上" => stats.demand_age.iter()
                .find(|(a, _)| a == "60代").map(|(_, c)| *c).unwrap_or(0),
            _ => stats.demand_age.iter()
                .find(|(a, _)| a == *label).map(|(_, c)| *c).unwrap_or(0),
        };
        demand_counts.push(cnt);
    }

    // 人口データ（V2外部統計）: 9区分→5区分に統合
    let has_pop = !pop_pyramid_rows.is_empty();
    let mut pop_counts: Vec<i64> = Vec::new();
    for label in &age_labels {
        let cnt: i64 = if has_pop {
            match *label {
                "20代" => pop_pyramid_rows.iter()
                    .find(|r| external::ext_str(r, "age_group") == "20-29")
                    .map(|r| ext_i64(r, "male_count") + ext_i64(r, "female_count")).unwrap_or(0),
                "30代" => pop_pyramid_rows.iter()
                    .find(|r| external::ext_str(r, "age_group") == "30-39")
                    .map(|r| ext_i64(r, "male_count") + ext_i64(r, "female_count")).unwrap_or(0),
                "40代" => pop_pyramid_rows.iter()
                    .find(|r| external::ext_str(r, "age_group") == "40-49")
                    .map(|r| ext_i64(r, "male_count") + ext_i64(r, "female_count")).unwrap_or(0),
                "50代" => pop_pyramid_rows.iter()
                    .find(|r| external::ext_str(r, "age_group") == "50-59")
                    .map(|r| ext_i64(r, "male_count") + ext_i64(r, "female_count")).unwrap_or(0),
                "60代以上" => {
                    ["60-69", "70-79", "80+"].iter().map(|ag| {
                        pop_pyramid_rows.iter()
                            .find(|r| external::ext_str(r, "age_group") == *ag)
                            .map(|r| ext_i64(r, "male_count") + ext_i64(r, "female_count")).unwrap_or(0)
                    }).sum()
                }
                _ => 0,
            }
        } else { 0 };
        pop_counts.push(cnt);
    }
    let pop_total: i64 = pop_counts.iter().sum();

    // データがどちらもない場合はセクションを表示しない
    let supply_total: i64 = supply_counts.iter().sum();
    let demand_total: i64 = demand_counts.iter().sum();
    if supply_total == 0 && demand_total == 0 {
        return String::new();
    }

    // 割合に変換（%）
    let supply_pcts: Vec<f64> = supply_counts.iter()
        .map(|c| if supply_total > 0 { *c as f64 / supply_total as f64 * 100.0 } else { 0.0 })
        .collect();
    let demand_pcts: Vec<f64> = demand_counts.iter()
        .map(|c| if demand_total > 0 { *c as f64 / demand_total as f64 * 100.0 } else { 0.0 })
        .collect();
    let pop_pcts: Vec<f64> = pop_counts.iter()
        .map(|c| if pop_total > 0 { *c as f64 / pop_total as f64 * 100.0 } else { 0.0 })
        .collect();

    // ECharts用JSON配列: 供給は負値（左側）、需要は正値（右側）
    let supply_vals: Vec<String> = supply_pcts.iter()
        .map(|p| format!("{:.1}", -p))
        .collect();
    let demand_vals: Vec<String> = demand_pcts.iter()
        .map(|p| format!("{:.1}", p))
        .collect();

    let labels_json: Vec<String> = age_labels.iter().map(|a| format!("\"{}\"", a)).collect();

    // テーブル行を生成
    let mut table_rows = String::new();
    for (i, label) in age_labels.iter().enumerate() {
        let sp = supply_pcts[i];
        let dp = demand_pcts[i];
        let pp = pop_pcts[i];
        let diff = dp - sp;
        let diff_color = if diff > 3.0 {
            "text-rose-400"
        } else if diff < -3.0 {
            "text-emerald-400"
        } else {
            "text-slate-400"
        };
        let diff_sign = if diff > 0.0 { "+" } else { "" };
        let hint = if diff > 3.0 {
            "採用しにくい"
        } else if diff < -3.0 {
            "採用しやすい"
        } else {
            "均衡"
        };

        let pop_cell = if has_pop {
            format!(r#"<td class="py-1.5 text-sm text-right text-slate-400">{:.1}%</td>"#, pp)
        } else {
            String::new()
        };

        table_rows.push_str(&format!(
            r#"<tr class="border-b border-slate-700/50">
                <td class="py-1.5 text-sm text-slate-300">{label}</td>
                {pop_cell}
                <td class="py-1.5 text-sm text-right text-cyan-400">{sp:.1}%</td>
                <td class="py-1.5 text-sm text-right text-amber-400">{dp:.1}%</td>
                <td class="py-1.5 text-sm text-right {diff_color}">{diff_sign}{diff:.1}%</td>
                <td class="py-1.5 text-xs text-right {diff_color}">{hint}</td>
            </tr>"#,
            label = label,
            pop_cell = pop_cell,
            sp = sp,
            dp = dp,
            diff_color = diff_color,
            diff_sign = diff_sign,
            diff = diff,
            hint = hint,
        ));
    }

    // チャートID（ユニーク）
    let chart_id = format!("pyramid-{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis());

    // 人口データがある場合、チャートに3本目のシリーズを追加
    let pop_series = if has_pop {
        let pop_vals: Vec<String> = pop_pcts.iter().map(|p| format!("{:.1}", -p)).collect();
        format!(
            r#",{{
                            name: '地域人口',
                            type: 'bar',
                            data: [{pop_vals}],
                            itemStyle: {{color: 'rgba(100,116,139,0.5)', borderRadius: [4, 0, 0, 4]}},
                            barWidth: '30%',
                            barGap: '-100%',
                            z: 0
                        }}"#,
            pop_vals = pop_vals.join(","),
        )
    } else {
        String::new()
    };

    let legend_data = if has_pop {
        "'地域人口','求職者（供給）','求人票（需要）'"
    } else {
        "'求職者（供給）','求人票（需要）'"
    };

    let pop_header = if has_pop {
        r#"<th class="py-1.5 text-xs text-right text-slate-500">人口</th>"#
    } else { "" };

    let subtitle = if has_pop {
        "左: 求職者（供給）＋地域人口 / 右: 求人の対象年代（需要）"
    } else {
        "左: 求職者の年代構成（供給） / 右: 求人の対象年代（需要）"
    };

    let pop_note = if has_pop {
        r#"<p class="text-xs text-slate-500">人口 = 国勢調査による地域の実際の年代構成（20歳以上）</p>"#
    } else { "" };

    format!(
        r##"<div class="stat-card">
    <h3 class="text-sm text-slate-400 mb-1">&#x1f4ca; 需給年代バランス</h3>
    <p class="text-xs text-slate-500 mb-3">{subtitle}</p>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div>
            <div id="{chart_id}" style="height:360px;"></div>
            <script>
            (function(){{
                var el = document.getElementById('{chart_id}');
                if (!el || typeof echarts === 'undefined') return;
                var c = echarts.init(el, 'dark');
                c.setOption({{
                    tooltip: {{
                        trigger: 'axis',
                        axisPointer: {{type: 'shadow'}},
                        formatter: function(params) {{
                            var tip = params[0].name;
                            params.forEach(function(p) {{
                                tip += '<br>' + p.marker + p.seriesName + ': ' + Math.abs(p.value).toFixed(1) + '%';
                            }});
                            return tip;
                        }}
                    }},
                    legend: {{data: [{legend_data}], top: 0, textStyle: {{color: '#94a3b8'}}}},
                    grid: {{left: '3%', right: '3%', bottom: '3%', top: '40px', containLabel: true}},
                    xAxis: {{
                        type: 'value',
                        axisLabel: {{
                            color: '#94a3b8',
                            formatter: function(v) {{ return Math.abs(v).toFixed(0) + '%'; }}
                        }},
                        splitLine: {{lineStyle: {{color: '#334155'}}}}
                    }},
                    yAxis: {{
                        type: 'category',
                        data: [{labels}],
                        axisTick: {{show: false}},
                        axisLabel: {{color: '#e2e8f0', fontSize: 13}}
                    }},
                    series: [
                        {{
                            name: '求職者（供給）',
                            type: 'bar',
                            stack: 'total',
                            data: [{supply_vals}],
                            itemStyle: {{color: '#22d3ee', borderRadius: [4, 0, 0, 4]}},
                            barWidth: '55%'
                        }},
                        {{
                            name: '求人票（需要）',
                            type: 'bar',
                            stack: 'total',
                            data: [{demand_vals}],
                            itemStyle: {{color: '#fbbf24', borderRadius: [0, 4, 4, 0]}},
                            barWidth: '55%'
                        }}
                        {pop_series}
                    ]
                }});
                new ResizeObserver(function(){{ c.resize(); }}).observe(el);
            }})();
            </script>
        </div>
        <div>
            <table class="w-full">
                <thead>
                    <tr class="border-b border-slate-600">
                        <th class="py-1.5 text-xs text-left text-slate-500">年代</th>
                        {pop_header}
                        <th class="py-1.5 text-xs text-right text-cyan-500">供給</th>
                        <th class="py-1.5 text-xs text-right text-amber-500">需要</th>
                        <th class="py-1.5 text-xs text-right text-slate-500">差分</th>
                        <th class="py-1.5 text-xs text-right text-slate-500">判定</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
            <div class="mt-3 space-y-1">
                {pop_note}
                <p class="text-xs text-slate-500">供給 = 登録求職者の年代構成比</p>
                <p class="text-xs text-slate-500">需要 = 求人票から推定される対象年代の構成比</p>
                <p class="text-xs text-rose-500/70">＋差分 = 需要超過（企業間の競争が激しく採用しにくい）</p>
                <p class="text-xs text-emerald-500/70">−差分 = 供給超過（候補者が多く採用しやすい）</p>
            </div>
            <div class="mt-3 pt-3 border-t border-slate-700/50 space-y-1">
                <p class="text-xs text-slate-500">&#x26a0;&#xfe0f; 供給側はあくまでインターネット上で求職活動を行っている人材です</p>
                <p class="text-xs text-slate-500">&#x26a0;&#xfe0f; 顕在層（積極的に転職活動中）だけでなく潜在層（情報収集段階）も含みます</p>
            </div>
        </div>
    </div>
</div>"##,
        subtitle = subtitle,
        chart_id = chart_id,
        legend_data = legend_data,
        labels = labels_json.join(","),
        supply_vals = supply_vals.join(","),
        demand_vals = demand_vals.join(","),
        pop_series = pop_series,
        pop_header = pop_header,
        table_rows = table_rows,
        pop_note = pop_note,
    )
}

/// V2外部統計からマクロ指標セクションを生成（都道府県選択時のみ）
async fn build_macro_indicators_section(state: &AppState, prefecture: &str) -> String {
    if prefecture.is_empty() {
        return String::new(); // 全国モードでは非表示
    }

    // 有効求人倍率の年度推移を取得
    let ratio_rows = external::fetch_job_openings_ratio(state, prefecture).await;
    // 人口データ
    let pop_data = external::fetch_population(state, prefecture, "").await;
    // 介護需要
    let care_data = external::fetch_care_demand(state, prefecture).await;
    // 離職率
    let turnover_data = external::fetch_turnover(state, prefecture).await;
    // HW賃金（最新）
    let hw_salary = external::fetch_hw_salary_latest(state, prefecture).await;
    // HW掲載日数（最新）
    let hw_fulfillment = external::fetch_hw_fulfillment_latest(state, prefecture).await;

    // データが全くない場合は非表示
    if ratio_rows.is_empty() && pop_data.is_none() && care_data.is_none()
        && hw_salary.is_empty() && hw_fulfillment.is_empty() {
        return String::new();
    }

    // KPIカード群を構築
    let mut kpi_cards = String::new();

    // 1. 有効求人倍率（最新値）
    if let Some(latest) = ratio_rows.last() {
        let ratio = ext_f64(latest, "ratio_total");
        let year = external::ext_str(latest, "fiscal_year");
        let ratio_color = if ratio >= 1.5 { "#ef4444" } else if ratio >= 1.0 { "#f59e0b" } else { "#22c55e" };
        let ratio_label = if ratio >= 1.5 { "人手不足" } else if ratio >= 1.0 { "やや不足" } else { "供給余裕" };
        kpi_cards.push_str(&format!(
            r#"<div class="stat-card">
                <div class="stat-value" style="color:{color}">{ratio:.2}<span class="text-lg">倍</span></div>
                <div class="stat-label">有効求人倍率 ({year})</div>
                <div class="text-xs mt-1" style="color:{color}">{label}</div>
            </div>"#,
            color = ratio_color, ratio = ratio, year = year, label = ratio_label,
        ));
    }

    // 2. 人口・高齢化率
    if let Some(ref pop) = pop_data {
        let total_pop = ext_i64(pop, "total_population");
        let aging_rate = ext_f64(pop, "aging_rate");
        let working_rate = ext_f64(pop, "working_age_rate");
        kpi_cards.push_str(&format!(
            r#"<div class="stat-card">
                <div class="stat-value text-cyan-400">{pop}<span class="text-lg">人</span></div>
                <div class="stat-label">総人口</div>
                <div class="text-xs text-slate-500 mt-1">高齢化率 {aging:.1}% / 生産年齢 {working:.1}%</div>
            </div>"#,
            pop = format_number(total_pop), aging = aging_rate, working = working_rate,
        ));
    }

    // 3. 介護需要（施設数・利用者数）
    if let Some(ref care) = care_data {
        let home_offices = ext_i64(care, "home_care_offices");
        let home_users = ext_i64(care, "home_care_users");
        let helpers = ext_i64(care, "home_helper_count");
        if home_offices > 0 || helpers > 0 {
            kpi_cards.push_str(&format!(
                r#"<div class="stat-card">
                    <div class="stat-value text-purple-400">{offices}<span class="text-lg">所</span></div>
                    <div class="stat-label">訪問介護事業所</div>
                    <div class="text-xs text-slate-500 mt-1">利用者 {users}人 / ヘルパー {helpers}人</div>
                </div>"#,
                offices = format_number(home_offices),
                users = format_number(home_users),
                helpers = format_number(helpers),
            ));
        }
    }

    // 4. 離職率
    if let Some(ref tn) = turnover_data {
        let sep_rate = ext_f64(tn, "separation_rate");
        let entry_rate = ext_f64(tn, "entry_rate");
        let year = external::ext_str(tn, "fiscal_year");
        if sep_rate > 0.0 {
            let net_rate = entry_rate - sep_rate;
            let net_color = if net_rate >= 0.0 { "#22c55e" } else { "#ef4444" };
            let net_sign = if net_rate >= 0.0 { "+" } else { "" };
            kpi_cards.push_str(&format!(
                r#"<div class="stat-card">
                    <div class="stat-value text-rose-400">{sep:.1}<span class="text-lg">%</span></div>
                    <div class="stat-label">医療福祉 離職率 ({year})</div>
                    <div class="text-xs mt-1" style="color:{net_color}">入職率 {entry:.1}% (純増減 {net_sign}{net:.1}%)</div>
                </div>"#,
                sep = sep_rate, year = year, entry = entry_rate,
                net_color = net_color, net_sign = net_sign, net = net_rate,
            ));
        }
    }

    // 5. HW平均賃金（O-6）
    for row in &hw_salary {
        let emp = external::ext_str(row, "emp_group");
        let avg_min = ext_i64(row, "avg_min");
        let avg_max = ext_i64(row, "avg_max");
        if avg_min > 0 {
            let color = if emp.contains("正") { "#10b981" } else { "#8b5cf6" };
            let salary_label = if avg_max > avg_min {
                format!("{} 〜 {}", format_number(avg_min), format_number(avg_max))
            } else {
                format!("{}", format_number(avg_min))
            };
            kpi_cards.push_str(&format!(
                r#"<div class="stat-card">
                    <div class="stat-value" style="color:{color}"><span class="text-lg">¥</span>{salary}</div>
                    <div class="stat-label">HW {emp} 平均月給</div>
                    <div class="text-xs text-slate-500 mt-1">ハローワーク掲載求人の平均値</div>
                </div>"#,
                color = color, salary = salary_label, emp = emp,
            ));
        }
    }

    // 6. HW掲載日数（O-7）
    for row in &hw_fulfillment {
        let emp = external::ext_str(row, "emp_group");
        let avg_days = ext_f64(row, "avg_days");
        let long_term = ext_i64(row, "long_term");
        let count = ext_i64(row, "count");
        if avg_days > 0.0 {
            let days_color = if avg_days > 90.0 { "#ef4444" } else if avg_days > 60.0 { "#f59e0b" } else { "#22c55e" };
            let difficulty = if avg_days > 90.0 { "充足困難" } else if avg_days > 60.0 { "やや困難" } else { "比較的容易" };
            let long_pct = if count > 0 { long_term as f64 / count as f64 * 100.0 } else { 0.0 };
            kpi_cards.push_str(&format!(
                r#"<div class="stat-card">
                    <div class="stat-value" style="color:{color}">{days:.0}<span class="text-lg">日</span></div>
                    <div class="stat-label">HW {emp} 平均掲載日数</div>
                    <div class="text-xs mt-1" style="color:{color}">{diff} (90日超: {long:.1}%)</div>
                </div>"#,
                color = days_color, days = avg_days, emp = emp,
                diff = difficulty, long = long_pct,
            ));
        }
    }

    if kpi_cards.is_empty() {
        return String::new();
    }

    // 有効求人倍率の推移チャート
    let mut ratio_chart = String::new();
    if ratio_rows.len() >= 2 {
        let chart_id = format!("macro-ratio-{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis());
        let years: Vec<String> = ratio_rows.iter()
            .map(|r| format!("'{}'", external::ext_str(r, "fiscal_year")))
            .collect();
        let ratios_pt: Vec<String> = ratio_rows.iter()
            .map(|r| format!("{:.2}", ext_f64(r, "ratio_total")))
            .collect();
        let ratios_nopt: Vec<String> = ratio_rows.iter()
            .map(|r| format!("{:.2}", ext_f64(r, "ratio_excl_part")))
            .collect();

        ratio_chart = format!(
            r##"<div class="stat-card">
            <h3 class="text-sm text-slate-400 mb-3">有効求人倍率の推移（{pref}）</h3>
            <div id="{id}" style="height:250px;"></div>
            <script>
            (function(){{
                var el = document.getElementById('{id}');
                if (!el || typeof echarts === 'undefined') return;
                var c = echarts.init(el, 'dark');
                c.setOption({{
                    tooltip: {{trigger:'axis'}},
                    legend: {{data:['パート含む','パート除く'], top:0, textStyle:{{color:'#94a3b8'}}}},
                    grid: {{left:'8%',right:'5%',top:'35px',bottom:'12%'}},
                    xAxis: {{type:'category', data:[{years}], axisLabel:{{color:'#94a3b8',fontSize:11}}}},
                    yAxis: {{type:'value', name:'倍', axisLabel:{{color:'#94a3b8'}}, splitLine:{{lineStyle:{{color:'#334155'}}}}}},
                    series: [
                        {{name:'パート含む',type:'line',data:[{pt}],itemStyle:{{color:'#3b82f6'}},smooth:true}},
                        {{name:'パート除く',type:'line',data:[{nopt}],itemStyle:{{color:'#f59e0b'}},smooth:true,lineStyle:{{type:'dashed'}}}}
                    ]
                }});
                new ResizeObserver(function(){{c.resize();}}).observe(el);
            }})();
            </script>
            <p class="text-xs text-slate-500 mt-2">※出典: e-Stat 社会・人口統計体系（厚生労働省）</p>
        </div>"##,
            pref = escape_html(prefecture),
            id = chart_id,
            years = years.join(","),
            pt = ratios_pt.join(","),
            nopt = ratios_nopt.join(","),
        );
    }

    format!(
        r#"<div class="space-y-4">
    <div class="flex items-center gap-2">
        <span class="text-sm font-semibold text-slate-400">&#x1f30d; 外部統計マクロ指標</span>
        <span class="text-xs text-blue-400 bg-blue-400/10 px-2 py-0.5 rounded">【{pref}】</span>
    </div>
    <div class="grid-stats">{kpi_cards}</div>
    {ratio_chart}
</div>"#,
        pref = escape_html(prefecture),
        kpi_cards = kpi_cards,
        ratio_chart = ratio_chart,
    )
}

/// HTMLレンダリング
fn render_overview(job_type: &str, stats: &NatStats, location_label: &str, prefecture: &str, macro_section: &str, pop_pyramid_rows: &[HashMap<String, Value>]) -> String {
    let total = stats.male_count + stats.female_count;
    let male_pct = if total > 0 {
        (stats.male_count as f64 / total as f64 * 100.0).round()
    } else {
        0.0
    };
    let female_pct = 100.0 - male_pct;

    // 年齢帯別のデータ（JSON配列）
    let age_labels: Vec<String> = stats.age_distribution.iter().map(|(a, _)| format!("\"{}\"", a)).collect();
    // 性別×年齢
    let age_male_vals: Vec<String> = stats.age_gender.iter().map(|(_, m, _)| m.to_string()).collect();
    let age_female_vals: Vec<String> = stats.age_gender.iter().map(|(_, _, f)| f.to_string()).collect();

    // 採用課題診断セクション
    let diagnosis_section = build_diagnosis_section(stats, prefecture);
    // 3層比較セクション（都道府県選択時のみ生成）
    let comparison_section = build_comparison_section(stats, prefecture, location_label);

    // 需給年代ピラミッド用データ（割合で計算）
    let pyramid_section = build_pyramid_section(stats, pop_pyramid_rows);

    include_str!("../../templates/tabs/overview.html")
        .replace("{{JOB_TYPE}}", &escape_html(job_type))
        .replace("{{LOCATION_LABEL}}", &escape_html(location_label))
        .replace("{{DIAGNOSIS_SECTION}}", &diagnosis_section)
        .replace("{{COMPARISON_SECTION}}", &comparison_section)
        .replace("{{PYRAMID_SECTION}}", &pyramid_section)
        .replace("{{MACRO_SECTION}}", macro_section)
        .replace("{{TOTAL_COUNT}}", &format_number(total))
        .replace("{{AVG_AGE}}", &format!("{:.1}", stats.avg_age))
        .replace("{{MALE_COUNT}}", &format_number(stats.male_count))
        .replace("{{FEMALE_COUNT}}", &format_number(stats.female_count))
        .replace("{{MALE_PCT}}", &format!("{:.0}", male_pct))
        .replace("{{FEMALE_PCT}}", &format!("{:.0}", female_pct))
        .replace("{{MALE_COUNT_RAW}}", &stats.male_count.to_string())
        .replace("{{FEMALE_COUNT_RAW}}", &stats.female_count.to_string())
        .replace("{{AGE_LABELS}}", &format!("[{}]", age_labels.join(",")))
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

#[cfg(test)]
mod tests {
    use super::*;

    // format_number テスト
    #[test]
    fn test_format_number_basic() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(1), "1");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn test_format_number_negative() {
        assert_eq!(format_number(-1234), "-1,234");
    }

    // build_location_filter テスト
    #[test]
    fn test_build_location_filter_empty() {
        let mut params = Vec::new();
        let clause = build_location_filter("", "", &mut params);
        assert!(clause.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_location_filter_prefecture_only() {
        let mut params = Vec::new();
        let clause = build_location_filter("東京都", "", &mut params);
        assert!(clause.contains("prefecture = ?"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_build_location_filter_both() {
        let mut params = Vec::new();
        let clause = build_location_filter("東京都", "新宿区", &mut params);
        assert!(clause.contains("prefecture = ?"));
        assert!(clause.contains("municipality = ?"));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_build_location_filter_multi_municipality() {
        let mut params = Vec::new();
        let clause = build_location_filter("東京都", "新宿区,渋谷区,港区", &mut params);
        assert!(clause.contains("prefecture = ?"));
        assert!(clause.contains("municipality IN (?, ?, ?)"));
        assert_eq!(params.len(), 4); // prefecture + 3 municipalities
    }

    #[test]
    fn test_parse_municipalities() {
        assert!(parse_municipalities("").is_empty());
        assert_eq!(parse_municipalities("新宿区"), vec!["新宿区"]);
        assert_eq!(parse_municipalities("新宿区,渋谷区"), vec!["新宿区", "渋谷区"]);
        assert_eq!(parse_municipalities("新宿区, 渋谷区 , 港区"), vec!["新宿区", "渋谷区", "港区"]);
    }

    // get_str テスト
    #[test]
    fn test_get_str_exists() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        assert_eq!(get_str(&map, "name"), "Alice");
    }

    #[test]
    fn test_get_str_missing() {
        let map = HashMap::new();
        assert_eq!(get_str(&map, "name"), "");
    }

    // get_i64 テスト
    #[test]
    fn test_get_i64_integer() {
        let mut map = HashMap::new();
        map.insert("count".to_string(), serde_json::json!(42));
        assert_eq!(get_i64(&map, "count"), 42);
    }

    #[test]
    fn test_get_i64_float_conversion() {
        let mut map = HashMap::new();
        map.insert("count".to_string(), serde_json::json!(42.9));
        assert_eq!(get_i64(&map, "count"), 42);
    }

    #[test]
    fn test_get_i64_string_parse() {
        let mut map = HashMap::new();
        map.insert("count".to_string(), Value::String("100".to_string()));
        assert_eq!(get_i64(&map, "count"), 100);
    }

    #[test]
    fn test_get_i64_missing() {
        let map = HashMap::new();
        assert_eq!(get_i64(&map, "count"), 0);
    }

    // get_f64 テスト
    #[test]
    fn test_get_f64_float() {
        let mut map = HashMap::new();
        map.insert("score".to_string(), serde_json::json!(3.14));
        assert!((get_f64(&map, "score") - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_get_f64_missing() {
        let map = HashMap::new();
        assert_eq!(get_f64(&map, "score"), 0.0);
    }
}
