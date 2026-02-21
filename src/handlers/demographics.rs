use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::models::job_seeker::{has_turso_data, render_no_turso_data};
use crate::AppState;

use super::overview::{get_str, get_i64, get_f64, format_number, get_session_filters, build_location_filter, make_location_label};

/// タブ2: ペルソナ分析 - HTMXパーシャルHTML
pub async fn tab_demographics(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    if !has_turso_data(&job_type) {
        return Html(render_no_turso_data(&job_type, "ペルソナ分析"));
    }

    let cache_key = format!("demographics_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_demographics(&state, &job_type, &prefecture, &municipality).await;
    let html = render_demographics(&job_type, &prefecture, &municipality, &stats);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

// ===== データ構造体 =====

struct PersonaItem {
    label: String,
    count: i64,
    share_pct: String,
}

struct QualificationItem {
    qualification: String,
    retention_rate: String,
    interpretation: String,
}

struct QualGenderItem {
    qualification: String,
    male: i64,
    female: i64,
}

struct EmploymentBreakdown {
    age_gender: String,
    employed: i64,
    unemployed: i64,
    student: i64,
}

struct AgeGenderStat {
    label: String,
    desired_areas: String,
    qualifications: String,
}

struct DemoStats {
    // ペルソナ構成比（全件、人数降順）
    persona_all: Vec<PersonaItem>,
    // 資格一覧（定着率付き）
    qualification_list: Vec<QualificationItem>,
    // 年齢×性別 (age_group, male, female)
    age_gender: Vec<(String, i64, i64)>,
    // 男女合計
    male_total: i64,
    female_total: i64,
    // KPI: 平均資格数, 平均移動距離
    avg_qualifications: f64,
    avg_distance_km: f64,
    // 就業状態別ペルソナTop10
    employment_breakdown: Vec<EmploymentBreakdown>,
    // 資格別男女Top10
    qual_gender: Vec<QualGenderItem>,
    // 年齢×性別統計（希望勤務地数/資格数）
    age_gender_stats: Vec<AgeGenderStat>,
    // 資格オプション（RARITY用）
    qual_options: Vec<(String, i64)>,
    // 緊急度×性別 (gender, count, avg_score)
    urgency_gender: Vec<(String, i64, f64)>,
    // 転職希望時期別 (category, count, avg_score)
    urgency_start: Vec<(String, i64, f64)>,

    // 言語化カード用
    target_segment: String,
    target_count: i64,
    target_pct: f64,
    timing_now_count: i64,
    timing_now_pct: f64,
    top_qualification: String,
    top_qual_count: i64,
    hidden_segments: Vec<(String, i64, f64)>,
}

impl Default for DemoStats {
    fn default() -> Self {
        Self {
            persona_all: Vec::new(),
            qualification_list: Vec::new(),
            age_gender: Vec::new(),
            male_total: 0,
            female_total: 0,
            avg_qualifications: 0.0,
            avg_distance_km: 0.0,
            employment_breakdown: Vec::new(),
            qual_gender: Vec::new(),
            age_gender_stats: Vec::new(),
            qual_options: Vec::new(),
            urgency_gender: Vec::new(),
            urgency_start: Vec::new(),
            target_segment: String::new(),
            target_count: 0,
            target_pct: 0.0,
            timing_now_count: 0,
            timing_now_pct: 0.0,
            top_qualification: String::new(),
            top_qual_count: 0,
            hidden_segments: Vec::new(),
        }
    }
}

// ===== データ取得 =====

async fn fetch_demographics(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> DemoStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    // メインクエリ: AGE_GENDER, QUALIFICATION_DETAIL, URGENCY_GENDER, URGENCY_START_CATEGORY, SUMMARY
    let sql = format!(
        "SELECT row_type, prefecture, municipality, \
               male_count, female_count, \
               category1, category2, category3, count, percentage, \
               avg_urgency_score, retention_rate, \
               avg_desired_areas, avg_qualifications, avg_reference_distance_km, applicant_count \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type IN ('SUMMARY', 'AGE_GENDER', 'QUALIFICATION_DETAIL', \
                           'URGENCY_GENDER', 'URGENCY_START_CATEGORY', \
                           'AGE_GENDER_RESIDENCE', 'PERSONA_MUNI', 'QUALIFICATION_PERSONA') \
          AND prefecture != ''{location_filter}"
    );

    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Demographics query failed: {e}");
            return DemoStats::default();
        }
    };

    let mut stats = DemoStats::default();

    let age_order = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
    let mut age_male: HashMap<String, i64> = HashMap::new();
    let mut age_female: HashMap<String, i64> = HashMap::new();

    // 資格map（count用 + retention用）
    let mut qual_count_map: HashMap<String, i64> = HashMap::new();
    let mut qual_retention_map: HashMap<String, (f64, i64)> = HashMap::new(); // (sum_rate, n)

    // 緊急度
    let mut urg_gender_map: HashMap<String, (i64, f64)> = HashMap::new(); // (total_cnt, weighted_score)
    let mut urg_start_map: HashMap<String, (i64, f64)> = HashMap::new();

    // ペルソナ（AGE_GENDER由来）
    let mut persona_map: HashMap<String, i64> = HashMap::new();

    // SUMMARY集計（男女合計）
    let mut summary_male: i64 = 0;
    let mut summary_female: i64 = 0;
    let mut summary_qual_sum: f64 = 0.0;
    let mut summary_dist_sum: f64 = 0.0;
    let mut summary_count: i64 = 0;

    // AGE_GENDER_RESIDENCE用
    let mut agr_persona_map: HashMap<String, i64> = HashMap::new();
    let mut agr_desired_map: HashMap<String, (f64, i64)> = HashMap::new(); // (sum, n)
    let mut agr_qual_map: HashMap<String, (f64, i64)> = HashMap::new();

    // PERSONA_MUNI用（就業状態別）
    let mut persona_muni_map: HashMap<String, HashMap<String, i64>> = HashMap::new();

    // QUALIFICATION_PERSONA用（資格×性別）
    let mut qual_gender_map: HashMap<String, (i64, i64)> = HashMap::new(); // (male, female)

    for row in &rows {
        let row_type = get_str(row, "row_type");
        match row_type.as_str() {
            "SUMMARY" => {
                let m = get_i64(row, "male_count");
                let f = get_i64(row, "female_count");
                summary_male += m;
                summary_female += f;
                let q = get_f64(row, "avg_qualifications");
                let d = get_f64(row, "avg_reference_distance_km");
                if q > 0.0 || d > 0.0 {
                    summary_qual_sum += q;
                    summary_dist_sum += d;
                    summary_count += 1;
                }
            }
            "AGE_GENDER" => {
                let age_group = get_str(row, "category1");
                let gender = get_str(row, "category2");
                let cnt = get_i64(row, "count");
                if gender.contains('男') {
                    *age_male.entry(age_group.clone()).or_insert(0) += cnt;
                } else if gender.contains('女') {
                    *age_female.entry(age_group.clone()).or_insert(0) += cnt;
                }
                if !age_group.is_empty() && !gender.is_empty() {
                    let label = format!("{}×{}", age_group, gender);
                    *persona_map.entry(label).or_insert(0) += cnt;
                }
            }
            "QUALIFICATION_DETAIL" => {
                let qual = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                let retention = get_f64(row, "retention_rate");
                if !qual.is_empty() {
                    *qual_count_map.entry(qual.clone()).or_insert(0) += cnt;
                    if retention > 0.0 {
                        let entry = qual_retention_map.entry(qual).or_insert((0.0, 0));
                        entry.0 += retention;
                        entry.1 += 1;
                    }
                }
            }
            "URGENCY_GENDER" => {
                let gender = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                let score = get_f64(row, "avg_urgency_score");
                if !gender.is_empty() {
                    let entry = urg_gender_map.entry(gender).or_insert((0, 0.0));
                    entry.0 += cnt;
                    entry.1 += score * cnt as f64;
                }
            }
            "URGENCY_START_CATEGORY" => {
                let start_cat = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                let score = get_f64(row, "avg_urgency_score");
                if !start_cat.is_empty() {
                    let entry = urg_start_map.entry(start_cat).or_insert((0, 0.0));
                    entry.0 += cnt;
                    entry.1 += score * cnt as f64;
                }
            }
            "AGE_GENDER_RESIDENCE" => {
                let age = get_str(row, "category1");
                let gender = get_str(row, "category2");
                let cnt = get_i64(row, "count");
                let desired = get_f64(row, "avg_desired_areas");
                let quals = get_f64(row, "avg_qualifications");
                if !age.is_empty() && !gender.is_empty() {
                    let key = format!("{}×{}", age, gender);
                    *agr_persona_map.entry(key.clone()).or_insert(0) += cnt;
                    if desired > 0.0 {
                        let e = agr_desired_map.entry(key.clone()).or_insert((0.0, 0));
                        e.0 += desired * cnt as f64;
                        e.1 += cnt;
                    }
                    if quals > 0.0 {
                        let e = agr_qual_map.entry(key).or_insert((0.0, 0));
                        e.0 += quals * cnt as f64;
                        e.1 += cnt;
                    }
                }
            }
            "PERSONA_MUNI" => {
                let persona_name = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                // "50代・女性・就業中" のようなフォーマット
                let parts: Vec<&str> = persona_name.split('・').collect();
                if parts.len() >= 3 {
                    let age_gender = format!("{}・{}", parts[0], parts[1]);
                    let emp_status = parts[2].to_string();
                    let inner = persona_muni_map.entry(age_gender).or_insert_with(HashMap::new);
                    *inner.entry(emp_status).or_insert(0) += cnt;
                }
            }
            "QUALIFICATION_PERSONA" => {
                let qual = get_str(row, "category1");
                let gender = get_str(row, "category3");
                let cnt = get_i64(row, "count");
                if !qual.is_empty() {
                    let entry = qual_gender_map.entry(qual).or_insert((0, 0));
                    if gender.contains("男") {
                        entry.0 += cnt;
                    } else if gender.contains("女") {
                        entry.1 += cnt;
                    }
                }
            }
            _ => {}
        }
    }

    // 年齢×性別
    for age in &age_order {
        let m = age_male.get(*age).copied().unwrap_or(0);
        let f = age_female.get(*age).copied().unwrap_or(0);
        stats.age_gender.push((age.to_string(), m, f));
    }

    // 男女合計
    stats.male_total = summary_male;
    stats.female_total = summary_female;

    // KPI
    if summary_count > 0 {
        stats.avg_qualifications = summary_qual_sum / summary_count as f64;
        stats.avg_distance_km = summary_dist_sum / summary_count as f64;
    }

    // ペルソナ一覧（AGE_GENDER_RESIDENCE優先、なければAGE_GENDER）
    let persona_source = if !agr_persona_map.is_empty() { &agr_persona_map } else { &persona_map };
    let persona_total: i64 = persona_source.values().sum();
    let mut persona_list: Vec<(String, i64)> = persona_source.iter().map(|(k, v)| (k.clone(), *v)).collect();
    persona_list.sort_by(|a, b| b.1.cmp(&a.1));

    stats.persona_all = persona_list.iter().map(|(label, count)| {
        let pct = if persona_total > 0 { (*count as f64 / persona_total as f64) * 100.0 } else { 0.0 };
        PersonaItem {
            label: label.clone(),
            count: *count,
            share_pct: format!("{:.1}%", pct),
        }
    }).collect();

    // 言語化カード: ターゲット
    if let Some(first) = stats.persona_all.first() {
        stats.target_segment = first.label.clone();
        stats.target_count = first.count;
        stats.target_pct = if persona_total > 0 { (first.count as f64 / persona_total as f64) * 100.0 } else { 0.0 };
    }

    // 言語化カード: 隠れた人材
    {
        let mut hidden: Vec<(String, i64, f64)> = persona_list.iter().filter_map(|(label, count)| {
            let pct = if persona_total > 0 { (*count as f64 / persona_total as f64) * 100.0 } else { 0.0 };
            if pct < 5.0 && pct > 0.5 && *count >= 100 { Some((label.clone(), *count, pct)) } else { None }
        }).collect();
        hidden.sort_by(|a, b| b.1.cmp(&a.1));
        stats.hidden_segments = hidden.into_iter().take(2).collect();
    }

    // 言語化カード: 資格戦略
    if let Some((qual, count)) = qual_count_map.iter().max_by_key(|(_, v)| **v) {
        stats.top_qualification = qual.clone();
        stats.top_qual_count = *count;
    }

    // 資格一覧（定着率付き）
    let mut qual_list: Vec<(String, i64)> = qual_count_map.iter().map(|(k, v)| (k.clone(), *v)).collect();
    qual_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.qualification_list = qual_list.iter().map(|(qual, _count)| {
        let (rate_str, interp) = if let Some((sum, n)) = qual_retention_map.get(qual.as_str()) {
            if *n > 0 {
                let avg = sum / *n as f64;
                let interpretation = if avg >= 1.0 { "地元志向" } else { "流出傾向" };
                (format!("{:.2}", avg), interpretation.to_string())
            } else {
                ("-".to_string(), "-".to_string())
            }
        } else {
            ("-".to_string(), "-".to_string())
        };
        QualificationItem {
            qualification: qual.clone(),
            retention_rate: rate_str,
            interpretation: interp,
        }
    }).collect();

    // 資格オプション（RARITY用）
    stats.qual_options = qual_list.into_iter().take(50).collect();

    // 言語化カード: タイミング（部分一致で「今すぐ」を含むカテゴリを集計）
    {
        let now_count: i64 = urg_start_map.iter()
            .filter(|(k, _)| k.contains("今すぐ"))
            .map(|(_, (c, _))| c)
            .sum();
        if now_count > 0 {
            stats.timing_now_count = now_count;
            let start_total: i64 = urg_start_map.values().map(|(c, _)| c).sum();
            stats.timing_now_pct = if start_total > 0 { (now_count as f64 / start_total as f64) * 100.0 } else { 0.0 };
        }
    }

    // 緊急度×性別
    let mut urg_gender_list: Vec<(String, i64, f64)> = urg_gender_map.into_iter().map(|(g, (cnt, ws))| {
        let avg = if cnt > 0 { ws / cnt as f64 } else { 0.0 };
        (g, cnt, avg)
    }).collect();
    urg_gender_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.urgency_gender = urg_gender_list;

    // 転職希望時期別
    let start_order = ["今すぐ", "1～3ヶ月", "3～6ヶ月", "6ヶ月以上"];
    let mut urg_start_list: Vec<(String, i64, f64)> = urg_start_map.into_iter().map(|(cat, (cnt, ws))| {
        let avg = if cnt > 0 { ws / cnt as f64 } else { 0.0 };
        (cat, cnt, avg)
    }).collect();
    urg_start_list.sort_by(|a, b| {
        let pos_a = start_order.iter().position(|&o| a.0.contains(o)).unwrap_or(99);
        let pos_b = start_order.iter().position(|&o| b.0.contains(o)).unwrap_or(99);
        pos_a.cmp(&pos_b)
    });
    stats.urgency_start = urg_start_list;

    // 就業状態別ペルソナTop10
    let mut emp_list: Vec<EmploymentBreakdown> = persona_muni_map.into_iter().map(|(ag, map)| {
        EmploymentBreakdown {
            age_gender: ag,
            employed: *map.get("就業中").unwrap_or(&0),
            unemployed: *map.get("離職中").unwrap_or(&0),
            student: *map.get("在学中").unwrap_or(&0),
        }
    }).collect();
    emp_list.sort_by(|a, b| {
        let total_b = b.employed + b.unemployed + b.student;
        let total_a = a.employed + a.unemployed + a.student;
        total_b.cmp(&total_a)
    });
    stats.employment_breakdown = emp_list.into_iter().take(10).collect();

    // 資格別男女Top10
    let mut qg_list: Vec<QualGenderItem> = qual_gender_map.into_iter().map(|(q, (m, f))| {
        QualGenderItem { qualification: q, male: m, female: f }
    }).collect();
    qg_list.sort_by(|a, b| (b.male + b.female).cmp(&(a.male + a.female)));
    stats.qual_gender = qg_list.into_iter().take(10).collect();

    // 年齢×性別統計
    let age_list = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
    let gender_list = ["男性", "女性"];
    for age in &age_list {
        for gender in &gender_list {
            let key = format!("{}×{}", age, gender);
            let desired = if let Some((sum, n)) = agr_desired_map.get(&key) {
                if *n > 0 { format!("{:.1}", sum / *n as f64) } else { "-".to_string() }
            } else { "-".to_string() };
            let quals = if let Some((sum, n)) = agr_qual_map.get(&key) {
                if *n > 0 { format!("{:.1}", sum / *n as f64) } else { "-".to_string() }
            } else { "-".to_string() };
            if desired != "-" || quals != "-" {
                stats.age_gender_stats.push(AgeGenderStat {
                    label: format!("{}{}", age, gender),
                    desired_areas: desired,
                    qualifications: quals,
                });
            }
        }
    }

    stats
}

// ===== レンダリング =====

fn render_demographics(job_type: &str, prefecture: &str, municipality: &str, stats: &DemoStats) -> String {
    let location_label = make_location_label(prefecture, municipality);
    let has_pref = !prefecture.is_empty() && prefecture != "全国";

    // 言語化カード
    let verbalization_cards = if has_pref {
        build_verbalization_cards(stats)
    } else {
        String::new()
    };

    // セクション1: ペルソナリスト + 横棒グラフ
    let persona_list = build_persona_list(&stats.persona_all);
    let persona_bar_chart = build_persona_bar_chart(&stats.persona_all);

    // セクション2: 資格一覧
    let qualification_list = build_qualification_list(&stats.qualification_list);

    // セクション3: 男女比ドーナツ + 年齢×性別 stacked bar
    let gender_pie = build_gender_pie(stats.male_total, stats.female_total);
    let age_gender_stacked = build_age_gender_stacked(&stats.age_gender);

    // セクション4: KPIカード
    let kpi_cards = build_kpi_cards(stats);

    // セクション5: 就業状態別
    let employment_chart = build_employment_chart(&stats.employment_breakdown);

    // セクション6: 資格別男女
    let qual_gender_chart = build_qual_gender_chart(&stats.qual_gender);

    // セクション7: ペルソナシェア横棒 + バッジ
    let (persona_share_chart, persona_share_badges) = build_persona_share(&stats.persona_all);

    // セクション8: 年齢×性別統計リスト
    let age_gender_stats_list = build_age_gender_stats_list(&stats.age_gender_stats);

    // セクション9: RARITY（チェックボックスHTML）
    let rarity_age_checkboxes = build_rarity_age_checkboxes();
    let rarity_qual_checkboxes = build_rarity_qual_checkboxes(&stats.qual_options);
    let rarity_qual_count = if !stats.qual_options.is_empty() {
        format!(" - 全{}種類・取得者数順", stats.qual_options.len())
    } else { String::new() };

    // セクション10-11: 緊急度（DBにデータがない場合はセクション全体を非表示）
    let urg_gender_section = if stats.urgency_gender.is_empty() {
        String::new()
    } else {
        let chart = build_urgency_chart(&stats.urgency_gender, "gender");
        format!(
            r#"<div class="stat-card">
                <div class="text-sm font-semibold text-white mb-3">🚨 緊急度×性別クロス分析</div>
                <div class="text-xs text-slate-500 mb-3">性別ごとの転職緊急度を分析（棒グラフ: 人数、折れ線: 平均スコア）</div>
                {chart}
            </div>"#
        )
    };
    let urg_start_section = if stats.urgency_start.is_empty() {
        String::new()
    } else {
        let chart = build_urgency_chart(&stats.urgency_start, "start");
        format!(
            r#"<div class="stat-card">
                <div class="text-sm font-semibold text-white mb-3">📅 転職希望時期別緊急度</div>
                <div class="text-xs text-slate-500 mb-3">転職希望時期ごとの緊急度を分析（棒グラフ: 人数、折れ線: 平均スコア）</div>
                {chart}
            </div>"#
        )
    };

    include_str!("../../templates/tabs/demographics.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{LOCATION_LABEL}}", &location_label)
        .replace("{{VERBALIZATION_CARDS}}", &verbalization_cards)
        .replace("{{PERSONA_LIST}}", &persona_list)
        .replace("{{PERSONA_BAR_CHART}}", &persona_bar_chart)
        .replace("{{QUALIFICATION_LIST}}", &qualification_list)
        .replace("{{GENDER_PIE}}", &gender_pie)
        .replace("{{AGE_GENDER_STACKED_BAR}}", &age_gender_stacked)
        .replace("{{KPI_CARDS}}", &kpi_cards)
        .replace("{{EMPLOYMENT_BREAKDOWN_CHART}}", &employment_chart)
        .replace("{{QUAL_GENDER_CHART}}", &qual_gender_chart)
        .replace("{{PERSONA_SHARE_CHART}}", &persona_share_chart)
        .replace("{{PERSONA_SHARE_BADGES}}", &persona_share_badges)
        .replace("{{AGE_GENDER_STATS_LIST}}", &age_gender_stats_list)
        .replace("{{RARITY_AGE_CHECKBOXES}}", &rarity_age_checkboxes)
        .replace("{{RARITY_QUAL_CHECKBOXES}}", &rarity_qual_checkboxes)
        .replace("{{RARITY_QUAL_COUNT}}", &rarity_qual_count)
        .replace("{{URG_GENDER_SECTION}}", &urg_gender_section)
        .replace("{{URG_START_SECTION}}", &urg_start_section)
}

// ===== 言語化カード =====

fn build_verbalization_cards(stats: &DemoStats) -> String {
    let mut cards = String::new();

    // ターゲットプロファイル
    if !stats.target_segment.is_empty() {
        cards.push_str(&format!(
            r##"<div class="stat-card" style="flex: 1; min-width: 280px; border: 2px solid #3B82F6; border-radius: 12px;">
    <div class="text-sm font-bold mb-2" style="color: #3B82F6;">👤 採用ターゲット言語化</div>
    <div class="text-white" style="font-size: 0.9rem;">最も多いセグメント: {} ({}人, {:.1}%)</div>
</div>"##,
            stats.target_segment, format_number(stats.target_count), stats.target_pct
        ));
    }

    // タイミング
    if stats.timing_now_count > 0 {
        cards.push_str(&format!(
            r##"<div class="stat-card" style="flex: 1; min-width: 280px; border: 2px solid #F59E0B; border-radius: 12px;">
    <div class="text-sm font-bold mb-2" style="color: #F59E0B;">⏰ タイミング言語化</div>
    <div class="text-white" style="font-size: 0.9rem;">「今すぐ」層: {}人 (全体の{:.1}%が即転職希望)</div>
</div>"##,
            format_number(stats.timing_now_count), stats.timing_now_pct
        ));
    }

    // 資格戦略
    if !stats.top_qualification.is_empty() {
        cards.push_str(&format!(
            r##"<div class="stat-card" style="flex: 1; min-width: 280px; border: 2px solid #10B981; border-radius: 12px;">
    <div class="text-sm font-bold mb-2" style="color: #10B981;">📜 資格戦略言語化</div>
    <div class="text-white" style="font-size: 0.9rem;">{} - {}人が保有（最も多い保有資格）</div>
</div>"##,
            stats.top_qualification, format_number(stats.top_qual_count)
        ));
    }

    // 隠れた人材
    if !stats.hidden_segments.is_empty() {
        let seg_html: Vec<String> = stats.hidden_segments.iter().map(|(name, count, pct)| {
            format!("{} ({}人, {:.1}%)", name, format_number(*count), pct)
        }).collect();
        cards.push_str(&format!(
            r##"<div class="stat-card" style="flex: 1; min-width: 280px; border: 2px solid #8B5CF6; border-radius: 12px;">
    <div class="text-sm font-bold mb-2" style="color: #8B5CF6;">💎 隠れた人材発見</div>
    <div class="text-white" style="font-size: 0.9rem;">注目: {}</div>
</div>"##,
            seg_html.join(", ")
        ));
    }

    if cards.is_empty() { return String::new(); }

    format!(
        r##"<div class="flex flex-wrap gap-4">{}</div>"##,
        cards
    )
}

// ===== ペルソナリスト（全件） =====

fn build_persona_list(personas: &[PersonaItem]) -> String {
    if personas.is_empty() {
        return r##"<p class="text-slate-500 text-sm">データがありません</p>"##.to_string();
    }
    let rows: Vec<String> = personas.iter().map(|p| {
        format!(
            r##"<div class="flex justify-between items-center py-1" style="border-bottom: 1px solid rgba(255,255,255,0.05);">
    <span class="font-semibold text-white" style="font-size: 0.85rem;">{}</span>
    <span class="text-slate-400" style="font-size: 0.85rem;">{}人 ({})</span>
</div>"##,
            p.label, format_number(p.count), p.share_pct
        )
    }).collect();
    rows.join("\n")
}

// ===== ペルソナ横棒グラフ =====

fn build_persona_bar_chart(personas: &[PersonaItem]) -> String {
    if personas.is_empty() {
        return r##"<p class="text-slate-500 text-sm">データがありません</p>"##.to_string();
    }
    let top10: Vec<&PersonaItem> = personas.iter().take(10).collect();
    let labels: Vec<String> = top10.iter().rev().map(|p| format!("\"{}\"", p.label)).collect();
    let values: Vec<String> = top10.iter().rev().map(|p| p.count.to_string()).collect();

    format!(
        r##"<div class="echart" style="height:350px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "grid": {{"left": "25%", "right": "10%", "top": "10%", "bottom": "10%"}},
            "xAxis": {{"type": "value"}},
            "yAxis": {{"type": "category", "data": [{}]}},
            "series": [{{"type": "bar", "data": [{}], "itemStyle": {{"color": "#6366F1"}}, "label": {{"show": true, "position": "right", "color": "#e2e8f0"}}}}]
        }}'></div>"##,
        labels.join(","),
        values.join(",")
    )
}

// ===== 資格一覧（定着率+解釈） =====

fn build_qualification_list(quals: &[QualificationItem]) -> String {
    if quals.is_empty() {
        return r##"<p class="text-slate-500 text-sm">データがありません</p>"##.to_string();
    }
    let rows: Vec<String> = quals.iter().map(|q| {
        let interp_color = if q.interpretation == "地元志向" { "#009E73" } else if q.interpretation == "流出傾向" { "#CC79A7" } else { "#94a3b8" };
        format!(
            r##"<div class="flex justify-between items-center py-2" style="border-bottom: 1px solid rgba(255,255,255,0.05);">
    <span class="font-semibold text-white" style="font-size: 0.9rem;">{}</span>
    <span class="flex gap-4 items-center">
        <span class="text-slate-400" style="font-size: 0.85rem;">定着率: {}</span>
        <span style="color: {}; font-size: 0.85rem;">{}</span>
    </span>
</div>"##,
            q.qualification, q.retention_rate, interp_color, q.interpretation
        )
    }).collect();
    rows.join("\n")
}

// ===== 男女比ドーナツ =====

fn build_gender_pie(male: i64, female: i64) -> String {
    if male == 0 && female == 0 {
        return r##"<p class="text-slate-500 text-sm text-center py-12">データがありません</p>"##.to_string();
    }
    format!(
        r##"<div class="echart" style="height:350px;" data-chart-config='{{
            "tooltip": {{"trigger": "item", "formatter": "{{b}}: {{c}} ({{d}}%)"}},
            "legend": {{"orient": "vertical", "left": "left"}},
            "series": [{{
                "type": "pie",
                "radius": ["40%", "70%"],
                "data": [
                    {{"value": {}, "name": "男性", "itemStyle": {{"color": "#0072B2"}}}},
                    {{"value": {}, "name": "女性", "itemStyle": {{"color": "#E69F00"}}}}
                ]
            }}]
        }}'></div>"##,
        male, female
    )
}

// ===== 年齢×性別 stacked bar =====

fn build_age_gender_stacked(data: &[(String, i64, i64)]) -> String {
    if data.is_empty() {
        return r##"<p class="text-slate-500 text-sm text-center py-12">データがありません</p>"##.to_string();
    }
    let labels: Vec<String> = data.iter().map(|(a, _, _)| format!("\"{}\"", a)).collect();
    let male_vals: Vec<String> = data.iter().map(|(_, m, _)| m.to_string()).collect();
    let female_vals: Vec<String> = data.iter().map(|(_, _, f)| f.to_string()).collect();

    format!(
        r##"<div class="echart" style="height:350px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis"}},
            "legend": {{"data": ["男性", "女性"], "top": 4}},
            "xAxis": {{"type": "category", "data": [{}]}},
            "yAxis": {{"type": "value", "name": "人数"}},
            "series": [
                {{"name": "男性", "type": "bar", "stack": "total", "data": [{}], "itemStyle": {{"color": "#0072B2"}}}},
                {{"name": "女性", "type": "bar", "stack": "total", "data": [{}], "itemStyle": {{"color": "#E69F00"}}}}
            ]
        }}'></div>"##,
        labels.join(","),
        male_vals.join(","),
        female_vals.join(",")
    )
}

// ===== KPIカード3枚 =====

fn build_kpi_cards(stats: &DemoStats) -> String {
    let total = stats.male_total + stats.female_total;
    let female_ratio = if total > 0 { (stats.female_total as f64 / total as f64) * 100.0 } else { 0.0 };

    format!(
        r##"<div class="stat-card" style="flex: 1; min-width: 150px;">
    <div class="text-sm text-slate-400">女性比率</div>
    <div class="text-2xl font-bold" style="color: #E69F00;">{:.1}%</div>
</div>
<div class="stat-card" style="flex: 1; min-width: 150px;">
    <div class="text-sm text-slate-400">平均資格数</div>
    <div class="text-2xl font-bold" style="color: #009E73;">{:.1}</div>
</div>
<div class="stat-card" style="flex: 1; min-width: 150px;">
    <div class="text-sm text-slate-400">平均移動距離</div>
    <div class="text-2xl font-bold" style="color: #6366F1;">{:.1}km</div>
</div>"##,
        female_ratio, stats.avg_qualifications, stats.avg_distance_km
    )
}

// ===== 就業状態別内訳チャート =====

fn build_employment_chart(data: &[EmploymentBreakdown]) -> String {
    if data.is_empty() {
        return r##"<p class="text-slate-500 text-sm text-center py-12">データがありません</p>"##.to_string();
    }
    let labels: Vec<String> = data.iter().map(|d| format!("\"{}\"", d.age_gender)).collect();
    let employed: Vec<String> = data.iter().map(|d| d.employed.to_string()).collect();
    let unemployed: Vec<String> = data.iter().map(|d| d.unemployed.to_string()).collect();
    let student: Vec<String> = data.iter().map(|d| d.student.to_string()).collect();

    format!(
        r##"<div class="echart" style="height:400px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "legend": {{"data": ["就業中", "離職中", "在学中"]}},
            "grid": {{"left": "15%", "right": "5%", "top": "15%", "bottom": "15%"}},
            "xAxis": {{"type": "category", "data": [{}], "axisLabel": {{"rotate": 45}}}},
            "yAxis": {{"type": "value"}},
            "series": [
                {{"name": "就業中", "type": "bar", "stack": "employment", "data": [{}], "itemStyle": {{"color": "#009E73"}}}},
                {{"name": "離職中", "type": "bar", "stack": "employment", "data": [{}], "itemStyle": {{"color": "#CC79A7"}}}},
                {{"name": "在学中", "type": "bar", "stack": "employment", "data": [{}], "itemStyle": {{"color": "#F0E442"}}}}
            ]
        }}'></div>"##,
        labels.join(","),
        employed.join(","),
        unemployed.join(","),
        student.join(",")
    )
}

// ===== 資格別男女横棒 =====

fn build_qual_gender_chart(data: &[QualGenderItem]) -> String {
    if data.is_empty() {
        return r##"<p class="text-slate-500 text-sm text-center py-12">データがありません</p>"##.to_string();
    }
    let labels: Vec<String> = data.iter().rev().map(|d| format!("\"{}\"", d.qualification)).collect();
    let male_vals: Vec<String> = data.iter().rev().map(|d| d.male.to_string()).collect();
    let female_vals: Vec<String> = data.iter().rev().map(|d| d.female.to_string()).collect();

    format!(
        r##"<div class="echart" style="height:400px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "legend": {{"data": ["男性", "女性"]}},
            "grid": {{"left": "20%", "right": "5%", "top": "15%", "bottom": "10%"}},
            "xAxis": {{"type": "value"}},
            "yAxis": {{"type": "category", "data": [{}]}},
            "series": [
                {{"name": "男性", "type": "bar", "data": [{}], "itemStyle": {{"color": "#0072B2"}}}},
                {{"name": "女性", "type": "bar", "data": [{}], "itemStyle": {{"color": "#E69F00"}}}}
            ]
        }}'></div>"##,
        labels.join(","),
        male_vals.join(","),
        female_vals.join(",")
    )
}

// ===== ペルソナシェア横棒 + バッジ =====

fn build_persona_share(personas: &[PersonaItem]) -> (String, String) {
    if personas.is_empty() {
        return (
            r##"<p class="text-slate-500 text-sm">シェアデータがありません</p>"##.to_string(),
            String::new()
        );
    }
    let top10: Vec<&PersonaItem> = personas.iter().take(10).collect();
    let labels: Vec<String> = top10.iter().rev().map(|p| format!("\"{}\"", p.label)).collect();
    let values: Vec<String> = top10.iter().rev().map(|p| p.count.to_string()).collect();

    let chart = format!(
        r##"<div class="echart" style="height:350px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "grid": {{"left": "25%", "right": "10%", "top": "5%", "bottom": "5%"}},
            "xAxis": {{"type": "value"}},
            "yAxis": {{"type": "category", "data": [{}]}},
            "series": [{{"type": "bar", "data": [{}], "itemStyle": {{"color": "#6366F1"}}, "label": {{"show": true, "position": "right", "color": "#e2e8f0"}}}}]
        }}'></div>"##,
        labels.join(","),
        values.join(",")
    );

    let badges: Vec<String> = personas.iter().take(6).map(|p| {
        format!(
            r##"<span class="text-white" style="background-color: rgba(99, 102, 241, 0.1); padding: 4px 8px; border-radius: 4px; font-size: 0.75rem;">{}: {}</span>"##,
            p.label, p.share_pct
        )
    }).collect();
    let badge_html = format!(r##"<div class="flex gap-2 flex-wrap mt-2">{}</div>"##, badges.join("\n"));

    (chart, badge_html)
}

// ===== 年齢×性別統計リスト =====

fn build_age_gender_stats_list(data: &[AgeGenderStat]) -> String {
    if data.is_empty() {
        return r##"<p class="text-slate-500 text-sm">統計データがありません</p>"##.to_string();
    }
    let rows: Vec<String> = data.iter().map(|d| {
        format!(
            r##"<div class="flex justify-between items-center py-2" style="border-bottom: 1px solid rgba(255,255,255,0.05);">
    <span class="font-semibold text-white" style="font-size: 0.85rem; min-width: 80px;">{}</span>
    <span class="flex gap-4">
        <span class="flex gap-1 items-center">
            <span class="text-slate-400" style="font-size: 0.75rem;">希望勤務地:</span>
            <span style="color: #6366F1; font-size: 0.85rem; font-weight: 500;">{}箇所</span>
        </span>
        <span class="flex gap-1 items-center">
            <span class="text-slate-400" style="font-size: 0.75rem;">資格:</span>
            <span style="color: #009E73; font-size: 0.85rem; font-weight: 500;">{}個</span>
        </span>
    </span>
</div>"##,
            d.label, d.desired_areas, d.qualifications
        )
    }).collect();
    rows.join("\n")
}

// ===== RARITY チェックボックス生成 =====

fn build_rarity_age_checkboxes() -> String {
    let ages = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
    ages.iter().map(|age| {
        format!(
            r##"<label class="flex items-center gap-1 text-sm text-white cursor-pointer"><input type="checkbox" name="age" value="{}" class="accent-blue-500"> {}</label>"##,
            age, age
        )
    }).collect::<Vec<String>>().join("\n")
}

fn build_rarity_qual_checkboxes(options: &[(String, i64)]) -> String {
    if options.is_empty() {
        return r##"<p class="text-slate-500 text-sm">資格データがありません</p>"##.to_string();
    }
    options.iter().map(|(qual, count)| {
        format!(
            r##"<label class="flex items-center gap-1 text-sm text-white cursor-pointer"><input type="checkbox" name="qualification" value="{}"> {} ({}人)</label>"##,
            qual, qual, format_number(*count)
        )
    }).collect::<Vec<String>>().join("\n")
}

// ===== 緊急度チャート（2軸: 棒+折れ線） =====

fn build_urgency_chart(data: &[(String, i64, f64)], mode: &str) -> String {
    if data.is_empty() {
        return r##"<p class="text-slate-500 text-sm text-center py-12">データがありません</p>"##.to_string();
    }
    let labels: Vec<String> = data.iter().map(|(l, _, _)| format!("\"{}\"", l)).collect();
    let counts: Vec<String> = data.iter().map(|(_, c, _)| c.to_string()).collect();
    let scores: Vec<String> = data.iter().map(|(_, _, s)| format!("{:.2}", s)).collect();

    let (bar_color, line_color, rotate) = if mode == "gender" {
        ("#6366F1", "#ef4444", "0")
    } else {
        ("#009E73", "#f59e0b", "15")
    };

    format!(
        r##"<div class="echart" style="height:350px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "cross"}}}},
            "legend": {{"data": ["人数", "平均スコア"], "top": 4}},
            "xAxis": {{"type": "category", "data": [{}], "axisLabel": {{"rotate": {}}}}},
            "yAxis": [
                {{"type": "value", "name": "人数", "position": "left"}},
                {{"type": "value", "name": "平均スコア", "position": "right", "min": 0, "max": 5, "splitNumber": 5}}
            ],
            "series": [
                {{"name": "人数", "type": "bar", "yAxisIndex": 0, "data": [{}], "itemStyle": {{"color": "{}"}}, "barWidth": "40%", "label": {{"show": true, "position": "top", "color": "#e2e8f0"}}}},
                {{"name": "平均スコア", "type": "line", "yAxisIndex": 1, "data": [{}], "lineStyle": {{"color": "{}", "width": 3}}, "itemStyle": {{"color": "{}"}}, "symbol": "circle", "symbolSize": 8, "label": {{"show": true, "position": "top", "color": "{}"}}}}
            ]
        }}'></div>"##,
        labels.join(","),
        rotate,
        counts.join(","),
        bar_color,
        scores.join(","),
        line_color, line_color, line_color
    )
}

// ===== RARITY API エンドポイント =====

/// HTMLフォームのチェックボックス(同名パラメータ重複: age=30代&age=40代)をパースする
/// axum::extract::Queryはserde_urlencodedを使用しVec<String>に対応しないため手動パース
fn parse_multi_value_query(query_str: &str) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for pair in query_str.split('&') {
        if let Some((key, val)) = pair.split_once('=') {
            let key_decoded = urlencoding::decode(key).unwrap_or_default().to_string();
            let val_decoded = urlencoding::decode(val).unwrap_or_default().to_string();
            if !val_decoded.is_empty() {
                map.entry(key_decoded).or_default().push(val_decoded);
            }
        }
    }
    map
}

/// RARITY検索APIハンドラー
pub async fn api_rarity(
    State(state): State<Arc<AppState>>,
    session: Session,
    raw_query: axum::extract::RawQuery,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let query_str = raw_query.0.unwrap_or_default();
    let params_map = parse_multi_value_query(&query_str);
    let age_list = params_map.get("age").cloned().unwrap_or_default();
    let gender_list = params_map.get("gender").cloned().unwrap_or_default();
    let qualification_list = params_map.get("qualification").cloned().unwrap_or_default();

    if age_list.is_empty() && gender_list.is_empty() && qualification_list.is_empty() {
        return Html(r##"<p class="text-slate-500 text-sm">条件を選択して検索してください</p>"##.to_string());
    }

    let mut params = vec![Value::String(job_type.clone())];
    let mut conditions = vec!["job_type = ?".to_string(), "row_type = 'AGE_GENDER_RESIDENCE'".to_string()];

    if !prefecture.is_empty() && prefecture != "全国" {
        conditions.push("prefecture = ?".to_string());
        params.push(Value::String(prefecture));
    }
    if !municipality.is_empty() && municipality != "すべて" {
        conditions.push("municipality LIKE ?".to_string());
        params.push(Value::String(format!("{}%", municipality)));
    }

    // 年代フィルタ
    if !age_list.is_empty() {
        let placeholders: Vec<&str> = age_list.iter().map(|_| "?").collect();
        conditions.push(format!("category1 IN ({})", placeholders.join(",")));
        for a in &age_list {
            params.push(Value::String(a.clone()));
        }
    }

    // 性別フィルタ
    if !gender_list.is_empty() {
        let placeholders: Vec<&str> = gender_list.iter().map(|_| "?").collect();
        conditions.push(format!("category2 IN ({})", placeholders.join(",")));
        for g in &gender_list {
            params.push(Value::String(g.clone()));
        }
    }

    let sql = format!(
        "SELECT category1, category2, SUM(count) as total \
         FROM job_seeker_data \
         WHERE {} \
         GROUP BY category1, category2 \
         ORDER BY total DESC \
         LIMIT 50",
        conditions.join(" AND ")
    );

    let rows = match state.turso.query(&sql, &params).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Rarity query failed: {e}");
            return Html(r##"<p class="text-red-400 text-sm">検索に失敗しました</p>"##.to_string());
        }
    };

    if rows.is_empty() {
        return Html(r##"<p class="text-slate-500 text-sm">該当データがありません</p>"##.to_string());
    }

    let mut total_count: i64 = 0;
    let mut result_items: Vec<(String, String, i64)> = Vec::new();

    for row in &rows {
        let age = get_str(row, "category1");
        let gender = get_str(row, "category2");
        let cnt = get_i64(row, "total");
        total_count += cnt;
        result_items.push((age, gender, cnt));
    }

    // 資格フィルタ処理: 資格が選択されていればさらにフィルタ
    // （簡略実装: 資格フィルタはDB側で完全にはできないため、バッジ表示のみ）
    let qual_badge = if !qualification_list.is_empty() {
        format!(
            r##"<span class="px-2 py-1 rounded text-xs" style="background-color: rgba(168, 85, 247, 0.2); color: #c084fc;">資格: {}</span>"##,
            qualification_list.join(", ")
        )
    } else { String::new() };

    let mut html = format!(
        r##"<div class="flex gap-2 mb-2 flex-wrap">
    <span class="px-2 py-1 rounded text-xs text-white" style="background-color: #6366F1;">該当: {}人</span>
    <span class="px-2 py-1 rounded text-xs text-slate-300" style="background-color: rgba(100,100,100,0.3);">組み合わせ: {}件</span>
    {}
</div>"##,
        format_number(total_count),
        result_items.len(),
        qual_badge
    );

    html.push_str(r##"<div style="max-height: 300px; overflow-y: auto;">"##);
    for (age, gender, cnt) in &result_items {
        let share = if total_count > 0 { (*cnt as f64 / total_count as f64) * 100.0 } else { 0.0 };
        html.push_str(&format!(
            r##"<div class="flex items-center gap-2 py-1" style="border-bottom: 1px solid rgba(255,255,255,0.05);">
    <span class="text-white font-semibold" style="font-size: 0.85rem; min-width: 50px;">{}</span>
    <span class="text-slate-400" style="font-size: 0.8rem; min-width: 40px;">{}</span>
    <span class="flex-1"></span>
    <span style="color: #6366F1; font-size: 0.85rem; font-weight: 500;">{}人</span>
    <span class="text-slate-400" style="font-size: 0.8rem;">({:.1}%)</span>
</div>"##,
            age, gender, format_number(*cnt), share
        ));
    }
    html.push_str("</div>");

    Html(html)
}

/// 数値をカンマ区切りでフォーマット（ローカル版）
fn _format_num(n: i64) -> String {
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
