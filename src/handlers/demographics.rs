use axum::extract::State;
use axum::response::Html;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;

use super::overview::{get_str, get_i64, get_f64, format_number, get_session_filters, build_location_filter, make_location_label};

/// タブ2: 人口動態 - HTMXパーシャルHTML
pub async fn tab_demographics(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, municipality) = get_session_filters(&session).await;

    let cache_key = format!("demographics_{}_{}_{}", job_type, prefecture, municipality);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let stats = fetch_demographics(&state, &job_type, &prefecture, &municipality).await;
    let location_label = make_location_label(&prefecture, &municipality);
    let html = render_demographics(&job_type, &stats, &location_label);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

struct DemoStats {
    /// 都道府県別求職者数 (name, total, male, female)
    pref_ranking: Vec<(String, i64, i64, i64)>,
    /// 年齢×性別 (age_group, male, female)
    age_gender: Vec<(String, i64, i64)>,
    /// 資格分布 (qualification, count)
    qual_dist: Vec<(String, i64)>,
    /// 緊急度×性別 (gender, count, avg_urgency_score)
    urgency_gender: Vec<(String, i64, f64)>,
    /// 転職希望時期別緊急度 (start_category, count, avg_urgency_score)
    urgency_start: Vec<(String, i64, f64)>,
    /// ペルソナ構成比 (label, count) - AGE_GENDER由来の年齢×性別ペルソナ
    persona_ratio: Vec<(String, i64)>,

    // --- 言語化カード用フィールド ---
    /// 最多年齢×性別セグメント名
    target_segment: String,
    /// そのセグメントの人数
    target_count: i64,
    /// そのセグメントの構成比(%)
    target_pct: f64,
    /// "今すぐ"層の人数
    timing_now_count: i64,
    /// "今すぐ"層の割合(%)
    timing_now_pct: f64,
    /// 最多保有資格名
    top_qualification: String,
    /// その資格の保有者数
    top_qual_count: i64,
    /// 見落としがちなセグメント (name, count, pct)
    hidden_segments: Vec<(String, i64, f64)>,
}

impl Default for DemoStats {
    fn default() -> Self {
        Self {
            pref_ranking: Vec::new(),
            age_gender: Vec::new(),
            qual_dist: Vec::new(),
            urgency_gender: Vec::new(),
            urgency_start: Vec::new(),
            persona_ratio: Vec::new(),
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

async fn fetch_demographics(state: &AppState, job_type: &str, prefecture: &str, municipality: &str) -> DemoStats {
    let mut params = vec![Value::String(job_type.to_string())];
    let location_filter = build_location_filter(prefecture, municipality, &mut params);

    let sql = format!(
        "SELECT row_type, prefecture, municipality, \
               male_count, female_count, \
               category1, category2, count, avg_urgency_score \
        FROM job_seeker_data \
        WHERE job_type = ? \
          AND row_type IN ('SUMMARY', 'AGE_GENDER', 'QUALIFICATION_DETAIL', \
                           'URGENCY_GENDER', 'URGENCY_START_CATEGORY') \
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

    // 都道府県別集計
    let mut pref_male: HashMap<String, i64> = HashMap::new();
    let mut pref_female: HashMap<String, i64> = HashMap::new();

    let age_order = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
    let mut age_male: HashMap<String, i64> = HashMap::new();
    let mut age_female: HashMap<String, i64> = HashMap::new();

    let mut qual_map: HashMap<String, i64> = HashMap::new();

    // 緊急度×性別: gender -> (合計count, urgency_score合計, 行数) で加重平均を算出
    let mut urg_gender_map: HashMap<String, (i64, f64, i64)> = HashMap::new();
    // 転職希望時期別: start_category -> (合計count, urgency_score合計, 行数)
    let mut urg_start_map: HashMap<String, (i64, f64, i64)> = HashMap::new();
    // ペルソナ構成比: "年齢層-性別" -> count
    let mut persona_map: HashMap<String, i64> = HashMap::new();

    for row in &rows {
        let row_type = get_str(row, "row_type");
        match row_type.as_str() {
            "SUMMARY" => {
                let pref = get_str(row, "prefecture");
                if !pref.is_empty() {
                    let male = get_i64(row, "male_count");
                    let female = get_i64(row, "female_count");
                    *pref_male.entry(pref.clone()).or_insert(0) += male;
                    *pref_female.entry(pref).or_insert(0) += female;
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
                // ペルソナ構成比用: "年齢層 / 性別" をラベルにする
                if !age_group.is_empty() && !gender.is_empty() {
                    let label = format!("{} / {}", age_group, gender);
                    *persona_map.entry(label).or_insert(0) += cnt;
                }
            }
            "QUALIFICATION_DETAIL" => {
                let qual = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                if !qual.is_empty() {
                    *qual_map.entry(qual).or_insert(0) += cnt;
                }
            }
            "URGENCY_GENDER" => {
                let gender = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                let score = get_f64(row, "avg_urgency_score");
                if !gender.is_empty() {
                    let entry = urg_gender_map.entry(gender).or_insert((0, 0.0, 0));
                    entry.0 += cnt;
                    entry.1 += score * cnt as f64;
                    entry.2 += 1;
                }
            }
            "URGENCY_START_CATEGORY" => {
                let start_cat = get_str(row, "category1");
                let cnt = get_i64(row, "count");
                let score = get_f64(row, "avg_urgency_score");
                if !start_cat.is_empty() {
                    let entry = urg_start_map.entry(start_cat).or_insert((0, 0.0, 0));
                    entry.0 += cnt;
                    entry.1 += score * cnt as f64;
                    entry.2 += 1;
                }
            }
            _ => {}
        }
    }

    // 都道府県ランキング（合計降順）
    let mut pref_list: Vec<(String, i64, i64, i64)> = pref_male
        .keys()
        .map(|p| {
            let m = *pref_male.get(p).unwrap_or(&0);
            let f = *pref_female.get(p).unwrap_or(&0);
            (p.clone(), m + f, m, f)
        })
        .collect();
    pref_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.pref_ranking = pref_list.into_iter().take(15).collect();

    // 年齢×性別
    for age in &age_order {
        let m = age_male.get(*age).copied().unwrap_or(0);
        let f = age_female.get(*age).copied().unwrap_or(0);
        stats.age_gender.push((age.to_string(), m, f));
    }

    // --- 言語化カード: 資格戦略（qual_map消費前に算出） ---
    if let Some((qual, count)) = qual_map.iter().max_by_key(|(_, v)| **v) {
        stats.top_qualification = qual.clone();
        stats.top_qual_count = *count;
    }

    // 資格分布（上位10）
    let mut qual_list: Vec<(String, i64)> = qual_map.into_iter().collect();
    qual_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.qual_dist = qual_list.into_iter().take(10).collect();

    // 緊急度×性別: 加重平均スコアを算出し、人数降順で並べる
    let mut urg_gender_list: Vec<(String, i64, f64)> = urg_gender_map
        .into_iter()
        .map(|(gender, (total_cnt, weighted_score, _))| {
            let avg = if total_cnt > 0 { weighted_score / total_cnt as f64 } else { 0.0 };
            (gender, total_cnt, avg)
        })
        .collect();
    urg_gender_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.urgency_gender = urg_gender_list;

    // --- 言語化カード: タイミング分析（urg_start_map消費前に算出） ---
    if let Some((total_cnt, _, _)) = urg_start_map.get("今すぐ") {
        stats.timing_now_count = *total_cnt;
        let start_total: i64 = urg_start_map.values().map(|(c, _, _)| c).sum();
        stats.timing_now_pct = if start_total > 0 {
            (*total_cnt as f64 / start_total as f64) * 100.0
        } else {
            0.0
        };
    }

    // 転職希望時期別緊急度: 定義順でソート
    let start_order = ["今すぐ", "1～3ヶ月", "3～6ヶ月", "6ヶ月以上"];
    let mut urg_start_list: Vec<(String, i64, f64)> = urg_start_map
        .into_iter()
        .map(|(cat, (total_cnt, weighted_score, _))| {
            let avg = if total_cnt > 0 { weighted_score / total_cnt as f64 } else { 0.0 };
            (cat, total_cnt, avg)
        })
        .collect();
    urg_start_list.sort_by(|a, b| {
        let pos_a = start_order.iter().position(|&o| a.0.contains(o)).unwrap_or(99);
        let pos_b = start_order.iter().position(|&o| b.0.contains(o)).unwrap_or(99);
        pos_a.cmp(&pos_b)
    });
    stats.urgency_start = urg_start_list;

    // --- 言語化カード: ターゲットプロファイル（persona_map消費前に算出） ---
    if let Some((label, count)) = persona_map.iter().max_by_key(|(_, v)| **v) {
        stats.target_segment = label.clone();
        stats.target_count = *count;
        let total: i64 = persona_map.values().sum();
        stats.target_pct = if total > 0 {
            (*count as f64 / total as f64) * 100.0
        } else {
            0.0
        };
    }

    // --- 言語化カード: 隠れた人材（構成比5%未満かつ100人以上、最大2件） ---
    {
        let persona_total: i64 = persona_map.values().sum();
        let mut hidden: Vec<(String, i64, f64)> = persona_map
            .iter()
            .filter_map(|(label, count)| {
                let pct = if persona_total > 0 {
                    (*count as f64 / persona_total as f64) * 100.0
                } else {
                    0.0
                };
                // 構成比0.5%超～5%未満、かつ100人以上を「隠れた人材」とみなす
                if pct < 5.0 && pct > 0.5 && *count >= 100 {
                    Some((label.clone(), *count, pct))
                } else {
                    None
                }
            })
            .collect();
        hidden.sort_by(|a, b| b.1.cmp(&a.1));
        stats.hidden_segments = hidden.into_iter().take(2).collect();
    }

    // ペルソナ構成比（上位10、人数降順）
    let mut persona_list: Vec<(String, i64)> = persona_map.into_iter().collect();
    persona_list.sort_by(|a, b| b.1.cmp(&a.1));
    stats.persona_ratio = persona_list.into_iter().take(10).collect();

    stats
}

/// 4種の言語化カード（ターゲットプロファイル、タイミング、資格戦略、隠れた人材）をHTML文字列として生成
fn build_verbalization_cards(stats: &DemoStats) -> String {
    let mut cards = String::new();

    // ターゲットプロファイルカード
    if !stats.target_segment.is_empty() {
        cards.push_str(&format!(
            r#"<div class="stat-card border-l-4 border-cyan-500">
    <h3 class="text-sm text-cyan-400 mb-1">&#x1F3AF; ターゲットプロファイル</h3>
    <p class="text-lg font-bold text-white">{}</p>
    <p class="text-sm text-slate-400">{}人（構成比 {:.1}%）</p>
</div>"#,
            stats.target_segment,
            format_number(stats.target_count),
            stats.target_pct
        ));
    }

    // タイミングカード
    if stats.timing_now_count > 0 {
        cards.push_str(&format!(
            r#"<div class="stat-card border-l-4 border-amber-500">
    <h3 class="text-sm text-amber-400 mb-1">&#x23F0; 採用タイミング</h3>
    <p class="text-lg font-bold text-white">「今すぐ」層: {}人</p>
    <p class="text-sm text-slate-400">全体の{:.1}%が即転職希望</p>
</div>"#,
            format_number(stats.timing_now_count),
            stats.timing_now_pct
        ));
    }

    // 資格戦略カード
    if !stats.top_qualification.is_empty() {
        cards.push_str(&format!(
            r#"<div class="stat-card border-l-4 border-purple-500">
    <h3 class="text-sm text-purple-400 mb-1">&#x1F4DC; 資格戦略</h3>
    <p class="text-lg font-bold text-white">{}</p>
    <p class="text-sm text-slate-400">{}人が保有 - 最も多い保有資格</p>
</div>"#,
            stats.top_qualification,
            format_number(stats.top_qual_count)
        ));
    }

    // 隠れた人材カード
    if !stats.hidden_segments.is_empty() {
        let segments_html: Vec<String> = stats
            .hidden_segments
            .iter()
            .map(|(name, count, pct)| {
                format!(
                    "<li>{} ({}人, {:.1}%)</li>",
                    name,
                    format_number(*count),
                    pct
                )
            })
            .collect();
        cards.push_str(&format!(
            r#"<div class="stat-card border-l-4 border-green-500">
    <h3 class="text-sm text-green-400 mb-1">&#x1F48E; 隠れた人材</h3>
    <p class="text-sm text-slate-300">見落とされやすいセグメント:</p>
    <ul class="text-sm text-slate-400 list-disc list-inside mt-1">{}</ul>
</div>"#,
            segments_html.join("")
        ));
    }

    if cards.is_empty() {
        return String::new();
    }

    format!(
        r#"<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">{}</div>"#,
        cards
    )
}

fn render_demographics(job_type: &str, stats: &DemoStats, _location_label: &str) -> String {
    // 都道府県ランキングテーブル行
    let pref_rows: String = stats
        .pref_ranking
        .iter()
        .enumerate()
        .map(|(i, (name, total, male, female))| {
            format!(
                r#"<tr><td class="text-center">{}</td><td>{}</td><td class="text-right">{}</td><td class="text-right text-cyan-400">{}</td><td class="text-right text-pink-400">{}</td></tr>"#,
                i + 1, name, format_number(*total), format_number(*male), format_number(*female)
            )
        })
        .collect();

    // 年齢×性別チャートデータ
    let age_labels: Vec<String> = stats.age_gender.iter().map(|(a, _, _)| format!("\"{}\"", a)).collect();
    let age_male_vals: Vec<String> = stats.age_gender.iter().map(|(_, m, _)| m.to_string()).collect();
    let age_female_vals: Vec<String> = stats.age_gender.iter().map(|(_, _, f)| f.to_string()).collect();

    // 資格分布チャートデータ
    let qual_labels: Vec<String> = stats.qual_dist.iter().map(|(q, _)| format!("\"{}\"", q)).collect();
    let qual_values: Vec<String> = stats.qual_dist.iter().map(|(_, v)| v.to_string()).collect();

    // 緊急度×性別チャートデータ（棒: 人数、折れ線: 平均スコア）
    let urg_gender_labels: Vec<String> = stats.urgency_gender.iter().map(|(g, _, _)| format!("\"{}\"", g)).collect();
    let urg_gender_counts: Vec<String> = stats.urgency_gender.iter().map(|(_, c, _)| c.to_string()).collect();
    let urg_gender_scores: Vec<String> = stats.urgency_gender.iter().map(|(_, _, s)| format!("{:.2}", s)).collect();

    // 転職希望時期別緊急度チャートデータ
    let urg_start_labels: Vec<String> = stats.urgency_start.iter().map(|(c, _, _)| format!("\"{}\"", c)).collect();
    let urg_start_counts: Vec<String> = stats.urgency_start.iter().map(|(_, c, _)| c.to_string()).collect();
    let urg_start_scores: Vec<String> = stats.urgency_start.iter().map(|(_, _, s)| format!("{:.2}", s)).collect();

    // ペルソナ構成比チャートデータ（横棒、上位10）
    let persona_labels: Vec<String> = stats.persona_ratio.iter().map(|(l, _)| format!("\"{}\"", l)).collect();
    let persona_values: Vec<String> = stats.persona_ratio.iter().map(|(_, v)| v.to_string()).collect();
    let persona_total: i64 = stats.persona_ratio.iter().map(|(_, v)| v).sum();
    // 構成比(%)を計算
    let persona_pct: Vec<String> = stats.persona_ratio.iter().map(|(_, v)| {
        if persona_total > 0 {
            format!("{:.1}", *v as f64 / persona_total as f64 * 100.0)
        } else {
            "0.0".to_string()
        }
    }).collect();

    // 言語化カードHTML生成
    let verbalization_cards = build_verbalization_cards(stats);

    include_str!("../../templates/tabs/demographics.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{VERBALIZATION_CARDS}}", &verbalization_cards)
        .replace("{{PREF_ROWS}}", &pref_rows)
        .replace("{{AGE_LABELS}}", &format!("[{}]", age_labels.join(",")))
        .replace("{{AGE_MALE_VALUES}}", &format!("[{}]", age_male_vals.join(",")))
        .replace("{{AGE_FEMALE_VALUES}}", &format!("[{}]", age_female_vals.join(",")))
        .replace("{{QUAL_LABELS}}", &format!("[{}]", qual_labels.join(",")))
        .replace("{{QUAL_VALUES}}", &format!("[{}]", qual_values.join(",")))
        .replace("{{URG_GENDER_LABELS}}", &format!("[{}]", urg_gender_labels.join(",")))
        .replace("{{URG_GENDER_COUNTS}}", &format!("[{}]", urg_gender_counts.join(",")))
        .replace("{{URG_GENDER_SCORES}}", &format!("[{}]", urg_gender_scores.join(",")))
        .replace("{{URG_START_LABELS}}", &format!("[{}]", urg_start_labels.join(",")))
        .replace("{{URG_START_COUNTS}}", &format!("[{}]", urg_start_counts.join(",")))
        .replace("{{URG_START_SCORES}}", &format!("[{}]", urg_start_scores.join(",")))
        .replace("{{PERSONA_LABELS}}", &format!("[{}]", persona_labels.join(",")))
        .replace("{{PERSONA_VALUES}}", &format!("[{}]", persona_values.join(",")))
        .replace("{{PERSONA_PCT}}", &format!("[{}]", persona_pct.join(",")))
}
