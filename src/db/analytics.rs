//! 分析テーブル(9テーブル)のクエリ関数モジュール
//!
//! geocoded_postings.db 内の layer_a_*, layer_b_*, layer_c_* テーブルから
//! 各種分析データを取得する関数群。

use serde_json::Value;
use std::collections::HashMap;

use super::local_sqlite::LocalDb;

// ---------------------------------------------------------------------------
// A-1: 給与統計 (layer_a_salary_stats)
// ---------------------------------------------------------------------------

/// 給与統計データを取得する。
///
/// - `prefecture` が空文字の場合: 全国データ(prefecture='全国')を返す
/// - それ以外: 指定都道府県でフィルタ
///
/// ORDER BY salary_type, employment_type
pub fn query_salary_stats(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        db.query(
            "SELECT job_type, prefecture, salary_type, employment_type,
                    count, mean, median, p25, p75, p90, std, gini,
                    has_salary_range_pct, salary_range_median
             FROM layer_a_salary_stats
             WHERE job_type = ?1 AND prefecture = '全国'
             ORDER BY salary_type, employment_type",
            &[&job_type as &dyn rusqlite::types::ToSql],
        )
    } else {
        db.query(
            "SELECT job_type, prefecture, salary_type, employment_type,
                    count, mean, median, p25, p75, p90, std, gini,
                    has_salary_range_pct, salary_range_median
             FROM layer_a_salary_stats
             WHERE job_type = ?1 AND prefecture = ?2
             ORDER BY salary_type, employment_type",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &prefecture as &dyn rusqlite::types::ToSql,
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// A-2: 法人集中度 (layer_a_facility_concentration)
// ---------------------------------------------------------------------------

/// 法人(施設)集中度データを取得する。
///
/// - `prefecture` が空文字の場合: 全国データ
/// - それ以外: 指定都道府県でフィルタ
pub fn query_facility_concentration(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        db.query(
            "SELECT job_type, prefecture, total_postings, unique_facilities,
                    top1_name, top1_count, top1_pct, top5_pct, top10_pct, top20_pct,
                    hhi, zipf_exponent
             FROM layer_a_facility_concentration
             WHERE job_type = ?1 AND prefecture = '全国'",
            &[&job_type as &dyn rusqlite::types::ToSql],
        )
    } else {
        db.query(
            "SELECT job_type, prefecture, total_postings, unique_facilities,
                    top1_name, top1_count, top1_pct, top5_pct, top10_pct, top20_pct,
                    hhi, zipf_exponent
             FROM layer_a_facility_concentration
             WHERE job_type = ?1 AND prefecture = ?2",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &prefecture as &dyn rusqlite::types::ToSql,
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// A-3: 雇用形態多様性 (layer_a_employment_diversity)
// ---------------------------------------------------------------------------

/// 雇用形態の多様性指標を取得する。
///
/// - `prefecture` が空文字の場合: 全国データ
/// - それ以外: 指定都道府県でフィルタ
pub fn query_employment_diversity(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        db.query(
            "SELECT job_type, prefecture, total_postings, n_types,
                    shannon_entropy, max_entropy, evenness,
                    dominant_type, dominant_pct, type_distribution
             FROM layer_a_employment_diversity
             WHERE job_type = ?1 AND prefecture = '全国'",
            &[&job_type as &dyn rusqlite::types::ToSql],
        )
    } else {
        db.query(
            "SELECT job_type, prefecture, total_postings, n_types,
                    shannon_entropy, max_entropy, evenness,
                    dominant_type, dominant_pct, type_distribution
             FROM layer_a_employment_diversity
             WHERE job_type = ?1 AND prefecture = ?2",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &prefecture as &dyn rusqlite::types::ToSql,
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// B-1: キーワード3層構造 (layer_b_keywords)
// ---------------------------------------------------------------------------

/// キーワード分析データを取得する。
///
/// - `prefecture` が空文字の場合: 全国データ
/// - `layer`: "universal", "job_type", "regional" のいずれか。Noneなら全層
/// - `limit`: 取得件数上限(デフォルト50)
///
/// ORDER BY rank ASC
pub fn query_keywords(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    layer: Option<&str>,
    limit: Option<i32>,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let effective_limit = limit.unwrap_or(50);
    let pref_value = if prefecture.is_empty() {
        "全国"
    } else {
        prefecture
    };

    match layer {
        Some(l) => db.query(
            "SELECT job_type, prefecture, layer, keyword,
                    tfidf_score, doc_freq, doc_freq_pct, rank
             FROM layer_b_keywords
             WHERE job_type = ?1 AND prefecture = ?2 AND layer = ?3
             ORDER BY rank ASC
             LIMIT ?4",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &pref_value as &dyn rusqlite::types::ToSql,
                &l as &dyn rusqlite::types::ToSql,
                &effective_limit as &dyn rusqlite::types::ToSql,
            ],
        ),
        None => db.query(
            "SELECT job_type, prefecture, layer, keyword,
                    tfidf_score, doc_freq, doc_freq_pct, rank
             FROM layer_b_keywords
             WHERE job_type = ?1 AND prefecture = ?2
             ORDER BY rank ASC
             LIMIT ?3",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &pref_value as &dyn rusqlite::types::ToSql,
                &effective_limit as &dyn rusqlite::types::ToSql,
            ],
        ),
    }
}

// ---------------------------------------------------------------------------
// B-2: 条件共起パターン (layer_b_cooccurrence)
// ---------------------------------------------------------------------------

/// 条件フラグ間の共起パターンを取得する。
///
/// - `prefecture` が空文字の場合: 全国データ
/// - `min_lift`: 最小lift値(デフォルト1.0)。lift >= min_lift でフィルタ
///
/// ORDER BY lift DESC, LIMIT 50
pub fn query_cooccurrence(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    min_lift: Option<f64>,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let effective_min_lift = min_lift.unwrap_or(1.0);
    let pref_value = if prefecture.is_empty() {
        "全国"
    } else {
        prefecture
    };

    db.query(
        "SELECT job_type, prefecture, flag_a, flag_b,
                cooccurrence_count, expected_count, lift,
                phi_coefficient, support_pct
         FROM layer_b_cooccurrence
         WHERE job_type = ?1 AND prefecture = ?2 AND lift >= ?3
         ORDER BY lift DESC
         LIMIT 50",
        &[
            &job_type as &dyn rusqlite::types::ToSql,
            &pref_value as &dyn rusqlite::types::ToSql,
            &effective_min_lift as &dyn rusqlite::types::ToSql,
        ],
    )
}

// ---------------------------------------------------------------------------
// B-3: 原稿品質 (layer_b_text_quality)
// ---------------------------------------------------------------------------

/// 原稿(テキスト)品質スコアを取得する。
///
/// - `prefecture` が空文字の場合: job_typeの全都道府県データを返す
/// - それ以外: 指定都道府県のみ
pub fn query_text_quality(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        db.query(
            "SELECT job_type, prefecture, count,
                    entropy_mean, entropy_median, entropy_std,
                    entropy_p25, entropy_p75,
                    kanji_ratio_mean, kanji_ratio_median, kanji_ratio_std,
                    quality_score_mean, quality_score_median,
                    benefits_score_mean, benefits_score_median,
                    desc_length_mean, desc_length_median, grade
             FROM layer_b_text_quality
             WHERE job_type = ?1
             ORDER BY prefecture",
            &[&job_type as &dyn rusqlite::types::ToSql],
        )
    } else {
        db.query(
            "SELECT job_type, prefecture, count,
                    entropy_mean, entropy_median, entropy_std,
                    entropy_p25, entropy_p75,
                    kanji_ratio_mean, kanji_ratio_median, kanji_ratio_std,
                    quality_score_mean, quality_score_median,
                    benefits_score_mean, benefits_score_median,
                    desc_length_mean, desc_length_median, grade
             FROM layer_b_text_quality
             WHERE job_type = ?1 AND prefecture = ?2",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &prefecture as &dyn rusqlite::types::ToSql,
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// C-1: クラスタプロファイル (layer_c_cluster_profiles)
// ---------------------------------------------------------------------------

/// クラスタプロファイルを取得する。
///
/// job_type単位で全クラスタを返す(都道府県なし)。
/// ORDER BY cluster_id ASC
pub fn query_cluster_profiles(
    db: &LocalDb,
    job_type: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    db.query(
        "SELECT job_type, cluster_id, cluster_label, size, size_pct,
                salary_min_mean, salary_min_median,
                text_entropy_mean, benefits_score_mean, content_richness_mean,
                fulltime_pct, has_salary_range_pct,
                top_benefits, dominant_employment, feature_means, description
         FROM layer_c_cluster_profiles
         WHERE job_type = ?1
         ORDER BY cluster_id ASC",
        &[&job_type as &dyn rusqlite::types::ToSql],
    )
}

// ---------------------------------------------------------------------------
// C-1: 地域ヒートマップ (layer_c_region_heatmap)
// ---------------------------------------------------------------------------

/// 地域xクラスタのヒートマップデータを取得する。
///
/// - `prefecture` が空文字の場合: 全都道府県を返す
/// - `cluster_id`: Noneなら全クラスタ
///
/// ORDER BY prefecture, cluster_id
pub fn query_region_heatmap(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    cluster_id: Option<i32>,
) -> Result<Vec<HashMap<String, Value>>, String> {
    match (prefecture.is_empty(), cluster_id) {
        // 全都道府県 x 全クラスタ
        (true, None) => db.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1
             ORDER BY prefecture, cluster_id",
            &[&job_type as &dyn rusqlite::types::ToSql],
        ),
        // 全都道府県 x 特定クラスタ
        (true, Some(cid)) => db.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1 AND cluster_id = ?2
             ORDER BY prefecture, cluster_id",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &cid as &dyn rusqlite::types::ToSql,
            ],
        ),
        // 特定都道府県 x 全クラスタ
        (false, None) => db.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1 AND prefecture = ?2
             ORDER BY prefecture, cluster_id",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &prefecture as &dyn rusqlite::types::ToSql,
            ],
        ),
        // 特定都道府県 x 特定クラスタ
        (false, Some(cid)) => db.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1 AND prefecture = ?2 AND cluster_id = ?3
             ORDER BY prefecture, cluster_id",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &prefecture as &dyn rusqlite::types::ToSql,
                &cid as &dyn rusqlite::types::ToSql,
            ],
        ),
    }
}

// ---------------------------------------------------------------------------
// A-2: 法人集中度 - 全都道府県取得
// ---------------------------------------------------------------------------

/// 全都道府県の法人集中度データを取得する（都道府県間比較用）。
pub fn query_facility_all_prefectures(
    db: &LocalDb,
    job_type: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    db.query(
        "SELECT job_type, prefecture, total_postings, unique_facilities,
                top1_name, top1_count, top1_pct, top5_pct, top10_pct, top20_pct,
                hhi, zipf_exponent
         FROM layer_a_facility_concentration
         WHERE job_type = ?1
         ORDER BY prefecture",
        &[&job_type as &dyn rusqlite::types::ToSql],
    )
}

// ---------------------------------------------------------------------------
// A-3: 雇用形態多様性 - 全都道府県取得
// ---------------------------------------------------------------------------

/// 全都道府県の雇用形態多様性データを取得する（都道府県間比較用）。
pub fn query_employment_all_prefectures(
    db: &LocalDb,
    job_type: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    db.query(
        "SELECT job_type, prefecture, total_postings, n_types,
                shannon_entropy, max_entropy, evenness,
                dominant_type, dominant_pct, type_distribution
         FROM layer_a_employment_diversity
         WHERE job_type = ?1
         ORDER BY prefecture",
        &[&job_type as &dyn rusqlite::types::ToSql],
    )
}

// ---------------------------------------------------------------------------
// 全体サマリー (複数テーブル集約)
// ---------------------------------------------------------------------------

/// 指定job_typeの分析全体サマリーを返す。
/// prefecture 指定時はpostingsテーブルから直接集計する。
///
/// 各テーブルから集約値を取得し、1つの HashMap にまとめる:
/// - salary_stat_count: 給与統計レコード数
/// - cluster_count: クラスタ数
/// - keyword_count: キーワード総数
/// - text_quality_grade: 原稿品質グレード
/// - facility_total_postings: 総求人数
/// - employment_n_types: 雇用形態数
/// - cooccurrence_count: 共起パターン数
pub fn query_analysis_summary(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<HashMap<String, Value>, String> {
    let mut summary = HashMap::new();

    // 市区町村指定時は postings テーブルから直接集計
    let use_postings = !municipality.is_empty();

    if use_postings {
        // postings テーブルから直接集計
        let (where_clause, params) = build_postings_where(job_type, prefecture, municipality);

        let total: i64 = db.query_scalar(
            &format!("SELECT COUNT(*) FROM postings WHERE {}", where_clause),
            &params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect::<Vec<_>>(),
        ).unwrap_or(0);
        summary.insert("facility_total_postings".to_string(), Value::from(total));

        // 給与パターン数（salary_type別）
        let salary_count: i64 = db.query_scalar(
            &format!("SELECT COUNT(DISTINCT salary_type || '|' || COALESCE(employment_type,'')) FROM postings WHERE {} AND salary_min > 0", where_clause),
            &params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect::<Vec<_>>(),
        ).unwrap_or(0);
        summary.insert("salary_stat_count".to_string(), Value::from(salary_count));

        // 雇用形態数
        let n_types: i64 = db.query_scalar(
            &format!("SELECT COUNT(DISTINCT employment_type) FROM postings WHERE {}", where_clause),
            &params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect::<Vec<_>>(),
        ).unwrap_or(0);
        summary.insert("employment_n_types".to_string(), Value::from(n_types));

        // キーワード / 共起 / クラスタ / グレードは全国値を表示
        summary.insert("keyword_count".to_string(), Value::from(0_i64));
        summary.insert("cooccurrence_count".to_string(), Value::from(0_i64));
        summary.insert("cluster_count".to_string(), Value::from(0_i64));
        summary.insert("text_quality_grade".to_string(), Value::String("-".to_string()));

    } else {
        let pref_filter = if prefecture.is_empty() { "全国" } else { prefecture };

        // 給与統計レコード数
        let salary_count: i64 = db
            .query_scalar(
                "SELECT COUNT(*) FROM layer_a_salary_stats
                 WHERE job_type = ?1 AND prefecture = ?2",
                &[&job_type as &dyn rusqlite::types::ToSql, &pref_filter as &dyn rusqlite::types::ToSql],
            )
            .unwrap_or(0);
        summary.insert("salary_stat_count".to_string(), Value::from(salary_count));

        // クラスタ数
        let cluster_count: i64 = db
            .query_scalar(
                "SELECT COUNT(*) FROM layer_c_cluster_profiles
                 WHERE job_type = ?1",
                &[&job_type as &dyn rusqlite::types::ToSql],
            )
            .unwrap_or(0);
        summary.insert("cluster_count".to_string(), Value::from(cluster_count));

        // キーワード総数
        let keyword_count: i64 = db
            .query_scalar(
                "SELECT COUNT(*) FROM layer_b_keywords
                 WHERE job_type = ?1",
                &[&job_type as &dyn rusqlite::types::ToSql],
            )
            .unwrap_or(0);
        summary.insert("keyword_count".to_string(), Value::from(keyword_count));

        // 原稿品質グレード
        let grade_rows = db.query(
            "SELECT grade FROM layer_b_text_quality
             WHERE job_type = ?1 AND prefecture = ?2
             LIMIT 1",
            &[&job_type as &dyn rusqlite::types::ToSql, &pref_filter as &dyn rusqlite::types::ToSql],
        );
        let grade = match grade_rows {
            Ok(rows) if !rows.is_empty() => rows[0]
                .get("grade")
                .cloned()
                .unwrap_or(Value::Null),
            _ => {
                // フォールバック: 全国
                let fallback = db.query(
                    "SELECT grade FROM layer_b_text_quality WHERE job_type = ?1 AND prefecture = '全国' LIMIT 1",
                    &[&job_type as &dyn rusqlite::types::ToSql],
                );
                match fallback {
                    Ok(rows) if !rows.is_empty() => rows[0].get("grade").cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                }
            }
        };
        summary.insert("text_quality_grade".to_string(), grade);

        // 法人集中度: 総求人数
        let facility_rows = db.query(
            "SELECT total_postings FROM layer_a_facility_concentration
             WHERE job_type = ?1 AND prefecture = ?2
             LIMIT 1",
            &[&job_type as &dyn rusqlite::types::ToSql, &pref_filter as &dyn rusqlite::types::ToSql],
        );
        let total_postings = match facility_rows {
            Ok(rows) if !rows.is_empty() => rows[0]
                .get("total_postings")
                .cloned()
                .unwrap_or(Value::Null),
            _ => {
                // フォールバック: postingsテーブルから直接カウント
                let (wc, ps) = build_postings_where(job_type, prefecture, "");
                let cnt: i64 = db.query_scalar(
                    &format!("SELECT COUNT(*) FROM postings WHERE {}", wc),
                    &ps.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect::<Vec<_>>(),
                ).unwrap_or(0);
                Value::from(cnt)
            }
        };
        summary.insert("facility_total_postings".to_string(), total_postings);

        // 雇用形態多様性: 雇用形態数
        let diversity_rows = db.query(
            "SELECT n_types FROM layer_a_employment_diversity
             WHERE job_type = ?1 AND prefecture = ?2
             LIMIT 1",
            &[&job_type as &dyn rusqlite::types::ToSql, &pref_filter as &dyn rusqlite::types::ToSql],
        );
        let n_types = match diversity_rows {
            Ok(rows) if !rows.is_empty() => rows[0]
                .get("n_types")
                .cloned()
                .unwrap_or(Value::Null),
            _ => Value::Null,
        };
        summary.insert("employment_n_types".to_string(), n_types);

        // 共起パターン数
        let cooccurrence_count: i64 = db
            .query_scalar(
                "SELECT COUNT(*) FROM layer_b_cooccurrence
                 WHERE job_type = ?1 AND prefecture = ?2",
                &[&job_type as &dyn rusqlite::types::ToSql, &pref_filter as &dyn rusqlite::types::ToSql],
            )
            .unwrap_or(0);
        summary.insert(
            "cooccurrence_count".to_string(),
            Value::from(cooccurrence_count),
        );
    }

    // job_type自体もサマリーに含める
    summary.insert("job_type".to_string(), Value::String(job_type.to_string()));

    Ok(summary)
}

// ---------------------------------------------------------------------------
// postings テーブルからの直接クエリ (市区町村対応)
// ---------------------------------------------------------------------------

/// WHERE句とパラメータを構築するヘルパー（市区町村マルチセレクト対応）
fn build_postings_where(job_type: &str, prefecture: &str, municipality: &str) -> (String, Vec<String>) {
    let mut clauses = vec!["job_type = ?1".to_string()];
    let mut params: Vec<String> = vec![job_type.to_string()];

    if !prefecture.is_empty() {
        params.push(prefecture.to_string());
        clauses.push(format!("prefecture = ?{}", params.len()));
    }
    if !municipality.is_empty() {
        let munis: Vec<&str> = municipality.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        if munis.len() == 1 {
            params.push(munis[0].to_string());
            clauses.push(format!("municipality = ?{}", params.len()));
        } else {
            let placeholders: Vec<String> = munis.iter().enumerate().map(|(i, _)| {
                format!("?{}", params.len() + i + 1)
            }).collect();
            for m in &munis {
                params.push(m.to_string());
            }
            clauses.push(format!("municipality IN ({})", placeholders.join(", ")));
        }
    }

    (clauses.join(" AND "), params)
}

/// postingsテーブルから給与統計を直接計算する。
/// salary_min/salary_max それぞれのP25/Median/P75/P90をsalary_type別、employment_type別に返す。
/// 最適化: 1クエリで全データ取得 → Rust内で集計（N+1クエリ問題を解消）
pub fn query_salary_from_postings(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (where_clause, params) = build_postings_where(job_type, prefecture, municipality);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();

    // 1クエリで全行取得
    let sql = format!(
        "SELECT salary_type, employment_type, salary_min, salary_max
         FROM postings
         WHERE {} AND salary_min > 0 AND salary_type IS NOT NULL
         ORDER BY salary_type, employment_type, salary_min",
        where_clause
    );
    let rows = db.query(&sql, &param_refs)?;

    // Rust内でグループ集計
    let mut groups: std::collections::BTreeMap<(String, String), Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();

    for row in &rows {
        let stype = row.get("salary_type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let etype = row.get("employment_type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let smin = row.get("salary_min").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let smax = row.get("salary_max").and_then(|v| v.as_f64()).unwrap_or(0.0);
        groups.entry((stype, etype)).or_default().push((smin, smax));
    }

    let mut results = Vec::new();

    for ((salary_type, emp_type), vals) in &groups {
        let count = vals.len() as i64;
        if count == 0 { continue; }

        // salary_min の集計
        let mut min_vals: Vec<f64> = vals.iter().map(|(smin, _)| *smin).collect();
        min_vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mean_min = min_vals.iter().sum::<f64>() / count as f64;
        let (p25_min, med_min, p75_min, p90_min) = percentiles(&min_vals);

        // salary_max の集計（salary_max > 0 のもののみ）
        let mut max_vals: Vec<f64> = vals.iter().map(|(_, smax)| *smax).filter(|v| *v > 0.0).collect();
        max_vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mean_max = if max_vals.is_empty() { 0.0 } else { max_vals.iter().sum::<f64>() / max_vals.len() as f64 };
        let (p25_max, med_max, p75_max, p90_max) = percentiles(&max_vals);

        let mut row = HashMap::new();
        row.insert("salary_type".to_string(), Value::String(salary_type.clone()));
        row.insert("employment_type".to_string(), Value::String(emp_type.clone()));
        row.insert("count".to_string(), Value::from(count));
        row.insert("mean_min".to_string(), Value::from(mean_min));
        row.insert("p25_min".to_string(), Value::from(p25_min));
        row.insert("median_min".to_string(), Value::from(med_min));
        row.insert("p75_min".to_string(), Value::from(p75_min));
        row.insert("p90_min".to_string(), Value::from(p90_min));
        row.insert("mean_max".to_string(), Value::from(mean_max));
        row.insert("p25_max".to_string(), Value::from(p25_max));
        row.insert("median_max".to_string(), Value::from(med_max));
        row.insert("p75_max".to_string(), Value::from(p75_max));
        row.insert("p90_max".to_string(), Value::from(p90_max));

        results.push(row);
    }

    Ok(results)
}

/// パーセンタイル計算ヘルパー (P25, Median, P75, P90)
fn percentiles(sorted_vals: &[f64]) -> (f64, f64, f64, f64) {
    if sorted_vals.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    let n = sorted_vals.len();
    let p = |pct: f64| -> f64 {
        let idx = (pct / 100.0 * (n as f64 - 1.0)).max(0.0);
        let lo = idx.floor() as usize;
        let hi = idx.ceil() as usize;
        if lo == hi || hi >= n {
            sorted_vals[lo.min(n - 1)]
        } else {
            let frac = idx - lo as f64;
            sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac
        }
    };
    (p(25.0), p(50.0), p(75.0), p(90.0))
}

/// postingsテーブルから法人集中度を直接計算する。
pub fn query_facility_from_postings(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (where_clause, params) = build_postings_where(job_type, prefecture, municipality);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();

    let total: i64 = db.query_scalar(
        &format!("SELECT COUNT(*) FROM postings WHERE {}", where_clause),
        &param_refs,
    ).unwrap_or(0);

    let unique: i64 = db.query_scalar(
        &format!("SELECT COUNT(DISTINCT facility_name) FROM postings WHERE {} AND facility_name IS NOT NULL", where_clause),
        &param_refs,
    ).unwrap_or(0);

    // Top法人
    let top_sql = format!(
        "SELECT facility_name, COUNT(*) as cnt
         FROM postings WHERE {} AND facility_name IS NOT NULL
         GROUP BY facility_name ORDER BY cnt DESC LIMIT 20",
        where_clause
    );
    let top_rows = db.query(&top_sql, &param_refs)?;

    let top1_name = top_rows.first().and_then(|r| r.get("facility_name")).and_then(|v| v.as_str()).unwrap_or("-").to_string();
    let top1_count = top_rows.first().and_then(|r| r.get("cnt")).and_then(|v| v.as_i64()).unwrap_or(0);

    let top1_pct = if total > 0 { top1_count as f64 / total as f64 * 100.0 } else { 0.0 };

    let top5_sum: i64 = top_rows.iter().take(5).map(|r| r.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0)).sum();
    let top10_sum: i64 = top_rows.iter().take(10).map(|r| r.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0)).sum();
    let top20_sum: i64 = top_rows.iter().take(20).map(|r| r.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0)).sum();

    let top5_pct = if total > 0 { top5_sum as f64 / total as f64 * 100.0 } else { 0.0 };
    let top10_pct = if total > 0 { top10_sum as f64 / total as f64 * 100.0 } else { 0.0 };
    let top20_pct = if total > 0 { top20_sum as f64 / total as f64 * 100.0 } else { 0.0 };

    let mut row = HashMap::new();
    row.insert("total_postings".to_string(), Value::from(total));
    row.insert("unique_facilities".to_string(), Value::from(unique));
    row.insert("top1_name".to_string(), Value::String(top1_name));
    row.insert("top1_count".to_string(), Value::from(top1_count));
    row.insert("top1_pct".to_string(), Value::from(top1_pct));
    row.insert("top5_pct".to_string(), Value::from(top5_pct));
    row.insert("top10_pct".to_string(), Value::from(top10_pct));
    row.insert("top20_pct".to_string(), Value::from(top20_pct));
    row.insert("hhi".to_string(), Value::from(0.0));
    row.insert("zipf_exponent".to_string(), Value::from(0.0));

    Ok(vec![row])
}

/// postingsテーブルから雇用形態多様性を直接計算する。
pub fn query_employment_from_postings(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (where_clause, params) = build_postings_where(job_type, prefecture, municipality);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();

    let total: i64 = db.query_scalar(
        &format!("SELECT COUNT(*) FROM postings WHERE {}", where_clause),
        &param_refs,
    ).unwrap_or(0);

    let type_sql = format!(
        "SELECT employment_type, COUNT(*) as cnt
         FROM postings WHERE {} AND employment_type IS NOT NULL
         GROUP BY employment_type ORDER BY cnt DESC",
        where_clause
    );
    let type_rows = db.query(&type_sql, &param_refs)?;

    let n_types = type_rows.len() as i64;
    let dominant = type_rows.first().and_then(|r| r.get("employment_type")).and_then(|v| v.as_str()).unwrap_or("-").to_string();
    let dominant_cnt = type_rows.first().and_then(|r| r.get("cnt")).and_then(|v| v.as_i64()).unwrap_or(0);
    let dominant_pct = if total > 0 { dominant_cnt as f64 / total as f64 * 100.0 } else { 0.0 };

    // Shannon entropy 計算
    let mut entropy = 0.0f64;
    let mut dist_map = serde_json::Map::new();
    for tr in &type_rows {
        let cnt = tr.get("cnt").and_then(|v| v.as_i64()).unwrap_or(0) as f64;
        let name = tr.get("employment_type").and_then(|v| v.as_str()).unwrap_or("-");
        let p = cnt / total as f64;
        if p > 0.0 {
            entropy -= p * p.ln();
        }
        dist_map.insert(name.to_string(), Value::from((p * 100.0 * 10.0).round() / 10.0));
    }
    let max_entropy = if n_types > 0 { (n_types as f64).ln() } else { 1.0 };
    let evenness = if max_entropy > 0.0 { entropy / max_entropy } else { 0.0 };

    let mut row = HashMap::new();
    row.insert("total_postings".to_string(), Value::from(total));
    row.insert("n_types".to_string(), Value::from(n_types));
    row.insert("shannon_entropy".to_string(), Value::from(entropy));
    row.insert("max_entropy".to_string(), Value::from(max_entropy));
    row.insert("evenness".to_string(), Value::from(evenness));
    row.insert("dominant_type".to_string(), Value::String(dominant));
    row.insert("dominant_pct".to_string(), Value::from(dominant_pct));
    row.insert("type_distribution".to_string(), Value::String(serde_json::to_string(&dist_map).unwrap_or_default()));

    Ok(vec![row])
}

/// postingsテーブルから原稿品質を直接計算する。
pub fn query_quality_from_postings(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (where_clause, params) = build_postings_where(job_type, prefecture, municipality);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();

    let sql = format!(
        "SELECT COUNT(*) as count,
                AVG(text_entropy) as entropy_mean,
                AVG(kanji_ratio) as kanji_ratio_mean,
                AVG(content_richness_score) as quality_score_mean,
                AVG(benefits_score) as benefits_score_mean
         FROM postings
         WHERE {} AND text_entropy IS NOT NULL",
        where_clause
    );

    let rows = db.query(&sql, &param_refs)?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let r = &rows[0];
    let entropy_mean = r.get("entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let quality_mean = r.get("quality_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);

    // グレード判定
    let grade = if entropy_mean >= 4.5 && quality_mean >= 15.0 { "A" }
        else if entropy_mean >= 4.0 && quality_mean >= 10.0 { "B" }
        else if entropy_mean >= 3.5 { "C" }
        else { "D" };

    let mut result = rows[0].clone();
    result.insert("grade".to_string(), Value::String(grade.to_string()));
    result.insert("prefecture".to_string(), Value::String(
        if municipality.is_empty() {
            if prefecture.is_empty() { "全国".to_string() } else { prefecture.to_string() }
        } else {
            municipality.to_string()
        }
    ));

    Ok(vec![result])
}

/// postingsテーブルから市区町村別の品質統計を集計する。
/// 指定都道府県群に含まれる全市区町村のAVG(text_entropy), AVG(kanji_ratio)等を返す。
/// HAVING COUNT(*) >= 5 で件数が少なすぎる市区町村は除外。
pub fn query_quality_by_municipality(
    db: &LocalDb,
    job_type: &str,
    prefectures: &[&str],
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefectures.is_empty() {
        return Ok(Vec::new());
    }

    // IN句の動的構築
    let placeholders: Vec<String> = (0..prefectures.len())
        .map(|i| format!("?{}", i + 2))
        .collect();
    let in_clause = placeholders.join(",");

    let sql = format!(
        "SELECT prefecture, municipality, COUNT(*) as count,
                AVG(text_entropy) as entropy_mean,
                AVG(kanji_ratio) as kanji_ratio_mean,
                AVG(content_richness_score) as quality_score_mean,
                AVG(benefits_score) as benefits_score_mean
         FROM postings
         WHERE job_type = ?1 AND prefecture IN ({}) AND text_entropy IS NOT NULL
         GROUP BY prefecture, municipality
         HAVING COUNT(*) >= 5
         ORDER BY entropy_mean DESC",
        in_clause
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    params.push(Box::new(job_type.to_string()));
    for p in prefectures {
        params.push(Box::new(p.to_string()));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let mut rows = db.query(&sql, &param_refs)?;

    // グレード判定を各行に追加
    for row in &mut rows {
        let entropy_mean = row.get("entropy_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let quality_mean = row.get("quality_score_mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let grade = if entropy_mean >= 4.5 && quality_mean >= 15.0 { "A" }
            else if entropy_mean >= 4.0 && quality_mean >= 10.0 { "B" }
            else if entropy_mean >= 3.5 { "C" }
            else { "D" };
        row.insert("grade".to_string(), Value::String(grade.to_string()));
    }

    Ok(rows)
}

/// 2地域の給与統計を同時取得する（地域間比較用）。
pub fn query_salary_stats_compare(
    db: &LocalDb,
    job_type: &str,
    pref1: &str,
    pref2: &str,
) -> Result<(Vec<HashMap<String, Value>>, Vec<HashMap<String, Value>>), String> {
    let rows1 = query_salary_stats(db, job_type, pref1)?;
    let rows2 = query_salary_stats(db, job_type, pref2)?;
    Ok((rows1, rows2))
}

/// 2地域の法人集中度を同時取得する。
pub fn query_facility_compare(
    db: &LocalDb,
    job_type: &str,
    pref1: &str,
    pref2: &str,
) -> Result<(Vec<HashMap<String, Value>>, Vec<HashMap<String, Value>>), String> {
    let rows1 = query_facility_concentration(db, job_type, pref1)?;
    let rows2 = query_facility_concentration(db, job_type, pref2)?;
    Ok((rows1, rows2))
}

/// 2地域の雇用形態多様性を同時取得する。
pub fn query_employment_compare(
    db: &LocalDb,
    job_type: &str,
    pref1: &str,
    pref2: &str,
) -> Result<(Vec<HashMap<String, Value>>, Vec<HashMap<String, Value>>), String> {
    let rows1 = query_employment_diversity(db, job_type, pref1)?;
    let rows2 = query_employment_diversity(db, job_type, pref2)?;
    Ok((rows1, rows2))
}

/// キーワードデータをフォールバック付きで取得する。
/// 指定都道府県にデータがなければ全国データにフォールバック。
pub fn query_keywords_with_fallback(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    layer: Option<&str>,
    limit: Option<i32>,
) -> (Vec<HashMap<String, Value>>, bool) {
    // まず指定都道府県で取得
    let rows = query_keywords(db, job_type, prefecture, layer, limit).unwrap_or_default();
    if !rows.is_empty() {
        return (rows, false);
    }
    // フォールバック: 全国データ
    let fallback = query_keywords(db, job_type, "", layer, limit).unwrap_or_default();
    (fallback, true)
}

/// 共起データをフォールバック付きで取得する。
/// 指定都道府県にデータがなければ全国データにフォールバック。
pub fn query_cooccurrence_with_fallback(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    min_lift: Option<f64>,
) -> (Vec<HashMap<String, Value>>, bool) {
    let rows = query_cooccurrence(db, job_type, prefecture, min_lift).unwrap_or_default();
    if !rows.is_empty() {
        return (rows, false);
    }
    let fallback = query_cooccurrence(db, job_type, "", min_lift).unwrap_or_default();
    (fallback, true)
}

// ===========================================================================
// Layer A/B/C テーブルクエリ — Turso経由 (async)
// ===========================================================================

use super::turso::TursoClient;

// ---------------------------------------------------------------------------
// A-1: 給与統計 (Turso版)
// ---------------------------------------------------------------------------

/// 給与統計データを取得する（Turso非同期版）。
pub async fn query_salary_stats_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        turso.query(
            "SELECT job_type, prefecture, salary_type, employment_type,
                    count, mean, median, p25, p75, p90, std, gini,
                    has_salary_range_pct, salary_range_median
             FROM layer_a_salary_stats
             WHERE job_type = ?1 AND prefecture = '全国'
             ORDER BY salary_type, employment_type",
            &[Value::String(job_type.to_string())],
        ).await
    } else {
        turso.query(
            "SELECT job_type, prefecture, salary_type, employment_type,
                    count, mean, median, p25, p75, p90, std, gini,
                    has_salary_range_pct, salary_range_median
             FROM layer_a_salary_stats
             WHERE job_type = ?1 AND prefecture = ?2
             ORDER BY salary_type, employment_type",
            &[
                Value::String(job_type.to_string()),
                Value::String(prefecture.to_string()),
            ],
        ).await
    }
}

// ---------------------------------------------------------------------------
// A-2: 法人集中度 (Turso版)
// ---------------------------------------------------------------------------

/// 法人集中度データを取得する（Turso非同期版）。
pub async fn query_facility_concentration_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        turso.query(
            "SELECT job_type, prefecture, total_postings, unique_facilities,
                    top1_name, top1_count, top1_pct, top5_pct, top10_pct, top20_pct,
                    hhi, zipf_exponent
             FROM layer_a_facility_concentration
             WHERE job_type = ?1 AND prefecture = '全国'",
            &[Value::String(job_type.to_string())],
        ).await
    } else {
        turso.query(
            "SELECT job_type, prefecture, total_postings, unique_facilities,
                    top1_name, top1_count, top1_pct, top5_pct, top10_pct, top20_pct,
                    hhi, zipf_exponent
             FROM layer_a_facility_concentration
             WHERE job_type = ?1 AND prefecture = ?2",
            &[
                Value::String(job_type.to_string()),
                Value::String(prefecture.to_string()),
            ],
        ).await
    }
}

// ---------------------------------------------------------------------------
// A-3: 雇用形態多様性 (Turso版)
// ---------------------------------------------------------------------------

/// 雇用形態多様性データを取得する（Turso非同期版）。
pub async fn query_employment_diversity_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        turso.query(
            "SELECT job_type, prefecture, total_postings, n_types,
                    shannon_entropy, max_entropy, evenness,
                    dominant_type, dominant_pct, type_distribution
             FROM layer_a_employment_diversity
             WHERE job_type = ?1 AND prefecture = '全国'",
            &[Value::String(job_type.to_string())],
        ).await
    } else {
        turso.query(
            "SELECT job_type, prefecture, total_postings, n_types,
                    shannon_entropy, max_entropy, evenness,
                    dominant_type, dominant_pct, type_distribution
             FROM layer_a_employment_diversity
             WHERE job_type = ?1 AND prefecture = ?2",
            &[
                Value::String(job_type.to_string()),
                Value::String(prefecture.to_string()),
            ],
        ).await
    }
}

// ---------------------------------------------------------------------------
// B-1: キーワード (Turso版)
// ---------------------------------------------------------------------------

/// キーワード分析データを取得する（Turso非同期版）。
pub async fn query_keywords_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    layer: Option<&str>,
    limit: Option<i32>,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let effective_limit = limit.unwrap_or(50);
    let pref_value = if prefecture.is_empty() {
        "全国"
    } else {
        prefecture
    };

    match layer {
        Some(l) => turso.query(
            "SELECT job_type, prefecture, layer, keyword,
                    tfidf_score, doc_freq, doc_freq_pct, rank
             FROM layer_b_keywords
             WHERE job_type = ?1 AND prefecture = ?2 AND layer = ?3
             ORDER BY rank ASC
             LIMIT ?4",
            &[
                Value::String(job_type.to_string()),
                Value::String(pref_value.to_string()),
                Value::String(l.to_string()),
                Value::Number(serde_json::Number::from(effective_limit)),
            ],
        ).await,
        None => turso.query(
            "SELECT job_type, prefecture, layer, keyword,
                    tfidf_score, doc_freq, doc_freq_pct, rank
             FROM layer_b_keywords
             WHERE job_type = ?1 AND prefecture = ?2
             ORDER BY rank ASC
             LIMIT ?3",
            &[
                Value::String(job_type.to_string()),
                Value::String(pref_value.to_string()),
                Value::Number(serde_json::Number::from(effective_limit)),
            ],
        ).await,
    }
}

// ---------------------------------------------------------------------------
// B-2: 条件共起パターン (Turso版)
// ---------------------------------------------------------------------------

/// 条件共起パターンを取得する（Turso非同期版）。
pub async fn query_cooccurrence_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    min_lift: Option<f64>,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let effective_min_lift = min_lift.unwrap_or(1.0);
    let pref_value = if prefecture.is_empty() {
        "全国"
    } else {
        prefecture
    };

    turso.query(
        "SELECT job_type, prefecture, flag_a, flag_b,
                cooccurrence_count, expected_count, lift,
                phi_coefficient, support_pct
         FROM layer_b_cooccurrence
         WHERE job_type = ?1 AND prefecture = ?2 AND lift >= ?3
         ORDER BY lift DESC
         LIMIT 50",
        &[
            Value::String(job_type.to_string()),
            Value::String(pref_value.to_string()),
            Value::Number(serde_json::Number::from_f64(effective_min_lift).unwrap_or(serde_json::Number::from(1))),
        ],
    ).await
}

// ---------------------------------------------------------------------------
// B-3: 原稿品質 (Turso版)
// ---------------------------------------------------------------------------

/// 原稿品質スコアを取得する（Turso非同期版）。
pub async fn query_text_quality_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    if prefecture.is_empty() {
        turso.query(
            "SELECT job_type, prefecture, count,
                    entropy_mean, entropy_median, entropy_std,
                    entropy_p25, entropy_p75,
                    kanji_ratio_mean, kanji_ratio_median, kanji_ratio_std,
                    quality_score_mean, quality_score_median,
                    benefits_score_mean, benefits_score_median,
                    desc_length_mean, desc_length_median, grade
             FROM layer_b_text_quality
             WHERE job_type = ?1
             ORDER BY prefecture",
            &[Value::String(job_type.to_string())],
        ).await
    } else {
        turso.query(
            "SELECT job_type, prefecture, count,
                    entropy_mean, entropy_median, entropy_std,
                    entropy_p25, entropy_p75,
                    kanji_ratio_mean, kanji_ratio_median, kanji_ratio_std,
                    quality_score_mean, quality_score_median,
                    benefits_score_mean, benefits_score_median,
                    desc_length_mean, desc_length_median, grade
             FROM layer_b_text_quality
             WHERE job_type = ?1 AND prefecture = ?2",
            &[
                Value::String(job_type.to_string()),
                Value::String(prefecture.to_string()),
            ],
        ).await
    }
}

// ---------------------------------------------------------------------------
// C-1: クラスタプロファイル (Turso版)
// ---------------------------------------------------------------------------

/// クラスタプロファイルを取得する（Turso非同期版）。
pub async fn query_cluster_profiles_turso(
    turso: &TursoClient,
    job_type: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    turso.query(
        "SELECT job_type, cluster_id, cluster_label, size, size_pct,
                salary_min_mean, salary_min_median,
                text_entropy_mean, benefits_score_mean, content_richness_mean,
                fulltime_pct, has_salary_range_pct,
                top_benefits, dominant_employment, feature_means, description
         FROM layer_c_cluster_profiles
         WHERE job_type = ?1
         ORDER BY cluster_id ASC",
        &[Value::String(job_type.to_string())],
    ).await
}

// ---------------------------------------------------------------------------
// C-2: 地域ヒートマップ (Turso版)
// ---------------------------------------------------------------------------

/// 地域xクラスタのヒートマップデータを取得する（Turso非同期版）。
pub async fn query_region_heatmap_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    cluster_id: Option<i32>,
) -> Result<Vec<HashMap<String, Value>>, String> {
    match (prefecture.is_empty(), cluster_id) {
        // 全都道府県 x 全クラスタ
        (true, None) => turso.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1
             ORDER BY prefecture, cluster_id",
            &[Value::String(job_type.to_string())],
        ).await,
        // 全都道府県 x 特定クラスタ
        (true, Some(cid)) => turso.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1 AND cluster_id = ?2
             ORDER BY prefecture, cluster_id",
            &[
                Value::String(job_type.to_string()),
                Value::Number(serde_json::Number::from(cid)),
            ],
        ).await,
        // 特定都道府県 x 全クラスタ
        (false, None) => turso.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1 AND prefecture = ?2
             ORDER BY prefecture, cluster_id",
            &[
                Value::String(job_type.to_string()),
                Value::String(prefecture.to_string()),
            ],
        ).await,
        // 特定都道府県 x 特定クラスタ
        (false, Some(cid)) => turso.query(
            "SELECT job_type, prefecture, cluster_id, cluster_label,
                    count, pct, national_pct, deviation
             FROM layer_c_region_heatmap
             WHERE job_type = ?1 AND prefecture = ?2 AND cluster_id = ?3
             ORDER BY prefecture, cluster_id",
            &[
                Value::String(job_type.to_string()),
                Value::String(prefecture.to_string()),
                Value::Number(serde_json::Number::from(cid)),
            ],
        ).await,
    }
}

// ---------------------------------------------------------------------------
// A-2: 法人集中度 - 全都道府県取得 (Turso版)
// ---------------------------------------------------------------------------

/// 全都道府県の法人集中度データを取得する（Turso非同期版）。
pub async fn query_facility_all_prefectures_turso(
    turso: &TursoClient,
    job_type: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    turso.query(
        "SELECT job_type, prefecture, total_postings, unique_facilities,
                top1_name, top1_count, top1_pct, top5_pct, top10_pct, top20_pct,
                hhi, zipf_exponent
         FROM layer_a_facility_concentration
         WHERE job_type = ?1
         ORDER BY prefecture",
        &[Value::String(job_type.to_string())],
    ).await
}

// ---------------------------------------------------------------------------
// A-3: 雇用形態多様性 - 全都道府県取得 (Turso版)
// ---------------------------------------------------------------------------

/// 全都道府県の雇用形態多様性データを取得する（Turso非同期版）。
pub async fn query_employment_all_prefectures_turso(
    turso: &TursoClient,
    job_type: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    turso.query(
        "SELECT job_type, prefecture, total_postings, n_types,
                shannon_entropy, max_entropy, evenness,
                dominant_type, dominant_pct, type_distribution
         FROM layer_a_employment_diversity
         WHERE job_type = ?1
         ORDER BY prefecture",
        &[Value::String(job_type.to_string())],
    ).await
}

// ---------------------------------------------------------------------------
// フォールバック付きキーワード取得 (Turso版)
// ---------------------------------------------------------------------------

/// キーワードデータをフォールバック付きで取得する（Turso非同期版）。
/// 指定都道府県にデータがなければ全国データにフォールバック。
pub async fn query_keywords_with_fallback_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    layer: Option<&str>,
    limit: Option<i32>,
) -> (Vec<HashMap<String, Value>>, bool) {
    let rows = query_keywords_turso(turso, job_type, prefecture, layer, limit)
        .await.unwrap_or_default();
    if !rows.is_empty() {
        return (rows, false);
    }
    // フォールバック: 全国データ
    let fallback = query_keywords_turso(turso, job_type, "", layer, limit)
        .await.unwrap_or_default();
    (fallback, true)
}

// ---------------------------------------------------------------------------
// フォールバック付き共起取得 (Turso版)
// ---------------------------------------------------------------------------

/// 共起データをフォールバック付きで取得する（Turso非同期版）。
/// 指定都道府県にデータがなければ全国データにフォールバック。
pub async fn query_cooccurrence_with_fallback_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    min_lift: Option<f64>,
) -> (Vec<HashMap<String, Value>>, bool) {
    let rows = query_cooccurrence_turso(turso, job_type, prefecture, min_lift)
        .await.unwrap_or_default();
    if !rows.is_empty() {
        return (rows, false);
    }
    let fallback = query_cooccurrence_turso(turso, job_type, "", min_lift)
        .await.unwrap_or_default();
    (fallback, true)
}

// ---------------------------------------------------------------------------
// 分析サマリー (Turso版) — layerテーブル部分のみ
// ---------------------------------------------------------------------------

/// 指定job_typeの分析全体サマリーを返す（Turso非同期版）。
/// postings テーブルはローカルDB用なのでここでは layer テーブルのみ集計。
pub async fn query_analysis_summary_turso(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
) -> Result<HashMap<String, Value>, String> {
    let mut summary = HashMap::new();
    let pref_filter = if prefecture.is_empty() { "全国" } else { prefecture };

    // 給与統計レコード数
    let salary_count_val = turso.query_scalar(
        "SELECT COUNT(*) FROM layer_a_salary_stats
         WHERE job_type = ?1 AND prefecture = ?2",
        &[Value::String(job_type.to_string()), Value::String(pref_filter.to_string())],
    ).await.unwrap_or(Value::from(0));
    let salary_count = salary_count_val.as_i64().unwrap_or(0);
    summary.insert("salary_stat_count".to_string(), Value::from(salary_count));

    // クラスタ数
    let cluster_count_val = turso.query_scalar(
        "SELECT COUNT(*) FROM layer_c_cluster_profiles
         WHERE job_type = ?1",
        &[Value::String(job_type.to_string())],
    ).await.unwrap_or(Value::from(0));
    let cluster_count = cluster_count_val.as_i64().unwrap_or(0);
    summary.insert("cluster_count".to_string(), Value::from(cluster_count));

    // キーワード総数
    let keyword_count_val = turso.query_scalar(
        "SELECT COUNT(*) FROM layer_b_keywords
         WHERE job_type = ?1",
        &[Value::String(job_type.to_string())],
    ).await.unwrap_or(Value::from(0));
    let keyword_count = keyword_count_val.as_i64().unwrap_or(0);
    summary.insert("keyword_count".to_string(), Value::from(keyword_count));

    // 原稿品質グレード
    let grade_rows = turso.query(
        "SELECT grade FROM layer_b_text_quality
         WHERE job_type = ?1 AND prefecture = ?2
         LIMIT 1",
        &[Value::String(job_type.to_string()), Value::String(pref_filter.to_string())],
    ).await;
    let grade = match grade_rows {
        Ok(rows) if !rows.is_empty() => rows[0]
            .get("grade")
            .cloned()
            .unwrap_or(Value::Null),
        _ => {
            // フォールバック: 全国
            let fallback = turso.query(
                "SELECT grade FROM layer_b_text_quality WHERE job_type = ?1 AND prefecture = '全国' LIMIT 1",
                &[Value::String(job_type.to_string())],
            ).await;
            match fallback {
                Ok(rows) if !rows.is_empty() => rows[0].get("grade").cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
    };
    summary.insert("text_quality_grade".to_string(), grade);

    // 法人集中度: 総求人数
    let facility_rows = turso.query(
        "SELECT total_postings FROM layer_a_facility_concentration
         WHERE job_type = ?1 AND prefecture = ?2
         LIMIT 1",
        &[Value::String(job_type.to_string()), Value::String(pref_filter.to_string())],
    ).await;
    let total_postings = match facility_rows {
        Ok(rows) if !rows.is_empty() => rows[0]
            .get("total_postings")
            .cloned()
            .unwrap_or(Value::Null),
        _ => Value::Null,
    };
    summary.insert("facility_total_postings".to_string(), total_postings);

    // 雇用形態多様性: 雇用形態数
    let diversity_rows = turso.query(
        "SELECT n_types FROM layer_a_employment_diversity
         WHERE job_type = ?1 AND prefecture = ?2
         LIMIT 1",
        &[Value::String(job_type.to_string()), Value::String(pref_filter.to_string())],
    ).await;
    let n_types = match diversity_rows {
        Ok(rows) if !rows.is_empty() => rows[0]
            .get("n_types")
            .cloned()
            .unwrap_or(Value::Null),
        _ => Value::Null,
    };
    summary.insert("employment_n_types".to_string(), n_types);

    // 共起パターン数
    let cooccurrence_count_val = turso.query_scalar(
        "SELECT COUNT(*) FROM layer_b_cooccurrence
         WHERE job_type = ?1 AND prefecture = ?2",
        &[Value::String(job_type.to_string()), Value::String(pref_filter.to_string())],
    ).await.unwrap_or(Value::from(0));
    let cooccurrence_count = cooccurrence_count_val.as_i64().unwrap_or(0);
    summary.insert("cooccurrence_count".to_string(), Value::from(cooccurrence_count));

    summary.insert("job_type".to_string(), Value::String(job_type.to_string()));

    Ok(summary)
}

// ===========================================================================
// Layer B v2: 6つの問い (6Q) テーブルクエリ — Turso経由 (async)
// ===========================================================================

/// 6Qクエリ共通: prefecture/municipality のデフォルト値処理
fn resolve_6q_scope(prefecture: &str, municipality: &str) -> (String, String) {
    let pref = if prefecture.is_empty() { "全国".to_string() } else { prefecture.to_string() };
    let muni = if pref == "全国" { String::new() } else { municipality.to_string() };
    (pref, muni)
}

/// Q2: 定型文率 + Q1: 差別化シグナルを取得
pub async fn query_text_analysis(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (pref, muni) = resolve_6q_scope(prefecture, municipality);
    turso.query(
        "SELECT t.job_type, t.employment_type, t.prefecture, t.municipality, t.count,
                t.template_ratio_mean, t.template_ratio_median, t.template_ratio_std,
                t.original_length_mean, t.original_length_median,
                t.template_length_mean, t.full_length_mean, t.full_length_median,
                d.diff_total_mean, d.diff_total_median,
                d.diff_facility_mean, d.diff_strength_mean, d.diff_workstyle_mean,
                d.diff_zero_pct
         FROM layer_b_template t
         JOIN layer_b_differentiation d
           ON t.job_type = d.job_type
          AND t.employment_type = d.employment_type
          AND t.prefecture = d.prefecture
          AND t.municipality = d.municipality
         WHERE t.job_type = ?1 AND t.prefecture = ?2 AND t.municipality = ?3
         ORDER BY t.employment_type",
        &[
            Value::String(job_type.to_string()),
            Value::String(pref),
            Value::String(muni),
        ],
    ).await
}

/// Q4: トーン分析を取得
pub async fn query_tone(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (pref, muni) = resolve_6q_scope(prefecture, municipality);
    turso.query(
        "SELECT job_type, employment_type, prefecture, municipality, count,
                urgency_score_mean, urgency_score_median,
                enthusiasm_score_mean, enthusiasm_score_median,
                casual_score_mean, casual_score_median,
                selectivity_score_mean, selectivity_score_median,
                emoji_pct, kaomoji_pct, decorative_pct
         FROM layer_b_tone
         WHERE job_type = ?1 AND prefecture = ?2 AND municipality = ?3
         ORDER BY employment_type",
        &[
            Value::String(job_type.to_string()),
            Value::String(pref),
            Value::String(muni),
        ],
    ).await
}

/// Q5 + Q3: 情報充足度 + 情報開示ギャップを取得
pub async fn query_info_score(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (pref, muni) = resolve_6q_scope(prefecture, municipality);
    turso.query(
        "SELECT s.job_type, s.employment_type, s.prefecture, s.municipality, s.count,
                s.info_score_mean, s.info_score_median, s.info_score_std,
                s.salary_detail_mean, s.work_hours_mean, s.holidays_mean,
                s.job_detail_mean, s.benefits_mean, s.transparency_mean,
                s.grade,
                g.gap_vs_national
         FROM layer_b_info_score s
         JOIN layer_b_info_gap g
           ON s.job_type = g.job_type
          AND s.employment_type = g.employment_type
          AND s.prefecture = g.prefecture
          AND s.municipality = g.municipality
         WHERE s.job_type = ?1 AND s.prefecture = ?2 AND s.municipality = ?3
         ORDER BY s.employment_type",
        &[
            Value::String(job_type.to_string()),
            Value::String(pref),
            Value::String(muni),
        ],
    ).await
}

/// Q6: ターゲティング分析を取得
pub async fn query_targeting(
    turso: &TursoClient,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let (pref, muni) = resolve_6q_scope(prefecture, municipality);
    turso.query(
        "SELECT job_type, employment_type, prefecture, municipality, count,
                demo_age_primary_mode, demo_age_primary_pct,
                demo_exp_primary_mode, demo_exp_primary_pct,
                demo_life_primary_mode, demo_life_primary_pct,
                demo_qual_primary_mode, demo_qual_primary_pct,
                demo_div_primary_mode, demo_div_primary_pct,
                psycho_top1_mode, psycho_top1_pct,
                psycho_growth_mean, psycho_stability_mean,
                psycho_wlb_mean, psycho_contribution_mean,
                psycho_autonomy_mean, psycho_belonging_mean,
                psycho_income_mean, psycho_convenience_mean,
                psycho_environment_mean,
                psycho_load_aversion_mean, psycho_rationality_mean,
                psycho_vision_mean
         FROM layer_b_targeting
         WHERE job_type = ?1 AND prefecture = ?2 AND municipality = ?3
         ORDER BY employment_type",
        &[
            Value::String(job_type.to_string()),
            Value::String(pref),
            Value::String(muni),
        ],
    ).await
}

/// 全職種のターゲティング比較（全国・全体のみ）
pub async fn query_targeting_comparison(
    turso: &TursoClient,
) -> Result<Vec<HashMap<String, Value>>, String> {
    turso.query(
        "SELECT job_type,
                demo_age_primary_mode, demo_exp_primary_mode,
                psycho_top1_mode,
                psycho_growth_mean, psycho_stability_mean,
                psycho_wlb_mean, psycho_contribution_mean,
                psycho_autonomy_mean, psycho_belonging_mean,
                psycho_income_mean, psycho_convenience_mean,
                psycho_environment_mean,
                psycho_load_aversion_mean, psycho_rationality_mean,
                psycho_vision_mean
         FROM layer_b_targeting
         WHERE prefecture = '全国' AND municipality = '' AND employment_type = '全体'
         ORDER BY job_type",
        &[],
    ).await
}
