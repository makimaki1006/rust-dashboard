use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;
use crate::handlers::competitive::escape_html;
use crate::handlers::overview::get_session_filters;

#[derive(Deserialize)]
pub struct RegionParams {
    #[serde(default)]
    pub prefecture: String,
    #[serde(default)]
    pub municipality: String,
}

// --- 1. 地域サマリー（TursoDB SUMMARY行） ---

pub async fn region_summary(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<RegionParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    if params.prefecture.is_empty() || params.municipality.is_empty() {
        return Html(r#"<p class="text-gray-400 text-xs">地域を選択してください</p>"#.to_string());
    }

    let sql = "SELECT applicant_count, avg_age, male_count, female_count, avg_qualifications, avg_reference_distance_km FROM job_seeker_data WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture = ? AND municipality = ?";
    let turso_params = vec![
        serde_json::Value::String(job_type.clone()),
        serde_json::Value::String(params.prefecture.clone()),
        serde_json::Value::String(params.municipality.clone()),
    ];

    let rows = match state.turso.query(sql, &turso_params).await {
        Ok(r) => r,
        Err(_) => {
            return Html(r#"<p class="text-gray-400 text-xs">データ取得エラー</p>"#.to_string());
        }
    };

    if rows.is_empty() {
        return Html(format!(
            r#"<p class="text-gray-400 text-xs">{}の{}データがありません</p>"#,
            escape_html(&params.municipality),
            escape_html(&job_type)
        ));
    }

    let row = &rows[0];
    let applicant_count = get_i64(row, "applicant_count");
    let avg_age = get_f64(row, "avg_age");
    let male_count = get_i64(row, "male_count");
    let female_count = get_i64(row, "female_count");
    let avg_qual = get_f64(row, "avg_qualifications");
    let avg_dist = get_f64(row, "avg_reference_distance_km");

    let total = male_count + female_count;
    let female_ratio = if total > 0 {
        (female_count as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let html = format!(
        r#"<div class="grid grid-cols-2 gap-2 text-xs">
  <div class="bg-gray-700/50 rounded p-2 text-center">
    <div class="text-gray-400">応募者数</div>
    <div class="text-lg font-bold text-blue-300">{}</div>
  </div>
  <div class="bg-gray-700/50 rounded p-2 text-center">
    <div class="text-gray-400">平均年齢</div>
    <div class="text-lg font-bold text-yellow-300">{:.1}歳</div>
  </div>
  <div class="bg-gray-700/50 rounded p-2 text-center">
    <div class="text-gray-400">女性比率</div>
    <div class="text-lg font-bold text-pink-300">{:.0}%</div>
  </div>
  <div class="bg-gray-700/50 rounded p-2 text-center">
    <div class="text-gray-400">平均資格数</div>
    <div class="text-lg font-bold text-green-300">{:.1}</div>
  </div>
  <div class="bg-gray-700/50 rounded p-2 col-span-2 text-center">
    <div class="text-gray-400">平均移動距離</div>
    <div class="text-lg font-bold text-purple-300">{:.1} km</div>
  </div>
</div>"#,
        applicant_count, avg_age, female_ratio, avg_qual, avg_dist
    );

    Html(html)
}

// --- 2. 年齢×性別（TursoDB AGE_GENDER行） ---

pub async fn region_age_gender(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<RegionParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    if params.prefecture.is_empty() || params.municipality.is_empty() {
        return Html(r#"<p class="text-gray-400 text-xs">地域を選択してください</p>"#.to_string());
    }

    let sql = "SELECT category1, male_count, female_count FROM job_seeker_data WHERE job_type = ? AND row_type = 'AGE_GENDER' AND prefecture = ? AND municipality = ? ORDER BY category1";
    let turso_params = vec![
        serde_json::Value::String(job_type.clone()),
        serde_json::Value::String(params.prefecture.clone()),
        serde_json::Value::String(params.municipality.clone()),
    ];

    let rows = match state.turso.query(sql, &turso_params).await {
        Ok(r) => r,
        Err(_) => {
            return Html(r#"<p class="text-gray-400 text-xs">データ取得エラー</p>"#.to_string());
        }
    };

    if rows.is_empty() {
        return Html(r#"<p class="text-gray-400 text-xs">年齢性別データなし</p>"#.to_string());
    }

    // EChartsのデータ構築
    let mut categories = Vec::new();
    let mut male_data = Vec::new();
    let mut female_data = Vec::new();

    for row in &rows {
        let cat = get_str(row, "category1");
        let male = get_i64(row, "male_count");
        let female = get_i64(row, "female_count");
        categories.push(cat);
        // 男性は左（負の値）で表示
        male_data.push(-male);
        female_data.push(female);
    }

    let cats_json = categories
        .iter()
        .map(|c| format!("'{}'", escape_html(c)))
        .collect::<Vec<_>>()
        .join(",");
    let male_json = male_data
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let female_json = female_data
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");

    // ユニークIDを生成（ECharts描画用）
    let chart_id = format!("region-age-gender-{}", rand_id());

    let html = format!(
        r#"<div id="{chart_id}" style="width:100%;height:220px;"></div>
<script>
(function(){{
  var el = document.getElementById('{chart_id}');
  if (!el || typeof echarts === 'undefined') return;
  var ch = echarts.init(el, 'dark');
  ch.setOption({{
    tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }},
      formatter: function(p) {{
        var s = p[0].name + '<br>';
        p.forEach(function(i) {{
          s += i.marker + i.seriesName + ': ' + Math.abs(i.value) + '人<br>';
        }});
        return s;
      }}
    }},
    legend: {{ data: ['男性','女性'], textStyle: {{ fontSize: 10 }} }},
    grid: {{ left: 60, right: 20, top: 30, bottom: 20 }},
    xAxis: {{ type: 'value',
      axisLabel: {{ formatter: function(v){{ return Math.abs(v); }} }}
    }},
    yAxis: {{ type: 'category', data: [{cats_json}], axisTick: {{ show: false }} }},
    series: [
      {{ name: '男性', type: 'bar', stack: 'pop', data: [{male_json}],
         itemStyle: {{ color: '#3b82f6' }}, barWidth: '60%' }},
      {{ name: '女性', type: 'bar', stack: 'pop', data: [{female_json}],
         itemStyle: {{ color: '#ec4899' }}, barWidth: '60%' }}
    ]
  }});
  new ResizeObserver(function(){{ ch.resize(); }}).observe(el);
}})();
</script>"#
    );

    Html(html)
}

// --- 3. 求人統計（geocoded_db postingsテーブル） ---

pub async fn region_posting_stats(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<RegionParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => {
            return Html(r#"<p class="text-gray-400 text-xs">求人DBなし</p>"#.to_string());
        }
    };

    if params.prefecture.is_empty() || params.municipality.is_empty() {
        return Html(r#"<p class="text-gray-400 text-xs">地域を選択してください</p>"#.to_string());
    }

    let jt = job_type.clone();
    let pref = params.prefecture.clone();
    let muni = params.municipality.clone();

    let mut html = String::with_capacity(2048);

    // 雇用形態別件数
    let emp_sql = "SELECT employment_type, COUNT(*) as cnt FROM postings WHERE job_type = ?1 AND prefecture = ?2 AND municipality = ?3 GROUP BY employment_type ORDER BY cnt DESC";
    let emp_rows = geocoded_db.query(
        emp_sql,
        &[&jt as &dyn rusqlite::types::ToSql, &pref, &muni],
    );

    html.push_str(r#"<div class="space-y-3 text-xs">"#);

    // 雇用形態テーブル
    html.push_str(r#"<div><div class="text-gray-400 mb-1 font-medium">雇用形態</div>"#);
    html.push_str(r#"<table class="w-full"><tbody>"#);
    if let Ok(rows) = &emp_rows {
        for row in rows {
            let emp = get_str(row, "employment_type");
            let cnt = get_i64(row, "cnt");
            html.push_str(&format!(
                r#"<tr><td class="text-gray-300 py-0.5">{}</td><td class="text-right text-white font-medium">{}件</td></tr>"#,
                escape_html(&emp),
                cnt
            ));
        }
    }
    html.push_str("</tbody></table></div>");

    // 給与統計
    let salary_sql = "SELECT salary_type, AVG(salary_min) as avg_min, AVG(salary_max) as avg_max, MIN(salary_min) as min_min, MAX(salary_max) as max_max, COUNT(*) as cnt FROM postings WHERE job_type = ?1 AND prefecture = ?2 AND municipality = ?3 AND salary_min > 0 GROUP BY salary_type";
    let salary_rows = geocoded_db.query(
        salary_sql,
        &[&jt as &dyn rusqlite::types::ToSql, &pref, &muni],
    );

    html.push_str(r#"<div><div class="text-gray-400 mb-1 font-medium">給与レンジ</div>"#);
    html.push_str(r#"<table class="w-full"><thead><tr class="text-gray-500"><th class="text-left">区分</th><th class="text-right">平均下限</th><th class="text-right">平均上限</th><th class="text-right">件</th></tr></thead><tbody>"#);
    if let Ok(rows) = &salary_rows {
        for row in rows {
            let st = get_str(row, "salary_type");
            let avg_min = get_f64(row, "avg_min");
            let avg_max = get_f64(row, "avg_max");
            let cnt = get_i64(row, "cnt");
            html.push_str(&format!(
                r#"<tr><td class="text-gray-300 py-0.5">{}</td><td class="text-right text-yellow-300">{}</td><td class="text-right text-yellow-300">{}</td><td class="text-right text-white">{}</td></tr>"#,
                escape_html(&st),
                format_yen(avg_min as i64),
                format_yen(avg_max as i64),
                cnt
            ));
        }
    }
    html.push_str("</tbody></table></div>");

    // サービス形態TOP5
    let svc_sql = "SELECT service_type, COUNT(*) as cnt FROM postings WHERE job_type = ?1 AND prefecture = ?2 AND municipality = ?3 AND service_type != '' GROUP BY service_type ORDER BY cnt DESC LIMIT 5";
    let svc_rows = geocoded_db.query(
        svc_sql,
        &[&jt as &dyn rusqlite::types::ToSql, &pref, &muni],
    );

    html.push_str(r#"<div><div class="text-gray-400 mb-1 font-medium">サービス形態 TOP5</div>"#);
    if let Ok(rows) = &svc_rows {
        let max_cnt = rows.first().map(|r| get_i64(r, "cnt")).unwrap_or(1).max(1);
        for row in rows {
            let svc = get_str(row, "service_type");
            let cnt = get_i64(row, "cnt");
            let pct = (cnt as f64 / max_cnt as f64) * 100.0;
            html.push_str(&format!(
                r#"<div class="flex items-center gap-2 py-0.5">
  <span class="text-gray-300 w-32 truncate" title="{full}">{label}</span>
  <div class="flex-1 bg-gray-700 rounded h-3">
    <div class="bg-blue-500 rounded h-3" style="width:{pct:.0}%"></div>
  </div>
  <span class="text-white w-8 text-right">{cnt}</span>
</div>"#,
                full = escape_html(&svc),
                label = escape_html(&truncate(&svc, 16)),
                pct = pct,
                cnt = cnt
            ));
        }
    }
    html.push_str("</div>");

    html.push_str("</div>");

    Html(html)
}

// --- 4. セグメント分析（geocoded_db postingsテーブル） ---

pub async fn region_segments(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<RegionParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => {
            return Html(r#"<p class="text-gray-400 text-xs">求人DBなし</p>"#.to_string());
        }
    };

    if params.prefecture.is_empty() || params.municipality.is_empty() {
        return Html(r#"<p class="text-gray-400 text-xs">地域を選択してください</p>"#.to_string());
    }

    let jt = job_type.clone();
    let pref = params.prefecture.clone();
    let muni = params.municipality.clone();

    let mut html = String::with_capacity(2048);
    html.push_str(r#"<div class="space-y-3 text-xs">"#);

    // Tier3セグメント分布TOP10
    let tier3_sql = "SELECT tier3_label_short, COUNT(*) as cnt FROM postings WHERE job_type = ?1 AND prefecture = ?2 AND municipality = ?3 AND tier3_label_short != '' GROUP BY tier3_label_short ORDER BY cnt DESC LIMIT 10";
    let tier3_rows = geocoded_db.query(
        tier3_sql,
        &[&jt as &dyn rusqlite::types::ToSql, &pref, &muni],
    );

    html.push_str(r#"<div><div class="text-gray-400 mb-1 font-medium">求人セグメント TOP10</div>"#);
    if let Ok(rows) = &tier3_rows {
        if rows.is_empty() {
            html.push_str(r#"<p class="text-gray-500">データなし</p>"#);
        } else {
            let max_cnt = rows.first().map(|r| get_i64(r, "cnt")).unwrap_or(1).max(1);
            for row in rows {
                let label = get_str(row, "tier3_label_short");
                let cnt = get_i64(row, "cnt");
                let pct = (cnt as f64 / max_cnt as f64) * 100.0;
                html.push_str(&format!(
                    r#"<div class="flex items-center gap-2 py-0.5">
  <span class="text-gray-300 w-36 truncate" title="{full}">{short}</span>
  <div class="flex-1 bg-gray-700 rounded h-3">
    <div class="bg-emerald-500 rounded h-3" style="width:{pct:.0}%"></div>
  </div>
  <span class="text-white w-8 text-right">{cnt}</span>
</div>"#,
                    full = escape_html(&label),
                    short = escape_html(&truncate(&label, 20)),
                    pct = pct,
                    cnt = cnt
                ));
            }
        }
    }
    html.push_str("</div>");

    // 経験・資格セグメント分布TOP10
    let eq_sql = "SELECT exp_qual_segment, COUNT(*) as cnt FROM postings WHERE job_type = ?1 AND prefecture = ?2 AND municipality = ?3 AND exp_qual_segment != '' GROUP BY exp_qual_segment ORDER BY cnt DESC LIMIT 10";
    let eq_rows = geocoded_db.query(
        eq_sql,
        &[&jt as &dyn rusqlite::types::ToSql, &pref, &muni],
    );

    html.push_str(r#"<div><div class="text-gray-400 mb-1 font-medium">経験・資格セグメント</div>"#);
    if let Ok(rows) = &eq_rows {
        if rows.is_empty() {
            html.push_str(r#"<p class="text-gray-500">データなし</p>"#);
        } else {
            let max_cnt = rows.first().map(|r| get_i64(r, "cnt")).unwrap_or(1).max(1);
            for row in rows {
                let label = get_str(row, "exp_qual_segment");
                let cnt = get_i64(row, "cnt");
                let pct = (cnt as f64 / max_cnt as f64) * 100.0;
                html.push_str(&format!(
                    r#"<div class="flex items-center gap-2 py-0.5">
  <span class="text-gray-300 w-36 truncate" title="{full}">{short}</span>
  <div class="flex-1 bg-gray-700 rounded h-3">
    <div class="bg-amber-500 rounded h-3" style="width:{pct:.0}%"></div>
  </div>
  <span class="text-white w-8 text-right">{cnt}</span>
</div>"#,
                    full = escape_html(&label),
                    short = escape_html(&truncate(&label, 20)),
                    pct = pct,
                    cnt = cnt
                ));
            }
        }
    }
    html.push_str("</div>");

    html.push_str("</div>");

    Html(html)
}

// --- ヘルパー ---

fn get_i64(row: &HashMap<String, serde_json::Value>, key: &str) -> i64 {
    row.get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

fn get_f64(row: &HashMap<String, serde_json::Value>, key: &str) -> f64 {
    row.get(key)
        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
        .unwrap_or(0.0)
}

fn get_str(row: &HashMap<String, serde_json::Value>, key: &str) -> String {
    row.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn format_yen(n: i64) -> String {
    if n == 0 {
        return "\u{2212}".to_string(); // −
    }
    // 3桁区切り
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    format!("\u{00a5}{}", result.chars().rev().collect::<String>())
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars - 1).collect();
        format!("{}…", truncated)
    }
}

/// 簡易ユニークID生成（チャート要素の衝突回避用）
fn rand_id() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    t % 1_000_000
}
