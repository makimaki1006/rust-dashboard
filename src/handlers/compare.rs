use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::job_seeker::{has_turso_data, job_type_names};
use crate::AppState;
use super::overview::{get_str, get_i64, get_f64, format_number};

#[derive(Deserialize)]
pub struct CompareParams {
    pub job_a: Option<String>,
    pub job_b: Option<String>,
}

struct JobStats {
    name: String,
    total: i64,
    male: i64,
    female: i64,
    avg_age: f64,
    female_ratio: f64,
    avg_qualifications: f64,
    avg_desired_areas: f64,
    age_dist: Vec<(String, i64)>,
}

impl Default for JobStats {
    fn default() -> Self {
        Self {
            name: String::new(),
            total: 0, male: 0, female: 0,
            avg_age: 0.0, female_ratio: 0.0,
            avg_qualifications: 0.0, avg_desired_areas: 0.0,
            age_dist: Vec::new(),
        }
    }
}

async fn fetch_job_stats(state: &AppState, job_type: &str) -> JobStats {
    let mut stats = JobStats { name: job_type.to_string(), ..Default::default() };

    if !has_turso_data(job_type) {
        return stats;
    }

    let sql = "SELECT male_count, female_count, avg_age, avg_qualifications, avg_desired_areas \
               FROM job_seeker_data \
               WHERE job_type = ? AND row_type = 'SUMMARY' AND municipality != ''";
    let params = vec![Value::String(job_type.to_string())];

    if let Ok(rows) = state.turso.query(sql, &params).await {
        let mut age_sum = 0.0;
        let mut age_count = 0.0;
        let mut qual_sum = 0.0;
        let mut desired_sum = 0.0;

        for row in &rows {
            let m = get_i64(row, "male_count");
            let f = get_i64(row, "female_count");
            stats.male += m;
            stats.female += f;
            let total = m + f;
            let avg_age = get_f64(row, "avg_age");
            if avg_age > 0.0 && total > 0 {
                age_sum += avg_age * total as f64;
                age_count += total as f64;
            }
            qual_sum += get_f64(row, "avg_qualifications") * total as f64;
            desired_sum += get_f64(row, "avg_desired_areas") * total as f64;
        }
        stats.total = stats.male + stats.female;
        if age_count > 0.0 {
            stats.avg_age = age_sum / age_count;
        }
        if stats.total > 0 {
            stats.female_ratio = stats.female as f64 / stats.total as f64 * 100.0;
            stats.avg_qualifications = qual_sum / stats.total as f64;
            stats.avg_desired_areas = desired_sum / stats.total as f64;
        }
    }

    // 年齢分布
    let ag_sql = "SELECT category1, SUM(CAST(count AS INTEGER)) as cnt \
                  FROM job_seeker_data \
                  WHERE job_type = ? AND row_type = 'AGE_GENDER' \
                  GROUP BY category1 ORDER BY category1";
    if let Ok(rows) = state.turso.query(ag_sql, &params).await {
        let age_order = ["20代", "30代", "40代", "50代", "60代", "70歳以上"];
        let mut age_map: HashMap<String, i64> = HashMap::new();
        for row in &rows {
            let ag = get_str(row, "category1");
            let cnt = get_i64(row, "cnt");
            *age_map.entry(ag).or_insert(0) += cnt;
        }
        for ag in &age_order {
            stats.age_dist.push((ag.to_string(), *age_map.get(*ag).unwrap_or(&0)));
        }
    }

    stats
}

fn render_kpi(label: &str, val_a: &str, val_b: &str) -> String {
    format!(
        r#"<tr>
            <td class="px-4 py-3 text-sm text-slate-300">{}</td>
            <td class="px-4 py-3 text-right text-sm font-bold text-blue-400">{}</td>
            <td class="px-4 py-3 text-right text-sm font-bold text-amber-400">{}</td>
        </tr>"#,
        label, val_a, val_b
    )
}

pub async fn tab_compare(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CompareParams>,
) -> Html<String> {
    let job_types = job_type_names();
    let job_a = params.job_a.unwrap_or_else(|| "介護職".to_string());
    let job_b = params.job_b.unwrap_or_else(|| "看護師".to_string());

    // 職種セレクターHTML生成
    let make_options = |selected: &str, id: &str| -> String {
        let opts: Vec<String> = job_types.iter()
            .filter(|jt| has_turso_data(jt))
            .map(|jt| {
                let sel = if *jt == selected { " selected" } else { "" };
                format!(r#"<option value="{}"{}>{}</option>"#, jt, sel, jt)
            })
            .collect();
        format!(
            r#"<select id="{}" name="{}" class="bg-slate-700 text-white px-3 py-2 rounded-lg text-sm border border-slate-600"
                hx-get="/tab/compare" hx-target="#content" hx-swap="innerHTML" hx-include="[name='job_a'],[name='job_b']">{}</select>"#,
            id, id, opts.join("")
        )
    };

    let selector_a = make_options(&job_a, "job_a");
    let selector_b = make_options(&job_b, "job_b");

    // 両職種のデータ取得（並列）
    let (stats_a, stats_b) = tokio::join!(
        fetch_job_stats(&state, &job_a),
        fetch_job_stats(&state, &job_b)
    );

    // KPI比較テーブル
    let kpi_rows = vec![
        render_kpi("求職者数", &format!("{}人", format_number(stats_a.total)), &format!("{}人", format_number(stats_b.total))),
        render_kpi("平均年齢", &format!("{:.1}歳", stats_a.avg_age), &format!("{:.1}歳", stats_b.avg_age)),
        render_kpi("女性比率", &format!("{:.1}%", stats_a.female_ratio), &format!("{:.1}%", stats_b.female_ratio)),
        render_kpi("平均資格数", &format!("{:.1}", stats_a.avg_qualifications), &format!("{:.1}", stats_b.avg_qualifications)),
        render_kpi("平均希望勤務地数", &format!("{:.1}", stats_a.avg_desired_areas), &format!("{:.1}", stats_b.avg_desired_areas)),
        render_kpi("男性", &format!("{}人", format_number(stats_a.male)), &format!("{}人", format_number(stats_b.male))),
        render_kpi("女性", &format!("{}人", format_number(stats_a.female)), &format!("{}人", format_number(stats_b.female))),
    ];

    // 年齢分布チャートデータ
    let age_labels: Vec<String> = stats_a.age_dist.iter().map(|(ag, _)| format!("'{}'", ag)).collect();
    let age_data_a: Vec<String> = stats_a.age_dist.iter().map(|(_, cnt)| {
        if stats_a.total > 0 { format!("{:.1}", *cnt as f64 / stats_a.total as f64 * 100.0) } else { "0".to_string() }
    }).collect();
    let age_data_b: Vec<String> = stats_b.age_dist.iter().map(|(_, cnt)| {
        if stats_b.total > 0 { format!("{:.1}", *cnt as f64 / stats_b.total as f64 * 100.0) } else { "0".to_string() }
    }).collect();

    let html = format!(
        r##"<div class="space-y-6">
    <h2 class="text-xl font-bold text-white">⚖️ 職種比較</h2>

    <div class="flex items-center gap-4 flex-wrap">
        <div class="flex items-center gap-2">
            <span class="text-blue-400 font-bold">A:</span>
            {selector_a}
        </div>
        <span class="text-slate-400 text-lg">vs</span>
        <div class="flex items-center gap-2">
            <span class="text-amber-400 font-bold">B:</span>
            {selector_b}
        </div>
    </div>

    <div class="bg-slate-800/50 rounded-xl p-4 border border-slate-700">
        <h3 class="text-lg font-bold text-white mb-3">KPI比較</h3>
        <table class="w-full">
            <thead>
                <tr class="border-b border-slate-700">
                    <th class="px-4 py-2 text-left text-xs text-slate-400">指標</th>
                    <th class="px-4 py-2 text-right text-xs text-blue-400">{job_a}</th>
                    <th class="px-4 py-2 text-right text-xs text-amber-400">{job_b}</th>
                </tr>
            </thead>
            <tbody>{kpi_table}</tbody>
        </table>
    </div>

    <div class="bg-slate-800/50 rounded-xl p-4 border border-slate-700">
        <h3 class="text-lg font-bold text-white mb-3">年齢分布比較（%）</h3>
        <canvas id="ageCompareChart" height="200"></canvas>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script>
new Chart(document.getElementById('ageCompareChart'), {{
    type: 'bar',
    data: {{
        labels: [{labels}],
        datasets: [
            {{ label: '{job_a}', data: [{data_a}], backgroundColor: 'rgba(96,165,250,0.7)', borderColor: '#60A5FA', borderWidth: 1 }},
            {{ label: '{job_b}', data: [{data_b}], backgroundColor: 'rgba(251,191,36,0.7)', borderColor: '#FBBf24', borderWidth: 1 }}
        ]
    }},
    options: {{
        responsive: true,
        scales: {{
            y: {{ beginAtZero: true, ticks: {{ color: '#94A3B8', callback: v => v + '%' }}, grid: {{ color: 'rgba(255,255,255,0.05)' }} }},
            x: {{ ticks: {{ color: '#94A3B8' }}, grid: {{ display: false }} }}
        }},
        plugins: {{
            legend: {{ labels: {{ color: '#E2E8F0' }} }},
            tooltip: {{ callbacks: {{ label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + '%' }} }}
        }}
    }}
}});
</script>"##,
        selector_a = selector_a,
        selector_b = selector_b,
        job_a = job_a,
        job_b = job_b,
        kpi_table = kpi_rows.join("\n"),
        labels = age_labels.join(","),
        data_a = age_data_a.join(","),
        data_b = age_data_b.join(","),
    );

    Html(html)
}
