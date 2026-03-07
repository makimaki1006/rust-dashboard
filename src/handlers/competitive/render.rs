use axum::response::Html;

use crate::handlers::overview::format_number;
use super::analysis::AnalysisData;
use super::fetch::{CompStats, PostingRow, SalaryStats};
use super::utils::{
    escape_html, extract_major_category, major_category_color, major_category_short_label,
    truncate_str,
};

pub(crate) fn render_competitive(job_type: &str, stats: &CompStats, pref_options: &[String], ftype_options: &[(String, i64)], stype_options: &[(String, i64)]) -> String {
    let pref_labels: Vec<String> = stats.pref_ranking.iter().map(|(p, _)| format!("\"{}\"", p)).collect();
    let pref_values: Vec<String> = stats.pref_ranking.iter().map(|(_, v)| v.to_string()).collect();

    let pref_rows: String = stats
        .pref_ranking
        .iter()
        .enumerate()
        .map(|(i, (name, cnt))| {
            format!(
                r#"<tr><td class="text-center">{}</td><td>{}</td><td class="text-right">{}</td></tr>"#,
                i + 1, name, format_number(*cnt)
            )
        })
        .collect();

    let pref_option_html: String = pref_options
        .iter()
        .map(|p| format!(r#"<option value="{p}">{p}</option>"#))
        .collect::<Vec<_>>()
        .join("\n");

    // 初期表示: 大カテゴリのみの簡易チェックボックス（都道府県選択で2階層に更新される）
    let ftype_checkbox_html: String = ftype_options
        .iter()
        .map(|(cat, cnt)| {
            let short = major_category_short_label(cat);
            let color = major_category_color(cat);
            format!(
                r#"<div class="ftype-group mb-1"><div class="flex items-center gap-1 py-1 px-1 hover:bg-slate-700 rounded cursor-pointer"><span class="w-4"></span><label class="flex items-center gap-2 text-sm text-white flex-1 cursor-pointer"><input type="checkbox" class="ftype-major-cb rounded" value="{}" onchange="onMajorToggle(this)"><span class="inline-block w-2 h-2 rounded-full flex-shrink-0" style="background:{}"></span><span class="flex-1">{}</span><span class="text-xs text-slate-400 flex-shrink-0">{}</span></label></div></div>"#,
                escape_html(cat), color, escape_html(short), format_number(*cnt)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    // 事業形態オプション
    let stype_option_html: String = stype_options
        .iter()
        .map(|(cat, cnt)| {
            format!(
                r#"<option value="{}">{} ({})</option>"#,
                escape_html(cat), escape_html(cat), format_number(*cnt)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    include_str!("../../../templates/tabs/competitive.html")
        .replace("{{JOB_TYPE}}", &escape_html(job_type))
        .replace("{{TOTAL_POSTINGS}}", &format_number(stats.total_postings))
        .replace("{{TOTAL_FACILITIES}}", &format_number(stats.total_facilities))
        .replace("{{PREF_LABELS}}", &format!("[{}]", pref_labels.join(",")))
        .replace("{{PREF_VALUES}}", &format!("[{}]", pref_values.join(",")))
        .replace("{{PREF_ROWS}}", &pref_rows)
        .replace("{{PREF_OPTIONS}}", &pref_option_html)
        .replace("{{FTYPE_CHECKBOXES}}", &ftype_checkbox_html)
        .replace("{{STYPE_OPTIONS}}", &stype_option_html)
}

/// 求人一覧テーブル（HTMXパーシャル）
pub(crate) fn render_posting_table(
    _job_type: &str,
    pref: &str,
    muni: &str,
    postings: &[PostingRow],
    stats: &SalaryStats,
    page: i64,
    total_pages: i64,
    total: i64,
    nearby: bool,
    radius_km: f64,
    emp: &str,
    ftype: &str,
    stype: &str,
) -> Html<String> {
    let show_distance = nearby && postings.iter().any(|p| p.distance_km.is_some());

    let mut html = String::new();

    // 統計サマリー
    let nearby_label = if nearby { format!("（半径{}km）", radius_km) } else { String::new() };
    if stats.has_data {
        html.push_str(&format!(
            r#"<div class="stat-card mb-4">
                <h3 class="text-sm text-slate-400 mb-2">給与統計（{} {}{} / {}件）</h3>
                <div class="overflow-x-auto">
                <table class="data-table text-xs">
                    <thead><tr><th></th><th class="text-right">月給下限</th><th class="text-right">月給上限</th></tr></thead>
                    <tbody>
                        <tr><td class="text-slate-300">最頻値（1万円単位）</td><td class="text-right">{}</td><td class="text-right">{}</td></tr>
                        <tr><td class="text-slate-300">中央値</td><td class="text-right">{}</td><td class="text-right">{}</td></tr>
                        <tr><td class="text-slate-300">平均値</td><td class="text-right">{}</td><td class="text-right">{}</td></tr>
                    </tbody>
                </table>
                </div>
                <div class="mt-2 text-xs text-slate-400">
                    賞与あり率: {} ｜ 平均年間休日: {}
                </div>
            </div>"#,
            pref, muni, &nearby_label,
            total,
            stats.salary_min_mode, stats.salary_max_mode,
            stats.salary_min_median, stats.salary_max_median,
            stats.salary_min_avg, stats.salary_max_avg,
            stats.bonus_rate, stats.avg_holidays,
        ));
    }

    // ページ情報
    html.push_str(&format!(
        r#"<div class="flex justify-between items-center mb-2">
            <span class="text-sm text-slate-400">全{}件中 {}〜{}件</span>
            <a href="/api/report?prefecture={}&municipality={}&employment_type={}&facility_type={}&service_type={}&nearby={}&radius_km={}"
               target="_blank"
               class="px-3 py-1.5 bg-amber-600 hover:bg-amber-500 text-white text-sm rounded-lg transition">
               HTMLレポート出力
            </a>
        </div>"#,
        total,
        (page - 1) * 50 + 1,
        ((page - 1) * 50 + postings.len() as i64).min(total),
        urlencoding::encode(pref),
        urlencoding::encode(muni),
        urlencoding::encode(emp),
        urlencoding::encode(ftype),
        urlencoding::encode(stype),
        nearby,
        radius_km,
    ));

    // テーブル
    html.push_str(r#"<div class="overflow-x-auto"><table class="data-table text-xs">"#);
    html.push_str("<thead><tr>");
    html.push_str(r#"<th class="text-center" style="width:30px">#</th>"#);
    html.push_str("<th>法人・施設名</th>");
    html.push_str("<th>施設形態</th>");
    html.push_str("<th>エリア</th>");
    html.push_str("<th>雇用形態</th>");
    html.push_str("<th>給与区分</th>");
    html.push_str(r#"<th class="text-right">月給下限</th>"#);
    html.push_str(r#"<th class="text-right">月給上限</th>"#);
    html.push_str(r#"<th class="text-right">基本給</th>"#);
    html.push_str(r#"<th style="min-width:200px">応募要件</th>"#);
    html.push_str("<th>賞与</th>");
    html.push_str(r#"<th class="text-right">年間休日</th>"#);
    html.push_str(r#"<th class="text-right">資格手当</th>"#);
    html.push_str(r#"<th style="min-width:200px">他手当</th>"#);
    html.push_str("<th>セグメント</th>");
    if show_distance {
        html.push_str(r#"<th class="text-right">距離</th>"#);
    }
    html.push_str("</tr></thead><tbody>");

    let start_num = (page - 1) * 50;
    for (i, p) in postings.iter().enumerate() {
        let fname = truncate_str(&escape_html(&p.facility_name), 40);
        let major_cat = extract_major_category(&p.facility_type);
        let cat_short = major_category_short_label(major_cat);
        let cat_color = major_category_color(major_cat);
        let ftype_detail = {
            let ft = p.facility_type.trim();
            if let Some(pos) = ft.find(' ') {
                &ft[pos+1..]
            } else {
                ""
            }
        };
        let ftype_display = if ftype_detail.is_empty() {
            format!(
                r#"<span class="inline-flex items-center gap-1"><span class="inline-block px-1.5 py-0.5 rounded text-xs font-medium" style="background:{};color:#fff">{}</span></span>"#,
                cat_color, escape_html(cat_short)
            )
        } else {
            format!(
                r#"<span class="inline-flex items-center gap-1 flex-wrap"><span class="inline-block px-1.5 py-0.5 rounded text-xs font-medium" style="background:{};color:#fff">{}</span><span class="text-xs text-slate-300">{}</span></span>"#,
                cat_color, escape_html(cat_short), truncate_str(&escape_html(ftype_detail), 20)
            )
        };
        let area = format!("{} {}", p.prefecture, p.municipality);
        let sal_type = escape_html(&p.salary_type);
        let sal_min = if p.salary_min > 0 { format_number(p.salary_min) } else { "-".to_string() };
        let sal_max = if p.salary_max > 0 { format_number(p.salary_max) } else { "-".to_string() };
        let base_sal = if p.base_salary > 0 { format_number(p.base_salary) } else { "-".to_string() };
        let reqs = escape_html(&p.requirements);
        let bonus = truncate_str(&escape_html(&p.bonus), 20);
        let holidays = if p.annual_holidays > 0 { p.annual_holidays.to_string() } else { "-".to_string() };
        let qual_allow = if p.qualification_allowance > 0 { format_number(p.qualification_allowance) } else { "-".to_string() };
        let other_allow = escape_html(&p.other_allowances);

        let seg_label = if p.tier3_label_short.is_empty() {
            "-".to_string()
        } else {
            let label = truncate_str(&escape_html(&p.tier3_label_short), 25);
            if p.tier3_id.is_empty() { label } else { format!(r#"<span title="{}">{}</span>"#, escape_html(&p.tier3_id), label) }
        };
        html.push_str(&format!(
            r#"<tr><td class="text-center">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class="text-right">{}</td><td class="text-right">{}</td><td class="text-right">{}</td><td><div class="cell-wrap">{}</div></td><td>{}</td><td class="text-right">{}</td><td class="text-right">{}</td><td><div class="cell-wrap">{}</div></td><td class="text-xs">{}</td>"#,
            start_num + i as i64 + 1, fname, ftype_display, area, escape_html(&p.employment_type),
            sal_type, sal_min, sal_max, base_sal, reqs, bonus, holidays, qual_allow, other_allow, seg_label,
        ));
        if show_distance {
            let dist = p.distance_km.map(|d| format!("{:.1}km", d)).unwrap_or("-".to_string());
            html.push_str(&format!(r#"<td class="text-right">{}</td>"#, dist));
        }
        html.push_str("</tr>");
    }
    html.push_str("</tbody></table></div>");

    // ページネーション
    if total_pages > 1 {
        html.push_str(r#"<div class="flex justify-center gap-2 mt-4">"#);
        let base_url = format!(
            "/api/competitive/filter?prefecture={}&municipality={}&employment_type={}&facility_type={}&service_type={}&nearby={}&radius_km={}",
            urlencoding::encode(pref),
            urlencoding::encode(muni),
            urlencoding::encode(emp),
            urlencoding::encode(ftype),
            urlencoding::encode(stype),
            nearby,
            radius_km,
        );
        if page > 1 {
            html.push_str(&format!(
                r##"<button class="px-3 py-1 bg-slate-700 hover:bg-slate-600 rounded text-sm" hx-get="{}&page={}" hx-target="#comp-results" hx-swap="innerHTML">前へ</button>"##,
                base_url, page - 1
            ));
        }
        html.push_str(&format!(
            r#"<span class="px-3 py-1 text-sm text-slate-400">{} / {} ページ</span>"#,
            page, total_pages
        ));
        if page < total_pages {
            html.push_str(&format!(
                r##"<button class="px-3 py-1 bg-slate-700 hover:bg-slate-600 rounded text-sm" hx-get="{}&page={}" hx-target="#comp-results" hx-swap="innerHTML">次へ</button>"##,
                base_url, page + 1
            ));
        }
        html.push_str("</div>");
    }

    Html(html)
}

/// HTMLレポート生成（A4横向き印刷対応）
pub(crate) fn render_report_html(
    job_type: &str,
    pref: &str,
    muni: &str,
    emp: &str,
    postings: &[PostingRow],
    stats: &SalaryStats,
    today: &str,
    nearby: bool,
    radius_km: f64,
) -> Html<String> {
    let region = if muni.is_empty() {
        pref.to_string()
    } else if nearby {
        format!("{} {}（半径{}km）", pref, muni, radius_km)
    } else {
        format!("{} {}", pref, muni)
    };
    let emp_label = if emp.is_empty() || emp == "全て" { String::new() } else { format!(" × {}", emp) };

    let show_distance = nearby && postings.iter().any(|p| p.distance_km.is_some());

    let mut table_rows = String::new();
    for (i, p) in postings.iter().enumerate() {
        let fname = truncate_str(&escape_html(&p.facility_name), 40);
        let major_cat = extract_major_category(&p.facility_type);
        let cat_short = major_category_short_label(major_cat);
        let ftype_detail = {
            let ft = p.facility_type.trim();
            if let Some(pos) = ft.find(' ') { &ft[pos+1..] } else { "" }
        };
        let ftype = if ftype_detail.is_empty() {
            format!("<b>{}</b>", escape_html(cat_short))
        } else {
            format!("<b>{}</b> {}", escape_html(cat_short), truncate_str(&escape_html(ftype_detail), 20))
        };
        let area = format!("{} {}", escape_html(&p.prefecture), escape_html(&p.municipality));
        let sal_type = escape_html(&p.salary_type);
        let sal_min = if p.salary_min > 0 { format_number(p.salary_min) } else { "-".to_string() };
        let sal_max = if p.salary_max > 0 { format_number(p.salary_max) } else { "-".to_string() };
        let base_sal = if p.base_salary > 0 { format_number(p.base_salary) } else { "-".to_string() };
        let reqs = escape_html(&p.requirements);
        let bonus = truncate_str(&escape_html(&p.bonus), 20);
        let holidays = if p.annual_holidays > 0 { p.annual_holidays.to_string() } else { "-".to_string() };
        let qual_allow = if p.qualification_allowance > 0 { format_number(p.qualification_allowance) } else { "-".to_string() };
        let other_allow = escape_html(&p.other_allowances);
        let dist_cell = if show_distance {
            let d = p.distance_km.map(|d| format!("{:.1}km", d)).unwrap_or("-".to_string());
            format!(r#"<td class="num">{}</td>"#, d)
        } else {
            String::new()
        };

        let seg = if p.tier3_label_short.is_empty() {
            "-".to_string()
        } else if p.tier3_id.is_empty() {
            escape_html(&p.tier3_label_short)
        } else {
            format!(r#"<span title="{}">{}</span>"#, escape_html(&p.tier3_id), escape_html(&p.tier3_label_short))
        };
        table_rows.push_str(&format!(
            r#"<tr><td style="text-align:center">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class="num">{}</td><td class="num">{}</td><td class="num">{}</td><td style="max-width:250px;word-break:break-all">{}</td><td>{}</td><td class="num">{}</td><td class="num">{}</td><td style="max-width:250px;word-break:break-all">{}</td><td>{}</td>{}</tr>"#,
            i + 1, fname, ftype, area, escape_html(&p.employment_type),
            sal_type, sal_min, sal_max, base_sal, reqs, bonus, holidays, qual_allow, other_allow, seg, dist_cell,
        ));
    }

    let distance_th = if show_distance { r#"<th>距離</th>"# } else { "" };

    let stats_html = if stats.has_data {
        format!(
            r#"<h2>給与統計サマリー</h2>
            <table>
                <thead><tr><th></th><th>月給下限</th><th>月給上限</th></tr></thead>
                <tbody>
                    <tr><td>最頻値（1万円単位）</td><td class="num">{}</td><td class="num">{}</td></tr>
                    <tr><td>中央値</td><td class="num">{}</td><td class="num">{}</td></tr>
                    <tr><td>平均値</td><td class="num">{}</td><td class="num">{}</td></tr>
                </tbody>
            </table>
            <p>件数: {} ｜ 賞与あり率: {} ｜ 平均年間休日: {}</p>"#,
            stats.salary_min_mode, stats.salary_max_mode,
            stats.salary_min_median, stats.salary_max_median,
            stats.salary_min_avg, stats.salary_max_avg,
            stats.count, stats.bonus_rate, stats.avg_holidays,
        )
    } else {
        String::new()
    };

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>競合調査レポート - {job_type} × {region}{emp_label}</title>
<style>
@page {{ size: A4 landscape; margin: 10mm; }}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: "Yu Gothic", "Meiryo", sans-serif; font-size: 11px; color: #333; background: #fff; padding: 15px; }}
h1 {{ font-size: 18px; color: #1a5276; margin-bottom: 8px; border-bottom: 2px solid #1a5276; padding-bottom: 4px; }}
h2 {{ font-size: 14px; color: #2c3e50; margin: 16px 0 8px 0; }}
.meta {{ font-size: 11px; color: #666; margin-bottom: 12px; }}
.meta span {{ margin-right: 16px; }}
table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
th {{ background-color: #2c3e50; color: #fff; font-weight: bold; text-align: center; padding: 6px 4px; font-size: 10px; white-space: nowrap; border: 1px solid #1a252f; }}
td {{ padding: 5px 4px; border: 1px solid #ddd; font-size: 10px; vertical-align: top; }}
tr:nth-child(even) {{ background-color: #f8f9fa; }}
.num {{ text-align: right; white-space: nowrap; }}
@media print {{
    body {{ padding: 0; font-size: 9px; }}
    th, td {{ font-size: 8px; padding: 3px 2px; }}
}}
</style>
</head>
<body>
<h1>競合調査レポート</h1>
<div class="meta">
    <span>職種: {job_type}</span>
    <span>地域: {region}</span>
    <span>生成日: {today}</span>
    <span>{count}件</span>
</div>

{stats_html}

<h2>求人一覧</h2>
<table>
<thead>
<tr>
    <th>#</th><th>法人・施設名</th><th>施設形態</th><th>エリア</th>
    <th>雇用形態</th><th>給与区分</th><th>月給下限</th><th>月給上限</th>
    <th>基本給</th><th>応募要件</th><th>賞与</th><th>年間休日</th>
    <th>資格手当</th><th>他手当</th><th>セグメント</th>{distance_th}
</tr>
</thead>
<tbody>
{table_rows}
</tbody>
</table>
</body>
</html>"#,
        job_type = escape_html(job_type),
        region = escape_html(&region),
        emp_label = escape_html(&emp_label),
        today = today,
        count = postings.len(),
        stats_html = stats_html,
        distance_th = distance_th,
        table_rows = table_rows,
    );

    Html(html)
}

pub(crate) fn render_analysis_html(job_type: &str, data: &AnalysisData) -> String {
    render_analysis_html_with_scope(job_type, "全国", data)
}

pub(crate) fn render_analysis_html_with_scope(job_type: &str, scope: &str, data: &AnalysisData) -> String {
    if data.total == 0 {
        return crate::handlers::render_empty_state(
            "求人データなし",
            &format!("「{}」の求人データがありません。職種や地域を変更してお試しください。", escape_html(job_type)),
        );
    }

    let pct = |n: i64| -> String {
        if data.total == 0 { "0".to_string() } else { format!("{:.1}", n as f64 / data.total as f64 * 100.0) }
    };

    let exp_chart_data = format!(
        r#"[{{"value":{},"name":"未経験OK"}},{{"value":{},"name":"経験者向け"}},{{"value":{},"name":"未記載"}}]"#,
        data.inexperienced_ok, data.experienced_only, data.experience_unknown
    );

    let emp_chart_data: String = data.employment_dist.iter()
        .map(|(name, cnt)| format!(r#"{{"value":{},"name":"{}"}}"#, cnt, escape_html(name)))
        .collect::<Vec<_>>()
        .join(",");

    let sal_type_data: String = data.salary_type_dist.iter()
        .map(|(name, cnt)| format!(r#"{{"value":{},"name":"{}"}}"#, cnt, escape_html(name)))
        .collect::<Vec<_>>()
        .join(",");

    let ftype_labels: String = data.facility_type_top.iter()
        .map(|(name, _)| format!(r#""{}""#, escape_html(name)))
        .collect::<Vec<_>>()
        .join(",");
    let ftype_values: String = data.facility_type_top.iter()
        .map(|(_, cnt)| cnt.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let range_labels: String = data.salary_range_dist.iter()
        .map(|(label, _)| format!(r#""{}""#, label))
        .collect::<Vec<_>>()
        .join(",");
    let range_values: String = data.salary_range_dist.iter()
        .map(|(_, cnt)| cnt.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let bonus_pct = pct(data.bonus_count);

    format!(
        r##"<div class="space-y-4">
    <h3 class="text-lg font-bold text-white"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z'/></svg>  {job_type} 求人データ分析 <span class="text-sm font-normal text-slate-400">（{scope} / {total}件）</span></h3>

    <!-- KPIサマリー -->
    <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div class="stat-card text-center">
            <div class="text-xs text-slate-400">未経験OK</div>
            <div class="text-2xl font-bold text-emerald-400">{inexperienced_ok}</div>
            <div class="text-xs text-slate-500">{inexperienced_pct}%</div>
        </div>
        <div class="stat-card text-center">
            <div class="text-xs text-slate-400">経験者向け</div>
            <div class="text-2xl font-bold text-amber-400">{experienced_only}</div>
            <div class="text-xs text-slate-500">{experienced_pct}%</div>
        </div>
        <div class="stat-card text-center">
            <div class="text-xs text-slate-400">月給平均（中央値）</div>
            <div class="text-2xl font-bold text-cyan-400">{salary_avg_fmt}</div>
            <div class="text-xs text-slate-500">中央値: {salary_median_fmt}</div>
        </div>
        <div class="stat-card text-center">
            <div class="text-xs text-slate-400">賞与あり率 / 平均休日</div>
            <div class="text-2xl font-bold text-purple-400">{bonus_pct}%</div>
            <div class="text-xs text-slate-500">年間休日: {holidays_avg}日 ({holidays_with_data}件)</div>
        </div>
    </div>

    <!-- チャート2列 -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <!-- 未経験OK / 経験者向け -->
        <div class="stat-card">
            <h4 class="text-sm text-slate-400 mb-2">未経験OK vs 経験者向け</h4>
            <div class="echart" style="height:280px;" data-chart-config='{{
                "tooltip": {{"trigger": "item", "formatter": "{{b}}: {{c}}件 ({{d}}%)"}},
                "legend": {{"bottom": 0, "textStyle": {{"color": "#94a3b8", "fontSize": 11}}}},
                "color": ["#10b981", "#f59e0b", "#64748b"],
                "series": [{{"type": "pie", "radius": ["40%","65%"], "center": ["50%","45%"],
                    "label": {{"formatter": "{{b}}\n{{d}}%", "color": "#e2e8f0", "fontSize": 11}},
                    "data": {exp_chart_data}
                }}]
            }}'></div>
        </div>

        <!-- 雇用形態分布 -->
        <div class="stat-card">
            <h4 class="text-sm text-slate-400 mb-2">雇用形態別分布</h4>
            <div class="echart" style="height:280px;" data-chart-config='{{
                "tooltip": {{"trigger": "item", "formatter": "{{b}}: {{c}}件 ({{d}}%)"}},
                "legend": {{"bottom": 0, "textStyle": {{"color": "#94a3b8", "fontSize": 11}}}},
                "color": ["#3b82f6", "#22c55e", "#eab308", "#ef4444", "#8b5cf6"],
                "series": [{{"type": "pie", "radius": ["40%","65%"], "center": ["50%","45%"],
                    "label": {{"formatter": "{{b}}\n{{d}}%", "color": "#e2e8f0", "fontSize": 11}},
                    "data": [{emp_chart_data}]
                }}]
            }}'></div>
        </div>

        <!-- 給与区分分布 -->
        <div class="stat-card">
            <h4 class="text-sm text-slate-400 mb-2">給与区分別分布</h4>
            <div class="echart" style="height:280px;" data-chart-config='{{
                "tooltip": {{"trigger": "item", "formatter": "{{b}}: {{c}}件 ({{d}}%)"}},
                "legend": {{"bottom": 0, "textStyle": {{"color": "#94a3b8", "fontSize": 11}}}},
                "color": ["#06b6d4", "#8b5cf6", "#f97316", "#64748b"],
                "series": [{{"type": "pie", "radius": ["40%","65%"], "center": ["50%","45%"],
                    "label": {{"formatter": "{{b}}\n{{d}}%", "color": "#e2e8f0", "fontSize": 11}},
                    "data": [{sal_type_data}]
                }}]
            }}'></div>
        </div>

        <!-- 月給レンジ分布 -->
        <div class="stat-card">
            <h4 class="text-sm text-slate-400 mb-2">月給レンジ分布（下限額）</h4>
            <div class="echart" style="height:280px;" data-chart-config='{{
                "tooltip": {{"trigger": "axis"}},
                "xAxis": {{"type": "category", "data": [{range_labels}], "axisLabel": {{"color": "#94a3b8", "fontSize": 10}}}},
                "yAxis": {{"type": "value", "axisLabel": {{"color": "#94a3b8"}}}},
                "series": [{{"type": "bar", "data": [{range_values}],
                    "itemStyle": {{"color": {{"type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
                        "colorStops": [{{"offset": 0, "color": "#06b6d4"}}, {{"offset": 1, "color": "#3b82f6"}}]
                    }}, "borderRadius": [4,4,0,0]}},
                    "barWidth": "60%"
                }}],
                "grid": {{"left": "12%", "right": "5%", "bottom": "12%"}}
            }}'></div>
        </div>
    </div>

    <!-- 施設形態 大カテゴリ別 -->
    <div class="stat-card">
        <h4 class="text-sm text-slate-400 mb-2">施設形態 大カテゴリ別</h4>
        <div class="echart" style="height:350px;" data-chart-config='{{
            "tooltip": {{"trigger": "axis", "axisPointer": {{"type": "shadow"}}}},
            "xAxis": {{"type": "value", "axisLabel": {{"color": "#94a3b8"}}}},
            "yAxis": {{"type": "category", "data": [{ftype_labels}], "inverse": true, "axisLabel": {{"color": "#94a3b8", "fontSize": 10}}}},
            "series": [{{"type": "bar", "data": [{ftype_values}],
                "itemStyle": {{"color": "#8b5cf6", "borderRadius": [0,4,4,0]}}, "barWidth": "60%"
            }}],
            "grid": {{"left": "30%", "right": "5%"}}
        }}'></div>
    </div>
</div>"##,
        job_type = escape_html(job_type),
        scope = escape_html(scope),
        total = format_number(data.total),
        inexperienced_ok = format_number(data.inexperienced_ok),
        inexperienced_pct = pct(data.inexperienced_ok),
        experienced_only = format_number(data.experienced_only),
        experienced_pct = pct(data.experienced_only),
        salary_avg_fmt = if data.salary_avg > 0 { format!("{}円", format_number(data.salary_avg)) } else { "-".to_string() },
        salary_median_fmt = if data.salary_median > 0 { format!("{}円", format_number(data.salary_median)) } else { "-".to_string() },
        bonus_pct = bonus_pct,
        holidays_avg = data.holidays_avg,
        holidays_with_data = format_number(data.holidays_with_data),
        exp_chart_data = exp_chart_data,
        emp_chart_data = emp_chart_data,
        sal_type_data = sal_type_data,
        ftype_labels = ftype_labels,
        ftype_values = ftype_values,
        range_labels = range_labels,
        range_values = range_values,
    )
}
