use super::fetch::DetailRow;
use crate::handlers::competitive::escape_html;

/// 初期ページHTML（テンプレート読み込み + 変数置換）
pub(crate) fn render_jobmap_page(
    job_type: &str,
    prefecture: &str,
    prefecture_options: &str,
) -> String {
    include_str!("../../../templates/tabs/jobmap.html")
        .replace("{{JOB_TYPE}}", &escape_html(job_type))
        .replace("{{PREFECTURE}}", &escape_html(prefecture))
        .replace("{{PREFECTURE_OPTIONS}}", prefecture_options)
}

/// 求人詳細カードHTML
pub(crate) fn render_detail_card(d: &DetailRow) -> String {
    let salary_display = if d.salary_min > 0 && d.salary_max > 0 {
        format!(
            "{} {}&nbsp;〜&nbsp;{}",
            escape_html(&d.salary_type),
            format_yen(d.salary_min),
            format_yen(d.salary_max)
        )
    } else if d.salary_min > 0 {
        format!("{} {}〜", escape_html(&d.salary_type), format_yen(d.salary_min))
    } else {
        "記載なし".to_string()
    };

    let mut html = String::with_capacity(2048);
    html.push_str(r#"<div class="space-y-2 text-sm">"#);

    // ヘッドライン
    if !d.headline.is_empty() {
        html.push_str(&format!(
            r#"<div class="text-base font-bold text-blue-300 border-b border-gray-600 pb-1">{}</div>"#,
            escape_html(&d.headline)
        ));
    }

    // 施設名
    html.push_str(&format!(
        r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">施設名</span><span class="font-medium text-white">{}</span></div>"#,
        escape_html(&d.facility_name)
    ));

    // 所在地
    html.push_str(&format!(
        r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">所在地</span><span>{} {}</span></div>"#,
        escape_html(&d.prefecture),
        escape_html(&d.municipality)
    ));

    // アクセス
    if !d.access.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">アクセス</span><span>{}</span></div>"#,
            escape_html(&d.access)
        ));
    }

    // サービス形態
    if !d.service_type.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">形態</span><span>{}</span></div>"#,
            escape_html(&d.service_type)
        ));
    }

    // 雇用形態
    html.push_str(&format!(
        r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">雇用形態</span><span class="px-2 py-0.5 rounded text-xs {}">{}</span></div>"#,
        emp_badge_class(&d.employment_type),
        escape_html(&d.employment_type)
    ));

    // 給与
    html.push_str(&format!(
        r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">給与</span><span class="text-yellow-300 font-medium">{}</span></div>"#,
        salary_display
    ));

    // 給与備考
    if !d.salary_detail.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">給与詳細</span><span class="text-xs text-gray-300">{}</span></div>"#,
            escape_html(&truncate(&d.salary_detail, 150))
        ));
    }

    // 仕事内容
    if !d.job_description.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">仕事内容</span><span class="text-xs">{}</span></div>"#,
            escape_html(&truncate(&d.job_description, 200))
        ));
    }

    // 応募要件
    if !d.requirements.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">応募要件</span><span class="text-xs">{}</span></div>"#,
            escape_html(&truncate(&d.requirements, 150))
        ));
    }

    // 勤務時間
    if !d.working_hours.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">勤務時間</span><span class="text-xs">{}</span></div>"#,
            escape_html(&truncate(&d.working_hours, 100))
        ));
    }

    // 休日
    if !d.holidays.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">休日</span><span class="text-xs">{}</span></div>"#,
            escape_html(&truncate(&d.holidays, 100))
        ));
    }

    // 待遇
    if !d.benefits.is_empty() {
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">待遇</span><span class="text-xs">{}</span></div>"#,
            escape_html(&truncate(&d.benefits, 150))
        ));
    }

    // タグ
    if !d.tags.is_empty() {
        let tags_html: String = d
            .tags
            .split(',')
            .filter(|t| !t.trim().is_empty())
            .take(5)
            .map(|t| {
                format!(
                    r#"<span class="px-1.5 py-0.5 bg-gray-700 rounded text-xs text-gray-300">{}</span>"#,
                    escape_html(t.trim())
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2"><span class="text-gray-400 w-20 flex-shrink-0">タグ</span><div class="flex flex-wrap gap-1">{}</div></div>"#,
            tags_html
        ));
    }

    // セグメント情報
    if !d.tier3_label_short.is_empty() || !d.exp_qual_segment.is_empty() {
        let mut seg_parts = Vec::new();
        if !d.tier3_label_short.is_empty() {
            seg_parts.push(escape_html(&d.tier3_label_short));
        }
        if !d.exp_qual_segment.is_empty() {
            seg_parts.push(escape_html(&d.exp_qual_segment));
        }
        html.push_str(&format!(
            r#"<div class="flex items-start gap-2 border-t border-gray-700 pt-1 mt-1"><span class="text-gray-400 w-20 flex-shrink-0">分類</span><span class="text-xs text-purple-300">{}</span></div>"#,
            seg_parts.join(" / ")
        ));
    }

    html.push_str("</div>");
    html
}

fn emp_badge_class(emp: &str) -> &'static str {
    match emp {
        "正職員" => "bg-green-700 text-green-200",
        "契約職員" => "bg-blue-700 text-blue-200",
        "パート・バイト" => "bg-orange-700 text-orange-200",
        "業務委託" => "bg-purple-700 text-purple-200",
        _ => "bg-gray-700 text-gray-300",
    }
}

fn format_yen(n: i64) -> String {
    if n == 0 {
        return "−".to_string();
    }
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    let formatted: String = result.chars().rev().collect();
    format!("¥{}", formatted)
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars).collect();
        format!("{}…", truncated)
    }
}

/// 職種データが未連携の場合の表示
pub(crate) fn render_no_data_message(job_type: &str) -> String {
    format!(
        r#"<div class="p-8 text-center">
            <div class="text-6xl mb-4">🗺️</div>
            <h2 class="text-2xl font-bold text-white mb-2">求人地図</h2>
            <div class="bg-yellow-900/30 border border-yellow-700 rounded-lg p-6 max-w-lg mx-auto">
                <p class="text-yellow-300 text-lg font-medium mb-2">データ未連携</p>
                <p class="text-gray-300">
                    「<span class="text-white font-medium">{}</span>」の求人地図データはまだ連携されていません。
                </p>
                <p class="text-gray-400 text-sm mt-3">
                    現在対応済み: 介護職、看護師、保育士、栄養士、理学療法士、サービス管理責任者、サービス提供責任者、学童支援、調理師・調理スタッフ、薬剤師、言語聴覚士、児童指導員、児童発達支援管理責任者、生活支援員、幼稚園教諭、生活相談員
                </p>
            </div>
        </div>"#,
        escape_html(job_type)
    )
}
