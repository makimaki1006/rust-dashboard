use serde_json::Value;
use std::collections::HashMap;

/// 施設形態の8大カテゴリ定義（DB実データに基づく）
pub(crate) const MAJOR_CATEGORIES: &[(&str, &str, &str)] = &[
    ("介護・福祉事業所", "介護・福祉", "#10b981"),
    ("美容・サロン・ジム", "美容・サロン", "#f472b6"),
    ("薬局・ドラッグストア", "薬局・DS", "#8b5cf6"),
    ("代替医療・リラクゼーション", "代替医療", "#f59e0b"),
    ("歯科診療所・技工所", "歯科", "#06b6d4"),
    ("訪問看護ステーション", "訪問看護", "#ec4899"),
    ("その他（企業・学校等）", "その他", "#64748b"),
    ("保育園・幼稚園", "保育・幼稚園", "#22c55e"),
];

/// facility_typeから大カテゴリラベルを抽出（先頭のスペース前部分）
/// 空文字列・NULL・不明なプレフィックスは「未分類」に分類
pub(crate) fn extract_major_category(facility_type: &str) -> &str {
    let ft = facility_type.trim();
    if ft.is_empty() {
        return "未分類";
    }
    let prefix = ft.split(' ').next().unwrap_or(ft);
    for &(cat_prefix, _label, _color) in MAJOR_CATEGORIES {
        if prefix == cat_prefix {
            return cat_prefix;
        }
    }
    "未分類"
}

/// facility_typeからプライマリサブカテゴリを抽出
/// 例: "介護・福祉事業所 放課後等デイサービス、障害者支援" → "放課後等デイサービス"
/// 例: "介護・福祉事業所" → ""（サブカテゴリなし）
pub(crate) fn extract_primary_subtype(facility_type: &str) -> &str {
    let ft = facility_type.trim();
    if let Some(space_pos) = ft.find(' ') {
        let after_space = &ft[space_pos + 1..];
        // 「、」で区切られた最初の要素を取得
        if let Some(comma_pos) = after_space.find('、') {
            &after_space[..comma_pos]
        } else {
            after_space
        }
    } else {
        ""
    }
}

/// facility_typeリストからサブカテゴリ別の集計を構築
/// 返り値: Vec<(major_cat, Vec<(sub_type, count)>)>（降順ソート済み）
pub(crate) fn aggregate_subtypes(
    rows: &[HashMap<String, Value>],
) -> Vec<(String, Vec<(String, i64)>)> {
    // (major, sub) → count
    let mut map: HashMap<String, HashMap<String, i64>> = HashMap::new();
    // major → total
    let mut major_totals: HashMap<String, i64> = HashMap::new();

    for row in rows {
        let ft = row.get("facility_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let cnt = row.get("cnt")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let major = extract_major_category(ft).to_string();
        let sub = extract_primary_subtype(ft);
        let sub_key = if sub.is_empty() { "その他".to_string() } else { sub.to_string() };

        *map.entry(major.clone())
            .or_default()
            .entry(sub_key)
            .or_insert(0) += cnt;
        *major_totals.entry(major).or_insert(0) += cnt;
    }

    // majorをtotal降順でソート
    let mut major_sorted: Vec<(String, i64)> = major_totals.into_iter().collect();
    major_sorted.sort_by(|a, b| b.1.cmp(&a.1));

    let mut result = Vec::new();
    for (major, _total) in major_sorted {
        if let Some(subs) = map.remove(&major) {
            let mut sub_sorted: Vec<(String, i64)> = subs.into_iter().collect();
            sub_sorted.sort_by(|a, b| b.1.cmp(&a.1));
            result.push((major, sub_sorted));
        }
    }

    result
}

/// 大カテゴリの短縮ラベルを取得
pub(crate) fn major_category_short_label(category: &str) -> &str {
    if category == "未分類" {
        return "未分類";
    }
    for &(cat_prefix, label, _color) in MAJOR_CATEGORIES {
        if category == cat_prefix {
            return label;
        }
    }
    "不明"
}

/// 大カテゴリのバッジ色を取得
pub(crate) fn major_category_color(category: &str) -> &str {
    if category == "未分類" {
        return "#6b7280";
    }
    for &(cat_prefix, _label, color) in MAJOR_CATEGORIES {
        if category == cat_prefix {
            return color;
        }
    }
    "#64748b"
}

/// HTMLエスケープ（XSS対策: シングルクォート含む）
pub fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// 文字列を指定文字数で切り詰め
pub(crate) fn truncate_str(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_chars - 1).collect();
        format!("{}…", truncated)
    }
}

/// <option>タグを生成（api.rsから参照）
pub fn build_option(value: &str, label: &str) -> String {
    format!(r#"<option value="{}">{}</option>"#, escape_html(value), escape_html(label))
}

/// serde_json::Valueから数値を取得（REAL/INTEGER両対応）
pub(crate) fn value_to_i64(v: &Value) -> i64 {
    v.as_i64().unwrap_or_else(|| v.as_f64().map(|f| f as i64).unwrap_or(0))
}

/// Haversine公式で2点間の距離を計算（km）
pub(crate) fn haversine(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let r = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}
