use super::analysis::calc_salary_stats;
use super::fetch::PostingRow;
use super::utils::{escape_html, truncate_str, value_to_i64, haversine};
use crate::handlers::overview::format_number;
use serde_json::Value;

fn make_posting(salary_min: i64, salary_max: i64, bonus: &str, holidays: i64) -> PostingRow {
    PostingRow {
        facility_name: "テスト施設".to_string(),
        facility_type: "病院".to_string(),
        prefecture: "群馬県".to_string(),
        municipality: "高崎市".to_string(),
        employment_type: "正職員".to_string(),
        salary_type: "月給".to_string(),
        salary_min,
        salary_max,
        base_salary: 0,
        requirements: String::new(),
        bonus: bonus.to_string(),
        annual_holidays: holidays,
        qualification_allowance: 0,
        other_allowances: String::new(),
        distance_km: None,
        tier3_id: String::new(),
        tier3_label_short: String::new(),
    }
}

// テスト26: value_to_i64: REAL 276250.0 → 276250
#[test]
fn test_value_to_i64_real() {
    let v = serde_json::json!(276250.0);
    assert_eq!(value_to_i64(&v), 276250);
}

// テスト26逆証明: NaN → 0
#[test]
fn test_value_to_i64_nan() {
    let v = Value::Null;
    assert_eq!(value_to_i64(&v), 0);
}

// テスト27: value_to_i64: INTEGER 300000 → 300000
#[test]
fn test_value_to_i64_integer() {
    let v = serde_json::json!(300000);
    assert_eq!(value_to_i64(&v), 300000);
}

// テスト28: value_to_i64: 文字列"abc" → 0
#[test]
fn test_value_to_i64_string() {
    let v = Value::String("abc".to_string());
    assert_eq!(value_to_i64(&v), 0);
}

// テスト29: haversine: 東京→大阪 ≈ 397km
#[test]
fn test_haversine_tokyo_osaka() {
    let dist = haversine(35.6762, 139.6503, 34.6937, 135.5023);
    assert!(dist > 390.0 && dist < 410.0, "dist={}", dist);
}

// テスト29逆証明: 同一地点 → 0.0
#[test]
fn test_haversine_same_point() {
    let dist = haversine(35.0, 135.0, 35.0, 135.0);
    assert!(dist < 0.001);
}

// テスト30: haversine: 南半球 → 正常値
#[test]
fn test_haversine_southern_hemisphere() {
    let dist = haversine(-33.8688, 151.2093, -37.8136, 144.9631);
    assert!(dist > 700.0 && dist < 900.0, "dist={}", dist);
}

// テスト31: format_number: 1234567 → "1,234,567"
#[test]
fn test_format_number_large() {
    assert_eq!(format_number(1234567), "1,234,567");
}

// テスト31逆証明: 0 → "0"
#[test]
fn test_format_number_zero() {
    assert_eq!(format_number(0), "0");
}

// テスト32: format_number: 負数
#[test]
fn test_format_number_negative() {
    assert_eq!(format_number(-1234), "-1,234");
}

// テスト33: truncate_str: 超過 → 切り詰め+"…"
#[test]
fn test_truncate_long_string() {
    let long = "あ".repeat(25);
    let result = truncate_str(&long, 20);
    assert!(result.ends_with('…'));
    assert!(result.chars().count() <= 20);
}

// テスト33逆証明: 20文字以下 → そのまま
#[test]
fn test_truncate_short_string() {
    let short = "短い文字列";
    assert_eq!(truncate_str(short, 20), short);
}

// テスト34: escape_html: <script> → &lt;script&gt; (シングルクォートもエスケープ)
#[test]
fn test_escape_html_script() {
    assert_eq!(escape_html("<script>alert('xss')</script>"),
               "&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;");
}

// テスト: escape_html シングルクォート
#[test]
fn test_escape_html_single_quote() {
    assert_eq!(escape_html("it's"), "it&#x27;s");
}

// テスト34逆証明: 通常文字 → 変換なし
#[test]
fn test_escape_html_normal() {
    assert_eq!(escape_html("通常テキスト"), "通常テキスト");
}

// テスト35: bounding_box概念テスト（10km → 緯度±0.09°）
#[test]
fn test_bounding_box_concept() {
    let radius_km = 10.0;
    let lat_delta: f64 = radius_km / 111.0;
    assert!((lat_delta - 0.09).abs() < 0.01);
}

// テスト37: salary統計: 空配列 → すべて0/ダッシュ
#[test]
fn test_salary_stats_empty() {
    let stats = calc_salary_stats(&[]);
    assert_eq!(stats.count, 0);
    assert!(!stats.has_data);
    assert_eq!(stats.salary_min_median, "-");
}

// テスト37逆証明: 1件 → 統計あり
#[test]
fn test_salary_stats_one_item() {
    let postings = vec![make_posting(200000, 300000, "あり", 120)];
    let stats = calc_salary_stats(&postings);
    assert_eq!(stats.count, 1);
    assert!(stats.has_data);
}

// テスト38: salary統計: [100000,200000,300000] → 中央値200000
#[test]
fn test_salary_stats_median() {
    let postings = vec![
        make_posting(100000, 150000, "", 0),
        make_posting(200000, 250000, "", 0),
        make_posting(300000, 350000, "", 0),
    ];
    let stats = calc_salary_stats(&postings);
    assert!(stats.salary_min_median.contains("200,000"));
}

// テスト39: mode計算: [260000,260000,270000] → 260000(1万丸めで260000)
#[test]
fn test_salary_stats_mode() {
    let postings = vec![
        make_posting(260000, 300000, "", 0),
        make_posting(260000, 300000, "", 0),
        make_posting(270000, 320000, "", 0),
    ];
    let stats = calc_salary_stats(&postings);
    assert!(stats.salary_min_mode.contains("260,000"));
}

// テスト: escape_html quotes
#[test]
fn test_escape_html_quotes() {
    assert!(escape_html("a\"b").contains("&quot;"));
}

// テスト: escape_html ampersand
#[test]
fn test_escape_html_ampersand() {
    assert_eq!(escape_html("a&b"), "a&amp;b");
}

// テスト40: 丸め境界値 275000 → ((275000+5000)/10000)*10000 = 280000
#[test]
fn test_salary_mode_rounding_275000() {
    let postings = vec![
        make_posting(275000, 300000, "", 0),
        make_posting(275000, 300000, "", 0),
        make_posting(300000, 350000, "", 0),
    ];
    let stats = calc_salary_stats(&postings);
    assert!(stats.salary_min_mode.contains("280,000"),
        "275000は1万円単位丸めで280,000になるべき: got {}", stats.salary_min_mode);
}

// テスト41: 丸め境界値 245000 → ((245000+5000)/10000)*10000 = 250000
#[test]
fn test_salary_mode_rounding_245000() {
    let postings = vec![
        make_posting(245000, 300000, "", 0),
        make_posting(245000, 300000, "", 0),
        make_posting(300000, 350000, "", 0),
    ];
    let stats = calc_salary_stats(&postings);
    assert!(stats.salary_min_mode.contains("250,000"),
        "245000は1万円単位丸めで250,000になるべき: got {}", stats.salary_min_mode);
}

// テスト42: 丸め境界値 255000 → ((255000+5000)/10000)*10000 = 260000
#[test]
fn test_salary_mode_rounding_255000() {
    let postings = vec![
        make_posting(255000, 300000, "", 0),
        make_posting(255000, 300000, "", 0),
        make_posting(300000, 350000, "", 0),
    ];
    let stats = calc_salary_stats(&postings);
    assert!(stats.salary_min_mode.contains("260,000"),
        "255000は1万円単位丸めで260,000になるべき: got {}", stats.salary_min_mode);
}
