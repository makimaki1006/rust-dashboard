/// コロプレス色計算
/// 値に基づいてグラデーション色を返す

/// 値を0-1の範囲に正規化し、色を返す
pub fn get_color_by_value(value: f64, max_value: f64, mode: &str) -> String {
    if max_value <= 0.0 {
        return "#9ca3af".to_string(); // グレー
    }

    let ratio = (value / max_value).clamp(0.0, 1.0);

    match mode {
        "blue" => interpolate_color(ratio, (219, 234, 254), (30, 64, 175)),
        "red" => interpolate_color(ratio, (254, 226, 226), (185, 28, 28)),
        "green" => interpolate_color(ratio, (220, 252, 231), (22, 101, 52)),
        "diverging" => {
            // -1 ~ +1 の範囲でblue→white→redのダイバージング
            let norm = (value / max_value).clamp(-1.0, 1.0);
            if norm < 0.0 {
                let r = -norm;
                interpolate_color(r, (255, 255, 255), (30, 64, 175))
            } else {
                interpolate_color(norm, (255, 255, 255), (185, 28, 28))
            }
        }
        _ => interpolate_color(ratio, (219, 234, 254), (30, 64, 175)),
    }
}

/// 2色間の線形補間
fn interpolate_color(ratio: f64, from: (u8, u8, u8), to: (u8, u8, u8)) -> String {
    let r = (from.0 as f64 + (to.0 as f64 - from.0 as f64) * ratio) as u8;
    let g = (from.1 as f64 + (to.1 as f64 - from.1 as f64) * ratio) as u8;
    let b = (from.2 as f64 + (to.2 as f64 - from.2 as f64) * ratio) as u8;
    format!("#{r:02x}{g:02x}{b:02x}")
}

/// コロプレスのスタイルJSON生成（Leaflet用）
pub fn generate_choropleth_style(
    municipality: &str,
    value: f64,
    max_value: f64,
    mode: &str,
) -> String {
    let color = get_color_by_value(value, max_value, mode);
    format!(
        r#"{{"fillColor":"{}","weight":1,"opacity":1,"color":"{}","fillOpacity":0.7,"name":"{}","value":{}}}"#,
        color, "#666", municipality, value
    )
}
