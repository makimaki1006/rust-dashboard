use axum::{
    extract::{Path, Query, State},
    response::{Html, Json},
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tower_sessions::Session;

use crate::auth::SESSION_JOB_TYPE_KEY;
use crate::AppState;

#[derive(Deserialize)]
pub struct GeoJsonQuery {
    pub pref: Option<String>,
}

/// GeoJSON API: /api/geojson/:filename
pub async fn get_geojson(
    State(_state): State<Arc<AppState>>,
    Path(filename): Path<String>,
) -> Json<Value> {
    let geojson_dir = "static/geojson";
    let path = format!("{geojson_dir}/{filename}");

    match std::fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(json) => Json(json),
            Err(_) => Json(Value::Null),
        },
        Err(_) => Json(Value::Null),
    }
}

#[derive(Deserialize)]
pub struct MarkersQuery {
    pub job_type: Option<String>,
}

/// マーカーAPI: /api/markers?job_type=看護師
/// ローカルSQLiteから都道府県別の求人数を集計し、マーカー座標として返す
pub async fn get_markers(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MarkersQuery>,
) -> Json<Value> {
    let job_type = params.job_type.unwrap_or_else(|| "介護職".to_string());

    let db = match &state.local_db {
        Some(db) => db,
        None => return Json(serde_json::json!([])),
    };

    // 都道府県別求人数を集計
    let rows = match db.query(
        "SELECT prefecture, COUNT(*) as cnt FROM job_postings WHERE job_type = ? GROUP BY prefecture ORDER BY cnt DESC",
        &[&job_type as &dyn rusqlite::types::ToSql],
    ) {
        Ok(r) => r,
        Err(_) => return Json(serde_json::json!([])),
    };

    // 都道府県→緯度経度マッピング（県庁所在地の概略座標）
    let pref_coords: Vec<(&str, f64, f64)> = vec![
        ("北海道", 43.06, 141.35), ("青森県", 40.82, 140.74), ("岩手県", 39.70, 141.15),
        ("宮城県", 38.27, 140.87), ("秋田県", 39.72, 140.10), ("山形県", 38.24, 140.34),
        ("福島県", 37.75, 140.47), ("茨城県", 36.34, 140.45), ("栃木県", 36.57, 139.88),
        ("群馬県", 36.39, 139.06), ("埼玉県", 35.86, 139.65), ("千葉県", 35.61, 140.12),
        ("東京都", 35.69, 139.69), ("神奈川県", 35.45, 139.64), ("新潟県", 37.90, 139.02),
        ("富山県", 36.70, 137.21), ("石川県", 36.59, 136.63), ("福井県", 36.07, 136.22),
        ("山梨県", 35.66, 138.57), ("長野県", 36.24, 138.18), ("岐阜県", 35.39, 136.72),
        ("静岡県", 34.98, 138.38), ("愛知県", 35.18, 136.91), ("三重県", 34.73, 136.51),
        ("滋賀県", 35.00, 135.87), ("京都府", 35.02, 135.76), ("大阪府", 34.69, 135.52),
        ("兵庫県", 34.69, 135.18), ("奈良県", 34.69, 135.83), ("和歌山県", 34.23, 135.17),
        ("鳥取県", 35.50, 134.24), ("島根県", 35.47, 133.05), ("岡山県", 34.66, 133.93),
        ("広島県", 34.40, 132.46), ("山口県", 34.19, 131.47), ("徳島県", 34.07, 134.56),
        ("香川県", 34.34, 134.04), ("愛媛県", 33.84, 132.77), ("高知県", 33.56, 133.53),
        ("福岡県", 33.61, 130.42), ("佐賀県", 33.25, 130.30), ("長崎県", 32.74, 129.87),
        ("熊本県", 32.79, 130.74), ("大分県", 33.24, 131.61), ("宮崎県", 31.91, 131.42),
        ("鹿児島県", 31.56, 130.56), ("沖縄県", 26.21, 127.68),
    ];

    let mut markers: Vec<Value> = Vec::new();
    for row in &rows {
        let pref = row.get("prefecture")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let cnt = row.get("cnt")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if let Some((_, lat, lng)) = pref_coords.iter().find(|(name, _, _)| *name == pref) {
            markers.push(serde_json::json!({
                "name": pref,
                "lat": lat,
                "lng": lng,
                "count": cnt
            }));
        }
    }

    Json(Value::Array(markers))
}

#[derive(Deserialize)]
pub struct PrefecturesQuery {
    pub job_type: Option<String>,
}

/// 都道府県一覧API（職種切り替え時にHTMXで取得）
pub async fn get_prefectures(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<PrefecturesQuery>,
) -> Html<String> {
    let job_type = if let Some(jt) = params.job_type {
        jt
    } else {
        session.get(SESSION_JOB_TYPE_KEY)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| "介護職".to_string())
    };

    let sql = "SELECT DISTINCT prefecture FROM job_seeker_data WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture != '' ORDER BY prefecture";
    let params_vec = vec![Value::String(job_type)];

    let prefs = match state.turso.query(sql, &params_vec).await {
        Ok(rows) => rows
            .iter()
            .filter_map(|r| r.get("prefecture").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };

    let html: String = prefs
        .iter()
        .map(|p| format!(r#"<option value="{p}">{p}</option>"#))
        .collect::<Vec<_>>()
        .join("\n");

    Html(html)
}

#[derive(Deserialize)]
pub struct MunicipalitiesCascadeQuery {
    pub prefecture: Option<String>,
}

/// 市区町村カスケードAPI（都道府県変更時にHTMXで取得）
pub async fn get_municipalities_cascade(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<MunicipalitiesCascadeQuery>,
) -> Html<String> {
    let job_type: String = session
        .get(SESSION_JOB_TYPE_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "介護職".to_string());

    let prefecture = params.prefecture.as_deref().unwrap_or("");
    if prefecture.is_empty() {
        return Html(String::new());
    }

    let sql = "SELECT DISTINCT municipality FROM job_seeker_data WHERE job_type = ? AND prefecture = ? AND row_type = 'SUMMARY' AND municipality != '' ORDER BY municipality";
    let params_vec = vec![
        Value::String(job_type),
        Value::String(prefecture.to_string()),
    ];

    let munis = match state.turso.query(sql, &params_vec).await {
        Ok(rows) => rows
            .iter()
            .filter_map(|r| r.get("municipality").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };

    let html: String = munis
        .iter()
        .map(|m| format!(r#"<option value="{m}">{m}</option>"#))
        .collect::<Vec<_>>()
        .join("\n");

    Html(html)
}
