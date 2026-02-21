use axum::extract::{Path, Query, State};
use axum::response::Html;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;
use crate::handlers::competitive::{build_option, escape_html};
use crate::handlers::overview::get_session_filters;

use super::fetch;
use super::render;
use super::stats;

#[derive(Deserialize)]
pub struct MarkerParams {
    #[serde(default)]
    pub prefecture: String,
    #[serde(default)]
    pub municipality: String,
    #[serde(default)]
    pub radius: Option<f64>,
    #[serde(default)]
    pub employment_type: String,
    #[serde(default)]
    pub salary_type: String,
}

#[derive(Deserialize)]
pub struct MuniParams {
    #[serde(default)]
    pub prefecture: String,
}

/// タブ6: 求人地図（初期ページ）
pub async fn tab_jobmap(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, _municipality) = get_session_filters(&session).await;

    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => {
            return Html(
                r#"<div class="p-8 text-center text-gray-400">
                    <h2 class="text-2xl mb-4">🗺️ 求人地図</h2>
                    <p>求人地図データベースが読み込まれていません。</p>
                    <p class="text-sm mt-2">data/geocoded_postings.db.gz を配置してください。</p>
                </div>"#
                    .to_string(),
            );
        }
    };

    // 選択職種のデータ存在チェック
    if !fetch::has_job_type_data(geocoded_db, &job_type) {
        return Html(render::render_no_data_message(&job_type));
    }

    let prefs = fetch::fetch_prefectures(geocoded_db, &job_type);
    let pref_options: String = std::iter::once(build_option("", "-- 都道府県 --"))
        .chain(prefs.iter().map(|p| {
            if p == &prefecture {
                format!(
                    r#"<option value="{}" selected>{}</option>"#,
                    escape_html(p),
                    escape_html(p)
                )
            } else {
                build_option(p, p)
            }
        }))
        .collect::<Vec<_>>()
        .join("\n");

    let html = render::render_jobmap_page(&job_type, &prefecture, &pref_options);
    Html(html)
}

/// マーカーJSON API
pub async fn jobmap_markers(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<MarkerParams>,
) -> Json<serde_json::Value> {
    let (job_type, session_pref, _session_muni) = get_session_filters(&session).await;

    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => return Json(serde_json::json!({"markers": [], "total": 0})),
    };

    let pref = if params.prefecture.is_empty() {
        &session_pref
    } else {
        &params.prefecture
    };

    if pref.is_empty() {
        return Json(serde_json::json!({
            "markers": [],
            "total": 0,
            "message": "都道府県を選択してください"
        }));
    }

    // GAS再現: 市区町村選択は必須
    if params.municipality.is_empty() {
        return Json(serde_json::json!({
            "markers": [],
            "total": 0,
            "message": "市区町村を選択してください"
        }));
    }

    let radius_km = params.radius.unwrap_or(10.0);

    // 市区町村中心座標を取得（local_db の municipality_geocode テーブル）
    let center = state.local_db.as_ref().and_then(|db| {
        fetch::get_muni_center(db, pref, &params.municipality)
    });

    let markers = if let Some((clat, clng)) = center {
        // Bounding Box + 半径検索（GAS方式）
        fetch::fetch_markers(
            geocoded_db,
            &job_type,
            pref,
            &params.municipality,
            &params.employment_type,
            &params.salary_type,
            clat,
            clng,
            radius_km,
        )
    } else {
        // 座標取得失敗 → 市区町村フィルタで直接取得
        fetch::fetch_markers_by_pref(
            geocoded_db,
            &job_type,
            pref,
            &params.municipality,
            &params.employment_type,
            &params.salary_type,
        )
    };

    markers_to_json(&markers, center)
}

/// 求人詳細カードHTML
pub async fn jobmap_detail(
    State(state): State<Arc<AppState>>,
    Path(posting_id): Path<i64>,
) -> Html<String> {
    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html("<p class='text-gray-400'>データなし</p>".to_string()),
    };

    match fetch::fetch_detail(geocoded_db, posting_id) {
        Some(detail) => Html(render::render_detail_card(&detail)),
        None => Html("<p class='text-gray-400'>求人が見つかりません</p>".to_string()),
    }
}

/// ピン留め統計API
pub async fn jobmap_stats(
    Json(req): Json<stats::StatsRequest>,
) -> Json<stats::StatsResult> {
    Json(stats::compute_stats(&req))
}

/// 都道府県→市区町村カスケード
pub async fn jobmap_municipalities(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<MuniParams>,
) -> Html<String> {
    let (job_type, _, _) = get_session_filters(&session).await;

    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(build_option("", "-- 市区町村 --")),
    };

    let munis = fetch::fetch_municipalities(geocoded_db, &job_type, &params.prefecture);
    let options: String = std::iter::once(build_option("", "-- 市区町村 --"))
        .chain(munis.iter().map(|m| build_option(m, m)))
        .collect::<Vec<_>>()
        .join("\n");

    Html(options)
}

fn markers_to_json(
    markers: &[fetch::MarkerRow],
    center: Option<(f64, f64)>,
) -> Json<serde_json::Value> {
    let marker_arr: Vec<serde_json::Value> = markers
        .iter()
        .map(|m| {
            serde_json::json!({
                "id": m.id,
                "lat": m.lat,
                "lng": m.lng,
                "facility": m.facility_name,
                "service": m.service_type,
                "emp": m.employment_type,
                "salaryType": m.salary_type,
                "salaryMin": m.salary_min,
                "salaryMax": m.salary_max,
            })
        })
        .collect();

    let mut result = serde_json::json!({
        "markers": marker_arr,
        "total": markers.len(),
    });

    if let Some((lat, lng)) = center {
        result["center"] = serde_json::json!({"lat": lat, "lng": lng});
    } else if !markers.is_empty() {
        // マーカーの中心を計算
        let avg_lat: f64 = markers.iter().map(|m| m.lat).sum::<f64>() / markers.len() as f64;
        let avg_lng: f64 = markers.iter().map(|m| m.lng).sum::<f64>() / markers.len() as f64;
        result["center"] = serde_json::json!({"lat": avg_lat, "lng": avg_lng});
    }

    Json(result)
}
