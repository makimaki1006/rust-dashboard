use axum::extract::{Path, Query, State};
use axum::response::Html;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;
use tower_sessions::Session;

use crate::AppState;
use crate::geo::pref_name_to_code;
use crate::handlers::competitive::{build_option, escape_html};
use crate::handlers::overview::get_session_filters;
use crate::handlers::talentmap;

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
    // ビューポートbounds検索用（V2バックポート）
    #[serde(default)]
    pub south: Option<f64>,
    #[serde(default)]
    pub north: Option<f64>,
    #[serde(default)]
    pub west: Option<f64>,
    #[serde(default)]
    pub east: Option<f64>,
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
                    <h2 class="text-2xl mb-4"><svg class='inline w-5 h-5 mr-1 -mt-0.5' fill='none' stroke='currentColor' stroke-width='1.5' viewBox='0 0 24 24'><path stroke-linecap='round' stroke-linejoin='round' d='M9 6.75V15m6-6v8.25m.503 3.498l4.875-2.437c.381-.19.622-.58.622-1.006V4.82c0-.836-.88-1.38-1.628-1.006l-3.869 1.934c-.317.159-.69.159-1.006 0L9.503 3.252a1.125 1.125 0 00-1.006 0L3.622 5.689C3.24 5.88 3 6.27 3 6.695V19.18c0 .836.88 1.38 1.628 1.006l3.869-1.934c.317-.159.69-.159 1.006 0l4.994 2.497c.317.158.69.158 1.006 0z'/></svg>  求人地図</h2>
                    <p>求人地図データベースが読み込まれていません。</p>
                    <p class="text-sm mt-2">data/geocoded_postings.db.gz を配置してください。</p>
                </div>"#
                    .to_string(),
            );
        }
    };

    // 選択職種のデータ存在チェック + 都道府県一覧取得（spawn_blocking）
    let db_clone = geocoded_db.clone();
    let jt = job_type.clone();
    let (has_data, prefs) = match tokio::task::spawn_blocking(move || {
        let has = fetch::has_job_type_data(&db_clone, &jt);
        let prefs = if has { fetch::fetch_prefectures(&db_clone, &jt) } else { vec![] };
        (has, prefs)
    }).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html("<p class='text-red-400'>データ取得エラー</p>".to_string());
        }
    };

    if !has_data {
        return Html(render::render_no_data_message(&job_type));
    }
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

    // ビューポートbounds検索（V2バックポート）
    // south/north/west/east が全て指定された場合、矩形検索を優先
    if let (Some(s), Some(n), Some(w), Some(e)) =
        (params.south, params.north, params.west, params.east)
    {
        let db_clone = geocoded_db.clone();
        let jt = job_type.clone();
        let emp = params.employment_type.clone();
        let sal = params.salary_type.clone();
        let (markers, total_available) = match tokio::task::spawn_blocking(move || {
            fetch::fetch_markers_by_bounds(&db_clone, &jt, &emp, &sal, s, n, w, e)
        }).await {
            Ok(r) => r,
            Err(err) => {
                tracing::error!("spawn_blocking failed: {err}");
                return Json(serde_json::json!({"markers": [], "total": 0}));
            }
        };
        return markers_to_json(&markers, None, total_available);
    }

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
    let center = if let Some(db) = &state.local_db {
        let db_clone = db.clone();
        let pref_owned = pref.to_string();
        let muni_owned = params.municipality.clone();
        match tokio::task::spawn_blocking(move || {
            fetch::get_muni_center(&db_clone, &pref_owned, &muni_owned)
                .or_else(|| {
                    extract_parent_city(&muni_owned)
                        .and_then(|parent| fetch::get_muni_center(&db_clone, &pref_owned, &parent))
                })
        }).await {
            Ok(c) => c,
            Err(_) => None,
        }
    } else {
        None
    };

    let gdb_clone = geocoded_db.clone();
    let jt = job_type.clone();
    let pref_owned = pref.to_string();
    let muni_owned = params.municipality.clone();
    let emp_owned = params.employment_type.clone();
    let sal_owned = params.salary_type.clone();
    let (markers, total_available) = match tokio::task::spawn_blocking(move || {
        if let Some((clat, clng)) = center {
            fetch::fetch_markers(
                &gdb_clone, &jt, &pref_owned, "", &emp_owned, &sal_owned,
                clat, clng, radius_km,
            )
        } else {
            fetch::fetch_markers_by_pref(
                &gdb_clone, &jt, &pref_owned, &muni_owned, &emp_owned, &sal_owned,
            )
        }
    }).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Json(serde_json::json!({"markers": [], "total": 0}));
        }
    };

    markers_to_json(&markers, center, total_available)
}

/// 求人詳細カードHTML
pub async fn jobmap_detail(
    State(state): State<Arc<AppState>>,
    Path(posting_id): Path<i64>,
) -> Html<String> {
    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => return Html(crate::handlers::render_empty_state("データなし", "求人データベースに接続できません")),
    };

    let db_clone = geocoded_db.clone();
    match tokio::task::spawn_blocking(move || {
        fetch::fetch_detail(&db_clone, posting_id)
    }).await {
        Ok(Some(detail)) => Html(render::render_detail_card(&detail)),
        Ok(None) => Html(crate::handlers::render_empty_state("求人が見つかりません", "指定された求人は存在しないか削除されています")),
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            Html("<p class='text-red-400'>データ取得エラー</p>".to_string())
        }
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

    let db_clone = geocoded_db.clone();
    let jt = job_type.clone();
    let pref_owned = params.prefecture.clone();
    let munis = match tokio::task::spawn_blocking(move || {
        fetch::fetch_municipalities(&db_clone, &jt, &pref_owned)
    }).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Html(build_option("", "-- 市区町村 --"));
        }
    };
    let options: String = std::iter::once(build_option("", "-- 市区町村 --"))
        .chain(munis.iter().map(|m| build_option(m, m)))
        .collect::<Vec<_>>()
        .join("\n");

    Html(options)
}

/// 求人詳細JSON API（ピンカード用、全フィールド返却）
pub async fn jobmap_detail_json(
    State(state): State<Arc<AppState>>,
    Path(posting_id): Path<i64>,
) -> Json<serde_json::Value> {
    let geocoded_db = match &state.geocoded_db {
        Some(db) => db,
        None => return Json(serde_json::json!({})),
    };

    let db_clone = geocoded_db.clone();
    let detail_opt = match tokio::task::spawn_blocking(move || {
        fetch::fetch_detail(&db_clone, posting_id)
    }).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!("spawn_blocking failed: {e}");
            return Json(serde_json::json!({}));
        }
    };
    match detail_opt {
        Some(d) => Json(serde_json::json!({
            "facility_name": d.facility_name,
            "service_type": d.service_type,
            "access": d.access,
            "employment_type": d.employment_type,
            "salary_type": d.salary_type,
            "salary_min": d.salary_min,
            "salary_max": d.salary_max,
            "salary_detail": d.salary_detail,
            "headline": d.headline,
            "job_description": d.job_description,
            "requirements": d.requirements,
            "benefits": d.benefits,
            "working_hours": d.working_hours,
            "holidays": d.holidays,
            "education_training": d.education_training,
            "special_holidays": d.special_holidays,
            "tags": d.tags,
            "tier3_label_short": d.tier3_label_short,
            "exp_qual_segment": d.exp_qual_segment,
            "geocode_confidence": d.geocode_confidence,
            "geocode_level": d.geocode_level,
        })),
        None => Json(serde_json::json!({})),
    }
}

// ===== 求職者データAPI（Tab 7 統合） =====

#[derive(Deserialize)]
pub struct SeekerParams {
    #[serde(default)]
    pub prefecture: String,
    #[serde(default)]
    pub municipality: String,
}

/// 求職者マーカー + フロー + コロプレスJSON API: /api/jobmap/seekers
pub async fn jobmap_seekers(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SeekerParams>,
) -> Json<serde_json::Value> {
    let (job_type, session_pref, session_muni) = get_session_filters(&session).await;

    let pref = if params.prefecture.is_empty() {
        &session_pref
    } else {
        &params.prefecture
    };
    let muni = if params.municipality.is_empty() {
        &session_muni
    } else {
        &params.municipality
    };

    if pref.is_empty() {
        return Json(serde_json::json!({
            "markers": [],
            "flows": [],
            "choropleth": {},
            "total": 0,
            "message": "都道府県を選択してください"
        }));
    }

    // talentmap.rsのfetch_talentmapを再利用
    let stats = talentmap::fetch_talentmap(&state, &job_type, pref, muni).await;

    // マーカーJSON
    let markers_json_str = talentmap::build_markers_json(&stats.markers);
    let markers_val: serde_json::Value =
        serde_json::from_str(&markers_json_str).unwrap_or(serde_json::json!([]));

    // フローJSON
    let flows_json_str = talentmap::build_flows_json(&stats.flows);
    let flows_val: serde_json::Value =
        serde_json::from_str(&flows_json_str).unwrap_or(serde_json::json!([]));

    // コロプレススタイル
    let muni_for_choropleth = if muni.is_empty() || muni == "すべて" { "" } else { muni };
    let choropleth_str = talentmap::build_choropleth_styles(&stats.markers, muni_for_choropleth);
    let choropleth_val: serde_json::Value =
        serde_json::from_str(&choropleth_str).unwrap_or(serde_json::json!({}));

    // GeoJSON URL
    let geojson_url = {
        let code_map = pref_name_to_code();
        if let Some(code) = code_map.get(pref.as_str()) {
            let romaji = talentmap::pref_code_to_romaji(code);
            format!("/api/geojson/{}_{}.json", code, romaji)
        } else {
            String::new()
        }
    };

    // 中心座標
    let (center_lat, center_lng) = if !stats.markers.is_empty() {
        let avg_lat = stats.markers.iter().map(|m| m.lat).sum::<f64>() / stats.markers.len() as f64;
        let avg_lng = stats.markers.iter().map(|m| m.lng).sum::<f64>() / stats.markers.len() as f64;
        (avg_lat, avg_lng)
    } else {
        (36.5, 138.0)
    };

    Json(serde_json::json!({
        "markers": markers_val,
        "flows": flows_val,
        "choropleth": choropleth_val,
        "geojsonUrl": geojson_url,
        "total": stats.total_count,
        "center": {"lat": center_lat, "lng": center_lng}
    }))
}

/// 求職者詳細サイドバーHTML API: /api/jobmap/seeker-detail
pub async fn jobmap_seeker_detail(
    State(state): State<Arc<AppState>>,
    session: Session,
    Query(params): Query<SeekerParams>,
) -> Html<String> {
    let (job_type, session_pref, _session_muni) = get_session_filters(&session).await;

    let pref = if params.prefecture.is_empty() {
        &session_pref
    } else {
        &params.prefecture
    };
    let muni = if params.municipality.is_empty() { "" } else { &params.municipality };

    if pref.is_empty() || muni.is_empty() {
        return Html(r#"<p class="text-gray-400 text-sm">市区町村を選択してください</p>"#.to_string());
    }

    // 市区町村詳細データ取得
    let detail = talentmap::fetch_muni_detail(&state, &job_type, pref, muni).await;

    // マーカーデータ取得（基本情報表示用）
    let marker_sql = "SELECT latitude, longitude, applicant_count, male_count, female_count \
                      FROM job_seeker_data \
                      WHERE job_type = ? AND row_type = 'SUMMARY' AND prefecture = ? AND municipality = ?";
    let marker_params = vec![
        serde_json::Value::String(job_type),
        serde_json::Value::String(pref.to_string()),
        serde_json::Value::String(muni.to_string()),
    ];

    use crate::handlers::overview::{get_f64, get_i64};
    let markers: Vec<talentmap::MarkerData> = if let Ok(rows) = state.turso.query(marker_sql, &marker_params).await {
        rows.iter().map(|row| talentmap::MarkerData {
            municipality: muni.to_string(),
            prefecture: pref.to_string(),
            lat: get_f64(row, "latitude"),
            lng: get_f64(row, "longitude"),
            count: get_i64(row, "applicant_count"),
            male_count: get_i64(row, "male_count"),
            female_count: get_i64(row, "female_count"),
        }).collect()
    } else {
        Vec::new()
    };

    // talentmap.rsのbuild_sidebarを再利用
    Html(talentmap::build_sidebar(muni, pref, &detail, &markers))
}

fn markers_to_json(
    markers: &[fetch::MarkerRow],
    center: Option<(f64, f64)>,
    total_available: usize,
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
        "totalAvailable": total_available,
    });

    if let Some((lat, lng)) = center {
        result["center"] = serde_json::json!({"lat": lat, "lng": lng});
    } else if !markers.is_empty() {
        let avg_lat: f64 = markers.iter().map(|m| m.lat).sum::<f64>() / markers.len() as f64;
        let avg_lng: f64 = markers.iter().map(|m| m.lng).sum::<f64>() / markers.len() as f64;
        result["center"] = serde_json::json!({"lat": avg_lat, "lng": avg_lng});
    }

    Json(result)
}

/// 政令指定都市の区名から親市名を抽出
/// 例: "大阪市北区" → Some("大阪市"), "新宿区" → None, "高崎市" → None
fn extract_parent_city(municipality: &str) -> Option<String> {
    // 「○○市△△区」パターンを検出（政令指定都市の区）
    // 市の後に区が続く場合のみマッチ
    if let Some(shi_pos) = municipality.find('市') {
        let after_shi = &municipality[shi_pos + '市'.len_utf8()..];
        if after_shi.ends_with('区') && !after_shi.is_empty() {
            // "大阪市北区" → "大阪市"
            return Some(municipality[..shi_pos + '市'.len_utf8()].to_string());
        }
    }
    None
}
