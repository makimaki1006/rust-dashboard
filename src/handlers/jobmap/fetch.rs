use serde_json::Value;

use crate::db::local_sqlite::LocalDb;

/// マーカー表示用の軽量データ
pub(crate) struct MarkerRow {
    pub(crate) id: i64,
    pub(crate) lat: f64,
    pub(crate) lng: f64,
    pub(crate) facility_name: String,
    pub(crate) service_type: String,
    pub(crate) employment_type: String,
    pub(crate) salary_type: String,
    pub(crate) salary_min: i64,
    pub(crate) salary_max: i64,
}

/// 詳細カード用の全カラムデータ
#[allow(dead_code)]
pub(crate) struct DetailRow {
    pub(crate) id: i64,
    pub(crate) job_type: String,
    pub(crate) prefecture: String,
    pub(crate) municipality: String,
    pub(crate) facility_name: String,
    pub(crate) service_type: String,
    pub(crate) employment_type: String,
    pub(crate) salary_type: String,
    pub(crate) salary_min: i64,
    pub(crate) salary_max: i64,
    pub(crate) salary_detail: String,
    pub(crate) headline: String,
    pub(crate) job_description: String,
    pub(crate) requirements: String,
    pub(crate) benefits: String,
    pub(crate) working_hours: String,
    pub(crate) holidays: String,
    pub(crate) education_training: String,
    pub(crate) access: String,
    pub(crate) special_holidays: String,
    pub(crate) tags: String,
    pub(crate) tier3_label_short: String,
    pub(crate) exp_qual_segment: String,
    pub(crate) lat: f64,
    pub(crate) lng: f64,
    pub(crate) geocode_confidence: i64,
    pub(crate) geocode_level: i64,
}

fn value_to_i64(v: &Value) -> i64 {
    match v {
        Value::Number(n) => n.as_i64().unwrap_or(0),
        Value::String(s) => s.parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn value_to_f64(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn value_to_str(v: Option<&Value>) -> String {
    v.and_then(|v| v.as_str()).unwrap_or("").to_string()
}

/// Haversine距離計算（km）
fn haversine_km(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let r = 6371.0; // 地球の半径 (km)
    let d_lat = (lat2 - lat1).to_radians();
    let d_lng = (lng2 - lng1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}

/// Bounding Box + Haversine距離フィルタでマーカーデータを取得
pub(crate) fn fetch_markers(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
    employment_type: &str,
    salary_type: &str,
    lat: f64,
    lng: f64,
    radius_km: f64,
) -> (Vec<MarkerRow>, usize) {
    let lat_delta = radius_km / 111.0;
    let lng_delta = radius_km / (111.0 * lat.to_radians().cos().abs().max(0.01));
    let lat_min = lat - lat_delta;
    let lat_max = lat + lat_delta;
    let lng_min = lng - lng_delta;
    let lng_max = lng + lng_delta;

    let mut sql = String::from(
        "SELECT id, lat, lng, facility_name, service_type, employment_type, \
         salary_type, salary_min, salary_max \
         FROM postings WHERE job_type = ? \
         AND lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?",
    );
    // rusqlite::types::Value を使い、REAL列にはREAL型でバインド
    // （String→TEXT型だとSQLiteの型比較ルールでBETWEENが常にFALSEになる）
    use rusqlite::types::Value as SqlValue;
    let mut param_values: Vec<SqlValue> = vec![
        SqlValue::Text(job_type.to_string()),
        SqlValue::Real(lat_min),
        SqlValue::Real(lat_max),
        SqlValue::Real(lng_min),
        SqlValue::Real(lng_max),
    ];

    // GAS方式: 半径検索時は prefecture/municipality でフィルタしない
    // 中心座標 + Bounding Box + Haversine で地理的に絞る
    // （隣接県・隣接市区町村の求人も含めるため）
    if !employment_type.is_empty() && employment_type != "全て選択" {
        sql.push_str(" AND employment_type = ?");
        param_values.push(SqlValue::Text(employment_type.to_string()));
    }
    if !salary_type.is_empty() && salary_type != "どちらも" {
        sql.push_str(" AND salary_type = ?");
        param_values.push(SqlValue::Text(salary_type.to_string()));
    }
    // Bounding Boxで粗くフィルタ → Haversineで正確に絞る
    sql.push_str(" LIMIT 50000");

    let params: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match db.query(&sql, &params) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("fetch_markers failed: {e}");
            return (Vec::new(), 0);
        }
    };

    // Bounding Box結果からHaversine距離で正確な円内フィルタ
    let mut result: Vec<MarkerRow> = rows
        .iter()
        .filter_map(|r| {
            let m_lat = r.get("lat").map(value_to_f64).unwrap_or(0.0);
            let m_lng = r.get("lng").map(value_to_f64).unwrap_or(0.0);
            let dist = haversine_km(lat, lng, m_lat, m_lng);
            if dist <= radius_km {
                Some(MarkerRow {
                    id: r.get("id").map(value_to_i64).unwrap_or(0),
                    lat: m_lat,
                    lng: m_lng,
                    facility_name: value_to_str(r.get("facility_name")),
                    service_type: value_to_str(r.get("service_type")),
                    employment_type: value_to_str(r.get("employment_type")),
                    salary_type: value_to_str(r.get("salary_type")),
                    salary_min: r.get("salary_min").map(value_to_i64).unwrap_or(0),
                    salary_max: r.get("salary_max").map(value_to_i64).unwrap_or(0),
                })
            } else {
                None
            }
        })
        .collect();

    let total_available = result.len();
    result.truncate(5000);
    (result, total_available)
}

/// 都道府県指定でマーカーを取得（半径なし・Bounding Boxなし）
pub(crate) fn fetch_markers_by_pref(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
    municipality: &str,
    employment_type: &str,
    salary_type: &str,
) -> (Vec<MarkerRow>, usize) {
    let mut sql = String::from(
        "SELECT id, lat, lng, facility_name, service_type, employment_type, \
         salary_type, salary_min, salary_max \
         FROM postings WHERE job_type = ? AND prefecture = ? AND lat IS NOT NULL",
    );
    let mut param_values: Vec<String> = vec![job_type.to_string(), prefecture.to_string()];

    if !municipality.is_empty() {
        let munis: Vec<&str> = municipality.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        if munis.len() == 1 {
            sql.push_str(" AND municipality = ?");
            param_values.push(munis[0].to_string());
        } else {
            let placeholders: Vec<&str> = munis.iter().map(|_| "?").collect();
            sql.push_str(&format!(" AND municipality IN ({})", placeholders.join(", ")));
            for m in &munis {
                param_values.push(m.to_string());
            }
        }
    }
    if !employment_type.is_empty() && employment_type != "全て選択" {
        sql.push_str(" AND employment_type = ?");
        param_values.push(employment_type.to_string());
    }
    if !salary_type.is_empty() && salary_type != "どちらも" {
        sql.push_str(" AND salary_type = ?");
        param_values.push(salary_type.to_string());
    }
    sql.push_str(" LIMIT 50000");

    let params: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match db.query(&sql, &params) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("fetch_markers_by_pref failed: {e}");
            return (Vec::new(), 0);
        }
    };

    let mut result: Vec<MarkerRow> = rows.iter()
        .map(|r| MarkerRow {
            id: r.get("id").map(value_to_i64).unwrap_or(0),
            lat: r.get("lat").map(value_to_f64).unwrap_or(0.0),
            lng: r.get("lng").map(value_to_f64).unwrap_or(0.0),
            facility_name: value_to_str(r.get("facility_name")),
            service_type: value_to_str(r.get("service_type")),
            employment_type: value_to_str(r.get("employment_type")),
            salary_type: value_to_str(r.get("salary_type")),
            salary_min: r.get("salary_min").map(value_to_i64).unwrap_or(0),
            salary_max: r.get("salary_max").map(value_to_i64).unwrap_or(0),
        })
        .collect();

    let total_available = result.len();
    result.truncate(5000);
    (result, total_available)
}

/// 求人詳細を1件取得
pub(crate) fn fetch_detail(db: &LocalDb, posting_id: i64) -> Option<DetailRow> {
    let rows = db
        .query(
            "SELECT id, job_type, prefecture, municipality, facility_name, service_type, \
             employment_type, salary_type, salary_min, salary_max, salary_detail, \
             headline, job_description, requirements, benefits, working_hours, \
             holidays, education_training, access, special_holidays, tags, \
             tier3_label_short, exp_qual_segment, lat, lng, \
             geocode_confidence, geocode_level \
             FROM postings WHERE id = ?",
            &[&posting_id as &dyn rusqlite::types::ToSql],
        )
        .ok()?;

    let r = rows.first()?;
    Some(DetailRow {
        id: r.get("id").map(value_to_i64).unwrap_or(0),
        job_type: value_to_str(r.get("job_type")),
        prefecture: value_to_str(r.get("prefecture")),
        municipality: value_to_str(r.get("municipality")),
        facility_name: value_to_str(r.get("facility_name")),
        service_type: value_to_str(r.get("service_type")),
        employment_type: value_to_str(r.get("employment_type")),
        salary_type: value_to_str(r.get("salary_type")),
        salary_min: r.get("salary_min").map(value_to_i64).unwrap_or(0),
        salary_max: r.get("salary_max").map(value_to_i64).unwrap_or(0),
        salary_detail: value_to_str(r.get("salary_detail")),
        headline: value_to_str(r.get("headline")),
        job_description: value_to_str(r.get("job_description")),
        requirements: value_to_str(r.get("requirements")),
        benefits: value_to_str(r.get("benefits")),
        working_hours: value_to_str(r.get("working_hours")),
        holidays: value_to_str(r.get("holidays")),
        education_training: value_to_str(r.get("education_training")),
        access: value_to_str(r.get("access")),
        special_holidays: value_to_str(r.get("special_holidays")),
        tags: value_to_str(r.get("tags")),
        tier3_label_short: value_to_str(r.get("tier3_label_short")),
        exp_qual_segment: value_to_str(r.get("exp_qual_segment")),
        lat: r.get("lat").map(value_to_f64).unwrap_or(0.0),
        lng: r.get("lng").map(value_to_f64).unwrap_or(0.0),
        geocode_confidence: r.get("geocode_confidence").map(value_to_i64).unwrap_or(0),
        geocode_level: r.get("geocode_level").map(value_to_i64).unwrap_or(0),
    })
}

/// 都道府県→市区町村カスケード用データ取得
pub(crate) fn fetch_municipalities(
    db: &LocalDb,
    job_type: &str,
    prefecture: &str,
) -> Vec<String> {
    let rows = db
        .query(
            "SELECT DISTINCT municipality FROM postings \
             WHERE job_type = ? AND prefecture = ? AND municipality != '' \
             ORDER BY municipality",
            &[
                &job_type as &dyn rusqlite::types::ToSql,
                &prefecture as &dyn rusqlite::types::ToSql,
            ],
        )
        .unwrap_or_default();

    rows.iter()
        .filter_map(|r| r.get("municipality").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect()
}

/// 都道府県の中心座標を取得（municipality_geocode テーブルから）
pub(crate) fn get_pref_center(
    local_db: &LocalDb,
    prefecture: &str,
) -> Option<(f64, f64)> {
    let rows = local_db
        .query(
            "SELECT AVG(latitude) as lat, AVG(longitude) as lng \
             FROM municipality_geocode WHERE prefecture = ?",
            &[&prefecture as &dyn rusqlite::types::ToSql],
        )
        .ok()?;
    let r = rows.first()?;
    let lat = r.get("lat").and_then(|v| v.as_f64())?;
    let lng = r.get("lng").and_then(|v| v.as_f64())?;
    if lat == 0.0 || lng == 0.0 {
        return None;
    }
    Some((lat, lng))
}

/// 市区町村の中心座標を取得
pub(crate) fn get_muni_center(
    local_db: &LocalDb,
    prefecture: &str,
    municipality: &str,
) -> Option<(f64, f64)> {
    let rows = local_db
        .query(
            "SELECT latitude, longitude FROM municipality_geocode \
             WHERE prefecture = ? AND municipality = ?",
            &[
                &prefecture as &dyn rusqlite::types::ToSql,
                &municipality as &dyn rusqlite::types::ToSql,
            ],
        )
        .ok()?;
    let r = rows.first()?;
    let lat = r.get("latitude").and_then(|v| v.as_f64())?;
    let lng = r.get("longitude").and_then(|v| v.as_f64())?;
    Some((lat, lng))
}

/// 指定職種がgeocode_dbに存在するかチェック
pub(crate) fn has_job_type_data(db: &LocalDb, job_type: &str) -> bool {
    let rows = db.query(
        "SELECT 1 FROM postings WHERE job_type = ? LIMIT 1",
        &[&job_type as &dyn rusqlite::types::ToSql],
    );
    matches!(rows, Ok(ref r) if !r.is_empty())
}

/// ビューポート矩形内のマーカーを取得（V2バックポート）
pub(crate) fn fetch_markers_by_bounds(
    db: &LocalDb,
    job_type: &str,
    employment_type: &str,
    salary_type: &str,
    south: f64,
    north: f64,
    west: f64,
    east: f64,
) -> (Vec<MarkerRow>, usize) {
    let mut sql = String::from(
        "SELECT id, lat, lng, facility_name, service_type, employment_type, \
         salary_type, salary_min, salary_max \
         FROM postings WHERE job_type = ? \
         AND lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?",
    );
    use rusqlite::types::Value as SqlValue;
    let mut param_values: Vec<SqlValue> = vec![
        SqlValue::Text(job_type.to_string()),
        SqlValue::Real(south),
        SqlValue::Real(north),
        SqlValue::Real(west),
        SqlValue::Real(east),
    ];

    if !employment_type.is_empty() && employment_type != "全て選択" {
        sql.push_str(" AND employment_type = ?");
        param_values.push(SqlValue::Text(employment_type.to_string()));
    }
    if !salary_type.is_empty() && salary_type != "どちらも" {
        sql.push_str(" AND salary_type = ?");
        param_values.push(SqlValue::Text(salary_type.to_string()));
    }
    sql.push_str(" LIMIT 50000");

    let params: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let rows = match db.query(&sql, &params) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("fetch_markers_by_bounds failed: {e}");
            return (Vec::new(), 0);
        }
    };

    let mut result: Vec<MarkerRow> = rows
        .iter()
        .map(|r| MarkerRow {
            id: r.get("id").map(value_to_i64).unwrap_or(0),
            lat: r.get("lat").map(value_to_f64).unwrap_or(0.0),
            lng: r.get("lng").map(value_to_f64).unwrap_or(0.0),
            facility_name: value_to_str(r.get("facility_name")),
            service_type: value_to_str(r.get("service_type")),
            employment_type: value_to_str(r.get("employment_type")),
            salary_type: value_to_str(r.get("salary_type")),
            salary_min: r.get("salary_min").map(value_to_i64).unwrap_or(0),
            salary_max: r.get("salary_max").map(value_to_i64).unwrap_or(0),
        })
        .collect();

    let total_available = result.len();
    result.truncate(5000);
    (result, total_available)
}

/// 都道府県一覧取得
pub(crate) fn fetch_prefectures(db: &LocalDb, job_type: &str) -> Vec<String> {
    let rows = db
        .query(
            "SELECT DISTINCT prefecture FROM postings WHERE job_type = ? ORDER BY prefecture",
            &[&job_type as &dyn rusqlite::types::ToSql],
        )
        .unwrap_or_default();

    rows.iter()
        .filter_map(|r| r.get("prefecture").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect()
}
