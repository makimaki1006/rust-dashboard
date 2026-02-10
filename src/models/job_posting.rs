use serde::{Deserialize, Serialize};

/// 求人データ構造体（ローカルSQLiteから取得）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPosting {
    pub id: Option<i64>,
    pub job_type: String,
    pub facility_name: String,
    pub prefecture: String,
    pub municipality: String,
    pub employment_type: Option<String>,
    pub salary_min: Option<i64>,
    pub salary_max: Option<i64>,
    pub facility_type: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

/// 求人統計
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPostingStats {
    pub count: i64,
    pub has_data: bool,
    pub avg_salary_min: Option<f64>,
    pub avg_salary_max: Option<f64>,
    pub employment_type_counts: Vec<(String, i64)>,
}
