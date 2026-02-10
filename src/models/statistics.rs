use serde::{Deserialize, Serialize};

/// 全国統計
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NationalStats {
    pub total_applicants: i64,
    pub total_prefectures: i64,
    pub total_municipalities: i64,
    pub avg_age: f64,
    pub male_ratio: f64,
    pub female_ratio: f64,
}

/// 都道府県統計
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PrefectureStats {
    pub prefecture: String,
    pub applicant_count: i64,
    pub municipality_count: i64,
    pub avg_age: f64,
    pub male_ratio: f64,
    pub female_ratio: f64,
}

/// 年齢・性別分布
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgeGenderEntry {
    pub age_group: String,
    pub male: i64,
    pub female: i64,
    pub total: i64,
}

/// フローデータ（人材移動）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowEntry {
    pub source: String,
    pub target: String,
    pub count: i64,
    pub flow_type: Option<String>,
}

/// 需給ギャップデータ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupplyDemandGap {
    pub municipality: String,
    pub prefecture: String,
    pub supply: f64,
    pub demand: f64,
    pub gap: f64,
    pub gap_ratio: f64,
}

/// 緊急度分布
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrgencyEntry {
    pub category: String,
    pub count: i64,
    pub ratio: f64,
}

/// 競合概況
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompetitionOverview {
    pub total_postings: i64,
    pub avg_salary: f64,
    pub competition_index: f64,
    pub top_facilities: Vec<(String, i64)>,
}
