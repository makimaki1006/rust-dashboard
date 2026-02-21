use serde::{Deserialize, Serialize};

/// ピン留め施設の給与統計リクエスト
#[derive(Deserialize)]
pub struct StatsRequest {
    pub(crate) salary_mins: Vec<i64>,
    pub(crate) salary_maxs: Vec<i64>,
}

/// 統計結果
#[derive(Serialize)]
pub struct StatsResult {
    pub(crate) count: usize,
    pub(crate) min_avg: i64,
    pub(crate) min_median: i64,
    pub(crate) min_mode: i64,
    pub(crate) max_avg: i64,
    pub(crate) max_median: i64,
    pub(crate) max_mode: i64,
}

pub(crate) fn compute_stats(req: &StatsRequest) -> StatsResult {
    let mins: Vec<i64> = req.salary_mins.iter().copied().filter(|&v| v > 0).collect();
    let maxs: Vec<i64> = req.salary_maxs.iter().copied().filter(|&v| v > 0).collect();
    let count = mins.len().max(maxs.len());

    StatsResult {
        count,
        min_avg: average(&mins),
        min_median: median(&mins),
        min_mode: mode(&mins),
        max_avg: average(&maxs),
        max_median: median(&maxs),
        max_mode: mode(&maxs),
    }
}

fn average(vals: &[i64]) -> i64 {
    if vals.is_empty() {
        return 0;
    }
    let sum: i64 = vals.iter().sum();
    sum / vals.len() as i64
}

fn median(vals: &[i64]) -> i64 {
    if vals.is_empty() {
        return 0;
    }
    let mut sorted = vals.to_vec();
    sorted.sort();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2
    } else {
        sorted[mid]
    }
}

fn mode(vals: &[i64]) -> i64 {
    if vals.is_empty() {
        return 0;
    }
    let mut freq = std::collections::HashMap::new();
    for &v in vals {
        *freq.entry(v).or_insert(0u32) += 1;
    }
    freq.into_iter()
        .max_by_key(|&(_, count)| count)
        .map(|(val, _)| val)
        .unwrap_or(0)
}
