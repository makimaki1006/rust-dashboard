use axum::extract::State;
use axum::response::Html;
use std::sync::Arc;
use tower_sessions::Session;
use serde_json::Value;

use crate::AppState;

use super::overview::get_session_filters;

/// タブ6: 求人マップ（Leaflet地図 + マーカー）
pub async fn tab_jobmap(
    State(state): State<Arc<AppState>>,
    session: Session,
) -> Html<String> {
    let (job_type, prefecture, _municipality) = get_session_filters(&session).await;

    let cache_key = format!("jobmap_{}_{}", job_type, prefecture);
    if let Some(cached) = state.cache.get(&cache_key) {
        if let Some(html) = cached.as_str() {
            return Html(html.to_string());
        }
    }

    let html = render_jobmap(&job_type, &prefecture);
    state.cache.set(cache_key, Value::String(html.clone()));
    Html(html)
}

fn render_jobmap(job_type: &str, prefecture: &str) -> String {
    include_str!("../../templates/tabs/jobmap.html")
        .replace("{{JOB_TYPE}}", job_type)
        .replace("{{PREFECTURE}}", prefecture)
}
