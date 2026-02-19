mod analysis;
mod fetch;
mod handlers;
mod render;
mod utils;

#[cfg(test)]
mod tests;

// ハンドラ（lib.rsから handlers::competitive::* として参照）
pub use handlers::{
    comp_analysis, comp_analysis_filtered, comp_facility_types, comp_filter,
    comp_municipalities, comp_report, tab_competitive,
};

// 他モジュールから参照されるユーティリティ（api.rs, segment.rs）
pub use utils::build_option;
pub use utils::escape_html;

