mod fetch;
mod handlers;
mod render;
mod stats;

pub use handlers::{
    tab_jobmap, jobmap_markers, jobmap_detail, jobmap_stats, jobmap_municipalities,
};
