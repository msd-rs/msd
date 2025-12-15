mod import;
mod query;

pub use import::handle_table;
pub use query::handle_data;

fn is_msd_client(headers: &axum::http::HeaderMap) -> bool {
  headers
    .get(axum::http::header::USER_AGENT)
    .and_then(|accept| accept.to_str().ok())
    .map(|accept| accept.contains("msd-client"))
    .unwrap_or(false)
}

fn is_msd_table_format(headers: &axum::http::HeaderMap) -> bool {
  headers
    .get(axum::http::header::CONTENT_TYPE)
    .and_then(|accept| accept.to_str().ok())
    .map(|accept| accept.contains("application/msd-table"))
    .unwrap_or(false)
}
