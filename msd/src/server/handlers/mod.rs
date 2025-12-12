mod data;
mod table;

pub use data::handle_data;
pub use table::handle_table;

fn is_msd_client(headers: &axum::http::HeaderMap) -> bool {
  headers
    .get(axum::http::header::USER_AGENT)
    .and_then(|accept| accept.to_str().ok())
    .map(|accept| accept.contains("msd-client"))
    .unwrap_or(false)
}
