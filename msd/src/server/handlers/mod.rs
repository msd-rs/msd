// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

mod import;
pub mod permission;
mod query;

pub use import::handle_table;
pub use query::handle_data;

use crate::app_config::{MSD_TABLE_FORMAT, MSD_USER_AGENT};

fn is_msd_client(headers: &axum::http::HeaderMap) -> bool {
  headers
    .get(axum::http::header::USER_AGENT)
    .and_then(|accept| accept.to_str().ok())
    .map(|accept| accept.contains(MSD_USER_AGENT))
    .unwrap_or(false)
}

fn is_msd_table_format(headers: &axum::http::HeaderMap) -> bool {
  headers
    .get(axum::http::header::CONTENT_TYPE)
    .and_then(|accept| accept.to_str().ok())
    .map(|accept| accept.contains(MSD_TABLE_FORMAT))
    .unwrap_or(false)
}
