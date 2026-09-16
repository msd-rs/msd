// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

mod read;
mod single;

use crate::app_config::ShellOptions;
use anyhow::Result;
use single::{import_csv, import_table};

pub async fn execute(
  opts: &ShellOptions,
  table: &str,
  file_path: &str,
  skip: usize,
  delimiter: u8,
) -> Result<()> {
  if file_path.ends_with(".csv") || file_path.ends_with(".txt") || file_path.eq("--") {
    return import_csv(opts, table, file_path, skip, delimiter).await;
  } else if file_path.ends_with(".tbl") || file_path.eq("---") {
    return import_table(opts, table, file_path, skip, delimiter).await;
  }
  Ok(())
}
