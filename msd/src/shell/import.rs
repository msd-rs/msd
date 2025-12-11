use crate::{app_config::ShellOptions, shell::table_handler::TableHandler};
use anyhow::Result;
use msd_table::Table;

pub async fn execute(opts: &ShellOptions, table: &str, file_path: &str) -> Result<()> {
  //TODO: import the csv file to the table
  // 1. build full endpoint form opts.server_url
  // 2. send request
  // please note: the file may be too large, so we should send it stream instead of all at once

  Ok(())
}
