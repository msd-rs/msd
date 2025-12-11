use crate::{app_config::ShellOptions, shell::table_handler::TableHandler};
use anyhow::Result;
use msd_table::Table;

pub async fn execute(opts: &ShellOptions, query: &str) -> Result<()> {
  //TODO: do query
  // 1. build full endpoint form opts.server_url
  // 2. build TableHandler from opts by `build_table_handler`
  // 3. send request
  // 4. parse response, call handler, please note the 'application/x-ndjson' response should be parsed line by line and call handler for each line
  Ok(())
}
