use std::sync::OnceLock;

use super::get_client;
use crate::{
  app_config::ShellOptions,
  shell::table_handler::{TableHandler, build_table_handler},
};
use anyhow::{Context, Result};
use futures::StreamExt;
use msd_table::Table;
use tokio::io::AsyncBufReadExt;

pub async fn execute(opts: &ShellOptions, query: &str) -> Result<()> {
  let client = get_client();
  let url = format!("{}/data", opts.server_url);
  let timer = std::time::Instant::now();

  let resp = client
    .post(&url)
    .json(&serde_json::json!({ "query": query }))
    .send()
    .await
    .context("Failed to send query request")?;

  if !resp.status().is_success() {
    let status = resp.status();
    let txt = resp.text().await.unwrap_or_default();
    anyhow::bail!("Query failed: {} - {}", status, txt);
  }

  let handler = build_table_handler(opts);

  // Stream the response body
  let stream = resp.bytes_stream();
  let stream_reader = tokio_util::io::StreamReader::new(
    stream.map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
  );
  let mut reader = tokio::io::BufReader::new(stream_reader).lines();

  let mut fetched_rows = 0;
  let mut objects = 0;
  while let Some(line) = reader.next_line().await? {
    if line.trim().is_empty() {
      continue;
    }
    let table: Table =
      serde_json::from_str(&line).context("Failed to parse table from response")?;
    fetched_rows += table.row_count();
    objects += 1;
    handler.handle(&table)?;
  }

  if opts.verbose {
    let s = timer.elapsed().as_secs_f64();
    eprintln!(
      "Fetched {} objects with {} rows in {:.3} s, {:.0} rows/s",
      objects,
      fetched_rows,
      s,
      fetched_rows as f64 / s
    );
  }

  Ok(())
}
