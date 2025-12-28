// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use super::get_client;
use crate::{
  app_config::{MSD_TABLE_FORMAT, ShellOptions},
  server::QUERY_PATH,
  shell::table_handler::{CsvHandler, TableHandler, build_table_handler},
};
use anyhow::{Context, Result};
use colored::Colorize;
use futures::StreamExt;
use msd_request::{check_table_frame, unpack_table_frame};
use msd_table::Table;
use reqwest::header;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tracing::error;

pub async fn execute(opts: &ShellOptions, query: &str) -> Result<()> {
  let client = get_client(opts);
  let url = format!("{}{}", opts.server_url, QUERY_PATH);
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

  let handler: Box<dyn TableHandler> = if let Some(path) = &opts.output_file {
    let file = std::fs::OpenOptions::new()
      .append(true)
      .create(true)
      .open(path)
      .context("Failed to open output file")?;
    Box::new(CsvHandler::new(Box::new(file)))
  } else {
    Box::new(build_table_handler(opts))
  };

  let is_table_frame = resp
    .headers()
    .get(header::CONTENT_TYPE)
    .is_some_and(|ct| ct.to_str().is_ok_and(|ct| ct.contains(MSD_TABLE_FORMAT)));

  let mut fetched_rows = 0;
  let mut objects = 0;
  // Stream the response body
  let stream = resp.bytes_stream();
  let stream_reader = tokio_util::io::StreamReader::new(
    stream.map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
  );
  if is_table_frame {
    let mut rd = tokio::io::BufReader::with_capacity(64 * 1024, stream_reader);

    let mut buf = Vec::with_capacity(8 * 1024);

    buf.resize(8, 0);
    while rd.read_exact(&mut buf).await.is_ok() {
      let (header_size, data_size) = match check_table_frame(&buf) {
        Ok(size) => size,
        Err(err) => {
          error!(%err, "invalid table frame");
          break;
        }
      };

      buf.resize(header_size + data_size, 0);
      rd.read_exact(&mut buf[header_size..]).await?;
      let table = unpack_table_frame(&buf, false)?;

      fetched_rows += table.row_count();
      objects += 1;
      handler.handle(&table)?;

      buf.clear();
      buf.resize(header_size, 0);
    }
  } else {
    // response is ndjson
    let mut reader = tokio::io::BufReader::new(stream_reader).lines();

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
  }

  if opts.verbose {
    let s = timer.elapsed().as_secs_f64();
    let stat = format!(
      "Fetched {} objects with {} rows in {:.3} s, {:.0} rows/s",
      objects,
      fetched_rows,
      s,
      fetched_rows as f64 / s
    );
    eprintln!("{}", stat.cyan());
  }

  Ok(())
}
