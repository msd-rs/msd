// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::{app_config::ShellOptions, server::TABLE_PUT_PATH};
use anyhow::{Context, Result};
use colored::Colorize;
use reqwest::Body;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

pub async fn execute(opts: &ShellOptions, table: &str, file_path: &str, skip: usize) -> Result<()> {
  let file = File::open(file_path)
    .await
    .context(format!("Failed to open file: {}", file_path))?;
  let stream = ReaderStream::with_capacity(file, 8 * 1024 * 1024);
  let body = Body::wrap_stream(stream);

  let client = reqwest::Client::new();
  let url = format!("{}{}{}", opts.server_url, TABLE_PUT_PATH, table);

  let resp = client
    .put(&url)
    .query(&[("skip", skip.to_string())])
    .header(reqwest::header::CONTENT_TYPE, "text/csv")
    .body(body)
    .send()
    .await
    .context("Failed to send import request")?;

  if !resp.status().is_success() {
    let status = resp.status();
    let txt = resp.text().await.unwrap_or_default();
    anyhow::bail!("Import failed: {} - {}", status, txt);
  } else {
    let txt = resp.text().await.unwrap_or_default();
    let stat = format!("Import successful: {}", txt);
    println!("{}", stat.cyan());
  }

  Ok(())
}
