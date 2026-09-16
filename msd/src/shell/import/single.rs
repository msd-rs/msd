// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::{
  app_config::{MSD_TABLE_FORMAT, ShellOptions},
  server::TABLE_PUT_PATH,
};
use anyhow::{Context, Result};
use colored::Colorize;

use super::read::read_as_body;

async fn import_single(src: &str, dst: &str, mime_type: &str) -> Result<()> {
  let body = read_as_body(src).await?;
  let client = reqwest::Client::new();

  let resp = client
    .put(dst)
    .header(reqwest::header::CONTENT_TYPE, mime_type)
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

pub async fn import_csv(
  opts: &ShellOptions,
  table: &str,
  file_path: &str,
  skip: usize,
  delimiter: u8,
) -> Result<()> {
  let url = format!(
    "{}{}{}?skip={}&delimiter={}",
    opts.server_url, TABLE_PUT_PATH, table, skip, delimiter
  );
  return import_single(file_path, &url, "text/csv").await;
}

pub async fn import_table(
  opts: &ShellOptions,
  table: &str,
  file_path: &str,
  _skip: usize,
  _delimiter: u8,
) -> Result<()> {
  let url = format!("{}{}{}", opts.server_url, TABLE_PUT_PATH, table);

  return import_single(file_path, &url, MSD_TABLE_FORMAT).await;
}
