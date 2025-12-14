use crate::app_config::ShellOptions;
use anyhow::{Context, Result};
use reqwest::Body;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

pub async fn execute(opts: &ShellOptions, table: &str, file_path: &str) -> Result<()> {
  let file = File::open(file_path)
    .await
    .context(format!("Failed to open file: {}", file_path))?;
  let stream = ReaderStream::with_capacity(file, 8 * 1024 * 1024);
  let body = Body::wrap_stream(stream);

  let client = reqwest::Client::new();
  let url = format!("{}/table/{}", opts.server_url, table);

  let resp = client
    .put(&url)
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
    println!("Import successful: {}", txt);
  }

  Ok(())
}
