use anyhow::{Context, Result};
use reqwest::Body;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

async fn file_to_body(file_path: &str) -> Result<Body> {
  if file_path.as_bytes().iter().all(|b| *b == b'-') {
    let file = tokio::io::stdin();
    let stream = ReaderStream::with_capacity(file, 8 * 1024 * 1024);
    Ok(Body::wrap_stream(stream))
  } else {
    let file = File::open(file_path)
      .await
      .context(format!("Failed to open file: {}", file_path))?;
    let stream = ReaderStream::with_capacity(file, 8 * 1024 * 1024);

    Ok(Body::wrap_stream(stream))
  }
}

async fn http_to_body(url: &str) -> Result<Body> {
  let client = reqwest::Client::new();
  let resp = client.get(url).send().await?;
  if !resp.status().is_success() {
    anyhow::bail!(
      "open src {} failed when import: {}",
      url,
      resp.status().as_u16()
    );
  }

  let body = Body::from(resp);

  Ok(body)
}

/// Read url or file path to `reqwest::Body`
pub(crate) async fn read_as_body(url_or_path: &str) -> Result<Body> {
  if url_or_path.starts_with("http://") || url_or_path.starts_with("https://") {
    return http_to_body(url_or_path).await;
  } else {
    return file_to_body(url_or_path).await;
  }
}
