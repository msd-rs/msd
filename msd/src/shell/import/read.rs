// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use anyhow::{Context, Result};
use reqwest::Body;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

pub(crate) fn strip_compression_suffix(path: &str) -> &str {
  if let Some(p) = path.strip_suffix(".gz") {
    p
  } else if let Some(p) = path.strip_suffix(".zstd") {
    p
  } else if let Some(p) = path.strip_suffix(".br") {
    p
  } else {
    path
  }
}

pub(crate) fn encoding_from_path(path: &str) -> Option<&'static str> {
  if path.ends_with(".gz") {
    Some("gzip")
  } else if path.ends_with(".zstd") {
    Some("zstd")
  } else if path.ends_with(".br") {
    Some("br")
  } else {
    None
  }
}

async fn file_to_body(file_path: &str) -> Result<(Body, Option<String>)> {
  let encoding = encoding_from_path(file_path).map(|s| s.to_string());
  if file_path.as_bytes().iter().all(|b| *b == b'-') {
    let file = tokio::io::stdin();
    let stream = ReaderStream::with_capacity(file, 8 * 1024 * 1024);
    Ok((Body::wrap_stream(stream), encoding))
  } else {
    let file = File::open(file_path)
      .await
      .context(format!("Failed to open file: {}", file_path))?;
    let stream = ReaderStream::with_capacity(file, 8 * 1024 * 1024);

    Ok((Body::wrap_stream(stream), encoding))
  }
}

async fn http_to_body(url: &str) -> Result<(Body, Option<String>)> {
  let client = reqwest::Client::builder()
    .no_gzip()
    .no_brotli()
    .no_zstd()
    .no_deflate()
    .build()?;
  let resp = client
    .get(url)
    .header(reqwest::header::ACCEPT_ENCODING, "gzip, zstd, br")
    .send()
    .await?;
  if !resp.status().is_success() {
    anyhow::bail!(
      "open src {} failed when import: {}",
      url,
      resp.status().as_u16()
    );
  }

  let encoding = resp
    .headers()
    .get(reqwest::header::CONTENT_ENCODING)
    .and_then(|v| v.to_str().ok())
    .filter(|&enc| !enc.eq_ignore_ascii_case("identity"))
    .map(|s| s.to_string())
    .or_else(|| {
      let clean_url = url.split('?').next().unwrap_or(url);
      let clean_url = clean_url.split('#').next().unwrap_or(clean_url);
      encoding_from_path(clean_url).map(|s| s.to_string())
    });

  let body = Body::from(resp);

  Ok((body, encoding))
}

/// Read url or file path to `reqwest::Body` and optional `Content-Encoding`
pub(crate) async fn read_as_body(url_or_path: &str) -> Result<(Body, Option<String>)> {
  if url_or_path.starts_with("http://") || url_or_path.starts_with("https://") {
    http_to_body(url_or_path).await
  } else {
    file_to_body(url_or_path).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use axum::{
    Router,
    http::{HeaderMap, StatusCode},
    routing::get,
  };

  #[test]
  fn test_strip_compression_suffix() {
    assert_eq!(strip_compression_suffix("data.csv.gz"), "data.csv");
    assert_eq!(strip_compression_suffix("data.tbl.zstd"), "data.tbl");
    assert_eq!(strip_compression_suffix("data.txt.br"), "data.txt");
    assert_eq!(strip_compression_suffix("data.csv"), "data.csv");
    assert_eq!(strip_compression_suffix("data"), "data");
  }

  #[test]
  fn test_encoding_from_path() {
    assert_eq!(encoding_from_path("data.csv.gz"), Some("gzip"));
    assert_eq!(encoding_from_path("data.tbl.zstd"), Some("zstd"));
    assert_eq!(encoding_from_path("data.txt.br"), Some("br"));
    assert_eq!(encoding_from_path("data.csv"), None);
    assert_eq!(encoding_from_path("data.tbl"), None);
  }

  #[tokio::test]
  async fn test_file_to_body_encoding() -> Result<()> {
    let dir = std::env::temp_dir();
    let gz_path = dir.join("test_import_data.csv.gz");
    let zstd_path = dir.join("test_import_data.csv.zstd");
    let br_path = dir.join("test_import_data.csv.br");
    let plain_path = dir.join("test_import_data.csv");

    tokio::fs::write(&gz_path, b"dummy gzip").await?;
    tokio::fs::write(&zstd_path, b"dummy zstd").await?;
    tokio::fs::write(&br_path, b"dummy br").await?;
    tokio::fs::write(&plain_path, b"dummy plain").await?;

    let (_, enc_gz) = read_as_body(gz_path.to_str().unwrap()).await?;
    assert_eq!(enc_gz, Some("gzip".to_string()));

    let (_, enc_zstd) = read_as_body(zstd_path.to_str().unwrap()).await?;
    assert_eq!(enc_zstd, Some("zstd".to_string()));

    let (_, enc_br) = read_as_body(br_path.to_str().unwrap()).await?;
    assert_eq!(enc_br, Some("br".to_string()));

    let (_, enc_plain) = read_as_body(plain_path.to_str().unwrap()).await?;
    assert_eq!(enc_plain, None);

    let _ = tokio::fs::remove_file(gz_path).await;
    let _ = tokio::fs::remove_file(zstd_path).await;
    let _ = tokio::fs::remove_file(br_path).await;
    let _ = tokio::fs::remove_file(plain_path).await;

    Ok(())
  }

  #[tokio::test]
  async fn test_http_to_body_encoding() -> Result<()> {
    let app = Router::new()
      .route(
        "/gz",
        get(|| async {
          let mut headers = HeaderMap::new();
          headers.insert(
            axum::http::header::CONTENT_ENCODING,
            axum::http::HeaderValue::from_static("gzip"),
          );
          (StatusCode::OK, headers, "compressed-gzip-bytes")
        }),
      )
      .route(
        "/zstd",
        get(|| async {
          let mut headers = HeaderMap::new();
          headers.insert(
            axum::http::header::CONTENT_ENCODING,
            axum::http::HeaderValue::from_static("zstd"),
          );
          (StatusCode::OK, headers, "compressed-zstd-bytes")
        }),
      )
      .route(
        "/br",
        get(|| async {
          let mut headers = HeaderMap::new();
          headers.insert(
            axum::http::header::CONTENT_ENCODING,
            axum::http::HeaderValue::from_static("br"),
          );
          (StatusCode::OK, headers, "compressed-br-bytes")
        }),
      )
      .route(
        "/plain",
        get(|| async {
          (StatusCode::OK, "plain-bytes")
        }),
      )
      .route(
        "/fallback.csv.gz",
        get(|| async {
          (StatusCode::OK, "fallback-bytes")
        }),
      );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
      axum::serve(listener, app.into_make_service()).await.unwrap();
    });

    let (_, enc_gz) = read_as_body(&format!("http://{}/gz", addr)).await?;
    assert_eq!(enc_gz, Some("gzip".to_string()));

    let (_, enc_zstd) = read_as_body(&format!("http://{}/zstd", addr)).await?;
    assert_eq!(enc_zstd, Some("zstd".to_string()));

    let (_, enc_br) = read_as_body(&format!("http://{}/br", addr)).await?;
    assert_eq!(enc_br, Some("br".to_string()));

    let (_, enc_plain) = read_as_body(&format!("http://{}/plain", addr)).await?;
    assert_eq!(enc_plain, None);

    let (_, enc_fallback) = read_as_body(&format!("http://{}/fallback.csv.gz", addr)).await?;
    assert_eq!(enc_fallback, Some("gzip".to_string()));

    Ok(())
  }
}
