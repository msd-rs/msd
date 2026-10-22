// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::{
  app_config::{MSD_TABLE_FORMAT, ShellOptions},
  server::TABLE_PUT_PATH,
};
use anyhow::{Context, Result};
use colored::Colorize;

use super::read::{encoding_from_path, read_as_body};

pub(crate) async fn import_single(
  src: &str,
  dst: &str,
  mime_type: &str,
  content_encoding: Option<&str>,
) -> Result<()> {
  let (body, detected_encoding) = read_as_body(src).await?;
  let encoding = content_encoding.or(detected_encoding.as_deref());
  let client = reqwest::Client::new();

  let mut req = client
    .put(dst)
    .header(reqwest::header::CONTENT_TYPE, mime_type);

  if let Some(enc) = encoding {
    req = req.header(reqwest::header::CONTENT_ENCODING, enc);
  }

  let resp = req
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
  let encoding = if !file_path.starts_with("http://") && !file_path.starts_with("https://") {
    encoding_from_path(file_path)
  } else {
    None
  };
  return import_single(file_path, &url, "text/csv", encoding).await;
}

pub async fn import_table(
  opts: &ShellOptions,
  table: &str,
  file_path: &str,
  _skip: usize,
  _delimiter: u8,
) -> Result<()> {
  let url = format!("{}{}{}", opts.server_url, TABLE_PUT_PATH, table);
  let encoding = if !file_path.starts_with("http://") && !file_path.starts_with("https://") {
    encoding_from_path(file_path)
  } else {
    None
  };
  return import_single(file_path, &url, MSD_TABLE_FORMAT, encoding).await;
}

#[cfg(test)]
mod tests {
  use super::*;
  use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, put},
  };
  use std::sync::{Arc, Mutex};

  #[derive(Clone, Default)]
  struct ReceivedData {
    content_type: Option<String>,
    content_encoding: Option<String>,
    body: Vec<u8>,
  }

  #[tokio::test]
  async fn test_import_single_with_file_gzip() -> Result<()> {
    let received = Arc::new(Mutex::new(ReceivedData::default()));
    let received_clone = received.clone();

    let app = Router::new().route(
      "/test_dst",
      put(
        |State(rec): State<Arc<Mutex<ReceivedData>>>, headers: HeaderMap, body: Bytes| async move {
          let mut data = rec.lock().unwrap();
          data.content_type = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
          data.content_encoding = headers
            .get(axum::http::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
          data.body = body.to_vec();
          (StatusCode::OK, "ok")
        },
      ),
    )
    .with_state(received_clone);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let dst_addr = listener.local_addr()?;
    tokio::spawn(async move {
      axum::serve(listener, app.into_make_service()).await.unwrap();
    });

    let dir = std::env::temp_dir();
    let gz_file = dir.join("test_import_payload.csv.gz");
    let test_bytes = b"\x1f\x8b\x08dummy_gzip_content";
    tokio::fs::write(&gz_file, test_bytes).await?;

    let dst_url = format!("http://{}/test_dst", dst_addr);
    import_single(
      gz_file.to_str().unwrap(),
      &dst_url,
      "text/csv",
      Some("gzip"),
    )
    .await?;

    let _ = tokio::fs::remove_file(gz_file).await;

    let res = received.lock().unwrap().clone();
    assert_eq!(res.content_type.as_deref(), Some("text/csv"));
    assert_eq!(res.content_encoding.as_deref(), Some("gzip"));
    assert_eq!(res.body, test_bytes);

    Ok(())
  }

  #[tokio::test]
  async fn test_import_single_with_http_zstd() -> Result<()> {
    let received = Arc::new(Mutex::new(ReceivedData::default()));
    let received_clone = received.clone();

    let test_bytes = b"zstd_compressed_payload_12345";

    let src_app = Router::new().route(
      "/src_data",
      get(|| async {
        let mut headers = HeaderMap::new();
        headers.insert(
          axum::http::header::CONTENT_ENCODING,
          axum::http::HeaderValue::from_static("zstd"),
        );
        (StatusCode::OK, headers, "zstd_compressed_payload_12345")
      }),
    );

    let dst_app = Router::new().route(
      "/dst_data",
      put(
        |State(rec): State<Arc<Mutex<ReceivedData>>>, headers: HeaderMap, body: Bytes| async move {
          let mut data = rec.lock().unwrap();
          data.content_type = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
          data.content_encoding = headers
            .get(axum::http::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
          data.body = body.to_vec();
          (StatusCode::OK, "ok")
        },
      ),
    )
    .with_state(received_clone);

    let src_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let src_addr = src_listener.local_addr()?;
    tokio::spawn(async move {
      axum::serve(src_listener, src_app.into_make_service()).await.unwrap();
    });

    let dst_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let dst_addr = dst_listener.local_addr()?;
    tokio::spawn(async move {
      axum::serve(dst_listener, dst_app.into_make_service()).await.unwrap();
    });

    let src_url = format!("http://{}/src_data", src_addr);
    let dst_url = format!("http://{}/dst_data", dst_addr);

    // Call import_single without specifying encoding: it detects it from http response!
    import_single(&src_url, &dst_url, "text/csv", None).await?;

    let res = received.lock().unwrap().clone();
    assert_eq!(res.content_type.as_deref(), Some("text/csv"));
    assert_eq!(res.content_encoding.as_deref(), Some("zstd"));
    assert_eq!(res.body, test_bytes);

    Ok(())
  }
}
