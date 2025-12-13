use super::get_client;
use crate::{
  app_config::ShellOptions,
  shell::table_handler::{TableHandler, build_table_handler},
};
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use msd_request::unpack_table_frame;
use msd_table::Table;
use reqwest::header;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

pub async fn execute(opts: &ShellOptions, query: &str) -> Result<()> {
  let client = get_client(opts);
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

  let is_table_frame = resp.headers().get(header::CONTENT_TYPE).is_some_and(|ct| {
    ct.to_str()
      .is_ok_and(|ct| ct.contains("application/x-msd-table-frame"))
  });

  let mut fetched_rows = 0;
  let mut objects = 0;
  // Stream the response body
  let stream = resp.bytes_stream();
  let stream_reader = tokio_util::io::StreamReader::new(
    stream.map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
  );
  if is_table_frame {
    let mut rd = tokio::io::BufReader::new(stream_reader);

    let mut buf = Vec::with_capacity(1024);

    buf.resize(8, 0);
    while rd.read_exact(&mut buf).await.is_ok() {
      if buf.starts_with(b"\x7c\x4d\x01\x00") {
        let frame_size = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        buf.resize((frame_size + 8) as usize, 0);
        rd.read_exact(&mut buf[8..]).await?;
        let (_, table) = unpack_table_frame(&buf, false)?;

        fetched_rows += table.row_count();
        objects += 1;
        handler.handle(&table)?;

        buf.clear();
        buf.resize(8, 0);
      } else {
        bail!("Invalid table frame");
      }
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
