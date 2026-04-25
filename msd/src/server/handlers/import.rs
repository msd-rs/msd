// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::{
  hash::{Hash, Hasher},
  net::SocketAddr,
  sync::Arc,
};

use axum::{
  Json,
  body::Body,
  extract::{ConnectInfo, Path, Query, State},
  http::HeaderMap,
};
use futures::StreamExt;
use http_body_util::BodyStream;
use memchr::memchr;
use msd_db::request::MsdRequest;
use msd_request::{
  InsertData, InsertRequest, RequestKey, TableFrameError, check_table_frame, unpack_table_frame,
};
use msd_table::{Table, Variant, get_local_offset};
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{debug, error, info};

use crate::server::{
  AppState, AppStateRef,
  handlers::{is_msd_table_format, ws},
};

#[derive(Debug, Serialize)]
pub struct TableResponse {
  pub total_rows: usize,
  pub time_used_ms: u64,
  pub rows_per_sec: u64,

  #[serde(skip)]
  pub start_at: std::time::Instant,
}

impl TableResponse {
  pub fn start() -> Self {
    Self {
      start_at: std::time::Instant::now(),
      total_rows: 0,
      time_used_ms: 0,
      rows_per_sec: 0,
    }
  }

  pub fn end(&mut self) {
    let d = self.start_at.elapsed();
    self.time_used_ms = d.as_millis() as u64;
    self.rows_per_sec = (self.total_rows as f64 / d.as_secs_f64()).round() as u64;
  }

  pub fn add_rows(&mut self, rows: usize) {
    self.total_rows += rows;
  }

  pub fn add_row(&mut self) {
    self.total_rows += 1;
  }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportQuery {
  pub skip: Option<usize>,
}

use crate::server::handlers::permission::Permission;

pub async fn handle_table(
  State(state): State<AppStateRef>,
  ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
  Path(table_name): Path<String>,
  Query(q): Query<ImportQuery>,
  headers: HeaderMap,
  body: Body,
) -> Result<Json<TableResponse>, (axum::http::StatusCode, String)> {
  Permission::check_write(&headers, &remote_addr)?;

  state
    .db
    .get_schema(&table_name)
    .map_err(|e| (axum::http::StatusCode::NOT_FOUND, e.to_string()))?;

  if is_msd_table_format(&headers) {
    handle_table_binary(state.clone(), table_name, body).await
  } else {
    handle_table_csv(state.clone(), table_name, body, q.skip.unwrap_or_default()).await
  }
}

fn spawn_csv_workers(
  worker_tasks: &mut JoinSet<Result<(), String>>,
  worker_idx: usize,
  db: AppStateRef,
  table_name: String,
  parse_schema: Table,
) -> mpsc::Sender<Vec<u8>> {
  let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);

  worker_tasks.spawn(async move {
    info!(id = worker_idx, "updater started");
    let mut rows = 0;

    while let Some(line) = rx.recv().await {
      if line.starts_with(b"#exit") {
        break;
      }
      match process_csv_block_simd(&line, &parse_schema) {
        Ok((obj, table)) => {
          if obj.is_empty() || table.row_count() == 0 {
            continue;
          }
          rows += table.row_count();
          let _ = flush_table(db.clone(), &table_name, &obj, table).await;
        }
        Err(e) => {
          error!(%e, id = worker_idx, line = %String::from_utf8_lossy(&line), "process line failed");
        }
      }
    }

    info!(id = worker_idx, rows, "updater completed");
    Ok::<_, String>(())
  });
  return tx;
}

fn spawn_binary_workers(
  worker_tasks: &mut JoinSet<Result<usize, String>>,
  worker_idx: usize,
  db: AppStateRef,
  table_name: String,
  schema: Table,
) -> mpsc::Sender<Vec<u8>> {
  let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);

  worker_tasks.spawn(async move {
    info!(id = worker_idx, "updater started");
    let mut rows = 0;

    while let Some(block) = rx.recv().await {
      if block.starts_with(b"#exit") {
        break;
      }

      match unpack_table_frame(&block, false) {
        Ok(table) => {
          rows += table.row_count();
          let obj = table
            .get_table_meta("obj")
            .and_then(|v| v.get_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
          if !schema.same_shape(&table) {
            return Err(format!(
              "schema mismatch for table '{}' obj '{}'",
              table_name, obj
            ));
          }
          match flush_table(db.clone(), &table_name, &obj, table).await {
            Ok(_) => {}
            Err(e) => {
              error!(%e, id = worker_idx, "process block failed");
            }
          }
        }
        Err(e) => {
          error!(%e, id = worker_idx, "process block failed");
        }
      }
    }

    info!(id = worker_idx, "updater completed");
    Ok::<_, String>(rows)
  });
  return tx;
}

async fn handle_table_csv(
  state: AppStateRef,
  table_name: String,
  body: Body,
  skip: usize,
) -> Result<Json<TableResponse>, (axum::http::StatusCode, String)> {
  let mut response = TableResponse::start();

  // 1. get the schema of the table by table_name
  let schema = state
    .db
    .get_schema(&table_name)
    .map_err(|e| (axum::http::StatusCode::NOT_FOUND, e.to_string()))?;

  let parse_schema = schema.clone();

  // 3. spawn workers
  let worker_count = 8;
  let mut senders = vec![None; worker_count];
  let mut worker_tasks = JoinSet::new();

  // 4. parse the csv lines and dispatch to workers
  let mut stream = BodyStream::new(body);
  let mut buffer = Vec::new();
  let mut skipped = 0;

  info!("start parsing csv lines");

  while let Some(frame_res) = stream.next().await {
    let frame = frame_res.map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

    if let Ok(chunk) = frame.into_data() {
      buffer.extend_from_slice(&chunk);

      let last_line_end = match buffer.iter().rposition(|&b| b == b'\n') {
        Some(n) => n,
        None => continue, // no line found, continue reading
      };

      let mut last_key: &[u8] = b"";
      let mut block_start = 0;
      let mut block_end = 0;
      let mut line_start = 0;

      while let Some(pos) = memchr(b'\n', &buffer[line_start..=last_line_end]) {
        if skipped < skip {
          skipped += 1;
          line_start += pos + 1;
          block_start = line_start;
          block_end = line_start;
          continue;
        }
        let line = &buffer[line_start..line_start + pos + 1];
        if line.trim_ascii().is_empty() {
          block_end += pos + 1;
          line_start = block_end;
          continue;
        }

        let first_col_pos = match memchr(b',', line) {
          Some(pos) => pos,
          None => continue,
        };
        if last_key.is_empty() {
          last_key = &line[..first_col_pos];
        }
        if last_key != &line[..first_col_pos] {
          let mut hasher = FxHasher::default();
          last_key.hash(&mut hasher);
          let hash = hasher.finish();
          // ensure order of the same obj
          let worker_idx = (hash as usize) % worker_count;

          let block = Vec::from(&buffer[block_start..block_end]);
          if senders[worker_idx].is_none() {
            senders[worker_idx] = Some(spawn_csv_workers(
              &mut worker_tasks,
              worker_idx,
              state.clone(),
              table_name.clone(),
              parse_schema.clone(),
            ));
          }
          debug!(
            id = worker_idx,
            obj = String::from_utf8_lossy(last_key).to_string(),
            len = block.len(),
            "dispatched, waiting for workers"
          );
          senders[worker_idx]
            .as_ref()
            .unwrap() // should be initialized
            .send(block)
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
          block_start = block_end;
          last_key = &line[..first_col_pos];
        }
        block_end += pos + 1;
        line_start = block_end;

        response.add_row();
      }

      if block_start < block_end {
        let block = Vec::from(&buffer[block_start..block_end]);
        let mut hasher = FxHasher::default();
        last_key.hash(&mut hasher);
        let hash = hasher.finish();
        // ensure order of the same obj
        let worker_idx = (hash as usize) % worker_count;
        if senders[worker_idx].is_none() {
          senders[worker_idx] = Some(spawn_csv_workers(
            &mut worker_tasks,
            worker_idx,
            state.clone(),
            table_name.clone(),
            parse_schema.clone(),
          ));
        }
        senders[worker_idx]
          .as_ref()
          .unwrap() // should be initialized
          .send(block)
          .await
          .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
      }

      buffer.drain(0..last_line_end + 1);
    }
  }

  // Process remaining buffer
  if !buffer.is_empty() {
    response.add_rows(buffer.iter().filter(|&&b| b == b'\n').count());

    let line_bytes = buffer;
    let end = line_bytes
      .iter()
      .position(|&b| b == b',')
      .unwrap_or(line_bytes.len());
    let key = &line_bytes[..end];
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let worker_idx = (hash as usize) % worker_count;
    if senders[worker_idx].is_none() {
      senders[worker_idx] = Some(spawn_csv_workers(
        &mut worker_tasks,
        worker_idx,
        state.clone(),
        table_name.clone(),
        parse_schema.clone(),
      ));
    }

    senders[worker_idx]
      .as_ref()
      .unwrap() // should be initialized
      .send(line_bytes)
      .await
      .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
  }

  // Close all channels
  for sender in senders.into_iter().filter_map(|s| s) {
    let _ = sender.send(b"#exit".to_vec()).await;
  }

  info!(
    lines = response.total_rows,
    "dispatched, waiting for workers"
  );

  // Wait for all workers
  while let Some(res) = worker_tasks.join_next().await {
    match res {
      Ok(Ok(_)) => {}
      Ok(Err(e)) => return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e)),
      Err(e) => return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
  }

  info!("all task completed");

  response.end();

  Ok(Json(response))
}

async fn handle_table_binary(
  state: AppStateRef,
  table_name: String,
  body: Body,
) -> Result<Json<TableResponse>, (axum::http::StatusCode, String)> {
  let mut response = TableResponse::start();

  let db = &state.db;

  // 1. get the schema of the table by table_name
  let schema = db
    .get_schema(&table_name)
    .map_err(|e| (axum::http::StatusCode::NOT_FOUND, e.to_string()))?;

  // 3. spawn workers
  let worker_count = 8;
  let mut senders = vec![None; worker_count];
  let mut worker_tasks = JoinSet::new();

  // 4. parse the binary stream and dispatch to workers
  let mut stream = BodyStream::new(body);
  let mut buffer = Vec::new();
  let mut tasks_idx = 0;

  info!("start parsing binary stream");
  while let Some(frame_res) = stream.next().await {
    let frame = frame_res.map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

    if let Ok(chunk) = frame.into_data() {
      buffer.extend_from_slice(&chunk);

      loop {
        let frame_size = match check_table_frame(&buffer) {
          Ok((header, data)) => header + data,
          Err(TableFrameError::BufferTooSmall(_, _)) => {
            break;
          }
          Err(err) => {
            error!(%err, "invalid table frame");
            break;
          }
        };
        if buffer.len() < frame_size {
          break;
        }
        let data = buffer.drain(0..frame_size).collect::<Vec<u8>>();

        // Dispatch DATA
        let worker_idx = tasks_idx % worker_count;
        if senders[worker_idx].is_none() {
          senders[worker_idx] = Some(spawn_binary_workers(
            &mut worker_tasks,
            worker_idx,
            state.clone(),
            table_name.clone(),
            schema.clone(),
          ));
        }

        senders[worker_idx]
          .as_ref()
          .unwrap()
          .send(data)
          .await
          .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        tasks_idx += 1;
      }
    }
  }

  // Close all channels
  for sender in senders.into_iter().filter_map(|s| s) {
    let _ = sender.send(b"#exit".to_vec()).await;
  }

  info!(
    blocks = response.total_rows,
    "dispatched, waiting for workers"
  );

  // Wait for all workers
  while let Some(res) = worker_tasks.join_next().await {
    match res {
      Ok(Ok(rows)) => response.add_rows(rows),
      Ok(Err(e)) => return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e)),
      Err(e) => return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
  }

  info!("all task completed");

  response.end();

  Ok(Json(response))
}

async fn flush_table(
  state: Arc<AppState>,
  table_name: &str,
  obj: &str,
  table: Table,
) -> Result<(), String> {
  let req = InsertRequest {
    key: RequestKey {
      table: table_name.to_string(),
      obj: obj.to_string(),
    },
    data: InsertData::Table(table),
  };
  // ignore response from rx
  let (req, _rx) = MsdRequest::insert(req);
  state.db.request(req).await.map_err(|e| e.to_string())?;
  state
    .broker
    .broadcast(Arc::new(ws::Message::build_notify(table_name, obj, 0, 0)));
  Ok(())
}

#[allow(dead_code)]
fn process_csv_block(lines: &[u8], parse_schema: &Table) -> Result<(String, Table), String> {
  let mut rdr = csv::ReaderBuilder::new()
    .has_headers(false)
    .from_reader(lines);

  let mut table = parse_schema.clone();
  let mut obj = String::default();
  while let Some(record) = rdr.records().next() {
    let record = record.map_err(|e| e.to_string())?;
    if record.len() != parse_schema.column_count() + 1 {
      return Err(format!(
        "Column count mismatch: expected {}, got {}",
        parse_schema.column_count() + 1,
        record.len()
      ));
    }

    if obj.is_empty() {
      obj = record[0].to_string();
    }

    let mut row = Vec::with_capacity(parse_schema.column_count());
    for (i, field) in parse_schema.columns().iter().enumerate() {
      let val_str = &record[i + 1];
      let variant = Variant::from_str(val_str, field.kind).map_err(|e| e.to_string())?;
      row.push(variant);
    }
    match table.push_row(row) {
      Ok(_) => {} // Ignore Ok result
      Err(e) => {
        error!(%e, "push row failed");
      }
    }
  }
  debug!(obj, rows = table.row_count(), "process_csv_block");
  Ok((obj, table))
}

fn process_csv_block_simd(lines: &[u8], parse_schema: &Table) -> Result<(String, Table), String> {
  let mut rdr = simd_csv::ZeroCopyReaderBuilder::new()
    .has_headers(false)
    .from_reader(lines);

  let mut table = parse_schema.clone();
  let mut obj = String::default();
  while let Some(record) = rdr.read_byte_record().map_err(|e| e.to_string())? {
    if record.is_empty() {
      continue;
    }
    if record.len() != parse_schema.column_count() + 1 {
      return Err(format!(
        "Column count mismatch: expected {}, got {}",
        parse_schema.column_count() + 1,
        record.len()
      ));
    }

    if obj.is_empty() {
      obj = record
        .get(0)
        .map(|s| String::from_utf8_lossy(s).to_string())
        .unwrap_or_default();
    }

    let tz = get_local_offset();
    let mut row = Vec::with_capacity(parse_schema.column_count());
    for (i, field) in parse_schema.columns().iter().enumerate() {
      let val_str = record
        .get(i + 1)
        .map(|s| String::from_utf8_lossy(s))
        .unwrap_or_default();
      let variant =
        Variant::from_str_with_tz(&val_str, field.kind, tz).map_err(|e| e.to_string())?;
      row.push(variant);
    }
    match table.push_row(row) {
      Ok(_) => {} // Ignore Ok result
      Err(e) => {
        error!(%e, "push row failed");
      }
    }
  }
  debug!(obj, rows = table.row_count(), "process_csv_block");
  Ok((obj, table))
}
