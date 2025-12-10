use std::hash::{Hash, Hasher};

use axum::{
  body::Body,
  extract::{Path, State},
  http::HeaderMap,
  response::IntoResponse,
};
use futures::StreamExt;
use http_body_util::BodyStream;
use memchr::memchr;
use msd_db::request::MsdRequest;
use msd_request::{InsertData, InsertRequest, RequestKey};
use msd_table::{DataType, Field, Table, Variant};
use rustc_hash::FxHasher;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::server::DBState;

pub async fn handle_table(
  State(db): State<DBState>,
  Path(table_name): Path<String>,
  body: Body,
) -> Result<(HeaderMap, impl IntoResponse), (axum::http::StatusCode, String)> {
  // 1. get the schema of the table by table_name
  let schema = db
    .get_schema(&table_name)
    .map_err(|e| (axum::http::StatusCode::NOT_FOUND, e.to_string()))?;

  let parse_schema = schema.clone();

  // 3. spawn workers
  let worker_count = 8;
  let mut senders = Vec::with_capacity(worker_count);
  let mut worker_tasks = JoinSet::new();

  for worker_idx in 0..worker_count {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
    senders.push(tx);

    let db = db.clone();
    let table_name = table_name.clone();
    let parse_schema = parse_schema.clone();

    worker_tasks.spawn(async move {
      info!(id = worker_idx, "updater started");
      //let mut flush_tasks = JoinSet::new();

      while let Some(line) = rx.recv().await {
        if line.starts_with(b"#exit") {
          break;
        }
        match process_line(&line, &parse_schema) {
          Ok((obj, table)) => {
            if obj.is_empty() || table.row_count() == 0 {
              continue;
            }
            let _ = flush_table(&db, &table_name, &obj, table);
          }
          Err(e) => {
            error!(%e, id = worker_idx, "process line failed");
          }
        }
      }

      info!(id = worker_idx, "updater completed");
      Ok::<_, String>(())
    });
  }

  // 4. parse the csv lines and dispatch to workers
  let mut stream = BodyStream::new(body);
  let mut buffer = Vec::new();

  info!("start parsing csv lines");
  let mut line_processed = 0;
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
        let line = &buffer[line_start..line_start + pos + 1];

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
          let worker_idx = (hash as usize) % worker_count;
          let block = Vec::from(&buffer[block_start..block_end]);
          senders[worker_idx]
            .send(block)
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
          block_start = block_end;
          last_key = &line[..first_col_pos];
        }
        block_end += pos + 1;
        line_start = block_end;

        line_processed += 1;
        if line_processed % 100_000 == 0 {
          info!(lines = line_processed, "processed lines");
        }
      }

      if block_start < block_end {
        let block = Vec::from(&buffer[block_start..block_end]);
        let mut hasher = FxHasher::default();
        last_key.hash(&mut hasher);
        let hash = hasher.finish();
        let worker_idx = (hash as usize) % worker_count;
        senders[worker_idx]
          .send(block)
          .await
          .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
      }

      buffer.drain(0..last_line_end + 1);
    }
  }

  // Process remaining buffer
  if !buffer.is_empty() {
    line_processed += buffer.iter().filter(|&&b| b == b'\n').count();

    let line_bytes = buffer;
    // Calculate hash
    let end = line_bytes
      .iter()
      .position(|&b| b == b',')
      .unwrap_or(line_bytes.len());
    let key = &line_bytes[..end];
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let worker_idx = (hash as usize) % worker_count;

    senders[worker_idx]
      .send(line_bytes)
      .await
      .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
  }

  // Close all channels
  for sender in senders {
    let _ = sender.send(b"#exit".to_vec()).await;
  }

  info!(lines = line_processed, "dispatched, waiting for workers");

  // Wait for all workers
  while let Some(res) = worker_tasks.join_next().await {
    match res {
      Ok(Ok(_)) => {}
      Ok(Err(e)) => return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e)),
      Err(e) => return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
  }

  info!("all task completed");

  Ok((HeaderMap::new(), "DONE"))
}

async fn flush_table(
  db: &DBState,
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
  let (req, rx) = MsdRequest::insert(req);
  db.request(req).await.map_err(|e| e.to_string())?;
  rx.await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;
  Ok(())
}

fn process_line(line: &[u8], parse_schema: &Table) -> Result<(String, Table), String> {
  let mut rdr = csv::ReaderBuilder::new()
    .has_headers(false)
    .from_reader(line);

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
    // if obj != record[0] {
    //   error!("obj mismatch: expected {}, got {}", obj, &record[0]);
    //   return Err(format!(
    //     "obj mismatch: expected {}, got {}",
    //     obj, &record[0]
    //   ));
    // }

    let mut row = Vec::with_capacity(parse_schema.column_count());
    for (i, field) in parse_schema.columns().iter().enumerate() {
      let val_str = &record[i + 1];
      let variant = Variant::from_str(val_str, field.kind).map_err(|e| e.to_string())?;
      row.push(variant);
    }
    match table.push_row(row) {
      Ok(_) => {}
      Err(e) => {
        error!(%e, "push row failed");
      }
    }
  }
  Ok((obj, table))
  /*
  let mut variants = Vec::with_capacity(parse_schema.column_count());

  let record = rdr.records().next();
  if let Some(res) = record {
    let record = res.map_err(|e| e.to_string())?;

    // Check header
    if record.len() == parse_schema.column_count() {
      if &record[0] == "obj" {
        return Ok(());
      }
    }

    if record.len() != parse_schema.column_count() {
      return Err(format!(
        "Column count mismatch: expected {}, got {}",
        parse_schema.column_count(),
        record.len()
      ));
    }

    let obj = record[0].to_string();

    let obj_changed = match current_obj {
      Some(curr) => curr != &obj,
      None => true,
    };

    if obj_changed {
      if current_table.row_count() > 0 {
        let old_obj = current_obj.clone().unwrap_or_default();
        let prev_table = std::mem::replace(current_table, current_table.to_empty());
        let db = db.clone();
        let table_name = table_name.to_string();
        flush_tasks.spawn(async move {
          flush_table(&db, &table_name, &old_obj, prev_table)
            .await
            .map_err(|e| e.to_string())
        });
      }
      *current_obj = Some(obj.clone());
    }

    let mut row_variants = Vec::with_capacity(parse_schema.column_count() - 1);
    for (i, field) in parse_schema.columns().iter().enumerate().skip(1) {
      let val_str = &record[i];
      let variant = Variant::from_str(val_str, field.kind).map_err(|e| e.to_string())?;
      row_variants.push(variant);
    }
    current_table
      .push_row(row_variants)
      .map_err(|e| e.to_string())?;

    // Check size limit (10k rows) to manage memory
    if current_table.row_count() >= 10000 {
      let old_obj = current_obj.clone().unwrap_or_default();
      let prev_table = std::mem::replace(current_table, current_table.to_empty());
      let db = db.clone();
      let table_name = table_name.to_string();
      flush_tasks.spawn(async move {
        flush_table(&db, &table_name, &old_obj, prev_table)
          .await
          .map_err(|e| e.to_string())
      });
    }
  }
  Ok(())
  */
}
