use axum::{
  body::Body,
  extract::{Path, State},
  http::HeaderMap,
  response::IntoResponse,
};
use futures::StreamExt;
use http_body_util::BodyStream;
use msd_db::request::MsdRequest;
use msd_request::{InsertData, InsertRequest, RequestKey};
use msd_table::{DataType, Field, Table, Variant};

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

  // 2. append the 'obj' column to the table schema,
  let mut parse_schema = schema.clone();
  parse_schema.insert_column(0, Field::new("obj", DataType::String, 0));

  // 3. parse the csv lines into table rows, rows with same obj (first column ) are appended to a Table
  let mut stream = BodyStream::new(body);
  let mut buffer = Vec::new();
  let mut current_obj: Option<String> = None;
  let mut current_table = schema.to_empty();

  while let Some(frame_res) = stream.next().await {
    let frame = frame_res.map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

    if let Ok(chunk) = frame.into_data() {
      buffer.extend_from_slice(&chunk);

      while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
        let line_bytes = buffer.drain(0..=pos).collect::<Vec<u8>>();
        process_line(
          &line_bytes,
          &parse_schema,
          &mut current_obj,
          &mut current_table,
          &db,
          &table_name,
        )
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e))?;
      }
    }
  }

  // Process remaining buffer
  if !buffer.is_empty() {
    process_line(
      &buffer,
      &parse_schema,
      &mut current_obj,
      &mut current_table,
      &db,
      &table_name,
    )
    .await
    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e))?;
  }

  // Flush remaining table
  if current_table.row_count() > 0 {
    flush_table(
      &db,
      &table_name,
      current_obj.as_deref().unwrap_or_default(),
      current_table,
    )
    .await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))?;
  }

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

async fn process_line(
  line: &[u8],
  parse_schema: &Table,
  current_obj: &mut Option<String>,
  current_table: &mut Table,
  db: &DBState,
  table_name: &str,
) -> Result<(), String> {
  // skip empty lines
  if line.iter().all(|b| b.is_ascii_whitespace()) {
    return Ok(());
  }

  let mut rdr = csv::ReaderBuilder::new()
    .has_headers(false)
    .from_reader(line);
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
        let old_obj = current_obj.as_ref().unwrap();
        let prev_table = std::mem::replace(current_table, current_table.to_empty());
        flush_table(db, table_name, old_obj, prev_table).await?;
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
      let old_obj = current_obj.as_ref().unwrap();
      let prev_table = std::mem::replace(current_table, current_table.to_empty());
      flush_table(db, table_name, old_obj, prev_table).await?;
    }
  }
  Ok(())
}
