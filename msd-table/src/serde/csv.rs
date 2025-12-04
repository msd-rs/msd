use tracing::warn;

use crate::{D64, DataType, Table, TableError, Variant};
use std::io::prelude::*;

pub fn table_to_csv<W: Write>(table: &Table, w: W, sep: u8) -> Result<(), TableError> {
  let mut writer = csv::WriterBuilder::new().delimiter(sep).from_writer(w);

  writer
    .write_record(table.columns().iter().map(|c| c.name.as_str()))
    .map_err(|e| TableError::CsvError(e))?;
  for row in table.rows(false) {
    writer
      .write_record(row.iter().map(|v| v.to_string()))
      .map_err(|e| TableError::CsvError(e))?;
  }
  Ok(())
}

pub fn table_from_csv<R: BufRead>(r: R, sep: u8, template: &Table) -> Result<Table, TableError> {
  let mut reader = csv::ReaderBuilder::new().delimiter(sep).from_reader(r);

  let headers = reader
    .headers()
    .map_err(|e| TableError::from(e))?
    .iter()
    .map(|h| h.to_string())
    .collect::<Vec<_>>();
  if headers.len() != template.column_count() {
    return Err(TableError::ColumnCountMismatch(
      template.column_count(),
      headers.len(),
    ));
  }
  if !headers
    .iter()
    .zip(template.columns().iter())
    .all(|(h, c)| h == &c.name)
  {
    return Err(TableError::ColumnSchemaMismatch(
      template.schema_debug(),
      headers.join(", "),
    ));
  }

  let mut table = template.to_empty();
  for (i, record) in reader.records().enumerate() {
    let record = match record {
      Ok(r) => r,
      Err(e) => {
        warn!(row=i, error=%e, "Failed to read CSV record");
        continue;
      }
    };
    if record.len() != headers.len() {
      warn!(
        row = i,
        record_len = record.len(),
        header_len = headers.len(),
        "Record length does not match header length"
      );
      continue;
    }
    let row = match template
      .columns()
      .iter()
      .zip(record.iter())
      .map(|(field, value)| Variant::from_str(value, field.kind))
      .collect::<Result<Vec<Variant>, TableError>>()
    {
      Ok(r) => r,
      Err(e) => {
        warn!(row=i, error=%e, "Failed to parse CSV record into table row");
        continue;
      }
    };
    match table.push_row(row) {
      Ok(_) => {}
      Err(e) => {
        warn!(row=i, error=%e, "Failed to push row into table");
      }
    }
  }
  Ok(table)
}
