use tracing::warn;

use crate::{D64, DataType, Table, Variant};
use std::io::prelude::*;

pub fn table_to_csv<W: Write>(table: &Table, w: W, sep: u8) -> Result<(), TableError> {
  let mut writer = csv::WriterBuilder::new().delimiter(sep).from_writer(w);

  writer.write_record(table.columns().iter().map(|c| c.schema.name.as_str()))?;
  for row in table.rows(false) {
    writer.write_record(row.iter().map(|v| v.to_string()))?;
  }
  Ok(())
}

const time_fields: &[&str] = &[
  "time",
  "ts",
  "timestamp",
  "datetime",
  "date",
  "created_at",
  "updated_at",
  "deleted_at",
];

const bool_fields: &[&str] = &["true", "false", "t", "f", "yes", "no"];

fn guess_type(f: (&str, &str)) -> DataType {
  let (name, value) = f;
  for field in time_fields {
    if name.eq_ignore_ascii_case(field) {
      if let Ok(_) = crate::date::parse_datetime(value) {
        return DataType::DateTime;
      }
    }
  }
  if value.parse::<i64>().is_ok() {
    return DataType::Int64;
  }
  if value.parse::<f64>().is_ok() {
    if value.ends_with('0') {
      return DataType::Decimal64;
    } else {
      return DataType::Float64;
    }
  }
  DataType::String
}

fn build_table(headers: &[String], first_row: &csv::StringRecord) -> Table {
  let mut table = Table::default();
  for (i, header) in headers.iter().enumerate() {
    let value = &first_row[i];
    let variant = Variant::from_string(value);
    let schema = ColumnSchema {
      name: header.clone(),
      variant_type: variant.get_type(),
      nullable: false,
    };
    table.add_column(Column::new(schema));
    table.columns_mut().last_mut().unwrap().data.push(variant);
  }
  table
}

pub fn table_from_csv<R: BufRead>(r: R, sep: u8, template: &Table) -> Result<Table, TableError> {
  let mut reader = csv::ReaderBuilder::new().delimiter(sep).from_reader(r);
  let headers = reader.headers()?.iter().map(|h| h).collect::<Vec<_>>();
  if headers.len() != template.column_count() {
    return Err(TableError::ColumnCountMismatch(
      template.column_count(),
      headers.len(),
    ));
  }
  if !headers
    .iter()
    .zip(template.columns().iter())
    .all(|(h, c)| h == &c.schema.name)
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
      .map(|(field, value)| Variant::from_str(value, field.schema.kind))
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
