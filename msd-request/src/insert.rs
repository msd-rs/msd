// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::{DataType, Field, RowsTable, Series, Table, Variant, table_from_csv};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::io::Cursor;
use std::ops::Deref;

use crate::errors::RequestError;

use super::base::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsertData {
  Rows(RowsTable),
  Columns(Vec<Series>),
  Csv(String),
  Table(Table),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
  #[serde(flatten)]
  pub key: RequestKey,
  pub data: InsertData,
}

impl Hash for InsertRequest {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

impl Deref for InsertRequest {
  type Target = RequestKey;

  fn deref(&self) -> &Self::Target {
    &self.key
  }
}

impl InsertRequest {
  /// to_table convert each kind of data to a table
  /// if the data contains multiple objects, it will be grouped by the object name
  pub fn to_table(self, schema: &Table) -> Result<Vec<InsertRequest>, RequestError> {
    if self.key.obj.is_empty() {
      // data contains multiple objects
      let mut table = schema.to_empty();
      if !schema.is_kv() {
        table.insert_column(0, Field::new("obj", DataType::String, 0));
      }
      let table = match self.data {
        InsertData::Rows(mut rows) => {
          for row in rows.rows.drain(..) {
            table.push_row(row).map_err(|e| RequestError::from(e))?;
          }
          table
        }
        InsertData::Columns(cols) => {
          table.set_columns(cols).map_err(|e| RequestError::from(e))?;
          table
        }
        InsertData::Csv(csv) => {
          table_from_csv(Cursor::new(&csv), b',', &table).map_err(|e| RequestError::from(e))?
        }
        InsertData::Table(table) => table,
      };
      let table_name = self.key.table.clone();
      Ok(
        table
          .group_by(0)?
          .into_iter()
          .map(|(k, t)| {
            let data = if schema.is_kv() {
              let key_col_schema = schema.column_by_index(0).unwrap();
              let val_col_schema = schema.column_by_index(1).unwrap();
              let n = t.row_count();
              let key_series = match key_col_schema.kind {
                DataType::Bytes => {
                  let b = k.get_bytes().map(|b| b.to_vec()).unwrap_or_default();
                  Series::Bytes(vec![b; n])
                }
                _ => {
                  let s = k.get_str().map(|s| s.to_string()).unwrap_or_default();
                  Series::String(vec![s; n])
                }
              };
              let value_series = t.column_by_index(0).unwrap().data.clone();
              let mut new_t = Table::from_columns(vec![
                Field::new_with_data(key_col_schema.name.clone(), key_col_schema.kind, key_series),
                Field::new_with_data(val_col_schema.name.clone(), val_col_schema.kind, value_series),
              ]);
              new_t.set_is_kv(true);
              InsertData::Table(new_t)
            } else {
              InsertData::Table(t)
            };
            InsertRequest {
              key: RequestKey {
                obj: k.get_str().map(|s| s.to_string()).unwrap_or_default(),
                table: table_name.clone(),
              },
              data,
            }
          })
          .collect(),
      )
    } else {
      let mut table = schema.to_empty();
      let table = match self.data {
        InsertData::Rows(mut rows) => {
          for mut row in rows.rows.drain(..) {
            if schema.is_kv() && row.len() == 1 {
              let mut new_row = vec![Variant::String(self.key.obj.clone())];
              new_row.append(&mut row);
              table.push_row(new_row).map_err(|e| RequestError::from(e))?;
            } else {
              table.push_row(row).map_err(|e| RequestError::from(e))?;
            }
          }
          table
        }
        InsertData::Columns(cols) => {
          let cols = if schema.is_kv() && cols.len() == 1 {
            let n = cols[0].len();
            let key_series = Series::String(vec![self.key.obj.clone(); n]);
            vec![key_series, cols[0].clone()]
          } else {
            cols
          };
          table.set_columns(cols).map_err(|e| RequestError::from(e))?;
          table
        }
        InsertData::Csv(csv) => {
          if schema.is_kv() {
            let val_col_schema = schema.column_by_index(1).unwrap();
            let temp_template = Table::from_columns(vec![val_col_schema.to_empty()]);
            let temp_table = table_from_csv(Cursor::new(&csv), b',', &temp_template).map_err(|e| RequestError::from(e))?;
            let n = temp_table.row_count();
            let key_series = Series::String(vec![self.key.obj.clone(); n]);
            let key_col_schema = schema.column_by_index(0).unwrap();
            let value_series = temp_table.column_by_index(0).unwrap().data.clone();
            let mut new_t = Table::from_columns(vec![
              Field::new_with_data(key_col_schema.name.clone(), key_col_schema.kind, key_series),
              Field::new_with_data(val_col_schema.name.clone(), val_col_schema.kind, value_series),
            ]);
            new_t.set_is_kv(true);
            new_t
          } else {
            table_from_csv(Cursor::new(&csv), b',', &table).map_err(|e| RequestError::from(e))?
          }
        }
        InsertData::Table(mut table) => {
          if schema.is_kv() {
            if table.column_count() == 1 {
              let key_col_schema = schema.column_by_index(0).unwrap();
              let val_col_schema = schema.column_by_index(1).unwrap();
              let n = table.row_count();
              let key_series = Series::String(vec![self.key.obj.clone(); n]);
              let value_series = table.column_by_index(0).unwrap().data.clone();
              table = Table::from_columns(vec![
                Field::new_with_data(key_col_schema.name.clone(), key_col_schema.kind, key_series),
                Field::new_with_data(val_col_schema.name.clone(), val_col_schema.kind, value_series),
              ]);
            }
            table.set_is_kv(true);
          }
          table
        }
      };
      let mut res_table = table;
      if schema.is_kv() {
        res_table.set_is_kv(true);
      }
      Ok(vec![InsertRequest {
        key: self.key.clone(),
        data: InsertData::Table(res_table),
      }])
    }
  }

  pub fn take_table(&mut self) -> Result<Table, RequestError> {
    match &mut self.data {
      InsertData::Table(table) => Ok(std::mem::take(table)),
      _ => Err(RequestError::InvalidRequest(
        "Data is not a table".to_string(),
      )),
    }
  }
  pub fn take_rows(&mut self) -> Result<RowsTable, RequestError> {
    match &mut self.data {
      InsertData::Rows(rows) => Ok(std::mem::take(rows)),
      _ => Err(RequestError::InvalidRequest(
        "Data is not a rows table".to_string(),
      )),
    }
  }
}

pub type InsertResponse = Table;
