// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::{DataType, Field, RowsTable, Series, Table, table_from_csv};
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
      table.insert_column(0, Field::new("obj", DataType::String, 0));
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
          .map(|(k, t)| InsertRequest {
            key: RequestKey {
              obj: k.get_str().map(|s| s.to_string()).unwrap_or_default(),
              table: table_name.clone(),
            },
            data: InsertData::Table(t),
          })
          .collect(),
      )
    } else {
      let mut table = schema.to_empty();
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
      Ok(vec![InsertRequest {
        key: self.key.clone(),
        data: InsertData::Table(table),
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
