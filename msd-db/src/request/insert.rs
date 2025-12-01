use msd_table::{Series, Table, Variant};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::Deref;

use crate::errors::DbError;

use super::base::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsertData {
  Rows(Vec<Vec<Variant>>),
  Columns(Vec<Series>),
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

impl InsertData {
  pub fn to_table(self, schema: &Table) -> Result<Table, DbError> {
    match self {
      InsertData::Rows(rows) => {
        let mut table = Table::to_empty(schema);
        for row in rows {
          table.push_row(row).map_err(|e| DbError::from(e))?;
        }
        Ok(table)
      }
      InsertData::Columns(cols) => {
        let mut table = Table::to_empty(schema);

        table.set_columns(cols).map_err(|e| DbError::from(e))?;

        Ok(table)
      }
    }
  }
}

pub type InsertResponse = ();

impl DbRequest for InsertRequest {
  type Response = InsertResponse;
}
