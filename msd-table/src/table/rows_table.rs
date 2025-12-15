use serde::{Deserialize, Serialize};

use crate::{Table, Variant};

/// # RowsTable
/// A table with rows orientation.
/// It internal used to keep incoming data in row orientation.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RowsTable {
  rows: Vec<Vec<Variant>>,
  schema: Option<Table>,
}

impl From<&Table> for RowsTable {
  fn from(table: &Table) -> Self {
    Self {
      rows: Vec::new(),
      schema: Some(table.to_empty()),
    }
  }
}
