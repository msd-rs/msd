// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use serde::{Deserialize, Serialize};

use crate::{DataType, Table, TableError, Variant};

/// # RowsTable
/// A table with rows orientation.
/// It internal used to keep incoming data in row orientation.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RowsTable {
  pub rows: Vec<Vec<Variant>>,
  schema: Option<Table>,
}

impl RowsTable {
  pub fn new(schema: Option<&Table>, rows: Vec<Vec<Variant>>) -> Self {
    Self {
      rows,
      schema: schema.map(|t| t.to_empty()),
    }
  }

  pub fn row_count(&self) -> usize {
    self.rows.len()
  }

  pub fn column_count(&self) -> usize {
    self
      .schema
      .as_ref()
      .map_or(self.rows.first().map_or(0, |row| row.len()), |t| {
        t.column_count()
      })
  }

  pub fn rows(&self) -> &[Vec<Variant>] {
    &self.rows
  }
}

impl RowsTable {
  pub fn sort_by_pk(&mut self, descending: bool) {
    let pk_col = match self.schema.as_ref().map(|schema| schema.pk_column()) {
      Some(pk_col) => pk_col,
      None => return,
    };

    self.rows.sort_by(|row1, row2| {
      let pk1 = row1[pk_col].get_datetime().unwrap_or(&0);
      let pk2 = row2[pk_col].get_datetime().unwrap_or(&0);
      if descending {
        pk2.cmp(&pk1)
      } else {
        pk1.cmp(&pk2)
      }
    });
  }
}

impl RowsTable {
  pub fn add_row(&mut self, row: Vec<Variant>) -> Result<(), TableError> {
    let want_columns = self.column_count();
    if row.len() != want_columns && want_columns != 0 {
      return Err(TableError::ColumnCountMismatch(want_columns, row.len()));
    }
    self.rows.push(row);
    Ok(())
  }

  pub fn add_rows_from_csv(
    &mut self,
    lines: &[u8],
    sep: u8,
    skip_col: usize,
  ) -> Result<(), TableError> {
    let mut rdr = csv::ReaderBuilder::new()
      .has_headers(false)
      .delimiter(sep)
      .from_reader(lines);

    let mut want_columns = self.column_count() - skip_col;
    match self.schema.as_ref() {
      Some(schema) => {
        for result in rdr.records() {
          let record = result?;
          if record.len() != want_columns {
            return Err(TableError::ColumnCountMismatch(want_columns, record.len()));
          }
          match record
            .iter()
            .skip(skip_col)
            .zip(schema.columns())
            .map(|(cell, field)| Variant::from_str(cell, field.kind))
            .collect::<Result<Vec<Variant>, TableError>>()
          {
            Ok(row) => self.rows.push(row),
            Err(e) => return Err(e),
          }
        }
      }
      None => {
        // no schema, all cell type are string
        for result in rdr.records() {
          let record = result?;
          // only first row can add to empty table
          if record.len() != want_columns {
            if want_columns == 0 {
              want_columns = record.len();
            } else {
              return Err(TableError::ColumnCountMismatch(want_columns, record.len()));
            }
          }
          let row = record
            .iter()
            .skip(skip_col)
            .map(|cell| Variant::from_str(cell, DataType::String))
            .collect::<Result<Vec<Variant>, TableError>>()?;
          self.rows.push(row);
        }
      }
    }

    Ok(())
  }
}
