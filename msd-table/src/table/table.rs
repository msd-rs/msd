use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{Field, Series, VariantMutRef, table::variant::VariantRef};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableColumn {
  pub schema: Field,
  pub data: Series,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
  columns: Vec<TableColumn>,
  metadata: Option<HashMap<String, String>>, // Optional field for additional metadata
}

impl Table {
  pub fn new(columns: Vec<Field>, rows: usize) -> Self {
    let data = columns
      .into_iter()
      .map(|field| TableColumn {
        data: Series::new(field.kind, rows),
        schema: field,
      })
      .collect();
    Self {
      columns: data,
      metadata: None,
    }
  }

  pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
    self.metadata = Some(metadata);
    self
  }

  pub fn column_count(&self) -> usize {
    self.columns.len()
  }

  pub fn columns(&self) -> &[TableColumn] {
    &self.columns
  }

  pub fn column_by_index(&self, index: usize) -> Option<&TableColumn> {
    self.columns.get(index)
  }

  pub fn column(&self, name: &str) -> Option<&TableColumn> {
    self.columns.iter().find(|col| col.schema.name == name)
  }

  pub fn column_mut(&mut self, name: &str) -> Option<&mut TableColumn> {
    self.columns.iter_mut().find(|col| col.schema.name == name)
  }

  pub fn add_column(&mut self, column: TableColumn) {
    self.columns.push(column);
  }

  pub fn remove_column(&mut self, name: &str) -> Option<TableColumn> {
    if let Some(index) = self.columns.iter().position(|col| col.schema.name == name) {
      Some(self.columns.remove(index))
    } else {
      None
    }
  }

  pub fn row_count(&self) -> usize {
    if self.columns.is_empty() {
      0
    } else {
      self.columns[0].data.len()
    }
  }

  pub fn rows(&self) -> RowIter {
    RowIter::new(self)
  }

  pub fn get_row(&self, index: usize) -> Option<Vec<VariantRef>> {
    if index >= self.row_count() {
      return None;
    }

    Some(
      self
        .columns
        .iter()
        .map(|col| col.data.get(index).unwrap_or(VariantRef::Null))
        .collect(),
    )
  }

  pub fn get_row_mut(&mut self, index: usize) -> Option<Vec<VariantMutRef>> {
    if index >= self.row_count() {
      return None;
    }

    Some(
      self
        .columns
        .iter_mut()
        .map(|col| {
          col
            .data
            .get_mut(index)
            .unwrap_or(super::variant::VariantMutRef::Null)
        })
        .collect(),
    )
  }
}

pub struct RowIter<'a> {
  table: &'a Table,
  index: usize,
}

impl<'a> RowIter<'a> {
  fn new(table: &'a Table) -> Self {
    Self { table, index: 0 }
  }
}

impl<'a> Iterator for RowIter<'a> {
  type Item = Vec<VariantRef<'a>>;
  fn next(&mut self) -> Option<Self::Item> {
    if self.index >= self.table.row_count() {
      return None;
    }

    let row = self
      .table
      .columns
      .iter()
      .map(|col| col.data.get(self.index).unwrap_or(VariantRef::Null))
      .collect();
    self.index += 1;
    Some(row)
  }
}
