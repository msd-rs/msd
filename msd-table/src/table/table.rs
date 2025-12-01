use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{Field, Series, TableError, Variant, VariantMutRef, VariantRef};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableColumn {
  pub schema: Field,
  pub data: Series,
}

impl TableColumn {
  pub fn new(schema: Field, data: Series) -> Self {
    let mut data = data;
    if schema.kind != data.data_type() {
      data = data.cast_to(schema.kind);
    }
    Self { schema, data }
  }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Table {
  columns: Vec<TableColumn>,
  metadata: Option<HashMap<String, Variant>>, // Optional field for additional metadata
}

/// # Table creation and schema management
impl Table {
  /// Create a new Table with the specified columns and number of rows
  /// rows can be 0, which means the table is empty
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

  /// Create a new Table from a vector of TableColumn
  /// This is useful when you want to create a table with pre-populated data
  pub fn from_columns(columns: Vec<TableColumn>) -> Self {
    Self {
      columns,
      metadata: None,
    }
  }

  pub fn to_empty(&self) -> Self {
    let columns = self
      .columns
      .iter()
      .map(|col| TableColumn {
        schema: col.schema.clone(),
        data: Series::new(col.schema.kind.clone(), 0),
      })
      .collect();
    Self {
      columns,
      metadata: self.metadata.clone(),
    }
  }

  /// attach metadata to the table
  pub fn with_metadata(mut self, metadata: HashMap<String, Variant>) -> Self {
    self.metadata = Some(metadata);
    self
  }

  pub fn same_shape(&self, other: &Table) -> bool {
    if self.column_count() != other.column_count() {
      return false;
    }

    for (col_self, col_other) in self.columns.iter().zip(other.columns.iter()) {
      if col_self.schema != col_other.schema {
        return false;
      }
    }
    true
  }

  pub(crate) fn schema_debug(&self) -> String {
    let schemas: Vec<String> = self
      .columns
      .iter()
      .enumerate()
      .map(|(i, col)| format!("({},{},{})", i, col.schema.name, col.schema.kind))
      .collect();
    schemas.join(", ")
  }
}

/// # Table cell access and manipulation
impl Table {
  /// get columns count
  pub fn column_count(&self) -> usize {
    self.columns.len()
  }

  /// get table columns slice
  pub fn columns(&self) -> &[TableColumn] {
    &self.columns
  }

  /// get table column by index
  /// returns None if the index is out of bounds
  pub fn column_by_index(&self, index: usize) -> Option<&TableColumn> {
    self.columns.get(index)
  }

  // get column by name
  /// returns None if the column with the given name does not exist
  pub fn column(&self, name: &str) -> Option<&TableColumn> {
    self.columns.iter().find(|col| col.schema.name == name)
  }

  /// get mutable column by name
  pub fn column_mut(&mut self, name: &str) -> Option<&mut TableColumn> {
    self.columns.iter_mut().find(|col| col.schema.name == name)
  }

  /// insert a new column at the end of the table
  pub fn add_column(&mut self, column: TableColumn) {
    self.columns.push(column);
  }

  /// remove a column by name
  pub fn remove_column(&mut self, name: &str) -> Option<TableColumn> {
    if let Some(index) = self.columns.iter().position(|col| col.schema.name == name) {
      Some(self.columns.remove(index))
    } else {
      None
    }
  }

  /// get the number of rows in the table
  pub fn row_count(&self) -> usize {
    if self.columns.is_empty() {
      0
    } else {
      self.columns[0].data.len()
    }
  }

  /// create a row iterator
  pub fn rows(&self, rev: bool) -> RowIter<'_> {
    RowIter::new(self, rev)
  }

  /// get a row by index
  pub fn get_row(&self, index: usize) -> Option<Vec<VariantRef<'_>>> {
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

  /// get a mutable row by index
  pub fn get_row_mut(&mut self, index: usize) -> Option<Vec<VariantMutRef<'_>>> {
    if index >= self.row_count() {
      return None;
    }

    Some(
      self
        .columns
        .iter_mut()
        .map(|col| col.data.get_mut(index).unwrap_or(VariantMutRef::Null))
        .collect(),
    )
  }

  /// set a row by index
  pub fn set_row(&mut self, index: usize, row: Vec<Variant>) -> Result<(), TableError> {
    if index >= self.row_count() {
      return Err(TableError::IndexOutOfBounds(index, self.row_count()));
    }

    if row.len() != self.column_count() {
      return Err(TableError::ColumnCountMismatch(
        row.len(),
        self.column_count(),
      ));
    }

    if let Some(dst_row) = self.get_row_mut(index) {
      row
        .into_iter()
        .zip(dst_row.into_iter())
        .map(|(src, dst)| dst.set(src))
        .collect::<Result<(), TableError>>()
    } else {
      Err(TableError::IndexOutOfBounds(index, self.row_count()))
    }
  }

  /// push a new row to the table
  pub fn push_row(&mut self, row: Vec<Variant>) -> Result<(), TableError> {
    if row.len() != self.column_count() {
      return Err(TableError::ColumnCountMismatch(
        row.len(),
        self.column_count(),
      ));
    }
    self
      .columns
      .iter_mut()
      .zip(row.into_iter())
      .map(|(col, value)| col.data.push(value))
      .collect::<Result<(), TableError>>()
  }

  /// get a cell value by row and column index
  /// if the row or column index is out of bounds, return None
  ///
  pub fn get_cell(&self, row: usize, col: usize) -> Option<VariantRef<'_>> {
    self.columns.get(col).and_then(|c| c.data.get(row))
  }

  /// get a mutable cell value by row and column index
  /// if the row or column index is out of bounds, return None
  pub fn get_cell_mut(&mut self, row: usize, col: usize) -> Option<VariantMutRef<'_>> {
    self.columns.get_mut(col).and_then(|c| c.data.get_mut(row))
  }

  /// get a cell value by row index and column index
  /// # Panics
  /// Panics if the index is out of bounds
  pub fn cell(&self, row: usize, col: usize) -> VariantRef<'_> {
    assert!(
      col < self.column_count(),
      "Column {0} index out of bounds {1}",
      col,
      self.column_count()
    );
    let s = unsafe { self.columns.get_unchecked(col) };
    assert!(
      row < s.data.len(),
      "Row {0} index out of bounds {1}",
      row,
      s.data.len()
    );
    unsafe { s.data.get_unchecked(row) }
  }

  /// get a mutable cell value by row and column index
  /// # Panics
  /// Panics if the index is out of bounds
  pub fn cell_mut(&mut self, row: usize, col: usize) -> VariantMutRef<'_> {
    assert!(
      col < self.column_count(),
      "Column {0} index out of bounds {1}",
      col,
      self.column_count()
    );
    let s = unsafe { self.columns.get_unchecked_mut(col) };
    assert!(
      row < s.data.len(),
      "Row {0} index out of bounds {1}",
      row,
      s.data.len()
    );
    unsafe { s.data.get_mut_unchecked(row) }
  }
}

/// # Table operations on rows
impl Table {
  pub fn pk_column(&self) -> usize {
    self
      .columns
      .iter()
      .position(|col| col.schema.is_pk())
      .unwrap_or(0)
  }

  /// reverse the order of rows in the table
  pub fn reverse_rows(&mut self) {
    for col in self.columns.iter_mut() {
      col.data.reverse();
    }
  }

  /// Extend the table by appending rows from another table.
  /// the order of rows appended is determined by the `rev` parameter.
  pub fn extend(&mut self, other: &Table, rev: bool) -> Result<(), TableError> {
    if self.column_count() != other.column_count() {
      return Err(TableError::ColumnCountMismatch(
        other.column_count(),
        self.column_count(),
      ));
    }
    if !self.same_shape(other) {
      return Err(TableError::ColumnSchemaMismatch(
        self.schema_debug(),
        other.schema_debug(),
      ));
    }

    for (col_self, col_other) in self.columns.iter_mut().zip(other.columns.iter()) {
      col_self.data.extend(&col_other.data, rev)?;
    }
    Ok(())
  }

  /// Extend the table by appending rows from another table with a filter.
  ///
  /// The filter function takes a row (as a vector of VariantRef) and returns true if the row should be included.
  /// the order of rows appended is determined by the `rev` parameter.
  pub fn extend_filtered<F: FnMut(&Vec<VariantRef<'_>>) -> bool>(
    &mut self,
    other: &Table,
    rev: bool,
    mut filter: F,
  ) -> Result<(), TableError> {
    if self.column_count() != other.column_count() {
      return Err(TableError::ColumnCountMismatch(
        other.column_count(),
        self.column_count(),
      ));
    }
    for row in other.rows(rev) {
      if filter(&row) {
        for (col_self, cell) in self.columns.iter_mut().zip(row.into_iter()) {
          col_self.data.push(cell.into())?;
        }
      }
    }

    Ok(())
  }
}

pub struct RowIter<'a> {
  table: &'a Table,
  index: usize,
  rev: bool,
}

impl<'a> RowIter<'a> {
  fn new(table: &'a Table, rev: bool) -> Self {
    Self {
      table,
      index: 0,
      rev,
    }
  }
}

impl<'a> Iterator for RowIter<'a> {
  type Item = Vec<VariantRef<'a>>;
  fn next(&mut self) -> Option<Self::Item> {
    if self.index >= self.table.row_count() {
      return None;
    }
    let i = if self.rev {
      self.table.row_count() - 1 - self.index
    } else {
      self.index
    };

    let row = self
      .table
      .columns
      .iter()
      .map(|col| col.data.get(i).unwrap_or(VariantRef::Null))
      .collect();
    self.index += 1;
    Some(row)
  }
}
