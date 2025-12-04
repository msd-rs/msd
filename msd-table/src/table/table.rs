use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{Field, Series, TableError, Variant, VariantMutRef, VariantRef};

const TABLE_VERSION_1: u32 = 0x4d7c << 16 | 1;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Table {
  version: u32,
  columns: Vec<Field>,
  metadata: Option<HashMap<String, Variant>>, // Optional field for additional metadata
}

/// # Table creation and schema management
impl Table {
  /// Create a new Table from a vector of TableColumn
  /// This is useful when you want to create a table with pre-populated data
  pub fn from_columns(columns: Vec<Field>) -> Self {
    Self {
      version: TABLE_VERSION_1,
      columns,
      metadata: None,
    }
  }

  pub fn to_empty(&self) -> Self {
    let columns = self.columns.iter().map(|col| col.to_empty()).collect();
    Self {
      version: self.version,
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
      if col_self != col_other {
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
      .map(|(i, col)| format!("({},{},{})", i, col.name, col.kind))
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
  pub fn columns(&self) -> &[Field] {
    &self.columns
  }

  /// get table column by index
  /// returns None if the index is out of bounds
  pub fn column_by_index(&self, index: usize) -> Option<&Field> {
    self.columns.get(index)
  }

  // get column by name
  /// returns None if the column with the given name does not exist
  pub fn column(&self, name: &str) -> Option<&Field> {
    self.columns.iter().find(|col| col.name == name)
  }

  /// get mutable column by name
  pub fn column_mut(&mut self, name: &str) -> Option<&mut Field> {
    self.columns.iter_mut().find(|col| col.name == name)
  }

  pub fn set_columns(&mut self, cols: Vec<Series>) -> Result<(), TableError> {
    if cols.len() != self.column_count() {
      return Err(TableError::ColumnCountMismatch(
        cols.len(),
        self.column_count(),
      ));
    }

    // try to cast each column to the schema type
    let cols = self
      .columns()
      .iter()
      .zip(cols.into_iter())
      .map(|(col_schema, col_data)| col_data.try_cast_to(col_schema.kind))
      .collect::<Result<Vec<_>, _>>()?;

    for (col, new_data) in self.columns.iter_mut().zip(cols.into_iter()) {
      col.data = new_data;
    }

    Ok(())
  }

  /// insert a new column at the end of the table
  pub fn add_column(&mut self, column: Field) {
    self.columns.push(column);
  }

  /// remove a column by name
  pub fn remove_column(&mut self, name: &str) -> Option<Field> {
    if let Some(index) = self.columns.iter().position(|col| col.name == name) {
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
    self.columns.iter().position(|col| col.is_pk()).unwrap_or(0)
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

  /// Sort the table by primary key column.
  /// If descending is true, sort in descending order, otherwise ascending.
  pub fn sort_by_pk(&mut self, descending: bool) {
    let pk_col_index = self.pk_column();
    let pk_series = match self.columns[pk_col_index].data.get_datetime() {
      Some(s) => s,
      None => return,
    };

    let mut indices: Vec<usize> = (0..self.row_count()).collect();

    if descending {
      indices.sort_by(|&a, &b| pk_series[b].cmp(&pk_series[a]));
    } else {
      indices.sort_by(|&a, &b| pk_series[a].cmp(&pk_series[b]));
    }

    for col in self.columns.iter_mut() {
      col.data.reorder(indices.as_slice());
    }
  }

  pub fn split_off_front(&mut self, size: usize) -> Table {
    let mut new_table = self.to_empty();
    for col in self.columns.iter_mut() {
      let left = col.data.split_off_front(size);
      new_table.add_column(col.with_data(left));
    }
    new_table
  }

  pub fn chunks(mut self, size: usize) -> Vec<Table> {
    let mut tables = Vec::new();
    while self.row_count() > size {
      tables.push(self.split_off_front(size));
    }
    if self.row_count() > 0 {
      tables.push(self);
    }
    tables
  }
}

/// # Table metadata access
impl Table {
  /// Get a table meta value
  pub fn get_table_meta<S: AsRef<str>>(&self, key: S) -> Option<&Variant> {
    self
      .metadata
      .as_ref()
      .and_then(|meta| meta.get(key.as_ref()))
  }

  /// get a field meta value
  pub fn get_field_meta<S1: AsRef<str>, S2: AsRef<str>>(
    &self,
    field_name: S1,
    key: S2,
  ) -> Option<&Variant> {
    self
      .columns
      .iter()
      .find(|col| col.name == field_name.as_ref())
      .and_then(|col| col.metadata.as_ref())
      .and_then(|meta| meta.get(key.as_ref()))
  }

  /// get field meta value by column index
  pub fn get_field_meta_by_index<S: AsRef<str>>(
    &self,
    field_index: usize,
    key: S,
  ) -> Option<&Variant> {
    self
      .columns
      .get(field_index)
      .and_then(|col| col.metadata.as_ref())
      .and_then(|meta| meta.get(key.as_ref()))
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
