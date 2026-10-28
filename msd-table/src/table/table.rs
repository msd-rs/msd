// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{D64_NAN, Field, Series, TableError, Variant, VariantMutRef, VariantRef};

pub const TABLE_VERSION_1: u32 = 0x4d7c << 16 | 1;

/// # Table
/// A table is a columnar data structure, where each column has the same data type.
/// It's efficient for columnar data processing.
/// It also provide some row orientation APIs for data processing.
#[derive(Debug, Serialize, Deserialize, Clone, bincode_next::Encode, bincode_next::Decode)]
pub struct Table {
  version: u32,
  columns: Vec<Field>,
  metadata: Option<HashMap<String, Variant>>, // Optional field for additional metadata
  #[serde(default)]
  is_kv: bool,
}

impl Default for Table {
  fn default() -> Self {
    Self {
      version: TABLE_VERSION_1,
      columns: Vec::new(),
      metadata: None,
      is_kv: false,
    }
  }
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
      is_kv: false,
    }
  }

  pub fn to_empty(&self) -> Self {
    let columns = self.columns.iter().map(|col| col.to_empty()).collect();
    Self {
      version: self.version,
      columns,
      metadata: self.metadata.clone(),
      is_kv: self.is_kv,
    }
  }

  /// attach metadata to the table
  pub fn with_metadata(mut self, metadata: HashMap<String, Variant>) -> Self {
    match self.metadata {
      Some(mut meta) => {
        meta.extend(metadata);
        self.metadata = Some(meta);
      }
      None => {
        self.metadata = Some(metadata);
      }
    }
    self
  }

  /// replace metadata
  pub fn replace_metadata<K, V, const N: usize>(mut self, metadata: [(K, V); N]) -> Self
  where
    K: Into<String>,
    V: Into<Variant>,
  {
    if metadata.is_empty() {
      self.metadata = None;
    } else {
      self.metadata = Some(
        metadata
          .into_iter()
          .map(|(k, v)| (k.into(), v.into()))
          .collect(),
      );
    }
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

  /// get mutable table columns slice
  pub fn columns_mut(&mut self) -> &mut [Field] {
    &mut self.columns
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

  /// insert a new column at the index
  pub fn insert_column(&mut self, index: usize, column: Field) {
    if index > self.column_count() {
      return self.columns.push(column);
    }
    self.columns.insert(index, column);
  }

  /// remove a column by name
  pub fn remove_column(&mut self, name: &str) -> Option<Field> {
    if let Some(index) = self.columns.iter().position(|col| col.name == name) {
      Some(self.columns.remove(index))
    } else {
      None
    }
  }

  /// retain columns by filter
  pub fn retain_columns_by(&mut self, filter: impl Fn(&Field) -> bool) {
    let pk_col = self.pk_column();
    let pk_col = self.column_by_index(pk_col).map(|col| col.name.clone());
    self.columns.retain(|col| {
      filter(col)
        || pk_col
          .as_ref()
          .is_some_and(|pk| pk.eq_ignore_ascii_case(col.name.as_str()))
    });
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
    let mut iter = other.rows(rev);
    while let Some(row) = iter.next_cached() {
      if filter(&row) {
        for (col_self, cell) in self.columns.iter_mut().zip(row.into_iter()) {
          col_self.data.push(cell.to_variant())?;
        }
      }
    }

    Ok(())
  }

  /// Sort the table by primary key column.
  /// If descending is true, sort in descending order, otherwise ascending.
  pub fn sort_by_pk(&mut self, descending: bool) {
    let pk_col_index = self.pk_column();
    let indices = self.columns[pk_col_index].data.sorted_indices(descending);
    for col in self.columns.iter_mut() {
      col.data.sort_by_indices(&indices);
    }
  }

  /// Split the table into two tables, the first table contains the first `size` rows, the second table contains the rest.
  /// The original table is modified in place.
  /// If the table is empty, return an empty table.
  /// If the table has less than `size` rows, return Table is same as self and self will be empty.
  pub fn split_off_front(&mut self, size: usize) -> Table {
    let mut columns = Vec::with_capacity(self.columns.len());
    for col in self.columns.iter_mut() {
      let left = col.data.split_off_front(size);
      columns.push(col.with_data(left));
    }
    Self {
      version: self.version,
      columns,
      metadata: self.metadata.clone(),
      is_kv: self.is_kv,
    }
  }

  /// Split the table into chunks of size `size`.
  /// The last chunk may have less than `size` rows.
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

  /// Group the table by a column, keep the original order of rows in each group.
  /// the result table is a HashMap where the key is the column value and the value is the table removed the column.
  pub fn group_by(mut self, column_index: usize) -> Result<HashMap<Variant, Table>, TableError> {
    if column_index >= self.column_count() {
      return Err(TableError::ColumnIndexOutOfBounds(
        column_index,
        self.column_count(),
      ));
    }

    // Move the grouping column out so we don't duplicate it in result tables
    // The user requirement says "removed the column".
    let group_col = self.columns.remove(column_index);

    let groups = group_col.data.group_indices();

    let mut result = HashMap::new();

    for (key, indices) in groups {
      let mut new_columns = Vec::with_capacity(self.columns.len());
      for col in &self.columns {
        let new_data = col.data.select(&indices);
        new_columns.push(col.with_data(new_data));
      }

      let table = Table {
        version: self.version,
        columns: new_columns,
        metadata: self.metadata.clone(),
        is_kv: self.is_kv,
      };

      result.insert(key, table);
    }

    Ok(result)
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

  pub fn set_table_meta<S: AsRef<str>>(
    &mut self,
    key: S,
    value: Variant,
  ) -> Result<(), TableError> {
    self
      .metadata
      .get_or_insert_with(HashMap::new)
      .insert(key.as_ref().into(), value);
    Ok(())
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

  pub fn set_field_meta<S1: AsRef<str>, S2: AsRef<str>>(
    &mut self,
    field_name: S1,
    key: S2,
    value: Variant,
  ) -> Result<(), TableError> {
    let col = self
      .columns
      .iter_mut()
      .find(|col| col.name == field_name.as_ref())
      .ok_or(TableError::FieldNotFound(field_name.as_ref().into()))?;
    col
      .metadata
      .get_or_insert_with(HashMap::new)
      .insert(key.as_ref().into(), value);
    Ok(())
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

  pub fn is_kv(&self) -> bool {
    self.is_kv
  }

  pub fn set_is_kv(&mut self, is_kv: bool) {
    self.is_kv = is_kv;
  }
}

/// # Table join operation

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FillMethod {
  #[default]
  Zero,
  Nan,
  Backward,
  Forward,
}

impl From<i32> for FillMethod {
  fn from(value: i32) -> Self {
    match value {
      1 => Self::Nan,
      2 => Self::Backward,
      3 => Self::Forward,
      _ => Self::Zero,
    }
  }
}

impl From<FillMethod> for i32 {
  fn from(value: FillMethod) -> i32 {
    match value {
      FillMethod::Zero => 0,
      FillMethod::Nan => 1,
      FillMethod::Backward => 2,
      FillMethod::Forward => 3,
    }
  }
}

impl From<&str> for FillMethod {
  fn from(value: &str) -> Self {
    match value {
      "nan" => Self::Nan,
      "backward" => Self::Backward,
      "forward" => Self::Forward,
      _ => Self::Zero,
    }
  }
}

impl From<String> for FillMethod {
  fn from(value: String) -> Self {
    Self::from(value.as_str())
  }
}

impl From<&String> for FillMethod {
  fn from(value: &String) -> Self {
    Self::from(value.as_str())
  }
}

impl From<FillMethod> for &'static str {
  fn from(value: FillMethod) -> Self {
    match value {
      FillMethod::Zero => "zero",
      FillMethod::Nan => "nan",
      FillMethod::Backward => "backward",
      FillMethod::Forward => "forward",
    }
  }
}

/// Join `left` and `right` according to `method`, and return in `result` the indices of `right` that match `left`.
/// Both `left` and `right` are already sorted in ascending order.
pub fn joined_index<T>(left: &[T], right: &[T], method: FillMethod, result: &mut Vec<usize>)
where
  T: PartialOrd + PartialEq,
{
  if result.len() != left.len() {
    result.resize(left.len(), usize::MAX);
  }

  if left.is_empty() {
    return;
  }

  if right.is_empty() {
    result.fill(usize::MAX);
    return;
  }

  match method {
    FillMethod::Backward => joined_index_backward(left, right, result),
    FillMethod::Forward => joined_index_forward(left, right, result),
    FillMethod::Zero | FillMethod::Nan => joined_index_exact(left, right, result),
  }
}

#[inline]
fn joined_index_backward<T: PartialOrd>(left: &[T], right: &[T], result: &mut [usize]) {
  let right_len = right.len();
  let mut curr: Option<usize> = None;

  for (x, out) in left.iter().zip(result.iter_mut()) {
    if x.partial_cmp(x).is_none() {
      *out = usize::MAX;
      continue;
    }

    let mut cand = match curr {
      Some(c) if right[c] <= *x => c,
      _ => {
        if right[0] > *x {
          *out = usize::MAX;
          continue;
        }
        0
      }
    };

    if cand + 1 < right_len && right[cand + 1] <= *x {
      cand += 1;
      if cand + 1 < right_len && right[cand + 1] <= *x {
        cand += 1;
        let mut step = 2usize;
        while cand + step < right_len && right[cand + step] <= *x {
          cand += step;
          step = step.saturating_mul(2);
        }
        let upper = (cand + step).min(right_len);
        let offset = right[cand + 1..upper].partition_point(|r| r <= x);
        cand += offset;
      }
    }

    *out = cand;
    curr = Some(cand);
  }
}

#[inline]
fn joined_index_exact<T: PartialOrd + PartialEq>(left: &[T], right: &[T], result: &mut [usize]) {
  let right_len = right.len();
  let mut curr: Option<usize> = None;

  for (x, out) in left.iter().zip(result.iter_mut()) {
    if x.partial_cmp(x).is_none() {
      *out = usize::MAX;
      continue;
    }

    let mut cand = match curr {
      Some(c) if right[c] <= *x => c,
      _ => {
        if right[0] > *x {
          *out = usize::MAX;
          continue;
        }
        0
      }
    };

    if cand + 1 < right_len && right[cand + 1] <= *x {
      cand += 1;
      if cand + 1 < right_len && right[cand + 1] <= *x {
        cand += 1;
        let mut step = 2usize;
        while cand + step < right_len && right[cand + step] <= *x {
          cand += step;
          step = step.saturating_mul(2);
        }
        let upper = (cand + step).min(right_len);
        let offset = right[cand + 1..upper].partition_point(|r| r <= x);
        cand += offset;
      }
    }

    curr = Some(cand);
    if right[cand] == *x {
      *out = cand;
    } else {
      *out = usize::MAX;
    }
  }
}

#[inline]
fn joined_index_forward<T: PartialOrd>(left: &[T], right: &[T], result: &mut [usize]) {
  let right_len = right.len();
  let mut curr = 0usize;

  for (x, out) in left.iter().zip(result.iter_mut()) {
    if x.partial_cmp(x).is_none() {
      *out = usize::MAX;
      continue;
    }

    if curr >= right_len {
      *out = usize::MAX;
      continue;
    }

    if right[curr] >= *x {
      *out = curr;
      continue;
    }

    if curr + 1 >= right_len {
      curr = right_len;
      *out = usize::MAX;
      continue;
    }
    if right[curr + 1] >= *x {
      curr += 1;
      *out = curr;
      continue;
    }

    if curr + 2 >= right_len {
      curr = right_len;
      *out = usize::MAX;
      continue;
    }
    if right[curr + 2] >= *x {
      curr += 2;
      *out = curr;
      continue;
    }

    let mut step = 4usize;
    curr += 2;
    while curr + step < right_len && right[curr + step] < *x {
      curr += step;
      step = step.saturating_mul(2);
    }

    let upper = (curr + step).min(right_len);
    let offset = right[curr + 1..upper].partition_point(|r| r < x);
    let found = curr + 1 + offset;

    if found < right_len && right[found] >= *x {
      curr = found;
      *out = found;
    } else {
      curr = right_len;
      *out = usize::MAX;
    }
  }
}

fn select_with_fill<T: Clone>(data: &[T], indices: &[usize], fill_val: T) -> Vec<T> {
  let mut result = Vec::with_capacity(indices.len());
  for &i in indices {
    if let Some(val) = data.get(i) {
      result.push(val.clone());
    } else {
      result.push(fill_val.clone());
    }
  }
  result
}

fn select_series_with_fill(series: &Series, indices: &[usize], fill: FillMethod) -> Series {
  let use_zero = matches!(fill, FillMethod::Zero);
  match series {
    Series::Null => Series::Null,
    Series::String(v) => Series::String(select_with_fill(v, indices, String::new())),
    Series::Bytes(v) => Series::Bytes(select_with_fill(v, indices, Vec::new())),
    Series::Int32(v) => Series::Int32(select_with_fill(v, indices, 0)),
    Series::UInt32(v) => Series::UInt32(select_with_fill(v, indices, 0)),
    Series::Int64(v) => Series::Int64(select_with_fill(v, indices, 0)),
    Series::UInt64(v) => Series::UInt64(select_with_fill(v, indices, 0)),
    Series::Float32(v) => {
      let fill_val = if use_zero { 0.0 } else { f32::NAN };
      Series::Float32(select_with_fill(v, indices, fill_val))
    }
    Series::Float64(v) => {
      let fill_val = if use_zero { 0.0 } else { f64::NAN };
      Series::Float64(select_with_fill(v, indices, fill_val))
    }
    Series::Decimal64(v) => {
      let fill_val = if use_zero {
        v.iter()
          .find(|d| !d.is_nan() && !d.is_inf())
          .map(|d| d.to_zero())
          .unwrap_or_default()
      } else {
        D64_NAN
      };
      Series::Decimal64(select_with_fill(v, indices, fill_val))
    }
    Series::Bool(v) => Series::Bool(select_with_fill(v, indices, false)),
    Series::DateTime(v) => Series::DateTime(select_with_fill(v, indices, 0)),
  }
}

impl Table {
  /// concat right table to self in horizontal order
  /// it required self and right has same rows count,
  /// join just append `right` columns to self.
  /// after join, right will be consumed
  pub fn h_concat(&mut self, right: Table) -> Result<(), Table> {
    if self.row_count() != right.row_count() {
      return Err(right);
    }
    for col in right.columns.into_iter() {
      self.columns.push(col);
    }
    Ok(())
  }
  /// concat right table to self in vertical order
  /// it required self and right has same columns count,
  /// join just append `right` columns to self.
  /// after join, right will be consumed
  pub fn v_concat(&mut self, right: &Table) -> Result<(), TableError> {
    self.extend(right, false)
  }

  pub fn join_asof(&self, other: &Table, fill: FillMethod) -> Result<Table, TableError> {
    if self.columns.is_empty() || other.columns.is_empty() {
      return Ok(self.clone());
    }

    let self_pk_idx = self.pk_column();
    let other_pk_idx = other.pk_column();
    let self_pk = &self.columns[self_pk_idx];
    let other_pk = &other.columns[other_pk_idx];

    if self_pk.kind != other_pk.kind {
      return Err(TableError::TypeMismatch(self_pk.kind, other_pk.kind));
    }

    let mut indices = Vec::with_capacity(self.row_count());
    match (&self_pk.data, &other_pk.data) {
      (Series::Null, Series::Null) => indices.resize(self.row_count(), usize::MAX),
      (Series::DateTime(l), Series::DateTime(r)) => joined_index(l, r, fill, &mut indices),
      (Series::Int64(l), Series::Int64(r)) => joined_index(l, r, fill, &mut indices),
      (Series::Float64(l), Series::Float64(r)) => joined_index(l, r, fill, &mut indices),
      (Series::Decimal64(l), Series::Decimal64(r)) => joined_index(l, r, fill, &mut indices),
      (Series::String(l), Series::String(r)) => joined_index(l, r, fill, &mut indices),
      (Series::Bool(l), Series::Bool(r)) => joined_index(l, r, fill, &mut indices),
      (Series::Int32(l), Series::Int32(r)) => joined_index(l, r, fill, &mut indices),
      (Series::UInt32(l), Series::UInt32(r)) => joined_index(l, r, fill, &mut indices),
      (Series::UInt64(l), Series::UInt64(r)) => joined_index(l, r, fill, &mut indices),
      (Series::Float32(l), Series::Float32(r)) => joined_index(l, r, fill, &mut indices),
      (Series::Bytes(l), Series::Bytes(r)) => joined_index(l, r, fill, &mut indices),
      (a, b) => return Err(TableError::TypeMismatch(a.data_type(), b.data_type())),
    }

    let mut columns = Vec::with_capacity(self.column_count() + other.column_count() - 1);
    columns.extend(self.columns.iter().cloned());

    for (idx, col) in other.columns.iter().enumerate() {
      if idx == other_pk_idx {
        continue;
      }
      let data = select_series_with_fill(&col.data, &indices, fill);
      columns.push(col.with_data(data));
    }

    Ok(Self {
      version: self.version,
      columns,
      metadata: self.metadata.clone(),
      is_kv: self.is_kv,
    })
  }
}

impl Into<Vec<Field>> for Table {
  fn into(self) -> Vec<Field> {
    self.columns
  }
}

pub struct RowIter<'t> {
  table: &'t Table,
  index: usize,
  rev: bool,
  rows: usize,
  template: Vec<VariantRef<'t>>,
}

impl<'t> RowIter<'t> {
  fn new(table: &'t Table, rev: bool) -> Self {
    Self {
      table,
      index: 0,
      rev,
      rows: table.row_count(),
      template: vec![VariantRef::Null; table.column_count()],
    }
  }

  pub fn next_cached(&mut self) -> Option<&Vec<VariantRef<'t>>> {
    if self.index >= self.rows {
      return None;
    }
    let i = if self.rev {
      self.rows - 1 - self.index
    } else {
      self.index
    };

    self
      .template
      .iter_mut()
      .zip(&self.table.columns)
      .for_each(|(cell, col)| {
        *cell = unsafe { col.data.get_unchecked(i) };
      });

    self.index += 1;
    Some(&self.template)
  }
}

impl<'t> Iterator for RowIter<'t> {
  type Item = Vec<VariantRef<'t>>;
  fn next(&mut self) -> Option<Self::Item> {
    self.next_cached().map(|row| row.to_vec())
  }
}
