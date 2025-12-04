use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{DataType, Series, Variant};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Field {
  pub name: String,
  pub kind: DataType,
  pub metadata: Option<HashMap<String, Variant>>, // Optional field for additional metadata
  pub data: Series,
}

impl Field {
  pub fn new(name: impl Into<String>, kind: DataType, rows: usize) -> Self {
    Self {
      name: name.into(),
      kind,
      metadata: None,
      data: Series::new(kind, rows),
    }
  }

  pub fn new_with_data(name: impl Into<String>, kind: DataType, mut data: Series) -> Self {
    if data.data_type() != kind {
      data = data.cast_to(kind);
    }
    Self {
      name: name.into(),
      kind,
      metadata: None,
      data,
    }
  }

  pub fn with_metadata(mut self, metadata: HashMap<String, Variant>) -> Self {
    self.metadata = Some(metadata);
    self
  }

  pub fn to_empty(&self) -> Self {
    Self {
      name: self.name.clone(),
      kind: self.kind.clone(),
      metadata: self.metadata.clone(),
      data: Series::new(self.kind.clone(), 0),
    }
  }

  pub fn with_data(&self, data: Series) -> Self {
    Self {
      name: self.name.clone(),
      kind: self.kind.clone(),
      metadata: self.metadata.clone(),
      data,
    }
  }

  /// check if this field is marked as primary key
  ///
  /// returns true if the field has metadata "primary_key" set and not null
  pub fn is_pk(&self) -> bool {
    if let Some(meta) = &self.metadata {
      if let Some(v) = meta.get("primary_key") {
        return !v.is_null();
      }
    }
    false
  }

  /// get metadata value by key
  pub fn get_metadata(&self, key: &str) -> Option<&Variant> {
    self.metadata.as_ref().and_then(|meta| meta.get(key))
  }
}

impl PartialEq for Field {
  fn eq(&self, other: &Self) -> bool {
    self.name == other.name && self.kind == other.kind
  }
}
impl Eq for Field {}
