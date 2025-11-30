use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{DataType, Variant};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Field {
  pub name: String,
  pub kind: DataType,
  pub metadata: Option<HashMap<String, Variant>>, // Optional field for additional metadata
}

impl Field {
  pub fn new(name: impl Into<String>, kind: DataType) -> Self {
    Self {
      name: name.into(),
      kind,
      metadata: None,
    }
  }
  pub fn with_metadata(mut self, metadata: HashMap<String, Variant>) -> Self {
    self.metadata = Some(metadata);
    self
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
}

impl PartialEq for Field {
  fn eq(&self, other: &Self) -> bool {
    self.name == other.name && self.kind == other.kind
  }
}
impl Eq for Field {}
