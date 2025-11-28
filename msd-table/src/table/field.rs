use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::DataType;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Field {
  pub name: String,
  pub kind: DataType,
  pub metadata: Option<HashMap<String, String>>, // Optional field for additional metadata
}

impl Field {
  pub fn new(name: impl Into<String>, kind: DataType) -> Self {
    Self {
      name: name.into(),
      kind,
      metadata: None,
    }
  }
  pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
    self.metadata = Some(metadata);
    self
  }
}
