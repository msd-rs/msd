use msd_table::{Table, Variant};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::Deref;

use super::base::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
  #[serde(flatten)]
  pub key: RequestKey,
}

impl Deref for QueryRequest {
  type Target = RequestKey;

  fn deref(&self) -> &Self::Target {
    &self.key
  }
}

impl Hash for QueryRequest {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

pub type QueryResponse = Table;

impl DbRequest for QueryRequest {
  type Response = QueryResponse;
}
