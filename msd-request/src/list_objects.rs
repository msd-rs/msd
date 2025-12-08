use msd_table::Table;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::Deref;

use super::base::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListObjectsRequest {
  #[serde(flatten)]
  pub key: RequestKey,
}

impl Deref for ListObjectsRequest {
  type Target = RequestKey;

  fn deref(&self) -> &Self::Target {
    &self.key
  }
}

impl Hash for ListObjectsRequest {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

pub type ListObjectsResponse = Table;
