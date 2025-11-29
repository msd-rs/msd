use msd_table::{Table, Variant};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::Deref;

use super::base::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsertData {
  Table(Table),
  Row(Vec<Variant>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
  #[serde(flatten)]
  pub key: RequestKey,
  pub data: InsertData,
}

impl Hash for InsertRequest {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

impl Deref for InsertRequest {
  type Target = RequestKey;

  fn deref(&self) -> &Self::Target {
    &self.key
  }
}

pub type InsertResponse = ();

impl DbRequest for InsertRequest {
  type Response = InsertResponse;
}
