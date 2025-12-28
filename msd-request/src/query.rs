// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::Table;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::Deref;

use super::base::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryRequest {
  #[serde(flatten)]
  pub key: RequestKey,
  /// fields to retrieve, None means all fields
  pub fields: Option<Vec<String>>,
  pub date_range: DateRange,
  /// whether to sort ascendancy
  pub ascending: Option<bool>,
  /// limit number of results, None means no limit
  pub limit: Option<usize>,
  /// objects to query, replace obj in key with this
  pub objects: Option<Vec<String>>,
}

impl QueryRequest {
  pub fn in_range(&self, ts: i64) -> bool {
    self.date_range.contains(ts)
  }
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
