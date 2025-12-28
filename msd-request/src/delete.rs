// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::Table;
use serde::{Deserialize, Serialize};

use crate::{DateRange, RequestKey};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeleteRequest {
  #[serde(flatten)]
  pub key: RequestKey,
  pub date_range: DateRange,
}

impl std::ops::Deref for DeleteRequest {
  type Target = RequestKey;

  fn deref(&self) -> &Self::Target {
    &self.key
  }
}

impl std::hash::Hash for DeleteRequest {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

pub type DeleteResponse = Table;
