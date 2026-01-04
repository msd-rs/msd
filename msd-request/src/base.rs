// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::{fmt::Display, hash::Hash, i64, sync::OnceLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct RequestKey {
  pub table: String,
  pub obj: String,
}

pub fn broadcast_key() -> &'static RequestKey {
  static BROADCAST_KEY: OnceLock<RequestKey> = OnceLock::new();
  BROADCAST_KEY.get_or_init(|| RequestKey {
    table: "__broadcast__".into(),
    obj: "__broadcast__".into(),
  })
}

pub fn once_key() -> &'static RequestKey {
  static ONCE_KEY: OnceLock<RequestKey> = OnceLock::new();
  ONCE_KEY.get_or_init(|| RequestKey {
    table: "__once__".into(),
    obj: "__once__".into(),
  })
}

impl RequestKey {
  pub fn new<S: Into<String>>(table: S, obj: S) -> Self {
    Self {
      table: table.into(),
      obj: obj.into(),
    }
  }
  pub fn is_broadcast(&self) -> bool {
    self == broadcast_key()
  }
}

impl Display for RequestKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:{}", self.table, self.obj)
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Copy)]
pub struct DateRange {
  /// start (timestamp, inclusive)
  pub start: Option<(i64, bool)>,
  /// end (timestamp, inclusive)
  pub end: Option<(i64, bool)>,
}

impl DateRange {
  // Check if the timestamp is within the range, default to true if range is not set
  pub fn contains(&self, ts: i64) -> bool {
    if !self.is_set() {
      return true;
    }
    let start = self
      .start
      .as_ref()
      .map(|(ts, inclusive)| if *inclusive { *ts } else { *ts - 1 })
      .unwrap_or(0);
    let end = self
      .end
      .as_ref()
      .map(|(ts, inclusive)| if *inclusive { *ts } else { *ts + 1 })
      .unwrap_or(i64::MAX);
    start <= ts && end >= ts
  }
  // Check if any start or end is set
  pub fn is_set(&self) -> bool {
    self.start.is_some() || self.end.is_some()
  }
}
