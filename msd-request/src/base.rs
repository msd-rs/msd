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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DateRange {
  /// start (timestamp, inclusive)
  pub start: Option<(i64, bool)>,
  /// end (timestamp, inclusive)
  pub end: Option<(i64, bool)>,
}

impl DateRange {
  pub fn contains(&self, ts: i64) -> bool {
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
}
