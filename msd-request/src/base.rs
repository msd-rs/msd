use std::{fmt::Display, hash::Hash, sync::OnceLock};

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
