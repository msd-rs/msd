use msd_table::Table;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::Deref;

use super::base::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
  #[serde(flatten)]
  pub key: RequestKey,
  /// fields to retrieve, None means all fields
  pub fields: Option<Vec<String>>,
  /// (timestamp, inclusive) start timestamp, None means from the beginning
  pub start: Option<(i64, bool)>,
  /// (timestamp, inclusive) end timestamp, None means until the end
  pub end: Option<(i64, bool)>,
  /// whether to sort ascendancy
  pub ascending: Option<bool>,
  /// limit number of results, None means no limit
  pub limit: Option<usize>,
}

impl QueryRequest {
  pub fn in_range(&self, ts: i64) -> bool {
    let start_ok = match self.start {
      Some((start_ts, inclusive)) => {
        if inclusive {
          ts >= start_ts
        } else {
          ts > start_ts
        }
      }
      None => true,
    };
    let end_ok = match self.end {
      Some((end_ts, inclusive)) => {
        if inclusive {
          ts <= end_ts
        } else {
          ts < end_ts
        }
      }
      None => true,
    };
    start_ok && end_ok
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

impl DbRequest for QueryRequest {
  type Response = QueryResponse;
}
