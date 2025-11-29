//! Index item definition.
//!
//! An `IndexItem` represents a range of timestamps and the count of items within that range.
//! It is used as part of cache state management in the Worker.

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct IndexItem {
  /// start timestamp (inclusive)
  pub start: u64,
  /// end timestamp (exclusive)
  pub end: u64,
  /// count of items in this range
  pub count: u64,
}

impl PartialOrd for IndexItem {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    match self.start.partial_cmp(&other.start) {
      Some(core::cmp::Ordering::Equal) => {}
      ord => return ord,
    }
    match self.end.partial_cmp(&other.end) {
      Some(core::cmp::Ordering::Equal) => {}
      ord => return ord,
    }
    self.count.partial_cmp(&other.count)
  }
}

impl PartialEq for IndexItem {
  fn eq(&self, other: &Self) -> bool {
    self.start == other.start && self.end == other.end
  }
}

impl Ord for IndexItem {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.start.cmp(&other.start).then(self.end.cmp(&other.end))
  }
}

impl Eq for IndexItem {}
