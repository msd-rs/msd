// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! Index item definition.
//!
//! An `IndexItem` represents a range of timestamps and the count of items within that range.
//! It is used as part of cache state management in the Worker.

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct IndexItem {
  /// start timestamp (inclusive)
  pub start: i64,
  /// end timestamp (exclusive)
  pub end: i64,
  /// count of items in this range
  pub count: u64,
}

impl Ord for IndexItem {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    if self.start < other.start {
      return std::cmp::Ordering::Less;
    } else if self.start > other.start {
      return std::cmp::Ordering::Greater;
    }
    if self.end < other.end {
      return std::cmp::Ordering::Less;
    } else if self.end > other.end {
      return std::cmp::Ordering::Greater;
    }
    std::cmp::Ordering::Equal
  }
}

impl PartialOrd for IndexItem {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl PartialEq for IndexItem {
  fn eq(&self, other: &Self) -> bool {
    self.cmp(other) == std::cmp::Ordering::Equal
  }
}

impl Eq for IndexItem {}
