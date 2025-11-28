use msd_table::{Table, Variant};
use rustc_hash::FxHashMap;

use crate::index::IndexItem;

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
struct CacheKey {
  table: String,
  object: String,
}

impl CacheKey {
  fn new(table: &str, object: &str) -> Self {
    Self {
      table: table.to_string(),
      object: object.to_string(),
    }
  }
}

#[derive(Debug, Default)]
pub struct CacheValue {
  cached: Table,
  index: Vec<IndexItem>,
  state: Vec<(u64, Variant)>,
}

pub type CacheMap = FxHashMap<CacheKey, CacheValue>;
