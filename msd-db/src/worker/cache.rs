use msd_table::Table;
use rustc_hash::FxHashMap;

use crate::{index::IndexItem, request::RequestKey, worker::agg_state::AggState};

#[derive(Debug, Default)]
pub struct CacheValue {
  pub cached: Table,
  pub index: Vec<IndexItem>,
  pub state: Vec<Option<AggState>>,
}

pub type CacheMap = FxHashMap<RequestKey, CacheValue>;

impl CacheValue {
  pub fn last_pk(&self) -> Option<i64> {
    self
      .cached
      .column_by_index(self.cached.pk_column())
      .and_then(|col| col.data.get_datetime())
      .and_then(|ts| ts.last())
      .copied()
  }
}
