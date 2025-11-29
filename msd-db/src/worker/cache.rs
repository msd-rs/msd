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
