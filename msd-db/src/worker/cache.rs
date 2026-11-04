// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::{LazyChunk, Table};
use rustc_hash::FxHashMap;

use crate::{
  errors::DbError, index::IndexItem, request::RequestKey, worker::agg_state::AggState,
  worker::chan::Chan,
};

#[derive(Debug, Default)]
pub struct CacheValue {
  pub cached: Table,
  pub lazy_last: Option<LazyChunk>,
  pub lazy_prev: Option<(u32, LazyChunk)>,
  pub index: Vec<IndexItem>,
  pub state: Vec<Option<AggState>>,
  pub chan: Option<Vec<Chan>>,
  pub last_changed: i64,
}

pub type CacheMap = FxHashMap<RequestKey, CacheValue>;

impl CacheValue {
  pub fn materialize_for_write(&mut self, schema: &Table) -> Result<(), DbError> {
    if let Some(lazy) = self.lazy_last.take() {
      self.cached = lazy.decode_full_table()?;
    }
    if self.state.is_empty() && schema.column_count() > 0 {
      self.state = AggState::table_states(schema);
      self.chan = Chan::parse_from_table(schema);
    }
    Ok(())
  }

  pub fn first_pk(&self) -> i64 {
    self
      .cached
      .column_by_index(self.cached.pk_column())
      .and_then(|col| col.data.get_datetime())
      .and_then(|ts| ts.first())
      .copied()
      .unwrap_or(0)
  }

  pub fn last_pk(&self) -> i64 {
    self
      .cached
      .column_by_index(self.cached.pk_column())
      .and_then(|col| col.data.get_datetime())
      .and_then(|ts| ts.last())
      .copied()
      .unwrap_or(0)
  }
}
