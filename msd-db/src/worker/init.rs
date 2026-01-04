// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::{
  errors::DbError,
  index::IndexItem,
  serde::DbBinary,
  worker::{agg_state::AggState, cache::CacheValue, chan::Chan},
};

use super::Worker;
use msd_request::{Key, RequestKey};
use msd_store::MsdStore;
use msd_table::{Table, now};

impl<S: MsdStore> Worker<S> {
  /// Ensure that the cache for the given request key is initialized.
  /// If not, load the necessary data from the store.
  /// # Returns
  /// - Ok(true) if the cache was successfully initialized or already present.
  /// - Ok(false) if there is no data to initialize the cache. Caller should handle this case.
  ///   - For queries, this means returning an empty result.
  ///   - For inserts, can proceed to create new data.
  /// - Err(DbError) if there was an error during the process.
  pub(crate) fn ensure_cache_initialized(&mut self, key: &RequestKey) -> Result<bool, DbError> {
    if self.cache.contains_key(key) {
      return Ok(true);
    }
    let index_key = Key::new_index(&key.obj);
    let index = match self.store.get(index_key, &key.table)? {
      Some(data) => data,
      None => return Ok(false),
    };
    let index: Vec<IndexItem> = DbBinary::from_bytes(&index)?;
    if index.is_empty() {
      return Ok(false);
    }
    let last_seq = (index.len() - 1) as u32;
    let data_key = Key::new_data(&key.obj, last_seq);
    let data = self
      .store
      .get(data_key, &key.table)?
      .ok_or(DbError::ChunkMissing(key.clone(), last_seq))?;
    let table: Table = DbBinary::from_bytes(&data)?;
    let state = AggState::table_states(&table);
    let chan = Chan::try_from(&table).ok();
    let last_saved = now();
    self.cache.insert(
      key.clone(),
      CacheValue {
        cached: table,
        index,
        state,
        chan,
        last_changed: last_saved,
      },
    );
    Ok(true)
  }
}
