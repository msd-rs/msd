// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::{
  errors::DbError,
  index::IndexItem,
  serde::DbBinary,
  worker::cache::CacheValue,
};

use super::Worker;
use msd_request::{Key, RequestKey};
use msd_store::MsdStore;
use msd_table::{LazyChunk, Table};

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
    let mut decode_err: Option<DbError> = None;
    let index_opt = self.store.get_with(index_key, &key.table, |bytes| {
      let res: Result<Vec<IndexItem>, DbError> = DbBinary::from_bytes(bytes);
      match res {
        Ok(idx) => Some(idx),
        Err(e) => {
          decode_err = Some(e);
          None
        }
      }
    })?;
    if let Some(e) = decode_err {
      return Err(e);
    }
    let index = match index_opt.flatten() {
      Some(idx) if !idx.is_empty() => idx,
      _ => return Ok(false),
    };
    let last_seq = (index.len() - 1) as u32;
    let data_key = Key::new_data(&key.obj, last_seq);
    let data = self
      .store
      .get(data_key, &key.table)?
      .ok_or(DbError::ChunkMissing(key.clone(), last_seq))?;
    self.cache.insert(
      key.clone(),
      CacheValue {
        cached: Table::default(),
        lazy_last: Some(LazyChunk::new(data.into_boxed_slice())),
        lazy_prev: None,
        index,
        state: Vec::new(),
        chan: None,
        last_changed: 0,
      },
    );
    Ok(true)
  }
}
