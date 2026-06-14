// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use super::Worker;
use crate::{errors::DbError, request::DeleteResponse, worker::cache::CacheValue};
use msd_request::{DeleteRequest, Key, RequestKey};
use msd_store::MsdStore;

impl<S: MsdStore> Worker<S> {
  pub(crate) fn handle_delete(&mut self, req: DeleteRequest) -> Result<DeleteResponse, DbError> {
    if req.table.is_empty() {
      return Err(DbError::RequestError(
        msd_request::RequestError::InvalidRequest("table is empty".into()),
      ));
    }
    if req.obj.contains(|c| c == '*' || c == '?') {
      return Err(DbError::RequestError(
        msd_request::RequestError::InvalidRequest("obj contains * or ?".into()),
      ));
    }

    //TODO: remove only the specified date range, but now remove the whole object

    if req.obj.is_empty() {
      // remove the all objects in the table, also remove the store entries when the store table exists
      self.cache.retain(|k, _| k.table != req.table);
      self.store.drop_table(&req.table).ok();
      self.store.new_table(&req.table).ok();
    } else {
      match self.cache.remove(&req.key) {
        Some(item) => {
          Self::delete_cache_item(&self.store, &req.key, &item)?;
        }
        None => {}
      }
    }

    Ok(DeleteResponse::default())
  }

  pub(crate) fn delete_cache_item(
    store: &S,
    key: &RequestKey,
    item: &CacheValue,
  ) -> Result<(), DbError> {
    let data_keys = (0..item.index.len()).map(|i| Key::new_data(&key.obj, i as u32));
    let index_key = Key::new_index(&key.obj);

    let _ = store.delete(&index_key, &key.table);
    for data_key in data_keys {
      let _ = store.delete(&data_key, &key.table);
    }

    Ok(())
  }
}
