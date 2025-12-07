//! Worker module handling database requests.
//!
//! Worker is responsible for processing database requests such as insertions and queries.
//! Each worker maintains its own cache and interacts with the underlying store.

use std::collections::HashMap;
use std::sync::Arc;

use msd_store::MsdStore;
use msd_table::Table;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::errors::DbError;
use crate::index::IndexItem;
use crate::request::{Broadcast, Request, RequestKey};
use crate::serde::DbBinary;
use crate::worker::cache::CacheMap;
use msd_request::Key;
mod agg_state;
mod cache;
mod init;
mod insert;
mod query;

/// Database worker that processes requests.
pub struct Worker<S: MsdStore> {
  pub id: usize,
  pub store: Arc<S>,
  pub cache: CacheMap,
  pub schema: HashMap<String, Table>,
}

/// # management functions for Worker
impl<S: MsdStore> Worker<S> {
  pub fn new(id: usize, store: Arc<S>) -> Self {
    Self {
      id,
      store,
      cache: CacheMap::default(),
      schema: HashMap::new(),
    }
  }

  pub async fn run(mut self, mut rx: mpsc::Receiver<Request>) {
    info!(id = self.id, "worker started");
    while let Some(req) = rx.recv().await {
      match req {
        Request::Insert { req, resp_tx } => {
          let res = self.handle_insert(req);
          let _ = resp_tx.send(res);
        }
        Request::Query { req, resp_tx } => {
          let res = self.handle_query(req);
          let _ = resp_tx.send(res);
        }
        Request::Broadcast(Broadcast::Shutdown) => {
          self.handle_shutdown();
          rx.close();
          break;
        }
        Request::Broadcast(message) => {
          self.handle_broadcast(message);
        }
      }
    }
    info!(id = self.id, "Worker stopped");
  }

  fn handle_broadcast(&mut self, message: Broadcast) {
    match message {
      Broadcast::UpdateSchema(schema) => {
        self.schema = schema;
      }
      Broadcast::CreateTable(name, table) => {
        self.schema.insert(name, table);
      }
      Broadcast::DropTable(table) => {
        self.schema.remove(&table);
      }
      _ => { /* ignore other broadcast messages */ }
    }
  }

  fn handle_shutdown(&self) {
    info!(id = self.id, "worker stopping");
    for (key, cache_item) in &self.cache {
      debug!(?key, "Flushing cache before shutdown");
      if let Err(err) = self.flush_index(key, &cache_item.index) {
        warn!(%key, %err, "Failed to flush index for key during shutdown");
      }
      if let Err(err) = self.flush_chunk(key, &cache_item.cached, cache_item.index.len() as u32 - 1)
      {
        warn!(%key, %err, "Failed to flush chunk for key during shutdown");
      }
    }
    info!(
      id = self.id,
      flushed = self.cache.len(),
      "worker cache flushed"
    );
  }
}

/// # helper functions for Worker
impl<S: MsdStore> Worker<S> {
  /// Flush the index for a given key to the store.
  pub(crate) fn flush_index(
    &self,
    key: &RequestKey,
    index: &Vec<IndexItem>,
  ) -> Result<(), DbError> {
    let index_key = Key::new_index(&key.obj);
    let index_val = DbBinary::to_bytes(index).map_err(|e| DbError::from(e))?;
    self
      .store
      .put(&index_key, index_val, &key.table, None)
      .map_err(|e| DbError::from(e))
  }

  /// Flush the chunk data for a given key to the store.
  pub(crate) fn flush_chunk(
    &self,
    key: &RequestKey,
    data: &Table,
    seq: u32,
  ) -> Result<(), DbError> {
    let data_key = Key::new_data(&key.obj, seq);
    let data_val = DbBinary::to_bytes(data).map_err(|e| DbError::from(e))?;
    self
      .store
      .put(&data_key, data_val, &key.table, None)
      .map_err(|e| DbError::from(e))
  }
}
