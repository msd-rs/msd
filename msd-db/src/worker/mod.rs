//! Worker module handling database requests.
//!
//! Worker is responsible for processing database requests such as insertions and queries.
//! Each worker maintains its own cache and interacts with the underlying store.

use std::collections::HashMap;
use std::sync::Arc;

use msd_store::MsdStore;
use msd_table::Table;
use tokio::sync::mpsc;
use tracing::info;

use crate::errors::DbError;
use crate::request::{Broadcast, InsertRequest, Request};
use crate::worker::cache::CacheMap;
mod agg_state;
mod cache;
mod init;
mod query;

/// Database worker that processes requests.
pub struct Worker<S: MsdStore> {
  pub id: usize,
  pub store: Arc<S>,
  pub cache: CacheMap,
  pub schema: HashMap<String, Table>,
}

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
    info!(id = self.id, "Worker started");
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
        Request::Broadcast(message) => {
          self.handle_broadcast(message);
        }
      }
    }
    info!(id = self.id, "Worker stopped");
  }

  fn handle_insert(&mut self, req: InsertRequest) -> Result<(), DbError> {
    todo!()
  }

  fn flush(&mut self, table: String, obj: String) -> Result<(), DbError> {
    Ok(())
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
    }
  }
}
