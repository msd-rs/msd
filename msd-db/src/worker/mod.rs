use std::ops::Deref;
use std::sync::Arc;

use msd_store::MsdStore;
use msd_table::Table;
use tokio::sync::mpsc;
use tracing::info;

use crate::errors::DbError;
use crate::request::{InsertRequest, QueryRequest, Request};
use crate::worker::cache::CacheMap;
mod agg_state;
mod cache;
mod init;

pub struct Worker<S: MsdStore> {
  pub id: usize,
  pub store: Arc<S>,
  pub cache: CacheMap,
}

impl<S: MsdStore> Worker<S> {
  pub fn new(id: usize, store: Arc<S>) -> Self {
    Self {
      id,
      store,
      cache: CacheMap::default(),
    }
  }

  pub async fn run(mut self, mut rx: mpsc::Receiver<Request>) {
    info!("Worker {} started", self.id);
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
        Request::Broadcast(_) => {
          info!("Worker {} received broadcast", self.id);
        }
      }
    }
    info!("Worker {} stopped", self.id);
  }

  fn handle_insert(&mut self, req: InsertRequest) -> Result<(), DbError> {
    todo!()
  }

  fn flush(&mut self, table: String, obj: String) -> Result<(), DbError> {
    Ok(())
  }

  fn handle_query(&mut self, req: QueryRequest) -> Result<Table, DbError> {
    Err(DbError::NotFound(req.deref().clone()))
  }
}
