//! MsdDb implementation.
//!
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::serde::DbBinary;
use msd_store::MsdStore;
use msd_table::Table;
use rustc_hash::FxHasher;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::errors::DbError;
use crate::request::{Broadcast, InsertRequest, QueryRequest, Request, RequestKey};
use crate::worker::Worker;

const SCHEMA_TABLE_NAME: &'static str = "__SCHEMA__";
const TABLE_SCHEMA_KEY_PREFIX: &'static str = "table.";

/// MSD Database
pub struct MsdDb<S: MsdStore> {
  store: Arc<S>,
  workers: Vec<mpsc::Sender<Request>>,
}

impl<S: MsdStore + Send + Sync + 'static> MsdDb<S> {
  /// Create a new MsdDb instance with the given store and number of workers
  pub async fn new(store: S, worker_count: usize) -> Result<Self, DbError> {
    let store = Arc::new(store);
    let mut workers = Vec::with_capacity(worker_count);

    info!(workers = worker_count, "Starting database workers");
    for i in 0..worker_count {
      let (tx, rx) = mpsc::channel(100);
      workers.push(tx);
      let worker = Worker::new(i, store.clone());
      tokio::spawn(worker.run(rx));
    }

    let db = Self {
      store: store.clone(),
      workers,
    };

    info!("Loading database schema");
    let schema_map = db.load_schema()?;
    db.broadcast(Broadcast::UpdateSchema(schema_map)).await?;

    Ok(db)
  }

  fn load_schema(&self) -> Result<HashMap<String, Table>, DbError> {
    let mut schema_map: HashMap<String, Table> = HashMap::new();
    self.store.prefix_with(
      TABLE_SCHEMA_KEY_PREFIX.as_bytes(),
      None,
      SCHEMA_TABLE_NAME,
      false,
      |k, v| {
        let key = String::from_utf8_lossy(&k).to_string();
        match DbBinary::from_bytes(&v) {
          Ok(table) => {
            schema_map.insert(key, table);
          }
          Err(e) => {
            warn!(%e, "failed to deserialize table for schema entry");
          }
        }
        true
      },
    )?;
    Ok(schema_map)
  }

  /// get the underlying store
  pub fn store(&self) -> &Arc<S> {
    &self.store
  }

  /// get the appropriate worker for a given hashable object
  fn get_worker(&self, key: &RequestKey) -> &mpsc::Sender<Request> {
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let index = (hash as usize) % self.workers.len();
    &self.workers[index]
  }

  /// broadcast a message to all workers
  async fn broadcast(&self, message: Broadcast) -> Result<(), DbError> {
    for worker in &self.workers {
      match worker.send(Request::build_broadcast(&message)).await {
        Ok(_) => {}
        Err(e) => {
          warn!("Failed to send broadcast to worker: {}", e);
        }
      }
    }
    Ok(())
  }

  /// dispatch insert request to the appropriate worker
  pub async fn insert(&self, req: InsertRequest) -> Result<(), DbError> {
    let worker = self.get_worker(&req);
    let (req, resp_rx) = Request::build_insert(req);
    worker.send(req).await?;
    resp_rx.await?
  }

  /// dispatch query request to the appropriate worker
  pub async fn query(&self, req: QueryRequest) -> Result<Table, DbError> {
    let worker = self.get_worker(&req);
    let (req, resp_rx) = Request::build_query(req);
    worker.send(req).await?;
    resp_rx.await?
  }
}
