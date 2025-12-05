//! MsdDb implementation.
//!
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use crate::serde::DbBinary;
use msd_store::MsdStore;
use msd_table::{DataType, Table};
use rustc_hash::FxHasher;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::errors::DbError;
use crate::request::{Broadcast, Request, RequestKey};
use crate::worker::Worker;

const SCHEMA_TABLE_NAME: &'static str = "__SCHEMA__";
const TABLE_SCHEMA_KEY_PREFIX: &'static str = "table.";

/// MSD Database
pub struct MsdDb<S: MsdStore> {
  store: Arc<S>,
  workers: Vec<mpsc::Sender<Request>>,
}

/// ## Public methods
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
    match db.load_schema() {
      Ok(schema_map) => {
        db.request(Request::update_schema(schema_map)).await?;
      }
      Err(e) => {
        warn!(%e, "Failed to load database schema");
      }
    }

    Ok(db)
  }

  pub async fn request(&self, req: Request) -> Result<(), DbError> {
    let key = req.deref();
    match key.is_broadcast() {
      true => {
        match &req {
          Request::Broadcast(Broadcast::CreateTable(name, table)) => {
            self.create_table(name, table)?;
          }
          Request::Broadcast(Broadcast::DropTable(name)) => {
            self.drop_table(name)?;
          }
          _ => {}
        }
        for worker in &self.workers {
          match worker.send(req.clone()).await {
            Ok(_) => {}
            Err(e) => {
              warn!("Failed to send broadcast to worker: {}", e);
            }
          }
        }
      }
      false => {
        let worker = self.get_worker(&key);
        worker.send(req).await?;
      }
    }
    Ok(())
  }

  /// get the underlying store
  pub fn store(&self) -> &Arc<S> {
    &self.store
  }
}

/// ## Private methods
impl<S: MsdStore + Send + Sync + 'static> MsdDb<S> {
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

  fn create_table(&self, name: &str, table: &Table) -> Result<(), DbError> {
    if table.column_count() == 0 {
      return Err(DbError::InvalidTableSchema(
        "table must have at least one column".to_string(),
      ));
    }
    match table.column_by_index(table.pk_column()) {
      Some(pk_column) => {
        if pk_column.kind != DataType::DateTime {
          return Err(DbError::InvalidTableSchema(
            "primary key must be of type datetime".to_string(),
          ));
        }
      }
      None => {
        return Err(DbError::InvalidTableSchema(
          "table must have a primary key".to_string(),
        ));
      }
    }

    self.store.new_table(SCHEMA_TABLE_NAME)?;
    self.store.new_table(name)?;

    let key = format!("{}{}", TABLE_SCHEMA_KEY_PREFIX, name);
    let value = table.to_bytes()?;
    self
      .store
      .put(key.as_bytes(), value, SCHEMA_TABLE_NAME, None)?;
    Ok(())
  }

  fn drop_table(&self, name: &str) -> Result<(), DbError> {
    let key = format!("{}{}", TABLE_SCHEMA_KEY_PREFIX, name);
    self.store.delete(key.as_bytes(), SCHEMA_TABLE_NAME)?;
    Ok(())
  }

  /// get the appropriate worker for a given hashable object
  fn get_worker(&self, key: &RequestKey) -> &mpsc::Sender<Request> {
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let index = (hash as usize) % self.workers.len();
    &self.workers[index]
  }
}
