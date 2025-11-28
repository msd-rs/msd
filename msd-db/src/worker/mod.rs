use std::collections::HashMap;
use std::sync::Arc;

use msd_store::MsdStore;
use msd_table::{Table, TableError};
use tokio::sync::mpsc;
use tracing::info;

use crate::errors::DBError;
use crate::request::Request;
use crate::worker::cache::CacheMap;
mod agg_state;
mod cache;

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
        Request::Insert {
          table,
          obj,
          data,
          resp,
        } => {
          let res = self.handle_insert(table, obj, data);
          let _ = resp.send(res);
        }
        Request::Query { table, obj, resp } => {
          let res = self.handle_query(table, obj);
          let _ = resp.send(res);
        }
      }
    }
    info!("Worker {} stopped", self.id);
  }

  fn handle_insert(&mut self, table: String, obj: String, data: Table) -> Result<(), DBError> {
    let key = (table.clone(), obj.clone());
    if let Some(buffer) = self.cache.get_mut(&key) {
      // Append data to buffer
      // TODO: Optimize this by implementing append in Table
      for row in data.rows() {
        // Convert VariantRef to Variant
        let row_owned: Vec<_> = row.into_iter().map(|v| v.to_variant()).collect();
        buffer.push_row(row_owned)?;
      }
    } else {
      self.cache.insert(key.clone(), data);
    }

    // Check buffer size and flush if needed
    // For now, let's just flush every time for simplicity or maybe a simple count check
    // In real implementation, we should check size or time
    if let Some(buffer) = self.cache.get(&key) {
      if buffer.row_count() >= 100 {
        self.flush(table, obj)?;
      }
    }

    Ok(())
  }

  fn flush(&mut self, table: String, obj: String) -> Result<(), DBError> {
    // if let Some(buffer) = self.buffers.remove(&(table.clone(), obj.clone())) {
    //   // Generate DataKey
    //   // For now, we use a simple counter or timestamp.
    //   // In real implementation, we need to manage sequence numbers.
    //   // Let's use system time for now as a simple sequence.
    //   let seq = std::time::SystemTime::now()
    //     .duration_since(std::time::UNIX_EPOCH)?
    //     .as_nanos() as u32;
    //   // Invert seq for descending order if needed, but design says:
    //   // SequenceNumber = Hex(-COUNT_OF_CHUNKS_BEFORE - 1)
    //   // Let's just use the seq for now.
    //   let key = keys::Key::new_data(&obj, seq);

    //   let value = buffer.to_bytes()?;
    //   self.store.put(key, value, &table, None)?;
    // }
    Ok(())
  }

  fn handle_query(&mut self, table: String, obj: String) -> Result<Table, DBError> {
    // 1. Read from store
    // 2. Read from buffer
    // 3. Merge

    // For now, just return what's in buffer or empty
    // TODO: Implement full query logic reading from store
    if let Some(buffer) = self.cache.get(&(table.clone(), obj.clone())) {
      Ok(buffer.clone())
    } else {
      // Return empty table? We don't know the schema here.
      // Maybe we should fetch schema from store or metadata.
      Err(DBError::TableNotFound(table, obj))
    }
  }
}
