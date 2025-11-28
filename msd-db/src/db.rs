use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use msd_store::MsdStore;
use msd_table::Table;
use tokio::sync::mpsc;

use crate::errors::DBError;
use crate::request::Request;
use crate::worker::Worker;

pub struct MsdDb<S: MsdStore> {
  store: Arc<S>,
  workers: Vec<mpsc::Sender<Request>>,
}

impl<S: MsdStore + Send + Sync + 'static> MsdDb<S> {
  pub fn new(store: S, worker_count: usize) -> Self {
    let store = Arc::new(store);
    let mut workers = Vec::with_capacity(worker_count);

    for i in 0..worker_count {
      let (tx, rx) = mpsc::channel(100);
      workers.push(tx);
      let worker = Worker::new(i, store.clone());
      tokio::spawn(worker.run(rx));
    }

    Self { store, workers }
  }

  pub fn store(&self) -> &Arc<S> {
    &self.store
  }

  fn get_worker(&self, table: &str, obj: &str) -> &mpsc::Sender<Request> {
    let mut hasher = DefaultHasher::new();
    table.hash(&mut hasher);
    obj.hash(&mut hasher);
    let hash = hasher.finish();
    let index = (hash as usize) % self.workers.len();
    &self.workers[index]
  }

  pub async fn insert(&self, table: &str, obj: &str, data: Table) -> Result<(), DBError> {
    let worker = self.get_worker(table, obj);
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    let req = Request::Insert {
      table: table.to_string(),
      obj: obj.to_string(),
      data,
      resp: resp_tx,
    };

    worker.send(req).await?;

    resp_rx.await?
  }

  pub async fn query(&self, table: &str, obj: &str) -> Result<Table, DBError> {
    let worker = self.get_worker(table, obj);
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    let req = Request::Query {
      table: table.to_string(),
      obj: obj.to_string(),
      resp: resp_tx,
    };

    worker.send(req).await?;
    resp_rx.await?
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use msd_store::StoreError;
  use msd_table::{DataType, Field};
  use std::sync::Mutex;

  struct MockStore {
    data: Mutex<HashMap<String, Vec<u8>>>,
  }

  impl MockStore {
    fn new() -> Self {
      Self {
        data: Mutex::new(HashMap::new()),
      }
    }
  }

  impl MsdStore for MockStore {
    fn get<K: AsRef<[u8]>>(&self, key: K, _table: &str) -> Result<Option<Vec<u8>>, StoreError> {
      let key = String::from_utf8_lossy(key.as_ref()).to_string();
      let data = self.data.lock().unwrap();
      Ok(data.get(&key).cloned())
    }

    fn put<K: AsRef<[u8]>, V: Into<Vec<u8>>>(
      &self,
      key: K,
      value: V,
      _table: &str,
      _ttl: Option<u64>,
    ) -> Result<(), StoreError> {
      let key = String::from_utf8_lossy(key.as_ref()).to_string();
      let mut data = self.data.lock().unwrap();
      data.insert(key, value.into());
      Ok(())
    }

    fn delete<K: AsRef<[u8]>>(&self, key: K, _table: &str) -> Result<(), StoreError> {
      let key = String::from_utf8_lossy(key.as_ref()).to_string();
      let mut data = self.data.lock().unwrap();
      data.remove(&key);
      Ok(())
    }

    fn prefix_with<K: AsRef<[u8]>, F: FnMut(Vec<u8>, Vec<u8>) -> bool>(
      &self,
      _start_from: K,
      _prefix: Option<usize>,
      _table: &str,
      _f: F,
    ) -> Result<(), StoreError> {
      Ok(())
    }

    fn new_table(&self, _name: &str) -> Result<(), StoreError> {
      Ok(())
    }

    fn drop_table(&self, _name: &str) -> Result<(), StoreError> {
      Ok(())
    }

    fn list_tables(&self) -> Result<Vec<String>, StoreError> {
      Ok(vec![])
    }

    fn remove_expired(&self) -> Result<(), StoreError> {
      Ok(())
    }
  }

  use std::collections::HashMap;

  #[tokio::test]
  async fn test_insert_and_query() {
    let store = MockStore::new();
    let db = MsdDb::new(store, 2);

    let schema = vec![
      Field::new("ts", DataType::Int64),
      Field::new("val", DataType::Float64),
    ];
    let mut table = Table::new(schema, 0);
    // Add a row
    // Table API is a bit verbose to construct rows manually in test, let's try
    // We need to use Variant
    use msd_table::Variant;
    let row = vec![Variant::Int64(100), Variant::Float64(1.23)];
    table.push_row(row).unwrap();

    db.insert("test_table", "obj1", table).await.unwrap();

    // Query
    let result = db.query("test_table", "obj1").await.unwrap();
    assert_eq!(result.row_count(), 1);

    let row = result.get_row(0).unwrap();
    // row is Vec<VariantRef>
    // We can check values
    // VariantRef doesn't implement PartialEq with Variant directly easily?
    // Let's check string representation or cast
    // Actually VariantRef implements Display
    assert_eq!(format!("{}", row[0]), "100");
    assert_eq!(format!("{}", row[1]), "1.23");
  }
}
