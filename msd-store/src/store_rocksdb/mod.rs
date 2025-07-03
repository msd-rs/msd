use std::sync::Arc;

use crate::MsdStore;
pub use crate::StoreError;
use rocksdb::{
  BoundColumnFamily, Cache, ColumnFamilyDescriptor, DBCompressionType, Direction, Env,
  IteratorMode, Options, WriteBatch, DB,
};
use time::OffsetDateTime;
use tracing::warn;

const TTL_CF: &'static str = "__TTL__";

#[derive(Debug)]
pub struct RocksDbStore {
  db: rocksdb::DB,
}

fn safe_utf8(s: &[u8]) -> &str {
  unsafe { std::str::from_utf8_unchecked(s) }
}

impl RocksDbStore {
  pub fn new(path: &str) -> Result<Self, StoreError> {
    let (opts, cfs) =
      rocksdb::Options::load_latest(path, Env::new()?, true, Cache::new_lru_cache(1024 * 8))
        .unwrap_or_else(|_e| {
          // create a new options and column families if the database does not exist
          let mut opts = Options::default();
          opts.create_if_missing(true);
          opts.create_missing_column_families(true);
          opts.set_enable_blob_files(true);
          opts.set_compression_type(DBCompressionType::Zstd);

          (
            opts,
            vec![TTL_CF]
              .iter()
              .map(|&cf| ColumnFamilyDescriptor::new(cf, Options::default()))
              .collect(),
          )
        });

    let db = DB::open_cf_descriptors(&opts, path, cfs).map_err(|e| StoreError::from(e))?;

    Ok(Self { db })
  }

  fn cf_handle(&self, name: &str) -> Result<Arc<BoundColumnFamily>, StoreError> {
    self
      .db
      .cf_handle(name)
      .ok_or(StoreError::TableNotFound(name.into()))
  }
}

fn make_ttl_key(ttl: u64) -> [u8; 8] {
  let now = OffsetDateTime::now_utc();
  let now = now.unix_timestamp() as u64;
  let ttl = u64::MAX - (now + ttl);
  ttl.to_be_bytes()
}

impl MsdStore for RocksDbStore {
  fn get<K: AsRef<[u8]>>(&self, key: K, table: &str) -> Result<Option<Vec<u8>>, StoreError> {
    let cf = self.cf_handle(table)?;
    self.db.get_cf(&cf, key.as_ref()).map_err(StoreError::from)
  }

  fn put<K: AsRef<[u8]>, V: Into<Vec<u8>>>(
    &self,
    key: K,
    value: V,
    table: &str,
    ttl: Option<u64>,
  ) -> Result<(), StoreError> {
    let cf = self.cf_handle(table)?;
    let key = key.as_ref();
    let value = value.into();
    self.db.put_cf(&cf, key, &value).map_err(StoreError::from)?;
    if let Some(ttl) = ttl {
      let cf = self.cf_handle(TTL_CF)?;
      let ttl_key = make_ttl_key(ttl);
      let ttl_value = [table.as_bytes(), b":", key].concat();
      self
        .db
        .put_cf(&cf, &ttl_key, &ttl_value)
        .map_err(StoreError::from)?;
    }
    Ok(())
  }

  fn delete<K: AsRef<[u8]>>(&self, key: K, table: &str) -> Result<(), StoreError> {
    let cf = self.cf_handle(table)?;
    let key = key.as_ref();
    self.db.delete_cf(&cf, key).map_err(StoreError::from)
  }

  fn prefix_with<K: AsRef<[u8]>, F: FnMut(Vec<u8>, Vec<u8>) -> bool>(
    &self,
    start_from: K,
    prefix: Option<usize>,
    table: &str,
    mut f: F,
  ) -> Result<(), StoreError> {
    let cf = self.cf_handle(table)?;
    let iter = self.db.iterator_cf(
      &cf,
      IteratorMode::From(start_from.as_ref(), Direction::Forward),
    );

    let prefix = prefix
      .map(|p| &start_from.as_ref()[..p])
      .unwrap_or(start_from.as_ref());

    for i in iter {
      match i {
        Ok((k, v)) => {
          if !k.starts_with(prefix) {
            break;
          }
          if !f(k.into_vec(), v.into_vec()) {
            break;
          }
        }
        Err(e) => return Err(StoreError::from(e)),
      }
    }
    Ok(())
  }

  fn new_table(&self, name: &str) -> Result<(), StoreError> {
    self
      .db
      .create_cf(name, &Options::default())
      .map_err(StoreError::from)
  }

  fn drop_table(&self, name: &str) -> Result<(), StoreError> {
    self.db.drop_cf(name).map_err(StoreError::from)
  }

  fn list_tables(&self) -> Result<Vec<String>, StoreError> {
    DB::list_cf(&Options::default(), self.db.path())
      .map_err(StoreError::from)
      .map(|cfs| cfs.into_iter().filter(|cf| !cf.starts_with("__")).collect())
  }

  fn remove_expired(&self) -> Result<(), StoreError> {
    let now = make_ttl_key(0);
    let cf = self.cf_handle(TTL_CF)?;
    let iter = self
      .db
      .iterator_cf(&cf, IteratorMode::From(&now, Direction::Forward));

    let mut batch = WriteBatch::new();
    for entry in iter {
      match entry {
        Ok((ttl_key, ttl_value)) => {
          if let Some(mid) = ttl_value.iter().position(|&b| b == b':') {
            let (table, key) = ttl_value.split_at(mid);
            match self.cf_handle(safe_utf8(table)) {
              Ok(table_cf) => {
                batch.delete_cf(&table_cf, &key[1..]);
              }
              Err(err) => {
                warn!(%err, ?table, "failed to get table");
              }
            }
            batch.delete_cf(&cf, ttl_key);
          } else {
            warn!(entry = ?ttl_value, "invalid ttl entry");
          }
        }
        Err(e) => return Err(StoreError::from(e)),
      }
    }
    self.db.write(batch).map_err(StoreError::from)
  }
}
