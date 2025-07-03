use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
  #[cfg(feature = "rocksdb")]
  #[error("rocksdb")]
  RocksDbError(rocksdb::Error),

  #[error("table {0} not found")]
  TableNotFound(String),
}

impl From<rocksdb::Error> for StoreError {
  fn from(e: rocksdb::Error) -> Self {
    StoreError::RocksDbError(e)
  }
}
