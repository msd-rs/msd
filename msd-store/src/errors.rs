use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
  #[cfg(feature = "rocksdb")]
  #[error("rocksdb")]
  RocksDbError(#[from] rocksdb::Error),

  #[error("table {0} not found")]
  TableNotFound(String),
}
