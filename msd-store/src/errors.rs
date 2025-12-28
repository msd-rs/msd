// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
  #[cfg(feature = "rocksdb")]
  #[error("rocksdb")]
  RocksDbError(#[from] rocksdb::Error),

  #[error("table {0} not found")]
  TableNotFound(String),
}
