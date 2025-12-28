// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

pub mod errors;

#[cfg(feature = "rocksdb")]
pub mod store_rocksdb;

pub use errors::StoreError;

/// Trait for the low-level storage engine of `msd`.
pub trait MsdStore {
  /// get a value named `key` in `table`
  fn get<K: AsRef<[u8]>>(&self, key: K, table: &str) -> Result<Option<Vec<u8>>, StoreError>;
  /// get the next key-value pair after `key` in `table` in lexicographical order
  ///
  /// An optional buffer(key, value) can be provided to avoid allocations.
  ///
  /// None is returned if there is no next key.
  fn get_next<K: AsRef<[u8]>>(
    &self,
    key: K,
    table: &str,
    buf: Option<(Vec<u8>, Vec<u8>)>,
  ) -> Result<Option<(Vec<u8>, Vec<u8>)>, StoreError>;
  /// put a value named `key` in table `table` with optional ttl
  fn put<K: AsRef<[u8]>, V: Into<Vec<u8>>>(
    &self,
    key: K,
    value: V,
    table: &str,
    ttl: Option<u64>,
  ) -> Result<(), StoreError>;
  /// delete a value named `key` in `table`
  fn delete<K: AsRef<[u8]>>(&self, key: K, table: &str) -> Result<(), StoreError>;
  /// seek to the first key in `table`, then iterate over all keys with the same prefix
  /// and call `f` for each key-value pair. If `f` returns false, stop iterating.
  ///
  /// # Arguments
  /// - start_from: the first key to seek to
  /// - prefix: extract the first `prefix` bytes from `start_from` and use it as a prefix, None means the whole key
  /// - table: the table to iterate over
  fn prefix_with<K: AsRef<[u8]>, F: FnMut(&[u8], &[u8]) -> bool>(
    &self,
    start_from: K,
    prefix: Option<usize>,
    table: &str,
    rev: bool,
    f: F,
  ) -> Result<(), StoreError>;

  /// create a new table, return false if the table already exists
  fn new_table(&self, name: &str) -> Result<bool, StoreError>;
  /// drop a table
  fn drop_table(&self, name: &str) -> Result<(), StoreError>;
  /// list tables
  fn list_tables(&self) -> Result<Vec<String>, StoreError>;
  /// remove expired keys(out of ttl)
  fn remove_expired(&self) -> Result<(), StoreError>;
}

#[cfg(feature = "rocksdb")]
pub use store_rocksdb::*;
