// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! Key definition to store/retrieve data from MsdStore.
//!
//! Each object's data is stored in chunks, with each chunk identified by a unique key.
//! There are two types of keys:
//! - Data Key: Used to store actual data ([msd_table::Table]) chunks for an object
//! - Index Key: Used to store metadata about the object's data chunks, Vec<[super::index::IndexItem]>.
//!
//! Key is ordered lexicographically to ensure that data chunks are sorted in descending order based on their sequence numbers.
//! For each object, The first is the Index Key, followed by Data Keys in descending order of their sequence numbers.
//! This ordering facilitates efficient retrieval of the latest data chunks for an object.
//!
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

use crate::errors::RequestError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Key(Vec<u8>);

const KEY_SEPARATOR: u8 = b'.';
const INDEX_PREFIX: &[u8] = b"\x00\x00\x00\x00";

impl Key {
  pub fn new_data(obj: &str, seq: u32) -> Self {
    let mut key = Vec::with_capacity(obj.len() + 1 + 4);
    key.extend_from_slice(obj.as_bytes());
    key.push(KEY_SEPARATOR); // separator
    let seq = (-(seq as i64) - 1) as i32;
    key.extend_from_slice(seq.to_be_bytes().as_ref());
    Key(key)
  }

  pub fn new_index(obj: &str) -> Self {
    let mut key = Vec::with_capacity(INDEX_PREFIX.len() + 1 + obj.len());
    key.extend_from_slice(INDEX_PREFIX);
    key.push(KEY_SEPARATOR); // separator
    key.extend_from_slice(obj.as_bytes());
    Key(key)
  }

  pub fn is_index(&self) -> bool {
    self.0.starts_with(INDEX_PREFIX)
  }

  pub fn index_prefix() -> &'static [u8] {
    INDEX_PREFIX
  }

  pub fn get_obj(&self) -> &str {
    let len = self.0.len();
    // We assume the key is always valid as it is constructed via new_data/new_index
    // The last 5 bytes are separator (1 byte) + seq/suffix (4 bytes)
    if len < 5 {
      return "";
    }
    std::str::from_utf8(&self.0[..len - 5]).unwrap_or("")
  }

  pub fn get_seq(&self) -> u32 {
    let len = self.0.len();
    if len < 9 {
      return 0;
    }
    let bytes = &self.0[len - 4..];
    let seq = u32::from_be_bytes(bytes.try_into().unwrap_or([0; 4])) as i64;
    -(seq + 1) as u32
  }

  pub fn into_bytes(self) -> Vec<u8> {
    self.0
  }

  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }
}

impl Ord for Key {
  fn cmp(&self, other: &Self) -> Ordering {
    self.0.cmp(&other.0)
  }
}

impl PartialOrd for Key {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Hash for Key {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.0.hash(state);
  }
}

impl AsRef<[u8]> for Key {
  fn as_ref(&self) -> &[u8] {
    &self.0
  }
}

impl TryFrom<&[u8]> for Key {
  type Error = RequestError;

  fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
    let is_index_key = value.starts_with(INDEX_PREFIX);
    let is_data_key = value.len() >= 5 && value[value.len() - 5] == KEY_SEPARATOR;
    if !is_index_key && !is_data_key {
      return Err(RequestError::InvalidKeyFormat(value.to_vec()));
    }
    Ok(Key(value.to_vec()))
  }
}

impl Display for Key {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !self.is_index() {
      write!(f, "{}.{:08}", self.get_obj(), self.get_seq())
    } else {
      write!(
        f,
        "{}",
        std::str::from_utf8(&self.0[INDEX_PREFIX.len() + 1..]).unwrap_or("InvalidKey")
      )
    }
  }
}

#[cfg(test)]
mod tests {

  use super::Key;

  #[test]
  fn test_key() {
    let key0 = Key::new_data("obj1", 0);
    let key1 = Key::new_data("obj1", 1);
    let key2 = Key::new_data("obj1", 2);
    let key_index = Key::new_index("obj1");

    assert!(key2 < key1);
    assert!(key1 < key0);

    assert!(key_index < key0);
    assert!(key_index < key1);
    assert!(key_index < key2);

    assert!(
      key1.get_obj() == key2.get_obj(),
      "{} != {}",
      key1.get_obj(),
      key2.get_obj()
    );
    assert!(key1.get_obj() == "obj1", "{} != obj1", key1.get_obj());
    assert!(key1.get_seq() == 1, "{} != 1", key1.get_seq());
    assert!(key2.get_seq() == 2, "{} != 2", key2.get_seq());
  }
}
