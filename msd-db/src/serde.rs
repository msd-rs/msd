//! Serialization and deserialization utilities for the database.
//!
//! When new types that need to be serialized/deserialized, implement the `DbBinary` trait for them.
//! When serializing/deserializing, use the methods provided by the `DbBinary` trait.
use std::io::{Read, Write};

use msd_table::Table;
use serde::{Serialize, de::DeserializeOwned};

use crate::{errors::DbError, index::IndexItem};

/// Trait for binary serialization and deserialization of database objects.
pub trait DbBinary<'a> {
  fn to_bytes(&self) -> Result<Vec<u8>, DbError>
  where
    Self: Sized + Serialize,
  {
    bincode::serde::encode_to_vec(self, bincode::config::standard())
      .map_err(DbError::BinaryEncodeError)
  }

  fn to_writer<W: Write>(&self, writer: &mut W) -> Result<usize, DbError>
  where
    Self: Sized + Serialize,
  {
    bincode::serde::encode_into_std_write(self, writer, bincode::config::standard())
      .map_err(DbError::BinaryEncodeError)
  }

  fn from_bytes(data: &'a [u8]) -> Result<Self, DbError>
  where
    Self: Sized + DeserializeOwned,
  {
    bincode::serde::decode_from_slice(data, bincode::config::standard())
      .map(|v| v.0)
      .map_err(DbError::BinaryDecodeError)
  }

  fn from_reader<R: Read>(reader: &mut R) -> Result<Self, DbError>
  where
    Self: Sized + DeserializeOwned,
  {
    bincode::serde::decode_from_std_read(reader, bincode::config::standard())
      .map_err(DbError::BinaryDecodeError)
  }
}

/// Implementations of DbBinary for specific types.
impl DbBinary<'_> for Vec<IndexItem> {}
impl DbBinary<'_> for Table {}
