//! Serialization and deserialization utilities for the database.
//!
//! When new types that need to be serialized/deserialized, implement the `DbBinary` trait for them.
//! When serializing/deserializing, use the methods provided by the `DbBinary` trait.
use std::io::{Read, Write};

use msd_table::Table;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{errors::DbError, index::IndexItem};

/// Trait for binary serialization and deserialization of database objects.
pub trait DbBinary<'a> {
  fn to_bytes(&self) -> Result<Vec<u8>, DbError>
  where
    Self: Sized + Serialize,
  {
    bincode::serialize(self).map_err(DbError::SerializationError)
  }

  fn to_writer<W: Write>(&self, writer: W) -> Result<(), DbError>
  where
    Self: Sized + Serialize,
  {
    bincode::serialize_into(writer, self).map_err(DbError::SerializationError)
  }

  fn from_bytes(data: &'a [u8]) -> Result<Self, DbError>
  where
    Self: Sized + Serialize + Deserialize<'a>,
  {
    bincode::deserialize(data).map_err(DbError::SerializationError)
  }

  fn from_reader<R: Read>(reader: R) -> Result<Self, DbError>
  where
    Self: Sized + Serialize + DeserializeOwned,
  {
    bincode::deserialize_from(reader).map_err(DbError::SerializationError)
  }
}

/// Implementations of DbBinary for specific types.
impl DbBinary<'_> for Vec<IndexItem> {}
impl DbBinary<'_> for Table {}
