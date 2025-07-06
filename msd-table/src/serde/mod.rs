use crate::{Table, TableError};

impl Table {
  pub fn to_bytes(&self) -> Result<Vec<u8>, TableError> {
    bincode::serde::encode_to_vec(self, bincode::config::standard())
      .map_err(TableError::BinaryEncodeError)
  }

  pub fn from_bytes(data: &[u8]) -> Result<Self, TableError> {
    bincode::serde::decode_from_slice(data, bincode::config::standard())
      .map(|(table, _)| table)
      .map_err(TableError::BinaryDecodeError)
  }
}
