use thiserror::Error;

use crate::DataType;

#[derive(Debug, Error)]
pub enum TableError {
  #[error("binary encode error: {0}")]
  BinaryEncodeError(bincode::error::EncodeError),
  #[error("binary decode error: {0}")]
  BinaryDecodeError(bincode::error::DecodeError),

  #[error("unknown data type: {0}")]
  UnknownDataType(String),

  #[error("type {0} is mismatched with {1}")]
  TypeMismatch(DataType, DataType),
}
