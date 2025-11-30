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

  #[error("index {0} is out of bounds for length {1}")]
  IndexOutOfBounds(usize, usize),

  #[error("column count mismatch: expected {0}, found {1}")]
  ColumnCountMismatch(usize, usize),

  #[error("column schema mismatch for column: {0}")]
  ColumnSchemaMismatch(String),

  #[error("original index {0} is greater than new index {1}")]
  InvalidOrder(String, String),
}
