//! Error types for MsdDb.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RequestError {
  #[error("Invalid key format: {0:?}")]
  InvalidKeyFormat(Vec<u8>),
  #[error("Table Error")]
  TableError(#[from] msd_table::TableError),
  #[error("SQL Parse Error")]
  SqlParseError(#[from] sqlparser::parser::ParserError),
  #[error("Unsupported SQL statement")]
  UnsupportedSqlStatement,
  #[error("Invalid request")]
  InvalidRequest(String),
}
