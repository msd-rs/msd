//! SQL parser for MSD requests.
//!

use crate::{InsertRequest, QueryRequest, RequestError};

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod msd_dialect;

#[derive(Debug)]
pub enum SqlRequest {
  Insert(InsertRequest),
  Query(QueryRequest),
}

pub fn parse_sql_to_request(sql: &str) -> Result<SqlRequest, RequestError> {
  let dialect = GenericDialect {}; // or AnsiDialect

  let ast = Parser::parse_sql(&dialect, sql)?;

  Err(RequestError::UnsupportedSqlStatement)
}

#[cfg(test)]
mod tests;
