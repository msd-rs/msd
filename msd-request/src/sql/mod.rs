//! SQL parser for MSD requests.
//!

use crate::sql::msd_dialect::MsdSqlDialect;
use crate::{InsertRequest, QueryRequest, RequestError};

use msd_table::Table;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod msd_dialect;

#[derive(Debug)]
pub enum SqlRequest {
  Query(QueryRequest),
  CreateTable(String, Table),
  Insert(InsertRequest),
}

pub fn parse_sql_to_request(sql: &str) -> Result<Vec<SqlRequest>, RequestError> {
  let dialect = MsdSqlDialect {};
  let ast = Parser::parse_sql(&dialect, sql)?;

  ast.into_iter().filter_map(parse_stmt).collect()
}

fn parse_stmt(stmt: Statement) -> Option<Result<SqlRequest, RequestError>> {
  match stmt {
    Statement::Insert { .. } => {
      //TODO: parse insert statement to InsertRequest
      todo!()
    }
    Statement::Query { .. } => {
      //TODO: parse insert statement to InsertRequest
      todo!()
    }
    Statement::CreateTable { .. } => {
      //TODO: parse create table statement to CreateTable
      todo!()
    }
    _ => Err(RequestError::UnsupportedSqlStatement).into(),
  }
}

#[cfg(test)]
mod tests;
