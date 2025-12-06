use anyhow::Result;
use sqlparser::{
  dialect::{GenericDialect, HiveDialect, SQLiteDialect},
  parser::Parser,
};

use crate::sql::msd_dialect::MsdSqlDialect;
#[test]
fn test_sql_parse_ast() -> Result<()> {
  let sql = r#"
 CREATE TABLE kline1d (
   ts DATETIME,
   open FLOAT64 AGG_FIRST,
   high DECIMAL64 AGG_MAX,
   low DECIMAL64 AGG_MIN,
   close DECIMAL64

 ) WITH (
   chunkSize = 10,
   round = '1d'
 ); 
  "#;

  let dialect = MsdSqlDialect {}; // or AnsiDialect
  let ast = Parser::parse_sql(&dialect, sql)?;

  println!("AST: {:?}", ast);

  Ok(())
}
