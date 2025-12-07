use anyhow::Result;
use sqlparser::parser::Parser;

use crate::sql::msd_dialect::MsdSqlDialect;
#[test]
fn test_sql_parse_create_table() -> Result<()> {
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

#[test]
fn test_sql_parse_insert() -> Result<()> {
  let sql = r#"
 INSERT INTO kline1d VALUES
   ('2023-01-01', 100.0, 110.0, 90.0, 105.0),
   ('2023-01-02', 105.0, 115.0, 95.0, 110.0);
  "#;

  let dialect = MsdSqlDialect {}; // or AnsiDialect
  let ast = Parser::parse_sql(&dialect, sql)?;

  let sql = r#"
  COPY kline1d FROM STDIN WITH (FORMAT 'csv');
  2023-01-03,110.0,120.0,100.0,115.0
  2023-01-04,115.0,125.0,105.0,120.0
  "#;

  let ast = Parser::parse_sql(&dialect, sql)?;
  println!("AST: {:?}", ast);

  Ok(())
}

#[test]
fn test_sql_parse_query() -> Result<()> {
  let sql = r#"
 SELECT ts, open, high, low, close
 FROM kline1d
 WHERE obj='SH6000000' AND ts >= '2023-01-01' AND ts < '2023-02-01'
 ORDER BY ts DESC
 LIMIT 10 OFFSET 5;
  "#;

  let dialect = MsdSqlDialect {}; // or AnsiDialect
  let ast = Parser::parse_sql(&dialect, sql)?;
  println!("AST: {:?}", ast);

  Ok(())
}
