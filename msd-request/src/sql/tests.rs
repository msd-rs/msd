// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use anyhow::Result;
use msd_table::{DataType, Variant, parse_datetime};

#[test]
fn test_sql_parse_create_table() -> Result<()> {
  let sql = r#"
 CREATE TABLE kline1d (
   ts DATETIME,
   open FLOAT64 AGG_FIRST COMMENT 'open price',
   high DECIMAL64 AGG_MAX,
   low DECIMAL64 AGG_MIN,
   close DECIMAL64
 ) 
 COMMENT 'daily kline'
 WITH (
   chunkSize = 10,
   round = '1d'
 ) 
 ; 
  "#;

  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);

  match &req[0] {
    super::SqlRequest::CreateTable(table_name, table) => {
      assert_eq!(table_name, "kline1d");
      assert_eq!(table.column_count(), 5);
      assert_eq!(table.column("ts").unwrap().kind, DataType::DateTime);
      assert_eq!(table.column("open").unwrap().kind, DataType::Float64);
      assert_eq!(table.column("high").unwrap().kind, DataType::Decimal64);
      assert_eq!(table.column("low").unwrap().kind, DataType::Decimal64);
      assert_eq!(table.column("close").unwrap().kind, DataType::Decimal64);

      let chunk_size = table.get_table_meta("chunkSize").cloned();
      assert_eq!(chunk_size, Some(Variant::UInt32(10)));

      let round = table.get_table_meta("round").cloned();
      assert_eq!(round, Some(Variant::String("1d".into())));

      let agg_open = table.get_field_meta("open", "agg").cloned();
      assert_eq!(agg_open, Some(Variant::String("first".into())));

      let agg_high = table.get_field_meta("high", "agg").cloned();
      assert_eq!(agg_high, Some(Variant::String("max".into())));

      let agg_low = table.get_field_meta("low", "agg").cloned();
      assert_eq!(agg_low, Some(Variant::String("min".into())));
    }
    _ => panic!("Expected CreateTable request"),
  }

  Ok(())
}

#[test]
fn test_sql_parse_insert() -> Result<()> {
  let sql = r#"
 INSERT INTO kline1d VALUES
   ('SH600000', '2023-01-01', 100.0, 110.0, 90.0, 105.0),
   ('SH600000', '2023-01-02', 105.0, 115.0, 95.0, 110.0),
   ('SH600001', '2023-01-01', 100.0, 110.0, 90.0, 105.0),
   ('SH600001', '2023-01-02', 105.0, 115.0, 95.0, 110.0);
  "#;

  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 2);

  match &req[0] {
    super::SqlRequest::Insert(insert_req) => {
      assert_eq!(insert_req.table, "kline1d");
      assert_eq!(insert_req.obj, "SH600000");
      match &insert_req.data {
        crate::sql::InsertData::Rows(table) => {
          assert_eq!(table.row_count(), 2);
          assert_eq!(table.rows[0].len(), 5);
          assert_eq!(table.rows[1].len(), 5);
        }
        _ => panic!("Expected Rows data"),
      }
    }
    _ => panic!("Expected Insert request"),
  }
  match &req[1] {
    super::SqlRequest::Insert(insert_req) => {
      assert_eq!(insert_req.table, "kline1d");
      assert_eq!(insert_req.obj, "SH600001");
      match &insert_req.data {
        crate::sql::InsertData::Rows(table) => {
          assert_eq!(table.row_count(), 2);
          assert_eq!(table.rows[0].len(), 5);
          assert_eq!(table.rows[1].len(), 5);
        }
        _ => panic!("Expected Rows data"),
      }
    }
    _ => panic!("Expected Insert request"),
  }
  Ok(())
}

#[test]
fn test_sql_parse_copy() -> Result<()> {
  let sql = r#"
COPY kline1d FROM STDIN WITH (FORMAT CSV, HEADER TRUE);
'SH600000','2023-01-01',100.0,110.0,90.0,105.0
'SH600000','2023-01-02',105.0,115.0,95.0,110.0
'SH600001','2023-01-01',100.0,110.0,90.0,105.0
'SH600001','2023-01-02',105.0,115.0,95.0,110.0
  "#;

  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 2);

  match &req[0] {
    super::SqlRequest::Insert(insert_req) => {
      assert_eq!(insert_req.table, "kline1d");
      assert_eq!(insert_req.obj, "SH600000");
      match &insert_req.data {
        crate::sql::InsertData::Csv(rows) => {
          assert_eq!(
            rows,
            "'2023-01-01',100.0,110.0,90.0,105.0\n'2023-01-02',105.0,115.0,95.0,110.0"
          );
        }
        _ => panic!("Expected Rows data"),
      }
    }
    _ => panic!("Expected Copy request"),
  }

  match &req[1] {
    super::SqlRequest::Insert(insert_req) => {
      assert_eq!(insert_req.table, "kline1d");
      assert_eq!(insert_req.obj, "SH600001");
      match &insert_req.data {
        crate::sql::InsertData::Csv(rows) => {
          assert_eq!(
            rows,
            "'2023-01-01',100.0,110.0,90.0,105.0\n'2023-01-02',105.0,115.0,95.0,110.0"
          );
        }
        _ => panic!("Expected Rows data"),
      }
    }
    _ => panic!("Expected Copy request"),
  }

  Ok(())
}

#[test]
fn test_sql_parse_query() -> Result<()> {
  let sql = r#"
 SELECT ts, open, high, low, close
 FROM kline1d
 WHERE obj="SH6000000" AND ts >= "2023-01-01" AND ts < '2023-02-01'
 ORDER BY ts DESC
 LIMIT 10;
  "#;

  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);
  match &req[0] {
    super::SqlRequest::Query(query_req) => {
      assert_eq!(query_req.key.table, "kline1d");
      assert_eq!(query_req.key.obj, "SH6000000");
      assert_eq!(
        query_req.fields,
        Some(vec![
          "ts".to_string(),
          "open".to_string(),
          "high".to_string(),
          "low".to_string(),
          "close".to_string()
        ])
      );
      assert_eq!(query_req.ascending, Some(false));
      assert_eq!(query_req.limit, Some(10));
      assert_eq!(
        query_req.date_range.start,
        Some((parse_datetime("2023-01-01").unwrap(), true))
      );
      assert_eq!(
        query_req.date_range.end,
        Some((parse_datetime("2023-02-01").unwrap(), false))
      );
    }
    _ => panic!("Expected Query request"),
  }

  Ok(())
}

#[test]
fn test_sql_parse_query_in() -> Result<()> {
  let sql = r#"
 SELECT ts, open, high, low, close
 FROM kline1d
 WHERE obj IN ('SH600000', 'SH600001')
 ORDER BY ts DESC
 LIMIT 10;
  "#;

  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);

  match &req[0] {
    super::SqlRequest::Query(query_req) => {
      assert_eq!(query_req.key.table, "kline1d");
      assert_eq!(query_req.key.obj, "");
      assert_eq!(
        query_req.fields,
        Some(vec![
          "ts".to_string(),
          "open".to_string(),
          "high".to_string(),
          "low".to_string(),
          "close".to_string()
        ])
      );
      assert_eq!(query_req.ascending, Some(false));
      assert_eq!(query_req.limit, Some(10));
      assert_eq!(query_req.date_range.start, None);
      assert_eq!(query_req.date_range.end, None);
      assert_eq!(
        query_req.objects,
        Some(vec!["SH600000".into(), "SH600001".into()])
      );
    }
    _ => panic!("Expected Query request"),
  }

  Ok(())
}

#[test]
fn test_sql_parse_delete() -> Result<()> {
  let sql = "DELETE FROM kline1d";
  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);
  match &req[0] {
    super::SqlRequest::Delete(del) => {
      assert_eq!(del.key.table, "kline1d");
      assert_eq!(del.key.obj, "");
    }
    _ => panic!("Expected Delete request"),
  }

  let sql = "DELETE FROM kline1d WHERE obj='SH600000'";
  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);
  match &req[0] {
    super::SqlRequest::Delete(del) => {
      assert_eq!(del.key.table, "kline1d");
      assert_eq!(del.key.obj, "SH600000");
    }
    _ => panic!("Expected Delete request"),
  }

  let sql = "DELETE FROM kline1d WHERE obj='SH600000' AND ts >= '2023-01-01'";
  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);
  match &req[0] {
    super::SqlRequest::Delete(del) => {
      assert_eq!(del.key.table, "kline1d");
      assert_eq!(del.key.obj, "SH600000");
      assert_eq!(
        del.date_range.start,
        Some((parse_datetime("2023-01-01").unwrap(), true))
      );
    }
    _ => panic!("Expected Delete request"),
  }

  Ok(())
}

#[test]
fn test_sql_parse_comment() -> Result<()> {
  let sql = "COMMENT ON TABLE kline1d IS 'daily kline'";
  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);
  match &req[0] {
    super::SqlRequest::Comment(table, field, comment) => {
      assert_eq!(table, "kline1d");
      assert_eq!(field, "");
      assert_eq!(comment, "daily kline");
    }
    _ => panic!("Expected Comment request"),
  }

  let sql = "COMMENT ON COLUMN kline1d.open IS 'open price'";
  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);
  match &req[0] {
    super::SqlRequest::Comment(table, field, comment) => {
      assert_eq!(table, "kline1d");
      assert_eq!(field, "open");
      assert_eq!(comment, "open price");
    }
    _ => panic!("Expected Comment request"),
  }

  let sql = "COMMENT ON TABLE kline1d.open IS 'open price'";
  let req = super::sql_to_request(sql)?;
  assert_eq!(req.len(), 1);
  match &req[0] {
    super::SqlRequest::Comment(table, field, comment) => {
      assert_eq!(table, "kline1d");
      assert_eq!(field, "open");
      assert_eq!(comment, "open price");
    }
    _ => panic!("Expected Comment request"),
  }

  Ok(())
}
