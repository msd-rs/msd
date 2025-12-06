use std::{collections::HashMap, vec};

use anyhow::Result;
use msd_db::{
  MsdDb,
  request::{InsertData, InsertRequest, QueryRequest, Request, RequestKey},
};
use msd_store::RocksDbStore;
use msd_table::{Series, Table, Variant, parse_datetime, table};

const DATA_DIR: &str = "/tmp/msd_store_test_db";

type Db = MsdDb<RocksDbStore>;

fn setup() {
  tracing_subscriber::fmt()
    .with_env_filter("msd_db=debug")
    .init();
}

async fn create_db() -> Result<Db> {
  let s = RocksDbStore::new(DATA_DIR)?;
  let db = MsdDb::new(s, 1).await?;
  Ok(db)
}

fn remove_db() -> Result<()> {
  let _ = std::fs::remove_dir_all(DATA_DIR);
  Ok(())
}

fn create_table() -> Table {
  let table = table!(
    {name: "ts", kind: datetime},
    {name: "open", kind: f64},
  );

  let metadata: HashMap<String, Variant> = HashMap::from([
    ("chunkSize".into(), 10u32.into()),
    ("round".into(), "1d".into()),
  ]);

  table.with_metadata(metadata)
}

async fn init_db(clear: bool) -> Result<Db> {
  if clear {
    remove_db()?;
  }
  let db = create_db().await?;
  let table = create_table();
  let req = Request::create_table("kline1d", table);
  db.request(req).await?;
  Ok(db)
}

fn sample_data(n: usize, start_date: &str) -> Vec<Series> {
  let ts = build_datetime_series(start_date, n, 86400).unwrap();
  let open = build_f64_series(10.0, n, 1.0);
  vec![ts, open]
}

#[tokio::test]
async fn test_create_db() -> Result<()> {
  let db = init_db(true).await?;

  let invalid_table = table!(
    {name: "ts", kind: u64}, // invalid primary key
    {name: "open", kind: f64},
  );
  let req = Request::create_table("invalid_t1", invalid_table);
  let res = db.request(req).await;
  assert!(res.is_err());

  Ok(())
}

fn build_datetime_series(start: &str, count: usize, step_secs: i64) -> Result<Series> {
  let ts = parse_datetime(start)?;
  let step = step_secs * 1_000_000; // microseconds
  let v = (0..count).map(|i| ts + i as i64 * step).collect();
  Ok(Series::DateTime(v))
}

fn build_f64_series(start: f64, count: usize, step: f64) -> Series {
  let v = (0..count).map(|i| start + i as f64 * step).collect();
  Series::Float64(v)
}

#[tokio::test]
async fn test_insert_new() -> Result<()> {
  let db = init_db(true).await?;
  let n = 25;
  let (req, rx) = Request::insert(InsertRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    data: InsertData::Columns(sample_data(n, "2023-01-01")),
  });

  db.request(req).await?;
  let _res = rx.await??;
  Ok(())
}

#[tokio::test]
async fn test_insert_existing() -> Result<()> {
  setup();

  let db = init_db(true).await?;
  let n = 25;
  let (req, rx) = Request::insert(InsertRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    data: InsertData::Columns(sample_data(n, "2023-01-01")),
  });

  db.request(req).await?;
  let _res = rx.await??;

  let (req, rx) = Request::insert(InsertRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    data: InsertData::Columns(sample_data(n, "2023-01-26")),
  });
  db.request(req).await?;
  let _res = rx.await??;

  let (req, rx) = Request::query(QueryRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    ..Default::default()
  });

  db.request(req).await?;
  let table = rx.await??;
  assert_eq!(table.column_count(), 2);
  assert_eq!(table.row_count(), n * 2);

  Ok(())
}

#[tokio::test]
async fn test_query() -> Result<()> {
  let db = init_db(true).await?;
  let n = 25;
  let (req, rx) = Request::insert(InsertRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    data: InsertData::Columns(sample_data(n, "2023-01-01")),
  });

  db.request(req).await?;
  let _res = rx.await??;

  let (req, rx) = Request::query(QueryRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    ..Default::default()
  });

  db.request(req).await?;
  let table = rx.await??;
  assert_eq!(table.column_count(), 2);
  assert_eq!(table.row_count(), n);
  Ok(())
}
