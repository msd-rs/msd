use std::{collections::HashMap, vec};

use anyhow::Result;
use msd_db::{
  MsdDb,
  request::{InsertData, InsertRequest, MsdRequest, QueryRequest, RequestKey},
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
  let req = MsdRequest::create_table("kline1d", table);
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
  let req = MsdRequest::create_table("invalid_t1", invalid_table);
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
  let (req, rx) = MsdRequest::insert(InsertRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    data: InsertData::Columns(sample_data(n, "2023-01-01")),
  });

  db.request(req).await?;
  let _res = rx.await??;
  Ok(())
}

async fn insert_data(
  db: &Db,
  table: &str,
  obj: &str,
  count: usize,
  start_date: &str,
) -> Result<()> {
  let req = InsertRequest {
    key: RequestKey::new(table, obj),
    data: InsertData::Columns(sample_data(count, start_date)),
  };
  let mut req = req.to_table(&db.get_schema(table)?)?;
  assert!(req.len() == 1);
  let (req, rx) = MsdRequest::insert(req.remove(0));
  db.request(req).await?;
  let _res = rx.await??;
  Ok(())
}

#[tokio::test]
async fn test_insert_existing() -> Result<()> {
  setup();

  let db = init_db(true).await?;
  let n = 25;
  insert_data(&db, "kline1d", "SH600000", n, "2023-01-01").await?;

  insert_data(&db, "kline1d", "SH600000", n, "2023-01-26").await?;

  let (req, rx) = MsdRequest::query(QueryRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    ..Default::default()
  });

  db.request(req).await?;
  let table = rx.await??;
  assert_eq!(table.column_count(), 2 + 1);
  assert_eq!(table.row_count(), n * 2);

  Ok(())
}

#[tokio::test]
async fn test_insert_multiple_objects() -> Result<()> {
  setup();
  let db = init_db(true).await?;
  let objects = vec![
    "SH600000", "SH600001", "SH600002", "SZ000001", "SZ000002", "SZ000003",
  ];
  for obj in objects {
    insert_data(&db, "kline1d", obj, 25, "2023-01-01").await?;
  }
  Ok(())
}

#[tokio::test]
async fn test_query() -> Result<()> {
  let db = init_db(false).await?;
  let n = 25;

  let (req, rx) = MsdRequest::query(QueryRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    ..Default::default()
  });

  db.request(req).await?;
  let table = rx.await??;
  assert_eq!(table.column_count(), 2 + 1);
  assert_eq!(table.row_count(), n * 2);
  Ok(())
}

#[tokio::test]
async fn test_create_db_kline() -> Result<()> {
  setup();
  let db = init_db(true).await?;

  let table = table!(
    {name: "ts", kind: datetime},
    {name: "open", kind: f64},
    {name: "high", kind: f64},
    {name: "low", kind: f64},
    {name: "close", kind: f64},
    {name: "volume", kind: f64},
    {name: "amount", kind: f64},
  );

  let metadata: HashMap<String, Variant> = HashMap::from([
    ("chunkSize".into(), 250u32.into()),
    ("round".into(), "1d".into()),
  ]);

  let table = table.with_metadata(metadata);

  let req = MsdRequest::create_table("kline_real", table);
  db.request(req).await?;

  Ok(())
}
