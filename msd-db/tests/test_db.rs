use std::{collections::HashMap, sync::Once, vec};

use anyhow::Result;
use msd_db::{
  MsdDb,
  request::{DeleteRequest, InsertData, InsertRequest, MsdRequest, QueryRequest, RequestKey}, // Added DeleteRequest
};
use msd_store::RocksDbStore;
use msd_table::{Series, Table, Variant, parse_datetime, table};

type Db = MsdDb<RocksDbStore>;

static INIT: Once = Once::new();

fn setup() {
  INIT.call_once(|| {
    tracing_subscriber::fmt()
      .with_env_filter("msd_db=debug")
      .try_init()
      .ok();
  });
}

async fn create_db(path: &str) -> Result<Db> {
  let s = RocksDbStore::new(path)?;
  let db = MsdDb::new(s, 1).await?;
  Ok(db)
}

fn remove_db(path: &str) -> Result<()> {
  let _ = std::fs::remove_dir_all(path);
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

async fn init_db(path: &str, clear: bool) -> Result<Db> {
  if clear {
    remove_db(path)?;
  }
  let db = create_db(path).await?;
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

async fn do_query(db: &Db, table: &str, obj: &str) -> Result<Table> {
  let (req, rx) = MsdRequest::query(QueryRequest {
    key: RequestKey::new(table, obj),
    ..Default::default()
  });
  db.request(req).await?;
  let table = rx.await??;
  Ok(table)
}
async fn do_delete(db: &Db, table: &str, obj: &str) -> Result<()> {
  let (req, rx) = MsdRequest::delete(DeleteRequest {
    key: RequestKey::new(table, obj),
    ..Default::default()
  });
  db.request(req).await?;
  rx.await??;
  Ok(())
}

async fn do_drop_table(db: &Db, table: &str) -> Result<()> {
  let req = MsdRequest::drop_table(table);
  db.request(req).await?;
  Ok(())
}

#[tokio::test]
async fn test_create_db() -> Result<()> {
  let path = "/tmp/msd_store_test_create_db";
  let db = init_db(path, true).await?;

  let invalid_table = table!(
    {name: "ts", kind: u64}, // invalid primary key
    {name: "open", kind: f64},
  );
  let req = MsdRequest::create_table("invalid_t1", invalid_table);
  let res = db.request(req).await;
  assert!(res.is_err());

  Ok(())
}

#[tokio::test]
async fn test_insert_new() -> Result<()> {
  let path = "/tmp/msd_store_test_insert_new";
  let db = init_db(path, true).await?;
  let n = 25;
  let req = InsertRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    data: InsertData::Columns(sample_data(n, "2023-01-01")),
  };
  // Convert to table using schema
  let schema = db.get_schema("kline1d")?;
  let mut req_vec = req.to_table(&schema)?;
  let (req, rx) = MsdRequest::insert(req_vec.remove(0));

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
  let path = "/tmp/msd_store_test_insert_existing";
  let db = init_db(path, true).await?;
  let n = 25;
  insert_data(&db, "kline1d", "SH600000", n, "2023-01-01").await?;

  insert_data(&db, "kline1d", "SH600000", n, "2023-01-26").await?;

  let table = do_query(&db, "kline1d", "SH600000").await?;
  assert_eq!(table.column_count(), 2 + 1);
  assert_eq!(table.row_count(), n * 2);

  Ok(())
}

#[tokio::test]
async fn test_insert_multiple_objects() -> Result<()> {
  setup();
  let path = "/tmp/msd_store_test_insert_multiple_objects";
  let db = init_db(path, true).await?;
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
  // Use insert_existing test path to reuse data? No, concurrency issue.
  // Re-init db with false? But previous test handles lifecycle.
  // Better just create new DB.
  let path = "/tmp/msd_store_test_query";
  let db = init_db(path, true).await?;

  // Need data to query
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01").await?;
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-26").await?;

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
  let path = "/tmp/msd_store_test_create_db_kline";
  let db = init_db(path, true).await?;

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

#[tokio::test]
async fn test_delete() -> Result<()> {
  setup();
  let path = "/tmp/msd_store_test_delete";
  let db = init_db(path, true).await?;

  // Insert data
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01").await?;
  insert_data(&db, "kline1d", "SH600001", 25, "2023-01-01").await?;

  let table = do_query(&db, "kline1d", "SH600000").await?;
  assert_eq!(table.row_count(), 25, "SH600000 inserted failed");

  // Delete SH600000
  do_delete(&db, "kline1d", "SH600000").await?;

  // Verify deleted, should return error
  assert!(
    do_query(&db, "kline1d", "SH600000").await.is_err(),
    "SH600000 deleted failed"
  );

  // Verify SH600001 still exists
  let table = do_query(&db, "kline1d", "SH600001").await?;
  assert_eq!(table.row_count(), 25, "SH600001 deleted failed");

  // Delete entire table
  do_delete(&db, "kline1d", "").await?;

  // Verify SH600001 is gone, should return error
  assert!(
    do_query(&db, "kline1d", "SH600001").await.is_err(),
    "SH600001 deleted failed"
  );

  // Delete entire table
  do_delete(&db, "kline1d", "").await?;

  // Verify SH600001 is gone, should return error
  assert!(
    do_query(&db, "kline1d", "SH600001").await.is_err(),
    "SH600001 deleted failed"
  );

  // Insert data again, should be ok
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01").await?;
  insert_data(&db, "kline1d", "SH600001", 25, "2023-01-01").await?;

  // Verify inserted
  let table = do_query(&db, "kline1d", "SH600000").await?;
  assert_eq!(table.row_count(), 25, "SH600000 inserted failed");

  // Drop table
  do_drop_table(&db, "kline1d").await?;

  // Verify SH600001 is gone, should return error
  assert!(
    do_query(&db, "kline1d", "SH600001").await.is_err(),
    "SH600001 deleted failed"
  );

  // can't insert data to dropped table
  assert!(
    insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01")
      .await
      .is_err(),
    "SH600000 inserted to dropped table failed"
  );

  Ok(())
}
