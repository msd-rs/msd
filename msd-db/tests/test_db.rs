use std::{collections::HashMap, path::Path, sync::Once, vec};

use anyhow::Result;
use msd_db::{
  MsdDb,
  request::{DeleteRequest, InsertData, InsertRequest, MsdRequest, QueryRequest, RequestKey}, // Added DeleteRequest
};
use msd_request::AggStateId;
use msd_store::RocksDbStore;
use msd_table::{Series, Table, Variant, parse_datetime, table};
use tempfile::Builder as TempDirBuilder;
use tracing::info;

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

async fn create_db<P: AsRef<Path>>(path: P) -> Result<Db> {
  let s = RocksDbStore::new(path)?;
  let db = MsdDb::new(s, 1).await?;
  Ok(db)
}

fn remove_db<P: AsRef<Path>>(path: P) -> Result<()> {
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

async fn init_db<P: AsRef<Path>>(path: P, clear: bool, table: &str, schema: Table) -> Result<Db> {
  if clear {
    remove_db(path.as_ref())?;
  }
  let db = create_db(path.as_ref()).await?;
  let req = MsdRequest::create_table(table, schema);
  db.request(req).await?;
  Ok(db)
}

const ONE_DAY: i64 = 86400;

fn sample_data(n: usize, start_date: &str, step_secs: i64) -> Vec<Series> {
  let ts = build_datetime_series(start_date, n, step_secs).unwrap();
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
  let path = TempDirBuilder::new().prefix("msd_test_").tempdir()?;
  let db = init_db(path, true, "kline1d", create_table()).await?;

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
  let path = TempDirBuilder::new().prefix("msd_test_").tempdir()?;
  let db = init_db(path, true, "kline1d", create_table()).await?;
  let n = 25;
  let req = InsertRequest {
    key: RequestKey::new("kline1d", "SH600000"),
    data: InsertData::Columns(sample_data(n, "2023-01-01", ONE_DAY)),
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
  step_secs: i64,
) -> Result<()> {
  let req = InsertRequest {
    key: RequestKey::new(table, obj),
    data: InsertData::Columns(sample_data(count, start_date, step_secs)),
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
  let path = TempDirBuilder::new().prefix("msd_test_").tempdir()?;
  let db = init_db(path, true, "kline1d", create_table()).await?;
  let n = 25;
  insert_data(&db, "kline1d", "SH600000", n, "2023-01-01", ONE_DAY).await?;

  insert_data(&db, "kline1d", "SH600000", n, "2023-01-26", ONE_DAY).await?;

  let table = do_query(&db, "kline1d", "SH600000").await?;
  assert_eq!(table.column_count(), 2 + 1);
  assert_eq!(table.row_count(), n * 2);

  Ok(())
}

#[tokio::test]
async fn test_insert_multiple_objects() -> Result<()> {
  setup();
  let path = TempDirBuilder::new().prefix("msd_test_").tempdir()?;
  let db = init_db(path, true, "kline1d", create_table()).await?;
  let objects = vec![
    "SH600000", "SH600001", "SH600002", "SZ000001", "SZ000002", "SZ000003",
  ];
  for obj in objects {
    insert_data(&db, "kline1d", obj, 25, "2023-01-01", ONE_DAY).await?;
  }
  Ok(())
}

#[tokio::test]
async fn test_query() -> Result<()> {
  // Use insert_existing test path to reuse data? No, concurrency issue.
  // Re-init db with false? But previous test handles lifecycle.
  // Better just create new DB.
  let path = TempDirBuilder::new().prefix("msd_test_").tempdir()?;
  let db = init_db(path, true, "kline1d", create_table()).await?;

  // Need data to query
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01", ONE_DAY).await?;
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-26", ONE_DAY).await?;

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
  let path = "/tmp/msd_store_test_db";
  let db = init_db(path, true, "kline1d", create_table()).await?;

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

  info!(?table, "Creating table");

  let req = MsdRequest::create_table("kline", table);
  db.request(req).await?;

  Ok(())
}

#[tokio::test]
async fn test_delete() -> Result<()> {
  setup();
  let path = TempDirBuilder::new().prefix("msd_test_").tempdir()?;
  let db = init_db(path, true, "kline1d", create_table()).await?;

  // Insert data
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01", ONE_DAY).await?;
  insert_data(&db, "kline1d", "SH600001", 25, "2023-01-01", ONE_DAY).await?;

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
  insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01", ONE_DAY).await?;
  insert_data(&db, "kline1d", "SH600001", 25, "2023-01-01", ONE_DAY).await?;

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
    insert_data(&db, "kline1d", "SH600000", 25, "2023-01-01", ONE_DAY)
      .await
      .is_err(),
    "SH600000 inserted to dropped table failed"
  );

  Ok(())
}

#[tokio::test]
async fn test_insert_agg() -> Result<()> {
  setup();
  let path = TempDirBuilder::new().prefix("msd_test_").tempdir()?;

  let table = table!(
    {name: "ts", kind: datetime},
    {name: "open", kind: f64},
    {name: "high", kind: f64},
    {name: "low", kind: f64},
    {name: "close", kind: f64},
    {name: "volume", kind: f64},
  );

  let mut table = table.with_metadata(HashMap::from([
    ("chunkSize".into(), 250u32.into()),
    ("round".into(), "1m".into()),
  ]));

  let fields_agg = [
    ("open", AggStateId::First),
    ("high", AggStateId::Max),
    ("low", AggStateId::Min),
    ("volume", AggStateId::Sum),
  ];

  fields_agg
    .iter()
    .for_each(|(name, agg)| match table.column_mut(name) {
      Some(col) => col.add_metadata("agg".into(), Variant::String(agg.to_string())),
      None => {}
    });

  let n = 60 * 4;

  // build snapshot data every 3 seconds
  let data = vec![
    build_datetime_series("2023-01-01 09:30:00", n, 3).unwrap(),
    build_f64_series(0.0, n, 1.0), // open
    build_f64_series(0.0, n, 1.0), // high
    build_f64_series(0.0, n, 1.0), // low
    build_f64_series(0.0, n, 1.0), // close
    build_f64_series(1.0, n, 0.0), // volume
  ];

  let db = init_db(path, true, "kline1m", table).await?;

  let req = InsertRequest {
    key: RequestKey::new("kline1m", "SH600000"),
    data: InsertData::Columns(data),
  };
  let mut req = req.to_table(&db.get_schema("kline1m")?)?;
  assert!(req.len() == 1);
  let (req, rx) = MsdRequest::insert(req.remove(0));
  db.request(req).await?;
  let _res = rx.await??;

  let table = do_query(&db, "kline1m", "SH600000").await?;
  assert_eq!(table.column_count(), 6 + 1);
  assert_eq!(table.row_count(), n * 3 / 60);

  assert_eq!(
    table.column("ts").unwrap().data,
    Series::DateTime(vec![
      1672536600000000, // 2023-01-01 09:30:00
      1672536660000000, // 2023-01-01 09:31:00
      1672536720000000, // 2023-01-01 09:32:00
      1672536780000000, // 2023-01-01 09:33:00
      1672536840000000, // 2023-01-01 09:34:00
      1672536900000000, // 2023-01-01 09:35:00
      1672536960000000, // 2023-01-01 09:36:00
      1672537020000000, // 2023-01-01 09:37:00
      1672537080000000, // 2023-01-01 09:38:00
      1672537140000000, // 2023-01-01 09:39:00
      1672537200000000, // 2023-01-01 09:40:00
      1672537260000000, // 2023-01-01 09:41:00
    ])
  );

  assert_eq!(
    table.column("open").unwrap().data,
    Series::Float64(vec![
      0.0, 20.0, 40.0, 60.0, 80.0, 100.0, 120.0, 140.0, 160.0, 180.0, 200.0, 220.0
    ]),
    "open wrong, should be first value of each minute"
  );

  assert_eq!(
    table.column("high").unwrap().data,
    Series::Float64(vec![
      19.0, 39.0, 59.0, 79.0, 99.0, 119.0, 139.0, 159.0, 179.0, 199.0, 219.0, 239.0
    ]),
    "high wrong, should be max value of each minute"
  );

  assert_eq!(
    table.column("low").unwrap().data,
    Series::Float64(vec![
      0.0, 20.0, 40.0, 60.0, 80.0, 100.0, 120.0, 140.0, 160.0, 180.0, 200.0, 220.0
    ]),
    "low wrong, should be min value of each minute"
  );

  assert_eq!(
    table.column("close").unwrap().data,
    Series::Float64(vec![
      19.0, 39.0, 59.0, 79.0, 99.0, 119.0, 139.0, 159.0, 179.0, 199.0, 219.0, 239.0
    ]),
    "close wrong, should be last value of each minute"
  );

  assert_eq!(
    table.column("volume").unwrap().data,
    Series::Float64(vec![
      20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0
    ]),
    "volume sum wrong, should be sum of each minute"
  );

  Ok(())
}
