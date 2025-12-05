use std::{collections::HashMap, sync::Arc, vec};

use anyhow::Result;
use msd_db::{
  MsdDb,
  request::{Broadcast, InsertData, InsertRequest, Request, RequestKey},
};
use msd_store::{MsdStore, RocksDbStore};
use msd_table::{Series, Table, Variant, parse_datetime, table};

const DATA_DIR: &str = "/tmp/msd_store_test_db";

type Db = MsdDb<RocksDbStore>;

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

#[tokio::test]
async fn test_create_db() -> Result<()> {
  remove_db()?;
  let db = create_db().await?;
  let table = create_table();

  let req = Request::create_table("kline1d".into(), table);

  db.request(req).await?;

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
  remove_db()?;
  let db = create_db().await?;
  let table = create_table();

  let req = Request::create_table("kline1d".into(), table);

  db.request(req).await?;

  let n = 25;
  let (req, rx) = Request::insert(InsertRequest {
    key: RequestKey::new("kline1d".into(), "SH600000".into()),
    data: InsertData::Columns(vec![
      build_datetime_series("2023-01-01", n, 86400)?,
      build_f64_series(10.0, n, 1.0),
    ]),
  });

  db.request(req).await?;

  let _res = rx.await??;

  Ok(())
}
