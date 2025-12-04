use std::collections::HashMap;

use anyhow::Result;
use msd_db::{
  MsdDb,
  request::{Broadcast, Request},
};
use msd_store::{MsdStore, RocksDbStore};
use msd_table::{Table, Variant, table};

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
  //remove_db()?;
  let db = create_db().await?;
  let table = create_table();

  let req = Request::create_table("kline1d".into(), table);

  db.request(req).await?;

  Ok(())
}
