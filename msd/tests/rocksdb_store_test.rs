// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::fs::{exists, remove_dir_all};

use anyhow::Result;
use msd_store::{MsdStore, RocksDbStore};

fn setup_debug_logging() {
  let filter = tracing_subscriber::EnvFilter::from_default_env();
  let log_writer = std::io::stderr;
  let ansi_color = true;

  tracing_subscriber::fmt()
    .with_env_filter(filter)
    .with_ansi(ansi_color)
    .with_level(true)
    .with_writer(log_writer)
    .init();
}

fn create_db(remove: bool) -> Result<RocksDbStore> {
  let db_path = "/tmp/msd_db_test";

  if remove && exists(db_path)? {
    remove_dir_all(db_path)?;
  }

  RocksDbStore::new(db_path).map_err(|e| e.into())
}

fn make_key(key: &str, ts: u64) -> Vec<u8> {
  let mut key = key.as_bytes().to_vec();
  let ts = u64::MAX - 1000 - ts;
  key.extend_from_slice(&ts.to_be_bytes());
  key
}

#[test]
fn store_get_put_test() -> Result<()> {
  let db = create_db(true)?;

  let tables = db.list_tables()?;

  assert!(tables.contains(&"default".to_string()));

  let ts = (0..10).map(|i| 1 << i).collect::<Vec<u64>>();
  let values = ts
    .iter()
    .map(|i| i.to_string().as_bytes().to_vec())
    .collect::<Vec<_>>();
  let key_a = "a";
  let key_b = "b";

  // test data is
  // (a,512) -> 512
  // (a,256) -> 256
  // ...
  // (a,  1) -> 1

  for (&ts, value) in ts.iter().zip(values.iter()) {
    let key = make_key(key_a, ts);
    db.put(&key, value.as_slice(), "default", None)?;

    // more other key
    let key = make_key(key_b, ts);
    db.put(&key, value.as_slice(), "default", None)?;
  }

  for (&ts, value) in ts.iter().zip(values.iter()) {
    let key = make_key(key_a, ts);
    let fetched_value = db.get(&key, "default")?;
    assert_eq!(fetched_value, Some(value.clone()));
  }

  let mut less_than_30 = vec![];
  db.prefix_with(make_key(key_a, 30), Some(1), "default", false, |_k, v| {
    less_than_30.push(String::from_utf8_lossy(v).to_string());
    true
  })?;
  assert_eq!(less_than_30, vec!["16", "8", "4", "2", "1"]);

  let mut less_than_32 = vec![];
  db.prefix_with(make_key(key_a, 32), Some(1), "default", false, |_k, v| {
    less_than_32.push(String::from_utf8_lossy(v).to_string());
    true
  })?;
  assert_eq!(less_than_32, vec!["32", "16", "8", "4", "2", "1"]);

  for &ts in ts.iter() {
    let key = make_key(key_a, ts);
    db.delete(&key, "default")?;
  }

  for &ts in ts.iter() {
    let key = make_key(key_a, ts);
    let value = db.get(&key, "default")?;
    assert_eq!(value, None);
  }

  Ok(())
}

#[test]
fn store_ttl_test() -> Result<()> {
  setup_debug_logging();
  let db = create_db(true)?;

  let key_a = "a";
  let key_b = "b";
  let value = "value".as_bytes().to_vec();
  db.put(key_a.as_bytes(), value.as_slice(), "default", Some(1))?;
  db.put(key_b.as_bytes(), value.as_slice(), "default", Some(3))?;

  let fetched_value = db.get(key_a.as_bytes(), "default")?;
  assert_eq!(fetched_value, Some(value.clone()));

  std::thread::sleep(std::time::Duration::from_secs(1));

  db.remove_expired()?;

  let expired_value = db.get(key_a.as_bytes(), "default")?;
  assert_eq!(expired_value, None);

  let fetched_value = db.get(key_b.as_bytes(), "default")?;
  assert_eq!(fetched_value, Some(value.clone()));

  Ok(())
}
