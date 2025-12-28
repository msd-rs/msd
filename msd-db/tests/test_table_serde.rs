// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_db::DbBinary;
use msd_table::{Series, Table, table};

fn build_table() -> Table {
  table!(
    {name:"ts", kind:datetime, data:vec!["2025-01-01", "2025-01-02", "2025-01-03"]},
    {name:"price", kind:d64, data:vec!["1.23", "2.0", "3.0"]},
    {name:"null", kind:null, data:Series::Null}
  )
}

#[test]
fn test_to_bytes() -> anyhow::Result<()> {
  let table = build_table();

  let bytes = DbBinary::to_bytes(&table)?;
  println!("Serialized bytes length: {}", bytes.len());
  println!("{:?}", bytes);

  let json_str = serde_json::to_string(&table)?;
  println!("Serialized JSON length: {}", json_str.len());
  println!("{}", json_str);

  Ok(())
}
