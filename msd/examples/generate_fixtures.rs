// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::{table, Variant, D64};
use std::fs::{create_dir_all, File};
use std::io::Write;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Create output directory if not exists
  create_dir_all("bindings/typescript/tests/fixtures")?;

  // Build the table
  let mut table = table!(
    { name: "ts", kind: datetime, data: vec![1735660800000000i64, 1735747200000000i64, 1735833600000000i64] },
    { name: "price", kind: d64, data: vec![D64::from_i64(123, 2), D64::from_i64(200, 2), D64::from_i64(300, 2)] },
    { name: "null", kind: null }
  );

  table = table.replace_metadata([("obj".to_string(), Variant::String("SH600000".to_string()))]);

  // 1. Serialize as raw bincode Table
  let table_bytes = bincode::serde::encode_to_vec(&table, bincode::config::standard())?;
  let mut raw_file = File::create("bindings/typescript/tests/fixtures/test_table.bin")?;
  raw_file.write_all(&table_bytes)?;

  // 2. Serialize as packed Table Frame
  let frame_bytes = msd_request::pack_table_frame(&table);
  let mut frame_file = File::create("bindings/typescript/tests/fixtures/test_frame.bin")?;
  frame_file.write_all(&frame_bytes)?;

  println!("Fixtures generated successfully.");
  Ok(())
}
