// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::*;

#[test]
fn test_series_downcast() {
  let series = Series::Int32(vec![1, 2, 3]);
  let v = series.get_int32();
  assert!(v.is_some());
  let v = v.unwrap();
  assert_eq!(v.len(), 3);
  assert_eq!(v[0], 1);

  let v1 = v!(1);
  let v2 = v1.as_ref();
  let v3: Variant = v2.into();

  assert_eq!(v1, v3);
}

#[test]
fn test_table_set() {
  let mut table = table!(
    { name: "id", kind: i32 },
    { name: "name", kind: string }
  );

  table.push_row(vec![v!(1), v!("Alice")]).unwrap();

  assert_eq!(table.row_count(), 1);
  assert_eq!(table.column_count(), 2);

  assert_eq!(table.cell(0, 0).get_i32(), Some(&1));
  assert_eq!(table.cell(0, 1).get_str(), Some("Alice"));

  table.cell_mut(0, 0).set(v!(2)).unwrap();
  table.cell_mut(0, 1).set(v!("Bob")).unwrap();

  assert_eq!(table.cell(0, 0).get_i32(), Some(&2));
  assert_eq!(table.cell(0, 1).get_str(), Some("Bob"));
}

#[test]
fn test_variant_v_macro() {
  assert_eq!(v!(1).get_i32(), Some(&1_i32));
  assert_eq!(v!(1_i32).get_i32(), Some(&1_i32));
  assert_eq!(v!(1_i64).get_i64(), Some(&1_i64));
  assert_eq!(v!(1.0_f32).get_f32(), Some(&1.0_f32));
  assert_eq!(v!("test").get_str(), Some("test"));
  assert_eq!(v!("test").get_string(), Some("test".to_string()).as_ref());

  assert_eq!(v!(1, datetime).get_datetime(), Some(&1_i64))
}

#[test]
fn test_series_s_macro() {
  assert_eq!(s!(1).get_int32(), Some(&vec![1_i32]));

  assert_eq!(
    s!("1", "2").get_string(),
    Some(&vec!["1".to_string(), "2".to_string()])
  );
}

#[test]
fn test_table_macro() {
  // Test table! macro with data
  let t = table!(
    { name: "id", kind: i64, data: vec![1i64, 2, 3] },
    { name: "value", kind: f64, data: vec![1.0, 2.0, 3.0] }
  );
  assert_eq!(t.column_count(), 2);
  assert_eq!(t.row_count(), 3);
  assert_eq!(t.column("id").unwrap().kind, DataType::Int64);
  assert_eq!(t.column("value").unwrap().kind, DataType::Float64);
  assert_eq!(t.cell(0, 0).get_i64(), Some(&1i64));
  assert_eq!(t.cell(2, 1).get_f64(), Some(&3.0f64));

  // Test table! macro without data (defaults to empty Vec)
  let t2 = table!(
    { name: "label", kind: string },
    { name: "count", kind: i32 }
  );
  assert_eq!(t2.column_count(), 2);
  assert_eq!(t2.row_count(), 0);
  assert_eq!(t2.column("label").unwrap().kind, DataType::String);
  assert_eq!(t2.column("count").unwrap().kind, DataType::Int32);

  // Test table! macro with mixed columns (some with data, some without)
  let t3 = table!(
    { name: "a", kind: i64, data: vec![10i64, 20] },
    { name: "b", kind: bool }
  );
  assert_eq!(t3.column_count(), 2);
  // Note: column "a" has 2 rows, column "b" has 0 rows - this is an edge case
  // The row_count is based on the first column
  assert_eq!(t3.column("a").unwrap().data.len(), 2);
  assert_eq!(t3.column("b").unwrap().data.len(), 0);

  // Test all supported types
  let t4 = table!(
    { name: "c_i32", kind: i32, data: vec![1i32] },
    { name: "c_u32", kind: u32, data: vec![2u32] },
    { name: "c_i64", kind: i64, data: vec![3i64] },
    { name: "c_u64", kind: u64, data: vec![4u64] },
    { name: "c_f32", kind: f32, data: vec![5.0f32] },
    { name: "c_f64", kind: f64, data: vec![6.0f64] },
    { name: "c_bool", kind: bool, data: vec![true] },
    { name: "c_string", kind: string, data: vec!["hello".to_string()] },
    { name: "c_datetime", kind: datetime, data: vec![1234567890i64] }
  );
  assert_eq!(t4.column_count(), 9);
  assert_eq!(t4.row_count(), 1);
}
