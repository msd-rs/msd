// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use msd_table::*;
use std::collections::HashMap;

#[test]
fn test_chunks() {
  let t = table!(
      { name: "id", kind: i32, data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
      { name: "val", kind: string, data: vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] }
  );

  let chunks = t.chunks(3);
  assert_eq!(chunks.len(), 4);
  assert_eq!(chunks[0].row_count(), 3);
  assert_eq!(chunks[1].row_count(), 3);
  assert_eq!(chunks[2].row_count(), 3);
  assert_eq!(chunks[3].row_count(), 1);

  // Verify content
  assert_eq!(chunks[0].cell(0, 0).get_i32(), Some(&1));
  assert_eq!(chunks[0].cell(2, 0).get_i32(), Some(&3));
  assert_eq!(chunks[1].cell(0, 0).get_i32(), Some(&4));
  assert_eq!(chunks[3].cell(0, 0).get_i32(), Some(&10));
}

#[test]
fn test_chunks_exact() {
  let t = table!(
      { name: "id", kind: i32, data: vec![1, 2, 3, 4, 5, 6] }
  );

  let chunks = t.chunks(3);
  assert_eq!(chunks.len(), 2);
  assert_eq!(chunks[0].row_count(), 3);
  assert_eq!(chunks[1].row_count(), 3);
}

#[test]
fn test_chunks_large_size() {
  let t = table!(
      { name: "id", kind: i32, data: vec![1, 2, 3] }
  );

  let chunks = t.chunks(10);
  assert_eq!(chunks.len(), 1);
  assert_eq!(chunks[0].row_count(), 3);
}

#[test]
fn test_chunks_empty() {
  let t = table!(
      { name: "id", kind: i32 }
  );

  let chunks = t.chunks(3);
  assert_eq!(chunks.len(), 0);
}

#[test]
fn test_extend() {
  let mut t1 = table!(
      { name: "id", kind: i32, data: vec![1, 2] },
      { name: "val", kind: string, data: vec!["a", "b"] }
  );
  let t2 = table!(
      { name: "id", kind: i32, data: vec![3, 4] },
      { name: "val", kind: string, data: vec!["c", "d"] }
  );

  t1.extend(&t2, false).unwrap();

  assert_eq!(t1.row_count(), 4);
  assert_eq!(t1.cell(2, 0).get_i32(), Some(&3));
  assert_eq!(t1.cell(3, 1).get_str(), Some("d"));
}

#[test]
fn test_extend_rev() {
  let mut t1 = table!(
      { name: "id", kind: i32, data: vec![1] }
  );
  let t2 = table!(
      { name: "id", kind: i32, data: vec![2, 3] }
  );

  t1.extend(&t2, true).unwrap();

  assert_eq!(t1.row_count(), 3);
  assert_eq!(t1.cell(1, 0).get_i32(), Some(&3));
  assert_eq!(t1.cell(2, 0).get_i32(), Some(&2));
}

#[test]
fn test_extend_filtered() {
  let mut t1 = table!(
      { name: "id", kind: i32, data: vec![1] }
  );
  let t2 = table!(
      { name: "id", kind: i32, data: vec![2, 3, 4, 5] }
  );

  // Filter keep even numbers
  t1.extend_filtered(&t2, false, |row| {
    let val = row[0].get_i32().unwrap();
    *val % 2 == 0
  })
  .unwrap();

  assert_eq!(t1.row_count(), 3); // 1 (original) + 2 (filtered from t2: 2, 4)
  assert_eq!(t1.cell(1, 0).get_i32(), Some(&2));
  assert_eq!(t1.cell(2, 0).get_i32(), Some(&4));
}

#[test]
fn test_sort_by_pk() {
  let mut t = table!(
      { name: "id", kind: datetime, data: vec![300i64, 100, 200] },
      { name: "val", kind: string, data: vec!["c", "a", "b"] }
  );

  // Set PK
  let col = t.column_mut("id").unwrap();
  let mut meta = HashMap::new();
  meta.insert("primary_key".to_string(), v!(true));
  col.metadata = Some(meta);

  // Sort ascending
  t.sort_by_pk(false);
  assert_eq!(t.cell(0, 0).get_datetime(), Some(&100));
  assert_eq!(t.cell(1, 0).get_datetime(), Some(&200));
  assert_eq!(t.cell(2, 0).get_datetime(), Some(&300));
  assert_eq!(t.cell(0, 1).get_str(), Some("a"));

  // Sort descending
  t.sort_by_pk(true);
  assert_eq!(t.cell(0, 0).get_datetime(), Some(&300));
  assert_eq!(t.cell(1, 0).get_datetime(), Some(&200));
  assert_eq!(t.cell(2, 0).get_datetime(), Some(&100));
  assert_eq!(t.cell(0, 1).get_str(), Some("c"));
}

#[test]
fn test_group_by() {
  let t = table!(
      { name: "group", kind: string, data: vec!["A", "B", "A", "C", "B"] },
      { name: "val", kind: i32, data: vec![1, 2, 3, 4, 5] },
      { name: "other", kind: i32, data: vec![10, 20, 30, 40, 50] }
  );

  let groups = t.group_by(0).unwrap();

  assert_eq!(groups.len(), 3);

  // Check Group A
  let t_a = groups.get(&v!("A")).unwrap();
  assert_eq!(t_a.row_count(), 2);
  assert_eq!(t_a.column_count(), 2); // 'group' column is removed
  assert_eq!(t_a.cell(0, 0).get_i32(), Some(&1));
  assert_eq!(t_a.cell(1, 0).get_i32(), Some(&3));
  assert_eq!(t_a.cell(0, 1).get_i32(), Some(&10));
  assert_eq!(t_a.cell(1, 1).get_i32(), Some(&30));

  // Check Group B
  let t_b = groups.get(&v!("B")).unwrap();
  assert_eq!(t_b.row_count(), 2);
  assert_eq!(t_b.cell(0, 0).get_i32(), Some(&2));
  assert_eq!(t_b.cell(1, 0).get_i32(), Some(&5));

  // Check Group C
  let t_c = groups.get(&v!("C")).unwrap();
  assert_eq!(t_c.row_count(), 1);
  assert_eq!(t_c.cell(0, 0).get_i32(), Some(&4));
}

#[test]
fn test_join_asof_fill_methods() {
  let left = table!(
    { name: "ts", kind: i64, data: vec![1i64, 2, 3, 4, 5] },
    { name: "a", kind: f64, data: vec![10.0, 20.0, 30.0, 40.0, 50.0] }
  );

  let right = table!(
    { name: "ts", kind: i64, data: vec![2i64, 4] },
    { name: "b", kind: f64, data: vec![200.0, 400.0] },
    { name: "c", kind: i32, data: vec![2, 4] }
  );

  // FillMethod::Zero
  let joined_zero = left.join_asof(&right, FillMethod::Zero).unwrap();
  assert_eq!(joined_zero.row_count(), 5);
  assert_eq!(joined_zero.column_count(), 4);
  assert_eq!(
    joined_zero.column("b").unwrap().data.get_float64().unwrap(),
    &vec![0.0, 200.0, 0.0, 400.0, 0.0]
  );
  assert_eq!(
    joined_zero.column("c").unwrap().data.get_int32().unwrap(),
    &vec![0, 2, 0, 4, 0]
  );

  // FillMethod::Nan
  let joined_nan = left.join_asof(&right, FillMethod::Nan).unwrap();
  assert_eq!(joined_nan.row_count(), 5);
  assert_eq!(joined_nan.column_count(), 4);
  let b_nan = joined_nan.column("b").unwrap().data.get_float64().unwrap();
  assert!(b_nan[0].is_nan());
  assert_eq!(b_nan[1], 200.0);
  assert!(b_nan[2].is_nan());
  assert_eq!(b_nan[3], 400.0);
  assert!(b_nan[4].is_nan());

  // FillMethod::Backward
  let joined_bwd = left.join_asof(&right, FillMethod::Backward).unwrap();
  assert_eq!(joined_bwd.row_count(), 5);
  assert_eq!(joined_bwd.column_count(), 4);
  let b_bwd = joined_bwd.column("b").unwrap().data.get_float64().unwrap();
  assert!(b_bwd[0].is_nan());
  assert_eq!(b_bwd[1], 200.0);
  assert_eq!(b_bwd[2], 200.0);
  assert_eq!(b_bwd[3], 400.0);
  assert_eq!(b_bwd[4], 400.0);
  assert_eq!(
    joined_bwd.column("c").unwrap().data.get_int32().unwrap(),
    &vec![0, 2, 2, 4, 4]
  );

  // FillMethod::Forward
  let joined_fwd = left.join_asof(&right, FillMethod::Forward).unwrap();
  assert_eq!(joined_fwd.row_count(), 5);
  assert_eq!(joined_fwd.column_count(), 4);
  let b_fwd = joined_fwd.column("b").unwrap().data.get_float64().unwrap();
  assert_eq!(b_fwd[0], 200.0);
  assert_eq!(b_fwd[1], 200.0);
  assert_eq!(b_fwd[2], 400.0);
  assert_eq!(b_fwd[3], 400.0);
  assert!(b_fwd[4].is_nan());
  assert_eq!(
    joined_fwd.column("c").unwrap().data.get_int32().unwrap(),
    &vec![2, 2, 4, 4, 0]
  );
}

#[test]
fn test_join_asof_non_zero_pk_column_and_d64() {
  let mut left = table!(
    { name: "val", kind: string, data: vec!["x", "y", "z"] },
    { name: "ts", kind: datetime, data: vec![100i64, 200, 300] }
  );
  left
    .set_field_meta("ts", "primary_key", v!(true))
    .unwrap();

  let d1: D64 = "12.34".parse().unwrap();
  let d2: D64 = "56.78".parse().unwrap();
  let mut right = table!(
    { name: "price", kind: d64, data: vec![d1, d2] },
    { name: "ts", kind: datetime, data: vec![100i64, 300] },
    { name: "tag", kind: string, data: vec!["p1", "p2"] }
  );
  right
    .set_field_meta("ts", "primary_key", v!(true))
    .unwrap();

  let joined_nan = left.join_asof(&right, FillMethod::Nan).unwrap();
  assert_eq!(joined_nan.row_count(), 3);
  assert_eq!(joined_nan.column_count(), 4);
  let prices_nan = joined_nan
    .column("price")
    .unwrap()
    .data
    .get_decimal64()
    .unwrap();
  assert_eq!(prices_nan[0], d1);
  assert!(prices_nan[1].is_nan());
  assert_eq!(prices_nan[2], d2);
  assert_eq!(
    joined_nan.column("tag").unwrap().data.get_string().unwrap(),
    &vec!["p1".to_string(), "".to_string(), "p2".to_string()]
  );

  let joined_zero = left.join_asof(&right, FillMethod::Zero).unwrap();
  let prices_zero = joined_zero
    .column("price")
    .unwrap()
    .data
    .get_decimal64()
    .unwrap();
  assert_eq!(prices_zero[0], d1);
  assert!(prices_zero[1].is_zero());
  assert_eq!(prices_zero[1].dec_num(), 2);
  assert_eq!(prices_zero[2], d2);
}
