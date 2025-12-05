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
