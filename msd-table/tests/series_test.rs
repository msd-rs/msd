use msd_table::*;

#[test]
fn test_series_downcast() {
  let series = Series::Int32(vec![1, 2, 3]);
  let v = series.get_int32();
  assert!(v.is_some());
  let v = v.unwrap();
  assert_eq!(v.len(), 3);
  assert_eq!(v[0], 1);
}

#[test]
fn test_serde() {
  let mut table = Table::new(
    vec![
      Field::new("id".to_string(), DataType::Int32),
      Field::new("name".to_string(), DataType::String),
    ],
    3,
  );

  table.rows().enumerate().for_each(|(i, row)| {
    println!("Row {}: {:?}", i, row);
  });

  let bytes = table.to_bytes().unwrap();

  let deserialized_table = Table::from_bytes(&bytes).unwrap();
}

#[test]
fn test_variant_v_macro() {
  assert_eq!(v!(1).get_i32(), Some(&1_i32));
  assert_eq!(v!(1_i32).get_i32(), Some(&1_i32));
  assert_eq!(v!(1_i64).get_i64(), Some(&1_i64));
  assert_eq!(v!(1.0_f32).get_f32(), Some(&1.0_f32));
  assert_eq!(v!("test").get_str(), Some("test"));
  assert_eq!(v!("test").get_string(), Some("test".to_string()).as_ref());

  assert_eq!(v!(1, 2, 3), vec![v!(1), v!(2), v!(3)]);
}

#[test]
fn test_series_s_macro() {
  assert_eq!(s!(1).get_int32(), Some(&vec![1_i32]));

  assert_eq!(
    s!("1", "2").get_string(),
    Some(&vec!["1".to_string(), "2".to_string()])
  );
}
