use std::io::Cursor;

use msd_table::{table, table_from_csv, table_to_csv};

#[test]
fn test_table_to_csv() -> anyhow::Result<()> {
  let t1 = table!(
    { name: "ts", kind: datetime, data: vec![1735689600f64, 1735747200.0, 1735833600.0] },
    { name: "price", kind: d64, data: vec!["1.0", "2.0", "3.0"] }
  );

  let mut csv_data = Vec::default();
  table_to_csv(&t1, Cursor::new(&mut csv_data), b',')?;
  let expected_csv = "ts,price\n1735689600000000,1.0\n1735747200000000,2.0\n1735833600000000,3.0\n";
  assert_eq!(String::from_utf8(csv_data)?, expected_csv);

  let t2 = table_from_csv(Cursor::new(expected_csv.as_bytes()), b',', &t1)?;
  assert!(t1.same_shape(&t2));
  assert_eq!(t1.row_count(), t2.row_count());

  Ok(())
}
