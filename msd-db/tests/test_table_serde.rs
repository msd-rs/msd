use msd_db::DbBinary;
use msd_table::{Table, table};

fn build_table() -> Table {
  table!(
    {name:"ts", kind:datetime, data:vec![1735689600f64, 1735747200.0, 1735833600.0]},
    {name:"price", kind:d64, data:vec!["1.23", "2.0", "3.0"]}
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
