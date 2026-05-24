use anyhow::Result;
use msd_table::{Table, Variant, table, v};
#[test]
fn test_ser_json() -> Result<()> {
  let t = table!(
    { name: "ts", kind: datetime, data: vec![1735689600f64, 1735747200.0, 1735833600.0] },
    { name: "price", kind: d64, data: vec!["1.0", "2.0", "3.0"] },
    { name: "sign", kind: string, data: vec!["a", "b", "c"]},
    { name: "nil", kind: null }
  );

  let t = t.replace_metadata([("a", v!(1i64)), ("b", v!("bbb"))]);

  let body = serde_json::to_string(&t)?;

  println!("{}", body);

  Ok(())
}

#[test]
fn test_ser_bincode() -> Result<()> {
  let t = table!(
    { name: "ts", kind: datetime, data: vec![1735689600f64, 1735747200.0, 1735833600.0] },
    { name: "price", kind: d64, data: vec!["1.0", "2.0", "3.0"] },
    { name: "sign", kind: string, data: vec!["a", "b", "c"]},
    { name: "nil", kind: null }
  );

  let t = t.replace_metadata([("a", v!(1i64)), ("b", v!("bbb"))]);

  let body = bincode_next::encode_to_vec(&t, bincode_next::config::standard())?;

  println!("bincode size: {}", body.len());

  let (t2, _): (Table, usize) =
    bincode_next::decode_from_slice(&body, bincode_next::config::standard())?;

  println!("{:?}", t2);

  Ok(())
}
