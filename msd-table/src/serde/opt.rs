//! optimized serde functions, some some type it's use less space

use serde::{Deserializer, Serializer};

use crate::D64;

/// # datetime optimize
///
/// for column `datetime` it's use i64 to store the datetime,
/// 1. get gcd of upto 10 values from `datetime`, if gcd is 1, then use it directly
/// 2. keep first and the gcd
/// 3. each value will be `value[i] = (values[i] - values[i-1]) / gcd`,  first value `value[0]` is `values[0]//gcd`
/// 4. serialize as `gcd, values...`
///

fn gcd(a: i64, b: i64) -> i64 {
  let mut a = a.unsigned_abs();
  let mut b = b.unsigned_abs();
  while b != 0 {
    let t = b;
    b = a % b;
    a = t;
  }
  a as i64
}

pub fn datetime_array_serialize<S>(value: &Vec<i64>, serializer: S) -> Result<S::Ok, S::Error>
where
  S: Serializer,
{
  use serde::ser::SerializeSeq;

  if value.is_empty() {
    let seq = serializer.serialize_seq(Some(0))?;
    return seq.end();
  }

  let _first = value[0];
  let limit = std::cmp::min(10, value.len());
  let mut g = value[0];
  for i in 1..limit {
    g = gcd(g, value[i]);
  }
  let gcd_val = if g <= 1 { 1 } else { g };

  let mut transformed = Vec::with_capacity(value.len());
  transformed.push(value[0] / gcd_val);
  for i in 1..value.len() {
    let diff = value[i] - value[i - 1];
    transformed.push(diff / gcd_val);
  }

  let mut seq = serializer.serialize_seq(Some(1 + transformed.len()))?;
  seq.serialize_element(&gcd_val)?;
  for v in &transformed {
    seq.serialize_element(v)?;
  }
  seq.end()
}

pub fn datetime_array_deserialize<'de, D>(deserializer: D) -> Result<Vec<i64>, D::Error>
where
  D: Deserializer<'de>,
{
  struct DatetimeVisitor;

  impl<'de> serde::de::Visitor<'de> for DatetimeVisitor {
    type Value = Vec<i64>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
      formatter.write_str("a sequence of i64 representing optimized datetime format")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
      A: serde::de::SeqAccess<'de>,
    {
      let gcd_val = match seq.next_element::<i64>()? {
        Some(val) => val,
        None => {
          return Ok(Vec::new());
        }
      };

      let mut transformed = Vec::new();
      while let Some(v) = seq.next_element::<i64>()? {
        transformed.push(v);
      }

      if transformed.is_empty() {
        return Ok(Vec::new());
      }

      let mut original = Vec::with_capacity(transformed.len());
      original.push(transformed[0] * gcd_val);
      for i in 1..transformed.len() {
        let prev = original[i - 1];
        let diff = transformed[i] * gcd_val;
        original.push(prev + diff);
      }

      Ok(original)
    }
  }

  deserializer.deserialize_seq(DatetimeVisitor)
}

// # D64 array optimize
// 1. get first normal value (not nan or inf) decimal use it's `dec_num` method
// 2. for each value, value[i] = (value[i].into::<i64> - value[i-1].into::<i64>()), first value is `value[0].into::<i64>()`
// 3. serialize as `dec_num, values...`

pub fn d64_array_serialize<S>(value: &Vec<D64>, serializer: S) -> Result<S::Ok, S::Error>
where
  S: Serializer,
{
  use serde::ser::SerializeSeq;

  if value.is_empty() {
    let seq = serializer.serialize_seq(Some(0))?;
    return seq.end();
  }

  let dec_num = value
    .iter()
    .find(|v| !v.is_nan() && !v.is_inf())
    .map(|v| v.dec_num())
    .unwrap_or(0);

  let mut transformed = Vec::with_capacity(value.len());
  let first_val = i64::from(&value[0]);
  transformed.push(first_val);
  for i in 1..value.len() {
    let curr = i64::from(&value[i]);
    let prev = i64::from(&value[i - 1]);
    transformed.push(curr - prev);
  }

  let mut seq = serializer.serialize_seq(Some(1 + transformed.len()))?;
  seq.serialize_element(&(dec_num as i64))?;
  for v in &transformed {
    seq.serialize_element(v)?;
  }
  seq.end()
}

pub fn d64_array_deserialize<'de, D>(deserializer: D) -> Result<Vec<D64>, D::Error>
where
  D: Deserializer<'de>,
{
  struct D64ArrayVisitor;

  impl<'de> serde::de::Visitor<'de> for D64ArrayVisitor {
    type Value = Vec<D64>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
      formatter.write_str("a sequence representing optimized D64 array format")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
      A: serde::de::SeqAccess<'de>,
    {
      let dec_num = match seq.next_element::<i64>()? {
        Some(d) => d as usize,
        None => return Ok(Vec::new()),
      };

      let mut diffs = Vec::new();
      while let Some(v) = seq.next_element::<i64>()? {
        diffs.push(v);
      }

      if diffs.is_empty() {
        return Ok(Vec::new());
      }

      let mut reconstructed = Vec::with_capacity(diffs.len());
      let mut current = diffs[0];
      reconstructed.push(D64::from_i64(current, dec_num));
      for i in 1..diffs.len() {
        current += diffs[i];
        reconstructed.push(D64::from_i64(current, dec_num));
      }

      Ok(reconstructed)
    }
  }

  deserializer.deserialize_seq(D64ArrayVisitor)
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde::{Deserialize, Serialize};

  #[derive(Debug, PartialEq, Serialize, Deserialize)]
  struct TestStruct {
    #[serde(
      serialize_with = "datetime_array_serialize",
      deserialize_with = "datetime_array_deserialize"
    )]
    dates: Vec<i64>,
  }

  #[test]
  fn test_datetime_opt_empty() {
    let s = TestStruct { dates: vec![] };
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, r#"{"dates":[]}"#);

    let deserialized: TestStruct = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, s);
  }

  #[test]
  fn test_datetime_opt_single() {
    let s = TestStruct { dates: vec![12345] };
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, r#"{"dates":[12345,1]}"#);

    let deserialized: TestStruct = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, s);
  }

  #[test]
  fn test_datetime_opt_gcd_greater_than_one() {
    // values: 100, 200, 300. Differences: 100, 100.
    // GCD of 100, 200, 300 is 100.
    // first = 100, gcd = 100.
    // value[0] = 100.
    // value[1] = (200 - 100) / 100 = 1.
    // value[2] = (300 - 200) / 100 = 1.
    // Serialized: [100, 100, 1, 1]
    let s = TestStruct {
      dates: vec![100, 200, 300],
    };
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, r#"{"dates":[100,1,1,1]}"#);

    let deserialized: TestStruct = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, s);
  }

  #[test]
  fn test_datetime_opt_gcd_one() {
    // values: 100, 101, 103. Differences: 1, 2.
    // GCD of 100, 101, 103 is 1.
    // first = 100, gcd = 1.
    // value[0] = 100.
    // value[1] = 101 - 100 = 1.
    // value[2] = 103 - 101 = 2.
    // Serialized: [1, 100, 1, 2]
    let s = TestStruct {
      dates: vec![100, 101, 103],
    };
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, r#"{"dates":[1,100,1,2]}"#);

    let deserialized: TestStruct = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, s);
  }

  #[test]
  fn test_datetime_opt_long() {
    // 15 values
    let dates = vec![
      1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000,
      15000,
    ];
    let s = TestStruct { dates };
    let json = serde_json::to_string(&s).unwrap();
    // GCD of first 10 is 1000.
    // first = 1000, gcd = 1000.
    // transformed = [1000, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    // Serialized: [1000, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    assert_eq!(json, r#"{"dates":[1000,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]}"#);

    let deserialized: TestStruct = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, s);
  }

  #[derive(Debug, PartialEq, Serialize, Deserialize)]
  struct TestD64Struct {
    #[serde(
      serialize_with = "d64_array_serialize",
      deserialize_with = "d64_array_deserialize"
    )]
    values: Vec<D64>,
  }

  #[test]
  fn test_d64_opt_empty() {
    let s = TestD64Struct { values: vec![] };
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, r#"{"values":[]}"#);

    let deserialized: TestD64Struct = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, s);
  }

  #[test]
  fn test_d64_opt_normal() {
    let s = TestD64Struct {
      values: vec![
        D64::from_f64(1.23, 2),
        D64::from_f64(2.34, 2),
        D64::from_f64(1.23, 2),
      ],
    };
    let json = serde_json::to_string(&s).unwrap();
    assert_eq!(json, r#"{"values":[2,123,111,-111]}"#);

    let deserialized: TestD64Struct = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, s);
  }
}
