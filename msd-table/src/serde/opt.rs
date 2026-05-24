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

pub fn datetime_array_serialize<A, S>(value: A, serializer: S) -> Result<S::Ok, S::Error>
where
  A: AsRef<[i64]>,
  S: Serializer,
{
  use serde::ser::SerializeSeq;

  let value = value.as_ref();

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

pub fn datetime_array_bincode_encode<A, E>(
  value: A,
  encoder: &mut E,
) -> Result<(), bincode_next::error::EncodeError>
where
  A: AsRef<[i64]>,
  E: bincode_next::enc::Encoder,
{
  let value = value.as_ref();
  if value.is_empty() {
    encoder.encode_slice_len(0)?;
    return Ok(());
  }
  let gcd = value
    .iter()
    .take(10)
    .fold(value[0], |acc, &x| if acc == 0 { x } else { gcd(acc, x) });

  encoder.encode_slice_len(value.len() + 1)?;

  encoder.encode_i64(gcd)?;
  encoder.encode_i64(value[0] / gcd)?;
  for i in 1..value.len() {
    let diff = (value[i] - value[i - 1]) / gcd;
    encoder.encode_i64(diff)?;
  }

  Ok(())
}

pub fn datetime_array_bincode_decode<D>(
  decoder: &mut D,
) -> Result<Vec<i64>, bincode_next::error::DecodeError>
where
  D: bincode_next::de::Decoder,
{
  let len = decoder.decode_slice_len()?;
  if len == 0 {
    return Ok(Vec::new());
  } else if len == 1 {
    debug_assert!(
      false,
      "Invalid encoded datetime array: length is 1, expected at least 2 (gcd and first value)"
    );
    let _ = decoder.decode_i64()?;
    return Ok(Vec::new());
  }
  let gcd = decoder.decode_i64()?;
  let mut values = Vec::with_capacity(len - 1);
  let mut prev = decoder.decode_i64()?;
  values.push(prev * gcd);

  for _ in 1..(len - 1) {
    let diff = decoder.decode_i64()?;
    values.push((prev + diff) * gcd);
    prev += diff;
  }

  Ok(values)
}

// # D64 array optimize
// 1. get first normal value (not nan or inf) decimal use it's `dec_num` method
// 2. for each value, value[i] = (value[i].into::<i64> - value[i-1].into::<i64>()), first value is `value[0].into::<i64>()`
// 3. serialize as `dec_num, values...`

pub fn d64_array_serialize<A, S>(value: A, serializer: S) -> Result<S::Ok, S::Error>
where
  A: AsRef<[D64]>,
  S: Serializer,
{
  use serde::ser::SerializeSeq;

  let value = value.as_ref();

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

pub fn d64_array_bincode_encode<A, E>(
  value: A,
  encoder: &mut E,
) -> Result<(), bincode_next::error::EncodeError>
where
  A: AsRef<[D64]>,
  E: bincode_next::enc::Encoder,
{
  let value = value.as_ref();
  if value.is_empty() {
    encoder.encode_slice_len(0)?;
    return Ok(());
  }

  let dec_num = value
    .iter()
    .find(|v| !v.is_nan() && !v.is_inf())
    .map(|v| v.dec_num())
    .unwrap_or(0);

  encoder.encode_slice_len(value.len() + 1)?;

  encoder.encode_i64(dec_num as i64)?;

  let mut prev = 0;
  for i in 0..value.len() {
    let curr = i64::from(&value[i]);
    let diff = curr - prev;
    encoder.encode_i64(diff)?;
    prev = curr;
  }
  Ok(())
}

pub fn d64_array_bincode_decode<D>(
  decoder: &mut D,
) -> Result<Vec<D64>, bincode_next::error::DecodeError>
where
  D: bincode_next::de::Decoder,
{
  let len = decoder.decode_slice_len()?;
  if len == 0 {
    return Ok(Vec::new());
  } else if len == 1 {
    debug_assert!(
      false,
      "Invalid encoded D64 array: length is 1, expected at least 2 (dec_num and first value)"
    );
    let _ = decoder.decode_i64()?;
    return Ok(Vec::new());
  }

  let dec_num = decoder.decode_i64()? as usize;

  let mut result = Vec::with_capacity(len - 1);
  let mut prev = decoder.decode_i64()?;
  result.push(D64::from_i64(prev, dec_num));
  for _ in 1..len - 1 {
    let diff = decoder.decode_i64()?;
    prev += diff;
    result.push(D64::from_i64(prev, dec_num));
  }

  Ok(result)
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

  #[test]
  fn test_datetime_bincode() {
    let data = vec![100, 200, 300];
    let mut buffer = Vec::new();
    let mut encoder =
      bincode_next::enc::EncoderImpl::new(&mut buffer, bincode_next::config::standard());
    datetime_array_bincode_encode(&data, &mut encoder).unwrap();

    assert_eq!(buffer, vec![4, 200, 2, 2, 2]);

    let reader = bincode_next::de::read::SliceReader::new(&buffer);
    let mut decoder =
      bincode_next::de::DecoderImpl::new(reader, bincode_next::config::standard(), 0);
    let decoded = datetime_array_bincode_decode(&mut decoder).unwrap();
    assert_eq!(decoded, data);
  }

  #[test]
  fn test_d64_bincode() {
    let data = vec![
      D64::from_f64(1.23, 2),
      D64::from_f64(2.34, 2),
      D64::from_f64(1.23, 2),
    ];
    let mut buffer = Vec::new();
    let mut encoder =
      bincode_next::enc::EncoderImpl::new(&mut buffer, bincode_next::config::standard());
    d64_array_bincode_encode(&data, &mut encoder).unwrap();

    println!("Encoded D64 array: {:?}", buffer);

    let reader = bincode_next::de::read::SliceReader::new(&buffer);
    let mut decoder =
      bincode_next::de::DecoderImpl::new(reader, bincode_next::config::standard(), 0);
    let decoded = d64_array_bincode_decode(&mut decoder).unwrap();
    assert_eq!(decoded, data);
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
