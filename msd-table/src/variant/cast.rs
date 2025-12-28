// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use super::Variant;
use crate::{D64, D128, DataType, TableError, parse_datetime};

macro_rules! impl_variant_from {
  ($name:ident, $type:ty) => {
    impl From<$type> for Variant {
      fn from(value: $type) -> Self {
        Variant::$name(value)
      }
    }

    impl From<&$type> for Variant {
      fn from(value: &$type) -> Self {
        Variant::$name(value.clone())
      }
    }
  };
}

impl_variant_from!(Int32, i32);
impl_variant_from!(UInt32, u32);
impl_variant_from!(Int64, i64);
impl_variant_from!(UInt64, u64);
impl_variant_from!(Float32, f32);
impl_variant_from!(Float64, f64);
impl_variant_from!(String, String);
impl_variant_from!(Bytes, Vec<u8>);
impl_variant_from!(Bool, bool);
impl_variant_from!(Decimal64, D64);
impl_variant_from!(Decimal128, D128);

impl From<&str> for Variant {
  fn from(value: &str) -> Self {
    Variant::String(value.to_string())
  }
}

impl From<&[u8]> for Variant {
  fn from(value: &[u8]) -> Self {
    Variant::Bytes(value.to_vec())
  }
}

impl From<usize> for Variant {
  fn from(value: usize) -> Self {
    Variant::UInt64(value as u64)
  }
}

impl From<(i64, DataType)> for Variant {
  fn from(value: (i64, DataType)) -> Self {
    let val = Variant::Int64(value.0);
    val.cast(value.1).unwrap()
  }
}

impl TryFrom<(&str, DataType)> for Variant {
  type Error = TableError;

  fn try_from(value: (&str, DataType)) -> Result<Self, Self::Error> {
    match value.1 {
      DataType::String => Ok(Variant::String(value.0.to_string())),
      DataType::Bytes => Ok(Variant::Bytes(value.0.as_bytes().to_vec())),
      DataType::Null => Ok(Variant::Null),
      DataType::DateTime => parse_datetime(value.0).map(|v| Variant::DateTime(v)),
      DataType::Int64 => value
        .0
        .parse()
        .map(|v| Variant::Int64(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::Float64 => value
        .0
        .parse()
        .map(|v| Variant::Float64(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::Decimal64 => value
        .0
        .parse()
        .map(|v| Variant::Decimal64(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::Bool => value
        .0
        .parse()
        .map(|v| Variant::Bool(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::Int32 => value
        .0
        .parse()
        .map(|v| Variant::Int32(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::UInt32 => value
        .0
        .parse()
        .map(|v| Variant::UInt32(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::UInt64 => value
        .0
        .parse()
        .map(|v| Variant::UInt64(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::Float32 => value
        .0
        .parse()
        .map(|v| Variant::Float32(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
      DataType::Decimal128 => value
        .0
        .parse()
        .map(|v| Variant::Decimal128(v))
        .map_err(|_| TableError::VariantParseError(value.0.to_string(), value.1.to_string())),
    }
  }
}
