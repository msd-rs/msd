use std::{
  fmt::{Debug, Display},
  hash::Hash,
  str::FromStr,
};

mod cast;
mod ops;

use serde::{Deserialize, Serialize};

use crate::{D64, D128, DataType, TableError, date::parse_datetime, to_datetime_str};

#[derive(Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum Variant {
  Null,             // 0
  DateTime(i64),    // 1
  Int64(i64),       // 2
  Float64(f64),     // 3
  Decimal64(D64),   // 4
  String(String),   // 5
  Bool(bool),       // 6
  Int32(i32),       // 7
  UInt32(u32),      // 8
  UInt64(u64),      // 9
  Float32(f32),     // 10
  Bytes(Vec<u8>),   // 11
  Decimal128(D128), // 12
}

impl Debug for Variant {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Variant::Null => write!(f, "Null"),
      Variant::Int32(v) => write!(f, "Int32({})", v),
      Variant::UInt32(v) => write!(f, "UInt32({})", v),
      Variant::Int64(v) => write!(f, "Int64({})", v),
      Variant::UInt64(v) => write!(f, "UInt64({})", v),
      Variant::Float32(v) => write!(f, "Float32({})", v),
      Variant::Float64(v) => write!(f, "Float64({})", v),
      Variant::String(v) => write!(f, "String({})", v),
      Variant::Bytes(v) => write!(f, "Bytes({:?})", v),
      Variant::Bool(v) => write!(f, "Bool({})", v),
      Variant::Decimal64(v) => write!(f, "Decimal64({})", v),
      Variant::Decimal128(v) => write!(f, "Decimal128({})", v),
      Variant::DateTime(v) => write!(f, "DateTime({})", v),
    }
  }
}

impl Eq for Variant {}

impl Hash for Variant {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    match self {
      Variant::Null => {
        0u8.hash(state);
      }
      Variant::Int32(v) => {
        1u8.hash(state);
        v.hash(state);
      }
      Variant::UInt32(v) => {
        2u8.hash(state);
        v.hash(state);
      }
      Variant::Int64(v) => {
        3u8.hash(state);
        v.hash(state);
      }
      Variant::UInt64(v) => {
        4u8.hash(state);
        v.hash(state);
      }
      Variant::Float32(v) => {
        5u8.hash(state);
        v.to_bits().hash(state);
      }
      Variant::Float64(v) => {
        6u8.hash(state);
        v.to_bits().hash(state);
      }
      Variant::String(v) => {
        7u8.hash(state);
        v.hash(state);
      }
      Variant::Bytes(v) => {
        8u8.hash(state);
        v.hash(state);
      }
      Variant::Bool(v) => {
        9u8.hash(state);
        v.hash(state);
      }
      Variant::Decimal64(v) => {
        10u8.hash(state);
        v.hash(state);
      }
      Variant::Decimal128(v) => {
        11u8.hash(state);
        v.hash(state);
      }
      Variant::DateTime(v) => {
        12u8.hash(state);
        v.hash(state);
      }
    }
  }
}

impl Variant {
  pub fn is_null(&self) -> bool {
    matches!(self, Variant::Null)
  }

  pub fn from_str(s: &str, dtype: DataType) -> Result<Self, TableError> {
    match dtype {
      DataType::Null => Ok(Variant::Null),
      DataType::Int32 => s
        .parse::<i32>()
        .map(Variant::Int32)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::UInt32 => s
        .parse::<u32>()
        .map(Variant::UInt32)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::Int64 => s
        .parse::<i64>()
        .map(Variant::Int64)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::UInt64 => s
        .parse::<u64>()
        .map(Variant::UInt64)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::Float32 => s
        .parse::<f32>()
        .map(Variant::Float32)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::Float64 => s
        .parse::<f64>()
        .map(Variant::Float64)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::String => Ok(Variant::String(s.to_string())),
      DataType::Bytes => Ok(Variant::Bytes(s.as_bytes().to_vec())),
      DataType::Bool => s
        .parse::<bool>()
        .map(Variant::Bool)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::Decimal64 => D64::from_str(s)
        .map(Variant::Decimal64)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::Decimal128 => D128::from_str(s)
        .map(Variant::Decimal128)
        .map_err(|_| TableError::UnknownDataType(s.to_string())),
      DataType::DateTime => crate::date::parse_datetime(s).map(Variant::DateTime),
    }
  }

  pub fn zero_value(&self) -> Self {
    match self {
      Variant::Null => Variant::Null,
      Variant::Int32(_) => Variant::Int32(0),
      Variant::UInt32(_) => Variant::UInt32(0),
      Variant::Int64(_) => Variant::Int64(0),
      Variant::UInt64(_) => Variant::UInt64(0),
      Variant::Float32(_) => Variant::Float32(0.0),
      Variant::Float64(_) => Variant::Float64(0.0),
      Variant::String(_) => Variant::String(String::new()),
      Variant::Bytes(_) => Variant::Bytes(Vec::new()),
      Variant::Bool(_) => Variant::Bool(false),
      Variant::Decimal64(_) => Variant::Decimal64(D64::default()),
      Variant::Decimal128(_) => Variant::Decimal128(D128::ZERO),
      Variant::DateTime(_) => Variant::DateTime(0),
    }
  }

  pub fn data_type(&self) -> DataType {
    match self {
      Variant::Null => DataType::Null,
      Variant::Int32(_) => DataType::Int32,
      Variant::UInt32(_) => DataType::UInt32,
      Variant::Int64(_) => DataType::Int64,
      Variant::UInt64(_) => DataType::UInt64,
      Variant::Float32(_) => DataType::Float32,
      Variant::Float64(_) => DataType::Float64,
      Variant::String(_) => DataType::String,
      Variant::Bytes(_) => DataType::Bytes,
      Variant::Bool(_) => DataType::Bool,
      Variant::Decimal64(_) => DataType::Decimal64,
      Variant::Decimal128(_) => DataType::Decimal128,
      Variant::DateTime(_) => DataType::DateTime,
    }
  }

  pub fn as_ref<'a>(&'a self) -> VariantRef<'a> {
    match self {
      Variant::Null => VariantRef::Null,
      Variant::Int32(v) => VariantRef::Int32(v),
      Variant::UInt32(v) => VariantRef::UInt32(v),
      Variant::Int64(v) => VariantRef::Int64(v),
      Variant::UInt64(v) => VariantRef::UInt64(v),
      Variant::Float32(v) => VariantRef::Float32(v),
      Variant::Float64(v) => VariantRef::Float64(v),
      Variant::String(v) => VariantRef::String(v),
      Variant::Bytes(v) => VariantRef::Bytes(v),
      Variant::Bool(v) => VariantRef::Bool(v),
      Variant::Decimal64(v) => VariantRef::Decimal64(v),
      Variant::Decimal128(v) => VariantRef::Decimal128(v),
      Variant::DateTime(v) => VariantRef::DateTime(v),
    }
  }

  pub fn as_mut_ref<'a>(&'a mut self) -> VariantMutRef<'a> {
    match self {
      Variant::Null => VariantMutRef::Null,
      Variant::Int32(v) => VariantMutRef::Int32(v),
      Variant::UInt32(v) => VariantMutRef::UInt32(v),
      Variant::Int64(v) => VariantMutRef::Int64(v),
      Variant::UInt64(v) => VariantMutRef::UInt64(v),
      Variant::Float32(v) => VariantMutRef::Float32(v),
      Variant::Float64(v) => VariantMutRef::Float64(v),
      Variant::String(v) => VariantMutRef::String(v),
      Variant::Bytes(v) => VariantMutRef::Bytes(v),
      Variant::Bool(v) => VariantMutRef::Bool(v),
      Variant::Decimal64(v) => VariantMutRef::Decimal64(v),
      Variant::Decimal128(v) => VariantMutRef::Decimal128(v),
      Variant::DateTime(v) => VariantMutRef::DateTime(v),
    }
  }

  getter!(Variant, get_str, String, str);
  getter!(Variant, get_string, String, String);
  getter!(Variant, get_slice, Bytes, [u8]);
  getter!(Variant, get_bytes, Bytes, Vec<u8>);
  getter!(Variant, get_i32, Int32, i32);
  getter!(Variant, get_u32, UInt32, u32);
  getter!(Variant, get_i64, Int64, i64);
  getter!(Variant, get_u64, UInt64, u64);
  getter!(Variant, get_f32, Float32, f32);
  getter!(Variant, get_f64, Float64, f64);
  getter!(Variant, get_d64, Decimal64, D64);
  getter!(Variant, get_d128, Decimal128, D128);
  getter!(Variant, get_bool, Bool, bool);
  getter!(Variant, get_datetime, DateTime, i64);

  getter_mut!(Variant, get_mut_string, String, String);
  getter_mut!(Variant, get_mut_bytes, Bytes, Vec<u8>);
  getter_mut!(Variant, get_mut_i32, Int32, i32);
  getter_mut!(Variant, get_mut_u32, UInt32, u32);
  getter_mut!(Variant, get_mut_i64, Int64, i64);
  getter_mut!(Variant, get_mut_u64, UInt64, u64);
  getter_mut!(Variant, get_mut_f32, Float32, f32);
  getter_mut!(Variant, get_mut_f64, Float64, f64);
  getter_mut!(Variant, get_mut_d64, Decimal64, D64);
  getter_mut!(Variant, get_mut_d128, Decimal128, D128);
  getter_mut!(Variant, get_mut_bool, Bool, bool);
  getter_mut!(Variant, get_mut_datetime, DateTime, i64);
}

impl Variant {
  pub fn cast(&self, target_type: DataType) -> Option<Variant> {
    if self.data_type().eq(&target_type) {
      return Some(self.clone());
    }
    match (self, target_type) {
      (Variant::DateTime(v), DataType::Int32) => Some(Variant::Int32(*v as i32)),
      (Variant::DateTime(v), DataType::UInt64) => Some(Variant::UInt64(*v as u64)),
      (Variant::DateTime(v), DataType::Float32) => {
        Some(Variant::Float32((*v as f64 / 1000.0) as f32))
      }
      (Variant::DateTime(v), DataType::Float64) => Some(Variant::Float64(*v as f64 / 1000.0)),
      (Variant::Int32(v), DataType::Int64) => Some(Variant::Int64(*v as i64)),
      (Variant::Int32(v), DataType::Float64) => Some(Variant::Float64(*v as f64)),
      (Variant::Int32(v), DataType::DateTime) => Some(Variant::DateTime(*v as i64)),

      (Variant::Int64(v), DataType::Int32) => Some(Variant::Int32(*v as i32)),
      (Variant::Int64(v), DataType::Float64) => Some(Variant::Float64(*v as f64)),
      (Variant::Int64(v), DataType::DateTime) => Some(Variant::DateTime(*v as i64)),

      (Variant::Float32(v), DataType::DateTime) => {
        Some(Variant::DateTime((*v as f64 * 1_000_000.0) as i64))
      }
      (Variant::Float32(v), DataType::Float64) => Some(Variant::Float64(*v as f64)),
      (Variant::Float32(v), DataType::Int32) => Some(Variant::Int32(*v as i32)),

      (Variant::Float64(v), DataType::Float32) => Some(Variant::Float32(*v as f32)),
      (Variant::Float64(v), DataType::DateTime) => {
        Some(Variant::DateTime((*v * 1_000_000.0) as i64))
      }
      (Variant::Float64(v), DataType::Decimal64) => Some(Variant::Decimal64(D64::from_f64(*v, 3))),

      (Variant::Decimal64(v), DataType::Float64) => Some(Variant::Float64(v.into())),

      (Variant::String(v), DataType::Int32) => v.parse().map(Variant::Int32).ok(),
      (Variant::String(v), DataType::Int64) => v.parse().map(Variant::Int64).ok(),
      (Variant::String(v), DataType::Float32) => v.parse().map(Variant::Float32).ok(),
      (Variant::String(v), DataType::Float64) => v.parse().map(Variant::Float64).ok(),
      (Variant::String(v), DataType::Decimal64) => v.parse().map(Variant::Decimal64).ok(),
      (Variant::String(v), DataType::Bytes) => Some(Variant::Bytes(v.as_bytes().to_vec())),
      (Variant::String(v), DataType::DateTime) => parse_datetime(v).map(Variant::DateTime).ok(),

      _ => None,
    }
  }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize)]
pub enum VariantRef<'a> {
  Null,
  String(&'a str),
  Bytes(&'a [u8]),
  Int32(&'a i32),
  UInt32(&'a u32),
  Int64(&'a i64),
  UInt64(&'a u64),
  Float32(&'a f32),
  Float64(&'a f64),
  Decimal64(&'a D64),
  Decimal128(&'a D128),
  Bool(&'a bool),
  DateTime(&'a i64),
}

impl From<VariantRef<'_>> for Variant {
  fn from(value: VariantRef) -> Self {
    match value {
      VariantRef::Null => Variant::Null,
      VariantRef::String(v) => Variant::String(v.to_string()),
      VariantRef::Bytes(v) => Variant::Bytes(v.to_vec()),
      VariantRef::Int32(v) => Variant::Int32(*v),
      VariantRef::UInt32(v) => Variant::UInt32(*v),
      VariantRef::Int64(v) => Variant::Int64(*v),
      VariantRef::UInt64(v) => Variant::UInt64(*v),
      VariantRef::Float32(v) => Variant::Float32(*v),
      VariantRef::Float64(v) => Variant::Float64(*v),
      VariantRef::Decimal64(v) => Variant::Decimal64(*v),
      VariantRef::Decimal128(v) => Variant::Decimal128(*v),
      VariantRef::Bool(v) => Variant::Bool(*v),
      VariantRef::DateTime(v) => Variant::DateTime(*v),
    }
  }
}

impl VariantRef<'_> {
  pub fn data_type(&self) -> DataType {
    match self {
      VariantRef::Null => DataType::Null,
      VariantRef::String(_) => DataType::String,
      VariantRef::Bytes(_) => DataType::Bytes,
      VariantRef::Int32(_) => DataType::Int32,
      VariantRef::UInt32(_) => DataType::UInt32,
      VariantRef::Int64(_) => DataType::Int64,
      VariantRef::UInt64(_) => DataType::UInt64,
      VariantRef::Float32(_) => DataType::Float32,
      VariantRef::Float64(_) => DataType::Float64,
      VariantRef::Decimal64(_) => DataType::Decimal64,
      VariantRef::Decimal128(_) => DataType::Decimal128,
      VariantRef::Bool(_) => DataType::Bool,
      VariantRef::DateTime(_) => DataType::DateTime,
    }
  }

  pub fn to_variant(&self) -> Variant {
    Variant::from(self.clone())
  }

  getter!(VariantRef, get_str, String, str);
  getter!(VariantRef, get_bytes, Bytes, [u8]);
  getter!(VariantRef, get_i32, Int32, i32);
  getter!(VariantRef, get_u32, UInt32, u32);
  getter!(VariantRef, get_i64, Int64, i64);
  getter!(VariantRef, get_u64, UInt64, u64);
  getter!(VariantRef, get_f32, Float32, f32);
  getter!(VariantRef, get_f64, Float64, f64);
  getter!(VariantRef, get_d64, Decimal64, D64);
  getter!(VariantRef, get_d128, Decimal128, D128);
  getter!(VariantRef, get_bool, Bool, bool);
  getter!(VariantRef, get_datetime, DateTime, i64);
}

#[derive(Debug)]
pub enum VariantMutRef<'a> {
  Null,
  String(&'a mut String),
  Bytes(&'a mut Vec<u8>),
  Int32(&'a mut i32),
  UInt32(&'a mut u32),
  Int64(&'a mut i64),
  UInt64(&'a mut u64),
  Float32(&'a mut f32),
  Float64(&'a mut f64),
  Decimal64(&'a mut D64),
  Decimal128(&'a mut D128),
  Bool(&'a mut bool),
  DateTime(&'a mut i64),
}

impl From<&VariantMutRef<'_>> for Variant {
  fn from(value: &VariantMutRef) -> Self {
    match value {
      VariantMutRef::Null => Variant::Null,
      VariantMutRef::String(v) => Variant::String(v.to_string()),
      VariantMutRef::Bytes(v) => Variant::Bytes(v.to_vec()),
      VariantMutRef::Int32(v) => Variant::Int32(**v),
      VariantMutRef::UInt32(v) => Variant::UInt32(**v),
      VariantMutRef::Int64(v) => Variant::Int64(**v),
      VariantMutRef::UInt64(v) => Variant::UInt64(**v),
      VariantMutRef::Float32(v) => Variant::Float32(**v),
      VariantMutRef::Float64(v) => Variant::Float64(**v),
      VariantMutRef::Decimal64(v) => Variant::Decimal64(**v),
      VariantMutRef::Decimal128(v) => Variant::Decimal128(**v),
      VariantMutRef::Bool(v) => Variant::Bool(**v),
      VariantMutRef::DateTime(v) => Variant::DateTime(**v),
    }
  }
}

impl VariantMutRef<'_> {
  pub fn data_type(&self) -> DataType {
    match self {
      VariantMutRef::Null => DataType::Null,
      VariantMutRef::String(_) => DataType::String,
      VariantMutRef::Bytes(_) => DataType::Bytes,
      VariantMutRef::Int32(_) => DataType::Int32,
      VariantMutRef::UInt32(_) => DataType::UInt32,
      VariantMutRef::Int64(_) => DataType::Int64,
      VariantMutRef::UInt64(_) => DataType::UInt64,
      VariantMutRef::Float32(_) => DataType::Float32,
      VariantMutRef::Float64(_) => DataType::Float64,
      VariantMutRef::Decimal64(_) => DataType::Decimal64,
      VariantMutRef::Decimal128(_) => DataType::Decimal128,
      VariantMutRef::Bool(_) => DataType::Bool,
      VariantMutRef::DateTime(_) => DataType::DateTime,
    }
  }

  getter_mut!(VariantMutRef, get_string, String, String);
  getter_mut!(VariantMutRef, get_bytes, Bytes, Vec<u8>);
  getter_mut!(VariantMutRef, get_i32, Int32, i32);
  getter_mut!(VariantMutRef, get_u32, UInt32, u32);
  getter_mut!(VariantMutRef, get_i64, Int64, i64);
  getter_mut!(VariantMutRef, get_u64, UInt64, u64);
  getter_mut!(VariantMutRef, get_f32, Float32, f32);
  getter_mut!(VariantMutRef, get_f64, Float64, f64);
  getter_mut!(VariantMutRef, get_d64, Decimal64, D64);
  getter_mut!(VariantMutRef, get_d128, Decimal128, D128);
  getter_mut!(VariantMutRef, get_bool, Bool, bool);
  getter_mut!(VariantMutRef, get_datetime, DateTime, i64);

  pub fn to_variant(&self) -> Variant {
    Variant::from(self)
  }

  pub fn set(self, value: Variant) -> Result<(), TableError> {
    match (self, value) {
      (VariantMutRef::Int32(v), Variant::Int32(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::UInt32(v), Variant::UInt32(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::Int64(v), Variant::Int64(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::UInt64(v), Variant::UInt64(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::Float32(v), Variant::Float32(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::Float64(v), Variant::Float64(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::String(v), Variant::String(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::Bytes(v), Variant::Bytes(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::Bool(v), Variant::Bool(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::Decimal64(v), Variant::Decimal64(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::Decimal128(v), Variant::Decimal128(val)) => {
        *v = val;
        Ok(())
      }
      (VariantMutRef::DateTime(v), Variant::DateTime(val)) => {
        *v = val;
        Ok(())
      }
      (dst, src) => Err(TableError::TypeMismatch(dst.data_type(), src.data_type())),
    }
  }
}

impl Display for Variant {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Variant::Null => write!(f, "null"),
      Variant::Int32(v) => write!(f, "{}", v),
      Variant::UInt32(v) => write!(f, "{}", v),
      Variant::Int64(v) => write!(f, "{}", v),
      Variant::UInt64(v) => write!(f, "{}", v),
      Variant::Float32(v) => write!(f, "{}", v),
      Variant::Float64(v) => write!(f, "{}", v),
      Variant::String(v) => write!(f, "{}", v),
      Variant::Bytes(v) => write!(f, "{:?}", v),
      Variant::Bool(v) => write!(f, "{}", v),
      Variant::Decimal64(v) => write!(f, "{}", v),
      Variant::Decimal128(v) => write!(f, "{}", v),
      Variant::DateTime(v) => write!(f, "{}", v),
    }
  }
}

impl Display for VariantRef<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      VariantRef::Null => write!(f, "null"),
      VariantRef::Int32(v) => write!(f, "{}", v),
      VariantRef::UInt32(v) => write!(f, "{}", v),
      VariantRef::Int64(v) => write!(f, "{}", v),
      VariantRef::UInt64(v) => write!(f, "{}", v),
      VariantRef::Float32(v) => write!(f, "{}", v),
      VariantRef::Float64(v) => write!(f, "{}", v),
      VariantRef::String(v) => write!(f, "{}", v),
      VariantRef::Bytes(v) => write!(f, "{:?}", v),
      VariantRef::Bool(v) => write!(f, "{}", v),
      VariantRef::Decimal64(v) => write!(f, "{}", v),
      VariantRef::Decimal128(v) => write!(f, "{}", v),
      VariantRef::DateTime(v) => write!(f, "{}", to_datetime_str(**v)),
    }
  }
}

impl Display for VariantMutRef<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      VariantMutRef::Null => write!(f, "null"),
      VariantMutRef::Int32(v) => write!(f, "{}", v),
      VariantMutRef::UInt32(v) => write!(f, "{}", v),
      VariantMutRef::Int64(v) => write!(f, "{}", v),
      VariantMutRef::UInt64(v) => write!(f, "{}", v),
      VariantMutRef::Float32(v) => write!(f, "{}", v),
      VariantMutRef::Float64(v) => write!(f, "{}", v),
      VariantMutRef::String(v) => write!(f, "{}", v),
      VariantMutRef::Bytes(v) => write!(f, "{:?}", v),
      VariantMutRef::Bool(v) => write!(f, "{}", v),
      VariantMutRef::Decimal64(v) => write!(f, "{}", v),
      VariantMutRef::Decimal128(v) => write!(f, "{}", v),
      VariantMutRef::DateTime(v) => write!(f, "{}", v),
    }
  }
}
