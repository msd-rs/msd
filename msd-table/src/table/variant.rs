use std::ops::{Add, Div, Mul, Sub};

use serde::{Deserialize, Serialize};

use crate::{D64, D128, DataType};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Variant {
  Null,
  Int32(i32),
  UInt32(u32),
  Int64(i64),
  UInt64(u64),
  Float32(f32),
  Float64(f64),
  String(String),
  Bytes(Vec<u8>),
  Bool(bool),
  Decimal64(D64),
  Decimal128(D128),
}

impl Variant {
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
}

macro_rules! into_variant {
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

into_variant!(Int32, i32);
into_variant!(UInt32, u32);
into_variant!(Int64, i64);
into_variant!(UInt64, u64);
into_variant!(Float32, f32);
into_variant!(Float64, f64);
into_variant!(String, String);
into_variant!(Bytes, Vec<u8>);
into_variant!(Bool, bool);
into_variant!(Decimal64, D64);
into_variant!(Decimal128, D128);

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

impl Variant {
  pub fn cast(&self, target_type: &DataType) -> Option<Variant> {
    if self.data_type().eq(target_type) {
      return Some(self.clone());
    }

    match (self, target_type) {
      (Variant::Int32(v), DataType::Int64) => Some(Variant::Int64(*v as i64)),
      (Variant::Int32(v), DataType::Float64) => Some(Variant::Float64(*v as f64)),

      (Variant::Int64(v), DataType::Int32) => Some(Variant::Int32(*v as i32)),
      (Variant::Int64(v), DataType::Float64) => Some(Variant::Float64(*v as f64)),

      (Variant::Float32(v), DataType::Float64) => Some(Variant::Float64(*v as f64)),
      (Variant::Float32(v), DataType::Int32) => Some(Variant::Int32(*v as i32)),

      (Variant::Float64(v), DataType::Float32) => Some(Variant::Float32(*v as f32)),

      (Variant::String(v), DataType::Int32) => v.parse().map(Variant::Int32).ok(),
      (Variant::String(v), DataType::Int64) => v.parse().map(Variant::Int64).ok(),
      (Variant::String(v), DataType::Float32) => v.parse().map(Variant::Float32).ok(),
      (Variant::String(v), DataType::Float64) => v.parse().map(Variant::Float64).ok(),
      (Variant::String(v), DataType::Bytes) => Some(Variant::Bytes(v.clone().into_bytes())),

      _ => None,
    }
  }

  pub fn cast_to(self, target_type: &DataType) -> Option<Variant> {
    if self.data_type().eq(target_type) {
      return Some(self);
    }

    match (self, target_type) {
      (Variant::Int32(v), DataType::Int64) => Some(Variant::Int64(v as i64)),
      (Variant::Int32(v), DataType::Float64) => Some(Variant::Float64(v as f64)),

      (Variant::Int64(v), DataType::Int32) => Some(Variant::Int32(v as i32)),
      (Variant::Int64(v), DataType::Float64) => Some(Variant::Float64(v as f64)),

      (Variant::Float32(v), DataType::Float64) => Some(Variant::Float64(v as f64)),
      (Variant::Float32(v), DataType::Int32) => Some(Variant::Int32(v as i32)),

      (Variant::Float64(v), DataType::Float32) => Some(Variant::Float32(v as f32)),

      (Variant::String(v), DataType::Int32) => v.parse().map(Variant::Int32).ok(),
      (Variant::String(v), DataType::Int64) => v.parse().map(Variant::Int64).ok(),
      (Variant::String(v), DataType::Float32) => v.parse().map(Variant::Float32).ok(),
      (Variant::String(v), DataType::Float64) => v.parse().map(Variant::Float64).ok(),
      (Variant::String(v), DataType::Bytes) => Some(Variant::Bytes(v.into_bytes())),

      _ => None,
    }
  }
}

macro_rules! impl_operators {
  ($trait:ident, $method:ident, $op:tt) => {
     impl $trait for Variant {
          type Output = Self;
          fn $method(self, other: Self) -> Self::Output {
            if let Some(other) = other.cast(&self.data_type()) {
              match (&self, other) {
                  (Variant::Int32(a), Variant::Int32(b)) => Variant::Int32(a $op b),
                  (Variant::Int64(a), Variant::Int64(b)) => Variant::Int64(a $op b),
                  (Variant::Float32(a), Variant::Float32(b)) => Variant::Float32(a $op b),
                  (Variant::Float64(a), Variant::Float64(b)) => Variant::Float64(a $op b),
                  (Variant::Decimal64(a), Variant::Decimal64(b)) => Variant::Decimal64(*a $op b),
                  (Variant::Decimal128(a), Variant::Decimal128(b)) => Variant::Decimal128(a $op b),
                  _ => self
              }
            } else {
              self
            }
          }
      }
  };
  ($trait:ident, $method:ident, $op:tt, $str_only:ident) => {
     impl $trait for Variant {
          type Output = Self;
          fn $method(self, other: Self) -> Self::Output {
            if let Some(other) = other.cast(&self.data_type()) {
              match (&self, other) {
                  (Variant::Int32(a), Variant::Int32(b)) => Variant::Int32(a $op b),
                  (Variant::Int64(a), Variant::Int64(b)) => Variant::Int64(a $op b),
                  (Variant::Float32(a), Variant::Float32(b)) => Variant::Float32(a $op b),
                  (Variant::Float64(a), Variant::Float64(b)) => Variant::Float64(a $op b),
                  (Variant::Decimal64(a), Variant::Decimal64(b)) => Variant::Decimal64(*a $op b),
                  (Variant::Decimal128(a), Variant::Decimal128(b)) => Variant::Decimal128(a $op b),
                  (Variant::String(a), Variant::String(b)) => Variant::String(a.to_owned() $op b.as_str()),
                  _ => self
              }
            } else {
              self
            }
          }
      }
  };
}

impl_operators!(Add, add, +, true);
impl_operators!(Sub, sub, -);
impl_operators!(Mul, mul, *);
impl_operators!(Div, div, /);

#[derive(Debug, Clone)]
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
    }
  }

  getter!(VariantRef, get_string, String, str);
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
}

#[derive(Debug)]
pub enum VariantMutRef<'a> {
  Null,
  String(&'a mut str),
  Bytes(&'a mut [u8]),
  Int32(&'a mut i32),
  UInt32(&'a mut u32),
  Int64(&'a mut i64),
  UInt64(&'a mut u64),
  Float32(&'a mut f32),
  Float64(&'a mut f64),
  Decimal64(&'a mut D64),
  Decimal128(&'a mut D128),
  Bool(&'a mut bool),
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
    }
  }

  getter_mut!(VariantMutRef, get_string, String, str);
  getter_mut!(VariantMutRef, get_bytes, Bytes, [u8]);
  getter_mut!(VariantMutRef, get_i32, Int32, i32);
  getter_mut!(VariantMutRef, get_u32, UInt32, u32);
  getter_mut!(VariantMutRef, get_i64, Int64, i64);
  getter_mut!(VariantMutRef, get_u64, UInt64, u64);
  getter_mut!(VariantMutRef, get_f32, Float32, f32);
  getter_mut!(VariantMutRef, get_f64, Float64, f64);
  getter_mut!(VariantMutRef, get_d64, Decimal64, D64);
  getter_mut!(VariantMutRef, get_d128, Decimal128, D128);
  getter_mut!(VariantMutRef, get_bool, Bool, bool);
}
