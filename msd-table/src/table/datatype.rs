use std::any::Any;
use std::any::TypeId;
use std::fmt::Display;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::TableError;
use crate::Variant;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Copy)]
pub enum DataType {
  Null,
  String,
  Bytes,
  Int32,
  UInt32,
  Int64,
  UInt64,
  Float32,
  Float64,
  Decimal64,
  Decimal128,
  Bool,
  DateTime,
}

impl DataType {
  pub fn sizeof(&self) -> usize {
    match self {
      DataType::Null => 0,   // Null has no size
      DataType::String => 8, // Pointer size for String
      DataType::Bytes => 8,  // Pointer size for Bytes
      DataType::Int32 => 4,
      DataType::UInt32 => 4,
      DataType::Int64 => 8,
      DataType::UInt64 => 8,
      DataType::Float32 => 4,
      DataType::Float64 => 8,
      DataType::Decimal64 => 8,
      DataType::Decimal128 => 16,
      DataType::Bool => 1,
      DataType::DateTime => 8,
    }
  }

  pub fn is_type<T: Any>(&self) -> bool {
    match self {
      DataType::String if TypeId::of::<T>() == TypeId::of::<String>() => true,
      DataType::Bytes if TypeId::of::<T>() == TypeId::of::<Vec<u8>>() => true,
      DataType::Int32 if TypeId::of::<T>() == TypeId::of::<i32>() => true,
      DataType::UInt32 if TypeId::of::<T>() == TypeId::of::<u32>() => true,
      DataType::Int64 if TypeId::of::<T>() == TypeId::of::<i64>() => true,
      DataType::UInt64 if TypeId::of::<T>() == TypeId::of::<u64>() => true,
      DataType::Float32 if TypeId::of::<T>() == TypeId::of::<f32>() => true,
      DataType::Float64 if TypeId::of::<T>() == TypeId::of::<f64>() => true,
      DataType::Decimal64 if TypeId::of::<T>() == TypeId::of::<i64>() => true, // Assuming Decimal64 is represented as i64
      DataType::Decimal128 if TypeId::of::<T>() == TypeId::of::<i128>() => true, // Assuming Decimal128 is represented as i128
      DataType::Bool if TypeId::of::<T>() == TypeId::of::<bool>() => true,
      DataType::DateTime if TypeId::of::<T>() == TypeId::of::<u64>() => true,
      _ => false,
    }
  }
}

impl Display for DataType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let type_str = match self {
      DataType::Null => "null",
      DataType::String => "string",
      DataType::Bytes => "bytes",
      DataType::Int32 => "i32",
      DataType::UInt32 => "u32",
      DataType::Int64 => "i64",
      DataType::UInt64 => "u64",
      DataType::Float32 => "f32",
      DataType::Float64 => "f64",
      DataType::Decimal64 => "d64",
      DataType::Decimal128 => "d128",
      DataType::Bool => "bool",
      DataType::DateTime => "datetime",
    };
    write!(f, "{}", type_str)
  }
}

impl FromStr for DataType {
  type Err = TableError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.to_lowercase().as_str() {
      "null" => Ok(DataType::Null),
      "string" => Ok(DataType::String),
      "bytes" => Ok(DataType::Bytes),
      "i32" => Ok(DataType::Int32),
      "u32" => Ok(DataType::UInt32),
      "i64" => Ok(DataType::Int64),
      "u64" => Ok(DataType::UInt64),
      "f32" => Ok(DataType::Float32),
      "f64" => Ok(DataType::Float64),
      "d64" => Ok(DataType::Decimal64),
      "d128" => Ok(DataType::Decimal128),
      "bool" => Ok(DataType::Bool),
      "datetime" => Ok(DataType::DateTime),
      _ => Err(TableError::UnknownDataType(s.to_string())),
    }
  }
}
