use std::any::Any;
use std::any::TypeId;
use std::fmt::Display;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::TableError;

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
      _ => false,
    }
  }
}

// impl ToString for DataType {
//   fn to_string(&self) -> String {
//     match self {
//       DataType::Null => "Null".to_string(),
//       DataType::String => "String".to_string(),
//       DataType::Bytes => "Bytes".to_string(),
//       DataType::Int32 => "Int32".to_string(),
//       DataType::UInt32 => "UInt32".to_string(),
//       DataType::Int64 => "Int64".to_string(),
//       DataType::UInt64 => "UInt64".to_string(),
//       DataType::Float32 => "Float32".to_string(),
//       DataType::Float64 => "Float64".to_string(),
//       DataType::Decimal64 => "Decimal64".to_string(),
//       DataType::Decimal128 => "Decimal128".to_string(),
//       DataType::Bool => "Bool".to_string(),
//     }
//   }
// }

impl Display for DataType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let type_str = match self {
      DataType::Null => "Null",
      DataType::String => "String",
      DataType::Bytes => "Bytes",
      DataType::Int32 => "Int32",
      DataType::UInt32 => "UInt32",
      DataType::Int64 => "Int64",
      DataType::UInt64 => "UInt64",
      DataType::Float32 => "Float32",
      DataType::Float64 => "Float64",
      DataType::Decimal64 => "Decimal64",
      DataType::Decimal128 => "Decimal128",
      DataType::Bool => "Bool",
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
      "int32" => Ok(DataType::Int32),
      "uint32" => Ok(DataType::UInt32),
      "int64" => Ok(DataType::Int64),
      "uint64" => Ok(DataType::UInt64),
      "float32" => Ok(DataType::Float32),
      "float64" => Ok(DataType::Float64),
      "decimal64" => Ok(DataType::Decimal64),
      "decimal128" => Ok(DataType::Decimal128),
      "bool" => Ok(DataType::Bool),
      _ => Err(TableError::UnknownDataType(s.to_string())),
    }
  }
}
