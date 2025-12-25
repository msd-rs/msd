///! TableRef used for serializing Table from other languages avoid memory copy
use std::collections::HashMap;

use serde::Serialize;

use crate::{D64, D128, DataType, Variant};

#[derive(Debug, Clone, Serialize)]
pub enum SeriesRef<'a> {
  Null,                      // 0
  DateTime(&'a Vec<i64>),    // 1
  Int64(&'a Vec<i64>),       // 2
  Float64(&'a Vec<f64>),     // 3
  Decimal64(&'a Vec<D64>),   // 4
  String(&'a Vec<String>),   // 5
  Bool(&'a Vec<bool>),       // 6
  Int32(&'a Vec<i32>),       // 7
  UInt32(&'a Vec<u32>),      // 8
  UInt64(&'a Vec<u64>),      // 9
  Float32(&'a Vec<f32>),     // 10
  Bytes(&'a Vec<Vec<u8>>),   // 11
  Decimal128(&'a Vec<D128>), // 12
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldRef<'a> {
  pub name: &'a str,
  pub kind: DataType,
  pub metadata: Option<HashMap<String, Variant>>,
  pub data: SeriesRef<'a>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableRef<'a> {
  pub version: u32,
  pub columns: Vec<FieldRef<'a>>,
  pub metadata: Option<HashMap<String, Variant>>,
}

impl<'a> TableRef<'a> {
  pub fn new(columns: Vec<FieldRef<'a>>, metadata: Option<HashMap<String, Variant>>) -> Self {
    Self {
      version: super::table::TABLE_VERSION_1,
      columns,
      metadata,
    }
  }
}
