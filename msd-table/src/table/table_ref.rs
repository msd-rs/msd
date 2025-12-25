///! TableRef used for serializing Table from other languages avoid memory copy
use std::collections::HashMap;

use serde::Serialize;

use crate::{D64, D128, DataType, Variant};

#[derive(Debug, Clone, Serialize)]
pub enum SeriesRef<'a> {
  Null,                   // 0
  DateTime(&'a [i64]),    // 1
  Int64(&'a [i64]),       // 2
  Float64(&'a [f64]),     // 3
  Decimal64(&'a [D64]),   // 4
  String(&'a [String]),   // 5
  Bool(&'a [bool]),       // 6
  Int32(&'a [i32]),       // 7
  UInt32(&'a [u32]),      // 8
  UInt64(&'a [u64]),      // 9
  Float32(&'a [f32]),     // 10
  Bytes(&'a [Vec<u8>]),   // 11
  Decimal128(&'a [D128]), // 12
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
