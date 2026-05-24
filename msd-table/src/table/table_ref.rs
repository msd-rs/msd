// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

///! TableRef used for serializing Table from other languages avoid memory copy
use std::collections::HashMap;

use serde::Serialize;

use crate::serde::{d64_array_serialize, datetime_array_serialize};
use crate::{D64, DataType, Series, Variant};

#[derive(Debug, Clone, Serialize, bincode_next::Encode)]
pub enum SeriesRef<'a> {
  Null, // 0
  #[serde(serialize_with = "datetime_array_serialize")]
  DateTime(&'a [i64]), // 1
  Int64(&'a [i64]), // 2
  Float64(&'a [f64]), // 3
  #[serde(serialize_with = "d64_array_serialize")]
  Decimal64(&'a [D64]), // 4
  String(&'a [String]), // 5
  Bool(&'a [bool]), // 6
  Int32(&'a [i32]), // 7
  UInt32(&'a [u32]), // 8
  UInt64(&'a [u64]), // 9
  Float32(&'a [f32]), // 10
  Bytes(&'a [Vec<u8>]), // 11
        //Decimal128(&'a [D128]), // 12
}

impl<'a> From<&'a Series> for SeriesRef<'a> {
  fn from(series: &'a Series) -> Self {
    match series {
      Series::Null => SeriesRef::Null,
      Series::DateTime(items) => SeriesRef::DateTime(items),
      Series::Decimal64(items) => SeriesRef::Decimal64(items),
      Series::String(items) => SeriesRef::String(items),
      Series::Bool(items) => SeriesRef::Bool(items),
      Series::Int64(items) => SeriesRef::Int64(items),
      Series::Float64(items) => SeriesRef::Float64(items),
      Series::Int32(items) => SeriesRef::Int32(items),
      Series::UInt32(items) => SeriesRef::UInt32(items),
      Series::UInt64(items) => SeriesRef::UInt64(items),
      Series::Float32(items) => SeriesRef::Float32(items),
      Series::Bytes(items) => SeriesRef::Bytes(items),
      // Series::Decimal128(decimals) => SeriesRef::Decimal128(decimals),
    }
  }
}

#[derive(Debug, Clone, Serialize, bincode_next::Encode)]
pub struct FieldRef<'a> {
  pub name: &'a str,
  pub kind: DataType,
  pub metadata: Option<HashMap<String, Variant>>,
  pub data: SeriesRef<'a>,
}

#[derive(Debug, Clone, Serialize, bincode_next::Encode)]
pub struct TableRef<'a> {
  pub version: u32,
  pub columns: Vec<FieldRef<'a>>,
  pub metadata: Option<HashMap<String, Variant>>,
  pub is_kv: bool,
}

impl<'a> TableRef<'a> {
  pub fn new(
    columns: Vec<FieldRef<'a>>,
    metadata: Option<HashMap<String, Variant>>,
    is_kv: bool,
  ) -> Self {
    Self {
      version: super::table::TABLE_VERSION_1,
      columns,
      metadata,
      is_kv,
    }
  }
}
