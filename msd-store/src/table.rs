use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum MsdFieldKind {
  String,
  Bytes,
  Int32,
  UInt32,
  Int64,
  UInt64,
  Float32,
  Float64,
  Bool,
  Array(Box<MsdFieldKind>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsdTableField {
  pub name: String,
  pub kind: MsdFieldKind,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsdTable {
  pub name: String,
  pub fields: Vec<MsdTableField>,
}
