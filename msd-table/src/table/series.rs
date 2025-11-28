use std::any::Any;

use serde::{Deserialize, Serialize};

use crate::{D64, D128, DataType, TableError, Variant, VariantMutRef, VariantRef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Series {
  Null,
  String(Vec<String>),
  Bytes(Vec<Vec<u8>>),
  Int32(Vec<i32>),
  UInt32(Vec<u32>),
  Int64(Vec<i64>),
  UInt64(Vec<u64>),
  Float32(Vec<f32>),
  Float64(Vec<f64>),
  Decimal64(Vec<D64>),
  Decimal128(Vec<D128>),
  Bool(Vec<bool>),
}

impl Series {
  pub fn new(data_type: DataType, rows: usize) -> Self {
    match data_type {
      DataType::Null => Series::Null,
      DataType::String => Series::String(vec![String::new(); rows]),
      DataType::Bytes => Series::Bytes(vec![Vec::new(); rows]),
      DataType::Int32 => Series::Int32(vec![0; rows]),
      DataType::UInt32 => Series::UInt32(vec![0; rows]),
      DataType::Int64 => Series::Int64(vec![0; rows]),
      DataType::UInt64 => Series::UInt64(vec![0; rows]),
      DataType::Float32 => Series::Float32(vec![0.0; rows]),
      DataType::Float64 => Series::Float64(vec![0.0; rows]),
      DataType::Decimal64 => Series::Decimal64(vec![D64::default(); rows]),
      DataType::Decimal128 => Series::Decimal128(vec![D128::default(); rows]),
      DataType::Bool => Series::Bool(vec![false; rows]),
    }
  }

  pub fn data_type(&self) -> DataType {
    match self {
      Series::Null => DataType::Null,
      Series::String(_) => DataType::String,
      Series::Bytes(_) => DataType::Bytes,
      Series::Int32(_) => DataType::Int32,
      Series::UInt32(_) => DataType::UInt32,
      Series::Int64(_) => DataType::Int64,
      Series::UInt64(_) => DataType::UInt64,
      Series::Float32(_) => DataType::Float32,
      Series::Float64(_) => DataType::Float64,
      Series::Decimal64(_) => DataType::Decimal64,
      Series::Decimal128(_) => DataType::Decimal128,
      Series::Bool(_) => DataType::Bool,
    }
  }

  getter!(Series, get_string, String, Vec<String>);
  getter!(Series, get_bytes, Bytes, Vec<Vec<u8>>);
  getter!(Series, get_int32, Int32, Vec<i32>);
  getter!(Series, get_uint32, UInt32, Vec<u32>);
  getter!(Series, get_int64, Int64, Vec<i64>);
  getter!(Series, get_uint64, UInt64, Vec<u64>);
  getter!(Series, get_float32, Float32, Vec<f32>);
  getter!(Series, get_float64, Float64, Vec<f64>);
  getter!(Series, get_decimal64, Decimal64, Vec<D64>);
  getter!(Series, get_decimal128, Decimal128, Vec<D128>);
  getter!(Series, get_bool, Bool, Vec<bool>);

  getter_mut!(Series, get_mut_string, String, Vec<String>);
  getter_mut!(Series, get_mut_bytes, Bytes, Vec<Vec<u8>>);
  getter_mut!(Series, get_mut_int32, Int32, Vec<i32>);
  getter_mut!(Series, get_mut_uint32, UInt32, Vec<u32>);
  getter_mut!(Series, get_mut_int64, Int64, Vec<i64>);
  getter_mut!(Series, get_mut_uint64, UInt64, Vec<u64>);
  getter_mut!(Series, get_mut_float32, Float32, Vec<f32>);
  getter_mut!(Series, get_mut_float64, Float64, Vec<f64>);
  getter_mut!(Series, get_mut_decimal64, Decimal64, Vec<D64>);
  getter_mut!(Series, get_mut_decimal128, Decimal128, Vec<D128>);
  getter_mut!(Series, get_mut_bool, Bool, Vec<bool>);

  pub fn is_type<T: Any>(&self) -> bool {
    self.data_type().is_type::<T>()
  }

  pub fn len(&self) -> usize {
    match self {
      Series::Null => 0,
      Series::String(v) => v.len(),
      Series::Bytes(v) => v.len(),
      Series::Int32(v) => v.len(),
      Series::UInt32(v) => v.len(),
      Series::Int64(v) => v.len(),
      Series::UInt64(v) => v.len(),
      Series::Float32(v) => v.len(),
      Series::Float64(v) => v.len(),
      Series::Decimal64(v) => v.len(),
      Series::Decimal128(v) => v.len(),
      Series::Bool(v) => v.len(),
    }
  }

  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  pub fn get(&self, index: usize) -> Option<VariantRef<'_>> {
    match self {
      Series::Null => None,
      Series::String(v) => v.get(index).map(|s| VariantRef::String(s)),
      Series::Bytes(v) => v.get(index).map(|b| VariantRef::Bytes(b)),
      Series::Int32(v) => v.get(index).map(|i| VariantRef::Int32(i)),
      Series::UInt32(v) => v.get(index).map(|i| VariantRef::UInt32(i)),
      Series::Int64(v) => v.get(index).map(|i| VariantRef::Int64(i)),
      Series::UInt64(v) => v.get(index).map(|i| VariantRef::UInt64(i)),
      Series::Float32(v) => v.get(index).map(|f| VariantRef::Float32(f)),
      Series::Float64(v) => v.get(index).map(|f| VariantRef::Float64(f)),
      Series::Decimal64(v) => v.get(index).map(|d| VariantRef::Decimal64(d)),
      Series::Decimal128(v) => v.get(index).map(|d| VariantRef::Decimal128(d)),
      Series::Bool(v) => v.get(index).map(|b| VariantRef::Bool(b)),
    }
  }

  pub unsafe fn get_unchecked(&self, index: usize) -> VariantRef<'_> {
    unsafe {
      match self {
        Series::Null => VariantRef::Null,
        Series::String(v) => VariantRef::String(v.get_unchecked(index)),
        Series::Bytes(v) => VariantRef::Bytes(v.get_unchecked(index)),
        Series::Int32(v) => VariantRef::Int32(v.get_unchecked(index)),
        Series::UInt32(v) => VariantRef::UInt32(v.get_unchecked(index)),
        Series::Int64(v) => VariantRef::Int64(v.get_unchecked(index)),
        Series::UInt64(v) => VariantRef::UInt64(v.get_unchecked(index)),
        Series::Float32(v) => VariantRef::Float32(v.get_unchecked(index)),
        Series::Float64(v) => VariantRef::Float64(v.get_unchecked(index)),
        Series::Decimal64(v) => VariantRef::Decimal64(v.get_unchecked(index)),
        Series::Decimal128(v) => VariantRef::Decimal128(v.get_unchecked(index)),
        Series::Bool(v) => VariantRef::Bool(v.get_unchecked(index)),
      }
    }
  }

  pub fn get_mut(&mut self, index: usize) -> Option<VariantMutRef<'_>> {
    if index >= self.len() {
      return None;
    }
    match self {
      Series::Null => None,
      Series::String(v) => v.get_mut(index).map(|s| VariantMutRef::String(s)),
      Series::Bytes(v) => v.get_mut(index).map(|b| VariantMutRef::Bytes(b)),
      Series::Int32(v) => v.get_mut(index).map(|i| VariantMutRef::Int32(i)),
      Series::UInt32(v) => v.get_mut(index).map(|i| VariantMutRef::UInt32(i)),
      Series::Int64(v) => v.get_mut(index).map(|i| VariantMutRef::Int64(i)),
      Series::UInt64(v) => v.get_mut(index).map(|i| VariantMutRef::UInt64(i)),
      Series::Float32(v) => v.get_mut(index).map(|f| VariantMutRef::Float32(f)),
      Series::Float64(v) => v.get_mut(index).map(|f| VariantMutRef::Float64(f)),
      Series::Decimal64(v) => v.get_mut(index).map(|d| VariantMutRef::Decimal64(d)),
      Series::Decimal128(v) => v.get_mut(index).map(|d| VariantMutRef::Decimal128(d)),
      Series::Bool(v) => v.get_mut(index).map(|b| VariantMutRef::Bool(b)),
    }
  }

  pub unsafe fn get_mut_unchecked(&mut self, index: usize) -> VariantMutRef<'_> {
    unsafe {
      match self {
        Series::Null => VariantMutRef::Null,
        Series::String(v) => VariantMutRef::String(v.get_unchecked_mut(index)),
        Series::Bytes(v) => VariantMutRef::Bytes(v.get_unchecked_mut(index)),
        Series::Int32(v) => VariantMutRef::Int32(v.get_unchecked_mut(index)),
        Series::UInt32(v) => VariantMutRef::UInt32(v.get_unchecked_mut(index)),
        Series::Int64(v) => VariantMutRef::Int64(v.get_unchecked_mut(index)),
        Series::UInt64(v) => VariantMutRef::UInt64(v.get_unchecked_mut(index)),
        Series::Float32(v) => VariantMutRef::Float32(v.get_unchecked_mut(index)),
        Series::Float64(v) => VariantMutRef::Float64(v.get_unchecked_mut(index)),
        Series::Decimal64(v) => VariantMutRef::Decimal64(v.get_unchecked_mut(index)),
        Series::Decimal128(v) => VariantMutRef::Decimal128(v.get_unchecked_mut(index)),
        Series::Bool(v) => VariantMutRef::Bool(v.get_unchecked_mut(index)),
      }
    }
  }

  pub fn push(&mut self, value: Variant) -> Result<(), TableError> {
    match (self, value) {
      (Series::String(v), Variant::String(s)) => {
        v.push(s);
        Ok(())
      }
      (Series::Bytes(v), Variant::Bytes(b)) => {
        v.push(b);
        Ok(())
      }
      (Series::Int32(v), Variant::Int32(i)) => {
        v.push(i);
        Ok(())
      }
      (Series::UInt32(v), Variant::UInt32(i)) => {
        v.push(i);
        Ok(())
      }
      (Series::Int64(v), Variant::Int64(i)) => {
        v.push(i);
        Ok(())
      }
      (Series::UInt64(v), Variant::UInt64(i)) => {
        v.push(i);
        Ok(())
      }
      (Series::Float32(v), Variant::Float32(f)) => {
        v.push(f);
        Ok(())
      }
      (Series::Float64(v), Variant::Float64(f)) => {
        v.push(f);
        Ok(())
      }
      (Series::Decimal64(v), Variant::Decimal64(d)) => {
        v.push(d);
        Ok(())
      }
      (Series::Decimal128(v), Variant::Decimal128(d)) => {
        v.push(d);
        Ok(())
      }
      (Series::Bool(v), Variant::Bool(b)) => {
        v.push(b);
        Ok(())
      }
      (a, b) => Err(TableError::TypeMismatch(a.data_type(), b.data_type())),
    }
  }
}

impl From<Vec<String>> for Series {
  fn from(v: Vec<String>) -> Self {
    Series::String(v)
  }
}

impl From<String> for Series {
  fn from(v: String) -> Self {
    Series::String(vec![v])
  }
}

impl From<&[&str]> for Series {
  fn from(v: &[&str]) -> Self {
    Series::String(v.iter().map(|s| s.to_string()).collect())
  }
}

impl From<&str> for Series {
  fn from(v: &str) -> Self {
    Series::String(vec![v.to_string()])
  }
}

impl From<Vec<&str>> for Series {
  fn from(v: Vec<&str>) -> Self {
    Series::String(v.into_iter().map(|s| s.to_string()).collect())
  }
}

impl From<Vec<Vec<u8>>> for Series {
  fn from(v: Vec<Vec<u8>>) -> Self {
    Series::Bytes(v)
  }
}

impl From<Vec<u8>> for Series {
  fn from(v: Vec<u8>) -> Self {
    Series::Bytes(vec![v])
  }
}

impl From<&[&[u8]]> for Series {
  fn from(v: &[&[u8]]) -> Self {
    Series::Bytes(v.iter().map(|b| b.to_vec()).collect())
  }
}

impl From<&[u8]> for Series {
  fn from(v: &[u8]) -> Self {
    Series::Bytes(vec![v.to_vec()])
  }
}

impl From<Vec<&[u8]>> for Series {
  fn from(v: Vec<&[u8]>) -> Self {
    Series::Bytes(v.into_iter().map(|b| b.to_vec()).collect())
  }
}

impl From<Vec<i32>> for Series {
  fn from(v: Vec<i32>) -> Self {
    Series::Int32(v)
  }
}

impl From<&[i32]> for Series {
  fn from(v: &[i32]) -> Self {
    Series::Int32(v.to_vec())
  }
}

impl From<i32> for Series {
  fn from(v: i32) -> Self {
    Series::Int32(vec![v])
  }
}
impl From<Vec<u32>> for Series {
  fn from(v: Vec<u32>) -> Self {
    Series::UInt32(v)
  }
}

impl From<&[u32]> for Series {
  fn from(v: &[u32]) -> Self {
    Series::UInt32(v.to_vec())
  }
}

impl From<u32> for Series {
  fn from(v: u32) -> Self {
    Series::UInt32(vec![v])
  }
}

impl From<Vec<i64>> for Series {
  fn from(v: Vec<i64>) -> Self {
    Series::Int64(v)
  }
}

impl From<&[i64]> for Series {
  fn from(v: &[i64]) -> Self {
    Series::Int64(v.to_vec())
  }
}

impl From<i64> for Series {
  fn from(v: i64) -> Self {
    Series::Int64(vec![v])
  }
}

impl From<Vec<u64>> for Series {
  fn from(v: Vec<u64>) -> Self {
    Series::UInt64(v)
  }
}

impl From<&[u64]> for Series {
  fn from(v: &[u64]) -> Self {
    Series::UInt64(v.to_vec())
  }
}

impl From<u64> for Series {
  fn from(v: u64) -> Self {
    Series::UInt64(vec![v])
  }
}

impl From<Vec<f32>> for Series {
  fn from(v: Vec<f32>) -> Self {
    Series::Float32(v)
  }
}

impl From<&[f32]> for Series {
  fn from(v: &[f32]) -> Self {
    Series::Float32(v.to_vec())
  }
}

impl From<f32> for Series {
  fn from(v: f32) -> Self {
    Series::Float32(vec![v])
  }
}

impl From<Vec<f64>> for Series {
  fn from(v: Vec<f64>) -> Self {
    Series::Float64(v)
  }
}

impl From<&[f64]> for Series {
  fn from(v: &[f64]) -> Self {
    Series::Float64(v.to_vec())
  }
}

impl From<f64> for Series {
  fn from(v: f64) -> Self {
    Series::Float64(vec![v])
  }
}

impl From<Vec<D64>> for Series {
  fn from(v: Vec<D64>) -> Self {
    Series::Decimal64(v)
  }
}

impl From<&[D64]> for Series {
  fn from(v: &[D64]) -> Self {
    Series::Decimal64(v.to_vec())
  }
}

impl From<D64> for Series {
  fn from(v: D64) -> Self {
    Series::Decimal64(vec![v])
  }
}

impl From<Vec<D128>> for Series {
  fn from(v: Vec<D128>) -> Self {
    Series::Decimal128(v)
  }
}

impl From<&[D128]> for Series {
  fn from(v: &[D128]) -> Self {
    Series::Decimal128(v.to_vec())
  }
}

impl From<D128> for Series {
  fn from(v: D128) -> Self {
    Series::Decimal128(vec![v])
  }
}

impl From<Vec<bool>> for Series {
  fn from(v: Vec<bool>) -> Self {
    Series::Bool(v)
  }
}

impl From<&[bool]> for Series {
  fn from(v: &[bool]) -> Self {
    Series::Bool(v.to_vec())
  }
}

impl From<bool> for Series {
  fn from(v: bool) -> Self {
    Series::Bool(vec![v])
  }
}

impl From<Vec<Variant>> for Series {
  fn from(a: Vec<Variant>) -> Self {
    if a.is_empty() {
      Series::Null
    } else {
      let dt = a[0].data_type();
      match dt {
        DataType::Null => Series::Null,
        DataType::String => Series::String(
          a.iter()
            .filter_map(|v| v.get_string())
            .map(|s| s.to_string())
            .collect(),
        ),
        DataType::Bytes => Series::Bytes(
          a.iter()
            .filter_map(|v| v.get_bytes())
            .map(|b| b.to_vec())
            .collect(),
        ),
        DataType::Int32 => {
          Series::Int32(a.iter().filter_map(|v| v.get_i32()).map(|i| *i).collect())
        }
        DataType::UInt32 => {
          Series::UInt32(a.iter().filter_map(|v| v.get_u32()).map(|i| *i).collect())
        }
        DataType::Int64 => {
          Series::Int64(a.iter().filter_map(|v| v.get_i64()).map(|i| *i).collect())
        }
        DataType::UInt64 => {
          Series::UInt64(a.iter().filter_map(|v| v.get_u64()).map(|i| *i).collect())
        }
        DataType::Float32 => {
          Series::Float32(a.iter().filter_map(|v| v.get_f32()).map(|i| *i).collect())
        }
        DataType::Float64 => {
          Series::Float64(a.iter().filter_map(|v| v.get_f64()).map(|i| *i).collect())
        }
        DataType::Decimal64 => {
          Series::Decimal64(a.iter().filter_map(|v| v.get_d64()).map(|i| *i).collect())
        }
        DataType::Decimal128 => {
          Series::Decimal128(a.iter().filter_map(|v| v.get_d128()).map(|i| *i).collect())
        }
        DataType::Bool => Series::Bool(a.iter().filter_map(|v| v.get_bool()).map(|i| *i).collect()),
      }
    }
  }
}
