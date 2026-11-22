// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::collections::HashMap;
use std::str::FromStr;

use msd_table::{
  D64, D64_NAN, DataType, Field, FieldRef, Series, SeriesRef, Table, TableRef, get_local_offset,
  pack_table_ref_frame,
};
use numpy::{
  PyArray1, PyArrayDescrMethods, PyArrayMethods, PyReadonlyArray1, PyUntypedArray,
  PyUntypedArrayMethods,
  datetime::{
    Datetime,
    units::{Microseconds, Milliseconds, Nanoseconds, Seconds},
  },
  dtype,
};
use pyo3::{
  exceptions::{PyTypeError, PyValueError},
  prelude::*,
  types::{PyBytes, PyDict, PyList, PyString},
};

pub(crate) fn table_to_py_list<'py>(py: Python<'py>, table: Table) -> Bound<'py, PyList> {
  let rows = table.row_count();
  let fields: Vec<Field> = table.into();

  PyList::new(
    py,
    fields
      .into_iter()
      .map(|field| (field.name, series_to_array(py, field.data, rows))),
  )
  .unwrap()
}

pub(crate) fn table_to_py_dict<'py>(py: Python<'py>, table: Table) -> Bound<'py, PyDict> {
  let dict = PyDict::new(py);

  let rows = table.row_count();
  let fields: Vec<Field> = table.into();

  fields.into_iter().for_each(|field| {
    let name = field.name;
    if !dict.contains(&name).unwrap_or(false) {
      let _ = dict.set_item(name, series_to_array(py, field.data, rows).into_any());
    }
  });

  dict
}

fn series_to_array<'py>(py: Python<'py>, series: Series, rows: usize) -> Bound<'py, PyAny> {
  match series {
    Series::Null => PyArray1::<bool>::zeros(py, [rows], true).into_any(),
    Series::DateTime(items) => {
      let offset = get_local_offset().whole_seconds() as i64 * 1_000_000;
      PyArray1::<Datetime<Microseconds>>::from_vec(
        py,
        items
          .into_iter()
          .map(|i| Datetime::from(i + offset))
          .collect(),
      )
      .into_any()
    }
    Series::Int64(items) => PyArray1::<i64>::from_vec(py, items).into_any(),
    Series::Float64(items) => PyArray1::<f64>::from_vec(py, items).into_any(),
    Series::Decimal64(d64s) => {
      PyArray1::<f64>::from_vec(py, d64s.into_iter().map(|d| d.into()).collect()).into_any()
    }
    Series::String(items) => PyArray1::<Py<PyAny>>::from_vec(
      py,
      items
        .into_iter()
        .map(|s| PyString::new(py, &s).into())
        .collect(),
    )
    .into_any(),
    Series::Bool(items) => PyArray1::<bool>::from_vec(py, items).into_any(),
    Series::Int32(items) => PyArray1::<i32>::from_vec(py, items).into_any(),
    Series::UInt32(items) => PyArray1::<u32>::from_slice(py, &items).into_any(),
    Series::UInt64(items) => PyArray1::<u64>::from_slice(py, &items).into_any(),
    Series::Float32(items) => PyArray1::<f32>::from_vec(py, items).into_any(),
    Series::Bytes(items) => PyArray1::<Py<PyAny>>::from_vec(
      py,
      items
        .into_iter()
        .map(|b| PyBytes::new(py, &b).into())
        .collect(),
    )
    .into_any(),
    // Series::Decimal128(decimals) => PyArray1::<f64>::from_vec(
    //   py,
    //   decimals
    //     .into_iter()
    //     .map(|d| d.to_string().as_str().parse().unwrap())
    //     .collect(),
    // )
    // .into_any(),
  }
}

#[derive(Debug)]
pub enum PyArrayTyped<'py> {
  Int64(PyReadonlyArray1<'py, i64>),
  UInt64(PyReadonlyArray1<'py, u64>),
  Float64(PyReadonlyArray1<'py, f64>),
  Int32(PyReadonlyArray1<'py, i32>),
  UInt32(PyReadonlyArray1<'py, u32>),
  Float32(PyReadonlyArray1<'py, f32>),
  Bool(PyReadonlyArray1<'py, bool>),
  DateTime(Vec<i64>),
  String(Vec<String>),
  Decimal64(Vec<D64>),
}

impl<'py> PyArrayTyped<'py> {
  pub fn kind(&self) -> DataType {
    match self {
      PyArrayTyped::Int64(_) => DataType::Int64,
      PyArrayTyped::UInt64(_) => DataType::UInt64,
      PyArrayTyped::Float64(_) => DataType::Float64,
      PyArrayTyped::Int32(_) => DataType::Int32,
      PyArrayTyped::UInt32(_) => DataType::UInt32,
      PyArrayTyped::Float32(_) => DataType::Float32,
      PyArrayTyped::Bool(_) => DataType::Bool,
      PyArrayTyped::DateTime(_) => DataType::DateTime,
      PyArrayTyped::String(_) => DataType::String,
      PyArrayTyped::Decimal64(_) => DataType::Decimal64,
    }
  }

  pub fn into_d64_series(&self, dec: usize) -> Option<Series> {
    match self {
      PyArrayTyped::Int64(arr) => Some(Series::Decimal64(
        arr
          .as_slice()
          .unwrap()
          .iter()
          .map(|v| D64::from_i64(*v, dec))
          .collect(),
      )),
      PyArrayTyped::Float64(arr) => Some(Series::Decimal64(
        arr
          .as_slice()
          .unwrap()
          .iter()
          .map(|v| D64::from_f64(*v, dec))
          .collect(),
      )),
      PyArrayTyped::Int32(arr) => Some(Series::Decimal64(
        arr
          .as_slice()
          .unwrap()
          .iter()
          .map(|v| D64::from_i64(*v as i64, dec))
          .collect(),
      )),
      PyArrayTyped::Float32(arr) => Some(Series::Decimal64(
        arr
          .as_slice()
          .unwrap()
          .iter()
          .map(|v| D64::from_f64(*v as f64, dec))
          .collect(),
      )),
      _ => None,
    }
  }
}

impl<'py, 'a> Into<SeriesRef<'a>> for &'a PyArrayTyped<'py> {
  fn into(self) -> SeriesRef<'a> {
    match self {
      PyArrayTyped::Int64(array) => SeriesRef::Int64(array.as_slice().unwrap()),
      PyArrayTyped::UInt64(array) => SeriesRef::UInt64(array.as_slice().unwrap()),
      PyArrayTyped::Float64(array) => SeriesRef::Float64(array.as_slice().unwrap()),
      PyArrayTyped::Int32(array) => SeriesRef::Int32(array.as_slice().unwrap()),
      PyArrayTyped::UInt32(array) => SeriesRef::UInt32(array.as_slice().unwrap()),
      PyArrayTyped::Float32(array) => SeriesRef::Float32(array.as_slice().unwrap()),
      PyArrayTyped::Bool(array) => SeriesRef::Bool(array.as_slice().unwrap()),
      PyArrayTyped::DateTime(array) => SeriesRef::DateTime(array),
      PyArrayTyped::String(array) => SeriesRef::String(array),
      PyArrayTyped::Decimal64(array) => SeriesRef::Decimal64(array),
    }
  }
}

fn extract_py_list<'py>(list: &Bound<'py, PyList>, dec_num: Option<usize>) -> PyResult<PyArrayTyped<'py>> {
  if let Some(dec) = dec_num {
    let mut decimals = Vec::with_capacity(list.len());
    for i in 0..list.len() {
      let mut val = list
        .get_item(i)?
        .extract::<String>()
        .ok()
        .and_then(|s| D64::from_str(&s).ok())
        .unwrap_or(D64_NAN);
      val.set_decimal(dec);
      decimals.push(val);
    }
    Ok(PyArrayTyped::Decimal64(decimals))
  } else {
    let mut strings = Vec::with_capacity(list.len());
    for i in 0..list.len() {
      strings.push(list.get_item(i)?.extract::<String>()?);
    }
    Ok(PyArrayTyped::String(strings))
  }
}

impl<'py> TryFrom<(Python<'py>, Bound<'py, PyAny>, Option<usize>)> for PyArrayTyped<'py> {
  type Error = PyErr;

  /// parse from a ndarray or list of strings
  fn try_from(
    (py, array, dec_num): (Python<'py>, Bound<'py, PyAny>, Option<usize>),
  ) -> PyResult<Self> {
    if let Ok(list) = array.cast::<PyList>() {
      return extract_py_list(list, dec_num);
    }

    if let Ok(untyped) = array.cast::<PyUntypedArray>() {
      let array = if untyped.is_c_contiguous() {
        array
      } else {
        untyped.call_method0("copy")?
      };
      let untyped = array.cast::<PyUntypedArray>()?;
      let dt = untyped.dtype();
      if dt.is_equiv_to(&dtype::<i64>(py)) {
        let array = array.cast::<PyArray1<i64>>()?;
        let r = array.readonly();
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Int64(r))
      } else if dt.is_equiv_to(&dtype::<u64>(py)) {
        let array = array.cast::<PyArray1<u64>>()?;
        let r = array.readonly();
        _ = r.as_slice()?;
        Ok(PyArrayTyped::UInt64(r))
      } else if dt.is_equiv_to(&dtype::<f64>(py)) {
        let array = array.cast::<PyArray1<f64>>()?;
        let r = array.readonly();
        if let Some(dec) = dec_num {
          let mut decimals = Vec::with_capacity(r.len());
          for v in r.as_slice()? {
            decimals.push(D64::from_f64(*v, dec));
          }
          Ok(PyArrayTyped::Decimal64(decimals))
        } else {
          _ = r.as_slice()?;
          Ok(PyArrayTyped::Float64(r))
        }
      } else if dt.is_equiv_to(&dtype::<i32>(py)) {
        let array = array.cast::<PyArray1<i32>>()?;
        let r = array.readonly();
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Int32(r))
      } else if dt.is_equiv_to(&dtype::<u32>(py)) {
        let array = array.cast::<PyArray1<u32>>()?;
        let r = array.readonly();
        _ = r.as_slice()?;
        Ok(PyArrayTyped::UInt32(r))
      } else if dt.is_equiv_to(&dtype::<f32>(py)) {
        let array = array.cast::<PyArray1<f32>>()?;
        let r = array.readonly();
        if let Some(dec) = dec_num {
          let mut decimals = Vec::with_capacity(r.len());
          for v in r.as_slice()? {
            decimals.push(D64::from_f64(*v as f64, dec));
          }
          Ok(PyArrayTyped::Decimal64(decimals))
        } else {
          _ = r.as_slice()?;
          Ok(PyArrayTyped::Float32(r))
        }
      } else if dt.is_equiv_to(&dtype::<bool>(py)) {
        let array = array.cast::<PyArray1<bool>>()?;
        let r = array.readonly();
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Bool(r))
      } else if dt.is_equiv_to(&dtype::<Datetime<Microseconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Microseconds>>>()?;
        let r = array.readonly();
        let v = r.as_slice()?.iter().map(|i| i64::from(*i)).collect();
        Ok(PyArrayTyped::DateTime(v))
      } else if dt.is_equiv_to(&dtype::<Datetime<Milliseconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Milliseconds>>>()?;
        let r = array.readonly();
        let v = r.as_slice()?.iter().map(|i| i64::from(*i) * 1_000).collect();
        Ok(PyArrayTyped::DateTime(v))
      } else if dt.is_equiv_to(&dtype::<Datetime<Seconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Seconds>>>()?;
        let r = array.readonly();
        let v = r.as_slice()?.iter().map(|i| i64::from(*i) * 1_000_000).collect();
        Ok(PyArrayTyped::DateTime(v))
      } else if dt.is_equiv_to(&dtype::<Datetime<Nanoseconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Nanoseconds>>>()?;
        let r = array.readonly();
        let v = r.as_slice()?.iter().map(|i| i64::from(*i) / 1000).collect();
        Ok(PyArrayTyped::DateTime(v))
      } else if matches!(dt.getattr("kind")?.extract::<String>()?.as_str(), "S" | "U" | "O") {
        let list_obj = array.call_method0("tolist")?;
        let list = list_obj.cast::<PyList>()?;
        extract_py_list(list, dec_num)
      } else {
        Err(PyValueError::new_err(format!("unsupported data type: {}", dt)))
      }
    } else {
      let array_type = array.get_type().name()?;
      Err(PyValueError::new_err(format!(
        "unsupported data type: {}",
        array_type
      )))
    }
  }
}

fn pack_typed_columns(obj: String, cols: Vec<(String, PyArrayTyped<'_>)>) -> Vec<u8> {
  let meta = HashMap::from([("obj".into(), obj.into())]);
  let fields = cols
    .iter()
    .map(|(name, series)| FieldRef {
      name,
      data: series.into(),
      metadata: None,
      kind: series.kind(),
    })
    .collect();
  let table = TableRef::new(fields, Some(meta), false);
  pack_table_ref_frame(&table)
}

pub(crate) fn pack_columns_to_frame<'py>(
  py: Python<'py>,
  obj: String,
  columns: &Bound<'py, PyList>,
  decimal_fields: Option<&HashMap<String, usize>>,
) -> PyResult<Vec<u8>> {
  let mut cols = Vec::with_capacity(columns.len());
  for col in columns.iter() {
    let (name, array) = col.extract::<(String, Bound<'py, PyAny>)>()?;
    let dec = decimal_fields.and_then(|df| df.get(&name)).copied();
    cols.push((name, PyArrayTyped::try_from((py, array, dec))?));
  }
  Ok(pack_typed_columns(obj, cols))
}

pub(crate) fn pack_dataframe_to_frame<'py>(
  py: Python<'py>,
  obj: String,
  df: &Bound<'py, PyAny>,
  decimal_fields: Option<&HashMap<String, usize>>,
) -> PyResult<Vec<u8>> {
  if let Ok(py_table) = df.extract::<pyo3::PyRef<crate::conn::PyTable>>() {
    let mut table = py_table.inner.clone();
    let _ = table.set_table_meta("obj", obj.into());
    return Ok(msd_table::pack_table_frame(&table));
  }

  if let Ok(columns) = df.cast::<PyList>() {
    return pack_columns_to_frame(py, obj, columns, decimal_fields);
  }

  let ty = df.get_type();
  let module = ty.module()?;
  let mod_str = module.to_str()?;
  if (mod_str.starts_with("pandas") || mod_str.starts_with("polars")) && df.hasattr("columns")? {
    let mut cols = Vec::new();
    if df.hasattr("index")? {
      let index = df.getattr("index")?;
      let index_name = index.getattr("name")?;
      if !index_name.is_none() {
        let name = index_name.str()?.extract::<String>()?;
        let arr = index.call_method0("to_numpy")?;
        let dec = decimal_fields.and_then(|m| m.get(&name)).copied();
        cols.push((name, PyArrayTyped::try_from((py, arr, dec))?));
      }
    }

    let columns = df.getattr("columns")?;
    for col_item in columns.try_iter()? {
      let col_key = col_item?;
      let name = col_key.str()?.extract::<String>()?;
      let series = df.get_item(&col_key)?;
      let arr = series.call_method0("to_numpy")?;
      let dec = decimal_fields.and_then(|m| m.get(&name)).copied();
      cols.push((name, PyArrayTyped::try_from((py, arr, dec))?));
    }
    return Ok(pack_typed_columns(obj, cols));
  }

  Err(PyTypeError::new_err(format!(
    "Unsupported DataFrame type: {}",
    ty.name()?
  )))
}
