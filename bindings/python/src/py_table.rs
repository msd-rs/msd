use msd_table::{DataType, Field, Series, SeriesRef, Table, get_local_offset};
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
  exceptions::PyValueError,
  prelude::*,
  types::{PyBytes, PyDict, PyList, PyString},
};

pub(crate) fn table_to_py_dict<'py>(py: Python<'py>, table: Table) -> Bound<'py, PyDict> {
  let dict = PyDict::new(py);

  let rows = table.row_count();
  let fields: Vec<Field> = table.into();

  fields.into_iter().for_each(|field| {
    let name = field.name;
    let _ = dict.set_item(name, series_to_array(py, field.data, rows));
  });

  dict
}

fn series_to_array<'py>(py: Python<'py>, series: Series, rows: usize) -> Bound<'py, PyAny> {
  match series {
    Series::Null => PyArray1::<bool>::zeros(py, [rows], true).into_any(),
    Series::DateTime(items) => {
      let offset = get_local_offset();
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
    Series::Decimal128(decimals) => PyArray1::<f64>::from_vec(
      py,
      decimals
        .into_iter()
        .map(|d| d.to_string().as_str().parse().unwrap())
        .collect(),
    )
    .into_any(),
  }
}

#[derive(Debug)]
pub enum PyArrayTyped<'py> {
  Int64(PyReadonlyArray1<'py, i64>),
  Float64(PyReadonlyArray1<'py, f64>),
  Int32(PyReadonlyArray1<'py, i32>),
  Float32(PyReadonlyArray1<'py, f32>),
  Bool(PyReadonlyArray1<'py, bool>),
  DateTime(Vec<i64>),
  String(Vec<String>),
}

impl<'py> PyArrayTyped<'py> {
  pub fn kind(&self) -> DataType {
    match self {
      PyArrayTyped::Int64(_) => DataType::Int64,
      PyArrayTyped::Float64(_) => DataType::Float64,
      PyArrayTyped::Int32(_) => DataType::Int32,
      PyArrayTyped::Float32(_) => DataType::Float32,
      PyArrayTyped::Bool(_) => DataType::Bool,
      PyArrayTyped::DateTime(_) => DataType::DateTime,
      PyArrayTyped::String(_) => DataType::String,
    }
  }
}

impl<'py, 'a> Into<SeriesRef<'a>> for &'a PyArrayTyped<'py> {
  fn into(self) -> SeriesRef<'a> {
    match self {
      PyArrayTyped::Int64(array) => SeriesRef::Int64(array.as_slice().unwrap()),
      PyArrayTyped::Float64(array) => SeriesRef::Float64(array.as_slice().unwrap()),
      PyArrayTyped::Int32(array) => SeriesRef::Int32(array.as_slice().unwrap()),
      PyArrayTyped::Float32(array) => SeriesRef::Float32(array.as_slice().unwrap()),
      PyArrayTyped::Bool(array) => SeriesRef::Bool(array.as_slice().unwrap()),
      PyArrayTyped::DateTime(array) => SeriesRef::DateTime(&array),
      PyArrayTyped::String(array) => SeriesRef::String(&array),
    }
  }
}

impl<'py> TryFrom<(Python<'py>, Bound<'py, PyAny>)> for PyArrayTyped<'py> {
  type Error = PyErr;

  /// parse from a ndarray or list of strings
  fn try_from((py, array): (Python<'py>, Bound<'py, PyAny>)) -> PyResult<Self> {
    let array_type = array.get_type().name()?;
    if array_type == "list" {
      let array = array.cast::<PyList>()?;
      let mut strings = Vec::with_capacity(array.len());
      for i in 0..array.len() {
        strings.push(array.get_item(i)?.extract::<String>()?);
      }
      return Ok(PyArrayTyped::String(strings));
    } else if array_type == "ndarray" {
      let array = array.cast::<PyUntypedArray>()?;
      let dt = array.dtype();
      if dt.is_equiv_to(&dtype::<i64>(py)) {
        let array = array.cast::<PyArray1<i64>>()?;
        let r = array.readonly();
        // check if the array is contiguous
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Int64(r))
      } else if dt.is_equiv_to(&dtype::<f64>(py)) {
        let array = array.cast::<PyArray1<f64>>()?;
        let r = array.readonly();
        // check if the array is contiguous
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Float64(r))
      } else if dt.is_equiv_to(&dtype::<i32>(py)) {
        let array = array.cast::<PyArray1<i32>>()?;
        let r = array.readonly();
        // check if the array is contiguous
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Int32(r))
      } else if dt.is_equiv_to(&dtype::<f32>(py)) {
        let array = array.cast::<PyArray1<f32>>()?;
        let r = array.readonly();
        // check if the array is contiguous
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Float32(r))
      } else if dt.is_equiv_to(&dtype::<bool>(py)) {
        let array = array.cast::<PyArray1<bool>>()?;
        let r = array.readonly();
        // check if the array is contiguous
        _ = r.as_slice()?;
        Ok(PyArrayTyped::Bool(r))
      } else if dt.is_equiv_to(&dtype::<Datetime<Microseconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Microseconds>>>()?;
        let r = array.readonly();
        // check if the array is contiguous
        _ = r.as_slice()?;
        let v = r.as_slice()?.iter().map(|i| i64::from(*i)).collect();
        Ok(PyArrayTyped::DateTime(v))
      } else if dt.is_equiv_to(&dtype::<Datetime<Milliseconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Milliseconds>>>()?;
        let r = array.readonly();
        let v = r
          .as_slice()?
          .iter()
          .map(|i| i64::from(*i) * 1_000)
          .collect();
        Ok(PyArrayTyped::DateTime(v))
      } else if dt.is_equiv_to(&dtype::<Datetime<Seconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Seconds>>>()?;
        let r = array.readonly();
        let v = r
          .as_slice()?
          .iter()
          .map(|i| i64::from(*i) * 1_000_000)
          .collect();
        Ok(PyArrayTyped::DateTime(v))
      } else if dt.is_equiv_to(&dtype::<Datetime<Nanoseconds>>(py)) {
        let array = array.cast::<PyArray1<Datetime<Nanoseconds>>>()?;
        let r = array.readonly();
        let v = r.as_slice()?.iter().map(|i| i64::from(*i) / 1000).collect();
        Ok(PyArrayTyped::DateTime(v))
      } else {
        Err(PyValueError::new_err(format!(
          "unsupported data type: {}",
          dt
        )))
      }
    } else {
      return Err(PyValueError::new_err(format!(
        "unsupported data type: {}",
        array_type
      )));
    }
  }
}
