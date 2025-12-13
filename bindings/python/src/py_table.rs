use msd_table::{Field, Series, Table, get_local_offset};
use numpy::{
  PyArray1,
  datetime::{Datetime, units::Microseconds},
};
use pyo3::{
  prelude::*,
  types::{PyBytes, PyDict, PyString},
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
