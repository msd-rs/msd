// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use pyo3::prelude::*;
mod py_table;

/// A Python module implemented in Rust.
#[pymodule]
mod msd {
  use std::collections::HashMap;

  use msd_request::{pack_table_ref_frame, unpack_table_frame};
  use msd_table::{FieldRef, TableRef};
  use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyList, PyTuple},
  };

  use crate::py_table::{PyArrayTyped, table_to_py_dict};

  #[pyfunction]
  fn set_local_zone(tz: i8) {
    msd_table::set_default_timezone(tz)
  }

  #[pyfunction]
  fn get_local_offset() -> i64 {
    msd_table::get_local_offset()
  }

  /// Checks if the buffer is a table frame.
  ///
  /// Returns the frame size if it is a table frame.
  #[pyfunction]
  #[pyo3(signature = (buffer, /))]
  fn check_table_frame(buffer: &[u8]) -> PyResult<usize> {
    if buffer.len() < 8 {
      return Err(PyValueError::new_err("buffer is too short"));
    }
    if !buffer.starts_with(b"\x7c\x4d\x01\x00") {
      return Err(PyValueError::new_err("buffer is not a table frame"));
    }
    let frame_size = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
    Ok(frame_size as usize)
  }

  /// Parses a table frame.
  ///
  /// Returns the a tuple of (object name, table), where table is a dict of (column name, numpy array).
  #[pyfunction]
  #[pyo3(signature = (buffer, /))]
  fn parse_table_frame<'py>(py: Python<'py>, buffer: &[u8]) -> PyResult<Bound<'py, PyTuple>> {
    let table =
      unpack_table_frame(buffer, true).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let obj = table
      .get_table_meta("obj")
      .and_then(|v| v.get_str())
      .map(|v| v.to_string())
      .unwrap_or_default();

    (obj, table_to_py_dict(py, table)).into_pyobject(py)
  }

  /// Pack columns into a table frame.
  ///
  /// obj: the object name, must not be empty
  /// columns: a list of (column name, numpy array)
  /// Returns the packed table frame
  #[pyfunction]
  #[pyo3(signature = (obj, columns, /))]
  fn pack_table_frame<'py>(
    py: Python<'py>,
    obj: String,
    columns: Bound<'py, PyList>,
  ) -> PyResult<Vec<u8>> {
    let mut cols = Vec::new();
    for col in columns.iter() {
      let tuple = col.cast::<PyTuple>()?;
      let name = tuple.get_item(0)?.extract::<String>()?;
      let array = tuple.get_item(1)?;
      cols.push((name, PyArrayTyped::try_from((py, array))?));
    }
    let meta = HashMap::from([("obj".into(), obj.into())]);
    let fields = cols
      .iter()
      .map(|(name, series)| FieldRef {
        name: &name,
        data: series.into(),
        metadata: None,
        kind: series.kind(),
      })
      .collect();
    let table = TableRef::new(fields, Some(meta));
    let frame = pack_table_ref_frame(&table);
    Ok(frame)
  }
}
