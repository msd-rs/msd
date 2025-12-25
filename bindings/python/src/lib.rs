use pyo3::prelude::*;
mod py_table;

/// A Python module implemented in Rust.
#[pymodule]
mod msd {
  use msd_request::unpack_table_frame;
  use numpy::{PyArray, PyUntypedArray};
  use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyList, PyTuple},
  };

  use crate::py_table::table_to_py_dict;

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

  /// Create a table from a numpy array.
  ///
  /// columns: a list of tuple (column name, numpy array)
  #[pyfunction]
  #[pyo3(signature = (columns, /))]
  fn pack_table_frame<'py>(py: Python<'py>, columns: Bound<'py, PyList>) -> PyResult<Vec<u8>> {
    //TODO: extract parameters from columns and build a TableRef from it, then call msd_table::pack_table_ref_frame to pack it
    Ok(Vec::new())
  }
}
