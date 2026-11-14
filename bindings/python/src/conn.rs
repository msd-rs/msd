use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use msd_table::DataType;
use msd_table::Field;
use msd_table::FillMethod;
use msd_table::JoinPlan;
use msd_table::Result;
use msd_table::Series;
use msd_table::SeriesMutSlice;
use msd_table::Table;
use msd_table::Variant;
use msd_table::anyhow;
use msd_table::bail;
use msd_table::check_table_frame;
use msd_table::compute_join_plan;
use msd_table::extend_series_with_fill;
use msd_table::fill_series_slice_missing;
use msd_table::unpack_frame_batch;
use msd_table::write_series_slice_with_fill;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBool;
use pyo3::types::PyByteArray;
use pyo3::types::PyDict;
use pyo3::types::PyFloat;
use pyo3::types::PyInt;
use pyo3::types::PyList;
use pyo3::types::PyNone;
use pyo3::types::PyString;
use reqwest::{
  Client, ClientBuilder,
  header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::bytes::Bytes;

use crate::py_table::table_to_py_dict;
use crate::py_table::table_to_py_list;
use crate::runtime::get_runtime;

// grouped table {obj: {table_name: table}}
type GroupedTables = HashMap<String, HashMap<String, Table>>;
type FrameBatch = Vec<(String, Arc<str>, Table)>;

#[pyclass(name = "MsdConnection")]
pub struct PyMsdConnection {
  inner: MsdConnection,
}

#[pymethods]
impl PyMsdConnection {
  #[new]
  #[pyo3(signature = (url, token="", compress=false))]
  fn new(url: &str, token: &str, compress: bool) -> PyResult<Self> {
    let inner = MsdConnection::new(url, token, compress)?;
    Ok(Self { inner })
  }

  /// raw query, group by the results to a 2 level dict
  /// level 1 : obj
  /// level 2 : table name
  /// value: a table
  #[pyo3(signature = (sql))]
  fn query<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyDict>> {
    let batches = py.detach(|| self.inner.async_query_batches(sql))?;

    let outer = PyDict::new(py);
    let mut cached_table: Option<(Arc<str>, Bound<'py, PyString>)> = None;

    // Check if all batches share a single table name (the common single-table query case)
    let mut single_table_name: Option<&str> = None;
    let mut is_single_table = true;
    'check: for batch in &batches {
      for (_, tname, _) in batch {
        match single_table_name {
          None => single_table_name = Some(tname.as_ref()),
          Some(prev) if prev == tname.as_ref() => {}
          Some(_) => {
            is_single_table = false;
            break 'check;
          }
        }
      }
    }

    if is_single_table {
      for batch in batches {
        for (obj, table_name, table) in batch {
          if cached_table.is_none() {
            let pystr = PyString::new(py, &table_name);
            cached_table = Some((Arc::clone(&table_name), pystr));
          }
          let (_, table_pystr) = cached_table.as_ref().unwrap();
          let inner = PyDict::new(py);
          let obj_pystr = PyString::new(py, &obj);
          let py_table = Py::new(
            py,
            PyTable {
              inner: table,
              obj,
              table_name,
            },
          )?;
          inner.set_item(table_pystr, py_table)?;
          outer.set_item(obj_pystr, inner)?;
        }
      }
    } else {
      for batch in batches {
        for (obj, table_name, table) in batch {
          let use_cached = cached_table
            .as_ref()
            .is_some_and(|(k, _)| k.as_ref() == table_name.as_ref());
          if !use_cached {
            let pystr = PyString::new(py, &table_name);
            cached_table = Some((Arc::clone(&table_name), pystr));
          }
          let (_, table_pystr) = cached_table.as_ref().unwrap();
          let obj_pystr = PyString::new(py, &obj);
          let py_table = Py::new(
            py,
            PyTable {
              inner: table,
              obj,
              table_name,
            },
          )?;
          if let Some(existing) = outer.get_item(&obj_pystr)? {
            let inner = existing.cast::<PyDict>()?;
            inner.set_item(table_pystr, py_table)?;
          } else {
            let inner = PyDict::new(py);
            inner.set_item(table_pystr, py_table)?;
            outer.set_item(obj_pystr, inner)?;
          }
        }
      }
    }

    Ok(outer)
  }

  /// Execute query and concatenate tables of the same table name.
  /// Returns a dict where:
  /// key: table name
  /// value: tuple[Table, list[tuple[obj, start, end]]]
  #[pyo3(signature = (sql))]
  fn query_concat<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyDict>> {
    let batches = py.detach(|| self.inner.async_query_batches(sql))?;
    let concatenated = concat_batches(batches).map_err(|e| PyValueError::new_err(e.to_string()))?;

    let outer = PyDict::new(py);
    for (table_name, table, ranges) in concatenated {
      let table_pystr = PyString::new(py, table_name.as_ref());
      let py_ranges = PyList::new(py, ranges)?;
      let py_table = Py::new(
        py,
        PyTable {
          inner: table,
          obj: String::new(),
          table_name,
        },
      )?;
      let val = (py_table, py_ranges).into_pyobject(py)?;
      outer.set_item(table_pystr, val)?;
    }

    Ok(outer)
  }

  /// query and join the result to one table
  /// the join result will
  /// - rows: count_of_objects * base_table.rows
  /// - columns: sum columns of join_asof for base_obj's tables
  /// return error when base_obj or base_table not found
  ///
  /// fill is map of table name and fill method, it can have a "*" as key for any table not found in map
  #[pyo3(signature = (sql, base_table, fill, base_obj))]
  fn query_join<'py>(
    &self,
    py: Python<'py>,
    sql: String,
    base_table: String,
    fill: HashMap<String, String>,
    base_obj: String,
  ) -> PyResult<(PyTable, Vec<String>, usize)> {
    let fill = fill
      .into_iter()
      .map(|(k, v)| (k, FillMethod::from(v)))
      .collect::<HashMap<String, FillMethod>>();
    let (table, objs, bars) =
      py.detach(|| self.inner.query_join(sql, base_table, fill, base_obj))?;
    Ok((PyTable::from_table(table), objs, bars))
  }

  /// take last snapshot of multiple tables of a set of objects,
  /// join them to one table by obj of each result
  /// table can be
  /// - name
  /// - name: field1, field2
  /// objects can be
  /// - single: eg. SH600000
  /// - pattern: eg. SH600*
  /// - block: eg. @stock_block:上证A股
  #[pyo3(signature = (tables, objects, date = None))]
  fn snapshot<'py>(
    &self,
    py: Python<'py>,
    tables: Vec<String>,
    objects: Vec<String>,
    date: Option<String>,
  ) -> PyResult<PyTable> {
    let table = py.detach(|| self.inner.snapshot(tables, objects, date.as_deref()))?;
    Ok(PyTable::from_table(table))
  }

  /// query and call handler for each table
  #[pyo3(signature = (sql, handler))]
  fn query_map<'py>(
    &self,
    py: Python<'py>,
    sql: String,
    handler: Bound<'py, PyAny>,
  ) -> PyResult<()> {
    if !handler.is_callable() {
      return Err(PyTypeError::new_err("handler must be callable"));
    }
    self.inner.query_map(py, sql, |(obj, table_name, table)| {
      handler.call1((PyTable {
        inner: table,
        obj,
        table_name,
      },))?;
      Ok(())
    })?;
    Ok(())
  }

  /// Execute raw query and return a list of `(table_name, obj, MsdTable)` tuples.
  #[pyo3(signature = (sql))]
  fn query_raw<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyList>> {
    let batches = py.detach(|| self.inner.async_query_batches(sql))?;
    let total: usize = batches.iter().map(|b| b.len()).sum();
    let mut items = Vec::with_capacity(total);
    let mut cached_table: Option<(Arc<str>, Bound<'py, PyString>)> = None;

    for batch in batches {
      for (obj, table_name, table) in batch {
        let use_cached = cached_table
          .as_ref()
          .is_some_and(|(k, _)| k.as_ref() == table_name.as_ref());
        if !use_cached {
          let pystr = PyString::new(py, &table_name);
          cached_table = Some((Arc::clone(&table_name), pystr));
        }
        let (_, table_pystr) = cached_table.as_ref().unwrap();
        let obj_pystr = PyString::new(py, &obj);
        let py_list = table_to_py_list(py, table);
        items.push((table_pystr.clone(), obj_pystr, py_list));
      }
    }

    PyList::new(py, items)
  }

  /// import csv data to msd table
  /// data can be bytes, a file path string, Path object, or a file-like object with read()
  #[pyo3(signature = (table, data, header = true, delimiter = None))]
  fn import_csv<'py>(
    &self,
    py: Python<'py>,
    table: String,
    data: Bound<'py, PyAny>,
    header: bool,
    delimiter: Option<String>,
  ) -> PyResult<Bound<'py, PyAny>> {
    // 1. If data is bytes or bytearray:
    if let Ok(bytes) = data.extract::<&[u8]>() {
      let json = self
        .inner
        .import_csv(&table, bytes.to_vec(), header, delimiter.as_deref())?;
      return json_to_py(py, json);
    }

    // 2. If data is a file path string or PathLike:
    let path_opt: Option<String> = if let Ok(path) = data.extract::<String>() {
      Some(path)
    } else if let Ok(fspath) = data.call_method0("__fspath__") {
      fspath.extract::<String>().ok()
    } else {
      None
    };

    if let Some(path) = path_opt {
      let bytes = std::fs::read(&path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("failed to read file: {e}")))?;
      let json = self
        .inner
        .import_csv(&table, bytes, header, delimiter.as_deref())?;
      return json_to_py(py, json);
    }

    // 3. If data is a file-like object with .read():
    if data.hasattr("read")? {
      let (tx, rx) = mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(4);
      let body = reqwest::Body::wrap_stream(ReceiverStream::new(rx));

      let (resp_tx, resp_rx) = oneshot::channel();
      let conn = self.inner.clone();
      let table_clone = table.clone();
      let delim_clone = delimiter.clone();
      get_runtime().spawn(async move {
        let res = conn
          .async_import_csv(&table_clone, body, header, delim_clone.as_deref())
          .await;
        let _ = resp_tx.send(res);
      });

      loop {
        let chunk = data.call_method1("read", (64 * 1024,))?;
        let bytes = chunk.extract::<&[u8]>()?;
        if bytes.is_empty() {
          break;
        }
        if tx.blocking_send(Ok(Bytes::copy_from_slice(bytes))).is_err() {
          break;
        }
      }
      drop(tx);

      let res = if let Ok(r) = resp_rx.blocking_recv() {
        r?
      } else {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
          "internal error during import",
        ));
      };
      return json_to_py(py, res);
    }

    Err(PyTypeError::new_err(
      "data must be bytes, a file-like object, or a file path string",
    ))
  }

  /// import table frame data to msd table
  /// data can be Table, bytes, a file path string, Path object, or a file-like object with read()
  #[pyo3(signature = (table, data))]
  fn import_table<'py>(
    &self,
    py: Python<'py>,
    table: String,
    data: Bound<'py, PyAny>,
  ) -> PyResult<Bound<'py, PyAny>> {
    // 0. If data is PyTable:
    if let Ok(py_table) = data.extract::<pyo3::PyRef<PyTable>>() {
      let bytes = msd_table::pack_table_frame(&py_table.inner);
      let json = self.inner.import_table(&table, bytes)?;
      return json_to_py(py, json);
    }

    // 1. If data is bytes or bytearray:
    if let Ok(bytes) = data.extract::<&[u8]>() {
      let json = self.inner.import_table(&table, bytes.to_vec())?;
      return json_to_py(py, json);
    }

    // 2. If data is a file path string or PathLike:
    let path_opt: Option<String> = if let Ok(path) = data.extract::<String>() {
      Some(path)
    } else if let Ok(fspath) = data.call_method0("__fspath__") {
      fspath.extract::<String>().ok()
    } else {
      None
    };

    if let Some(path) = path_opt {
      let bytes = std::fs::read(&path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("failed to read file: {e}")))?;
      let json = self.inner.import_table(&table, bytes)?;
      return json_to_py(py, json);
    }

    // 3. If data is a file-like object with .read():
    if data.hasattr("read")? {
      let (tx, rx) = mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(4);
      let body = reqwest::Body::wrap_stream(ReceiverStream::new(rx));

      let (resp_tx, resp_rx) = oneshot::channel();
      let conn = self.inner.clone();
      let table_clone = table.clone();
      get_runtime().spawn(async move {
        let res = conn.async_import_table(&table_clone, body).await;
        let _ = resp_tx.send(res);
      });

      loop {
        let chunk = data.call_method1("read", (64 * 1024,))?;
        let bytes = chunk.extract::<&[u8]>()?;
        if bytes.is_empty() {
          break;
        }
        if tx.blocking_send(Ok(Bytes::copy_from_slice(bytes))).is_err() {
          break;
        }
      }
      drop(tx);

      let res = if let Ok(r) = resp_rx.blocking_recv() {
        r?
      } else {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
          "internal error during import",
        ));
      };
      return json_to_py(py, res);
    }

    Err(PyTypeError::new_err(
      "data must be Table, bytes, a file-like object, or a file path string",
    ))
  }

  /// import dataframes or table frames to msd table
  /// data can be an iterator of (obj_name, df), a single (obj_name, df) tuple, or bytes
  #[pyo3(signature = (table, data, decimal_fields = None))]
  fn import_dataframes<'py>(
    &self,
    py: Python<'py>,
    table: String,
    data: Bound<'py, PyAny>,
    decimal_fields: Option<Bound<'py, PyAny>>,
  ) -> PyResult<Bound<'py, PyAny>> {
    let (tx, rx) = mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(4);
    let body = reqwest::Body::wrap_stream(ReceiverStream::new(rx));

    let (resp_tx, resp_rx) = oneshot::channel();
    let conn = self.inner.clone();
    let table_clone = table.clone();
    get_runtime().spawn(async move {
      let res = conn.async_import_table(&table_clone, body).await;
      let _ = resp_tx.send(res);
    });

    let pack_dataframe = py.import("pymsd.pack")?.getattr("pack_dataframe")?;

    let is_single = if let Ok(tuple) = data.cast::<pyo3::types::PyTuple>() {
      tuple.len() == 2
        && tuple
          .get_item(0)
          .map_or(false, |x| x.is_instance_of::<PyString>())
    } else {
      false
    };

    let pack_item = |item: &Bound<'py, PyAny>| -> PyResult<Vec<u8>> {
      if let Ok(b) = item.extract::<&[u8]>() {
        Ok(b.to_vec())
      } else if let Ok(tuple) = item.cast::<pyo3::types::PyTuple>() {
        let obj = tuple.get_item(0)?;
        let df = tuple.get_item(1)?;
        if let Ok(py_table) = df.extract::<pyo3::PyRef<PyTable>>() {
          let mut table = py_table.inner.clone();
          let obj_str = obj.extract::<String>()?;
          let _ = table.set_table_meta("obj", obj_str.into());
          Ok(msd_table::pack_table_frame(&table))
        } else {
          let packed = pack_dataframe.call1((obj, df, &decimal_fields))?;
          Ok(packed.extract::<Vec<u8>>()?)
        }
      } else if let Ok(list) = item.cast::<PyList>() {
        let obj = list.get_item(0)?;
        let df = list.get_item(1)?;
        if let Ok(py_table) = df.extract::<pyo3::PyRef<PyTable>>() {
          let mut table = py_table.inner.clone();
          let obj_str = obj.extract::<String>()?;
          let _ = table.set_table_meta("obj", obj_str.into());
          Ok(msd_table::pack_table_frame(&table))
        } else {
          let packed = pack_dataframe.call1((obj, df, &decimal_fields))?;
          Ok(packed.extract::<Vec<u8>>()?)
        }
      } else {
        Err(PyTypeError::new_err(
          "each item must be a tuple of (obj_name, dataframe) or bytes",
        ))
      }
    };

    if is_single {
      let bytes_vec = pack_item(&data)?;
      let _ = tx.blocking_send(Ok(Bytes::from(bytes_vec)));
    } else {
      let iter = data.try_iter()?;
      for item_res in iter {
        let item = item_res?;
        let bytes_vec = pack_item(&item)?;
        if tx.blocking_send(Ok(Bytes::from(bytes_vec))).is_err() {
          break;
        }
      }
    }
    drop(tx);

    let res = if let Ok(r) = resp_rx.blocking_recv() {
      r?
    } else {
      return Err(pyo3::exceptions::PyRuntimeError::new_err(
        "internal error during import",
      ));
    };
    json_to_py(py, res)
  }

  #[pyo3(signature = (table, data, decimal_fields = None))]
  fn import_dataframe<'py>(
    &self,
    py: Python<'py>,
    table: String,
    data: Bound<'py, PyAny>,
    decimal_fields: Option<Bound<'py, PyAny>>,
  ) -> PyResult<Bound<'py, PyAny>> {
    self.import_dataframes(py, table, data, decimal_fields)
  }
}

#[pyclass(name = "Table")]
pub struct PyTable {
  inner: Table,
  obj: String,
  table_name: Arc<str>,
}

impl PyTable {
  fn from_table(table: Table) -> Self {
    let obj = table
      .get_table_meta("obj")
      .and_then(|v| v.get_string())
      .cloned()
      .unwrap_or_default();
    let table_name = table
      .get_table_meta("table")
      .and_then(|v| v.get_string())
      .map(|s| Arc::from(s.as_str()))
      .unwrap_or_else(|| Arc::from(""));
    Self {
      inner: table,
      obj,
      table_name,
    }
  }
}

fn defer_drop_table(table: Table, obj: String) {
  static DROP_TX: std::sync::OnceLock<std::sync::mpsc::Sender<(Table, String)>> =
    std::sync::OnceLock::new();
  let tx = DROP_TX.get_or_init(|| {
    let (tx, rx) = std::sync::mpsc::channel::<(Table, String)>();
    std::thread::Builder::new()
      .name("pymsd-drop".into())
      .spawn(move || {
        while let Ok(item) = rx.recv() {
          drop(item);
        }
      })
      .expect("failed to spawn pymsd-drop thread");
    tx
  });
  let _ = tx.send((table, obj));
}

impl Drop for PyTable {
  fn drop(&mut self) {
    if self.inner.column_count() > 0 {
      let table = std::mem::take(&mut self.inner);
      let obj = std::mem::take(&mut self.obj);
      defer_drop_table(table, obj);
    }
  }
}

#[pymethods]
impl PyTable {
  #[getter]
  fn rows(&self) -> usize {
    self.inner.row_count()
  }
  #[getter]
  fn columns(&self) -> usize {
    self.inner.column_count()
  }
  #[getter]
  fn name(&self) -> String {
    if !self.table_name.is_empty() {
      return self.table_name.as_ref().to_owned();
    }
    self
      .inner
      .get_table_meta("table")
      .and_then(|v| v.get_string())
      .map(|s| s.clone())
      .unwrap_or_default()
  }
  #[getter]
  fn obj(&self) -> String {
    if !self.obj.is_empty() {
      return self.obj.clone();
    }
    self
      .inner
      .get_table_meta("obj")
      .and_then(|v| v.get_string())
      .map(|s| s.clone())
      .unwrap_or_default()
  }

  #[getter]
  fn header<'py>(&self) -> Vec<String> {
    self
      .inner
      .columns()
      .iter()
      .map(|s| s.name.clone())
      .collect()
  }

  fn __getitem__<'py>(&self, py: Python<'py>, index: (usize, usize)) -> Bound<'py, PyAny> {
    if let Some(v) = self.inner.get_cell(index.0, index.1) {
      match v {
        msd_table::VariantRef::Null => PyNone::get(py).to_owned().into_any(),
        msd_table::VariantRef::String(s) => PyString::new(py, s).into_any(),
        msd_table::VariantRef::Bytes(s) => PyByteArray::new(py, s).into_any(),
        msd_table::VariantRef::Int32(n) => PyInt::new(py, n).into_any(),
        msd_table::VariantRef::UInt32(n) => PyInt::new(py, n).into_any(),
        msd_table::VariantRef::Int64(n) => PyInt::new(py, n).into_any(),
        msd_table::VariantRef::UInt64(n) => PyInt::new(py, n).into_any(),
        msd_table::VariantRef::Float32(f) => PyFloat::new(py, *f as f64).into_any(),
        msd_table::VariantRef::Float64(f) => PyFloat::new(py, *f).into_any(),
        msd_table::VariantRef::Decimal64(d) => PyFloat::new(py, f64::from(d)).into_any(),
        msd_table::VariantRef::Bool(b) => PyBool::new(py, *b).to_owned().into_any(),
        msd_table::VariantRef::DateTime(n) => PyInt::new(py, n).into_any(),
      }
    } else {
      PyNone::get(py).to_owned().into_any()
    }
  }

  /// transform this table to a list of (name, data)
  /// the data is a typed numpy ndarray
  /// after transform, this object will be reset
  /// caller can use the result to build data frame or do numpy operation directly without copy memory
  fn to_list<'py>(&mut self, py: Python<'py>) -> Bound<'py, PyList> {
    let mut table = Table::default();
    std::mem::swap(&mut table, &mut self.inner);
    table_to_py_list(py, table)
  }

  /// transform this table to a dict of (name, data)
  /// the data is a typed numpy ndarray
  /// after transform, this object will be reset
  /// caller can use the result to build data frame or do numpy operation directly without copy memory
  fn to_dict<'py>(&mut self, py: Python<'py>) -> Bound<'py, PyDict> {
    let mut table = Table::default();
    std::mem::swap(&mut table, &mut self.inner);
    table_to_py_dict(py, table)
  }
}

fn concat_batches(
  batches: Vec<FrameBatch>,
) -> Result<Vec<(Arc<str>, Table, Vec<(String, usize, usize)>)>> {
  if batches.is_empty() {
    return Ok(Vec::new());
  }

  // Check if all batches share a single table name (the common case)
  let mut single_table_name: Option<Arc<str>> = None;
  let mut is_single_table = true;
  let mut total_tables = 0usize;
  let mut total_rows = 0usize;

  'check: for batch in &batches {
    for (_, tname, t) in batch {
      total_tables += 1;
      total_rows += t.row_count();
      match &single_table_name {
        None => single_table_name = Some(Arc::clone(tname)),
        Some(prev) if prev.as_ref() == tname.as_ref() => {}
        Some(_) => {
          is_single_table = false;
          break 'check;
        }
      }
    }
  }

  let Some(single_table_name) = single_table_name else {
    return Ok(Vec::new());
  };

  if is_single_table {
    let mut concat_table: Option<Table> = None;
    let mut ranges: Vec<(String, usize, usize)> = Vec::with_capacity(total_tables);

    for batch in batches {
      for (obj, _, table) in batch {
        match &mut concat_table {
          None => {
            let rows = table.row_count();
            let mut table = table;
            if total_rows > rows {
              table.reserve(total_rows - rows);
            }
            let _ = table.set_table_meta("obj", Variant::Null);
            ranges.push((obj, 0, rows));
            concat_table = Some(table);
          }
          Some(dest) => {
            let start = dest.row_count();
            dest.extend(&table, false)?;
            let end = dest.row_count();
            ranges.push((obj, start, end));
          }
        }
      }
    }

    if let Some(table) = concat_table {
      Ok(vec![(single_table_name, table, ranges)])
    } else {
      Ok(Vec::new())
    }
  } else {
    let mut table_stats: HashMap<Arc<str>, (usize, usize)> = HashMap::new();
    for batch in &batches {
      for (_, tname, t) in batch {
        let entry = table_stats.entry(Arc::clone(tname)).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += t.row_count();
      }
    }

    struct TableGroup {
      table_name: Arc<str>,
      table: Table,
      ranges: Vec<(String, usize, usize)>,
    }

    let mut groups: Vec<TableGroup> = Vec::with_capacity(table_stats.len());
    let mut name_to_group_idx: HashMap<Arc<str>, usize> = HashMap::with_capacity(table_stats.len());

    for batch in batches {
      for (obj, table_name, table) in batch {
        if let Some(&idx) = name_to_group_idx.get(&table_name) {
          let group = &mut groups[idx];
          let start = group.table.row_count();
          group.table.extend(&table, false)?;
          let end = group.table.row_count();
          group.ranges.push((obj, start, end));
        } else {
          let (tot_tables, tot_rows) = table_stats
            .get(&table_name)
            .copied()
            .unwrap_or((1, table.row_count()));
          let mut ranges = Vec::with_capacity(tot_tables);
          let rows = table.row_count();
          let mut table = table;
          if tot_rows > rows {
            table.reserve(tot_rows - rows);
          }
          let _ = table.set_table_meta("obj", Variant::Null);
          ranges.push((obj, 0, rows));
          let idx = groups.len();
          name_to_group_idx.insert(Arc::clone(&table_name), idx);
          groups.push(TableGroup {
            table_name,
            table,
            ranges,
          });
        }
      }
    }

    let result = groups
      .into_iter()
      .map(|g| (g.table_name, g.table, g.ranges))
      .collect();
    Ok(result)
  }
}

#[derive(Debug, Clone)]
struct MsdConnection {
  client: Client,
  url: String,
}

struct JoinBaseContext {
  pk_col: Field,
  pk_strictly_increasing: bool,
}

struct JoinBatchItem {
  obj: String,
  table_name: Arc<str>,
  table: Table,
  plan: Option<JoinPlan>,
}

struct TableEntry {
  table: Table,
  plan: Option<JoinPlan>,
}

struct TableJoinSpec<'a> {
  table_id: usize,
  fill: FillMethod,
  schema: &'a Table,
  schema_pk_idx: usize,
  dst_cols: Vec<usize>,
}

fn defer_drop_join_entries(entries: Vec<(String, Vec<Option<TableEntry>>)>) {
  static DROP_TX: OnceLock<std::sync::mpsc::Sender<Vec<(String, Vec<Option<TableEntry>>)>>> =
    OnceLock::new();
  let tx = DROP_TX.get_or_init(|| {
    let (tx, rx) = std::sync::mpsc::channel::<Vec<(String, Vec<Option<TableEntry>>)>>();
    std::thread::Builder::new()
      .name("pymsd-join-drop".into())
      .spawn(move || {
        while let Ok(item) = rx.recv() {
          drop(item);
        }
      })
      .expect("failed to spawn pymsd-join-drop thread");
    tx
  });
  let _ = tx.send(entries);
}

#[inline]
fn resolve_fill(
  fill: &HashMap<String, FillMethod>,
  default_fill: FillMethod,
  table_name: &Arc<str>,
  cache: &mut Option<(Arc<str>, FillMethod)>,
) -> FillMethod {
  if let Some((cached_name, cached_fill)) = cache
    && (Arc::ptr_eq(cached_name, table_name) || cached_name.as_ref() == table_name.as_ref())
  {
    return *cached_fill;
  }
  let method = fill
    .get(table_name.as_ref())
    .copied()
    .unwrap_or(default_fill);
  *cache = Some((Arc::clone(table_name), method));
  method
}

fn write_table_to_slices(
  local_slices: &mut [SeriesMutSlice<'_>],
  start: usize,
  end: usize,
  spec: &TableJoinSpec<'_>,
  entry: Option<&TableEntry>,
  base_ctx: &JoinBaseContext,
) -> Result<()> {
  if let Some(entry) = entry
    && !entry.table.columns().is_empty()
  {
    let t = &entry.table;
    let t_pk_idx = t.pk_column();
    let computed_plan;
    let plan = match &entry.plan {
      Some(p) => p,
      None => {
        let t_pk = &t.columns()[t_pk_idx];
        computed_plan = compute_join_plan(
          &base_ctx.pk_col,
          t_pk,
          base_ctx.pk_strictly_increasing,
          spec.fill,
        )?;
        &computed_plan
      }
    };

    let src_cols = t.columns().iter().enumerate().filter(|&(i, _)| i != t_pk_idx);
    for (&dst_col, (_, col)) in spec.dst_cols.iter().zip(src_cols) {
      match plan {
        JoinPlan::ExactMatch => {
          local_slices[dst_col].copy_range_from_series(start, end, &col.data)?;
        }
        JoinPlan::Indices {
          indices,
          has_missing,
        } => {
          write_series_slice_with_fill(
            &mut local_slices[dst_col],
            start,
            end,
            &col.data,
            indices,
            *has_missing,
            spec.fill,
          );
        }
      }
    }
  } else {
    let schema_cols = spec
      .schema
      .columns()
      .iter()
      .enumerate()
      .filter(|&(i, _)| i != spec.schema_pk_idx);
    for (&dst_col, (_, col)) in spec.dst_cols.iter().zip(schema_cols) {
      fill_series_slice_missing(
        &mut local_slices[dst_col],
        start,
        end,
        &col.data,
        spec.fill,
      );
    }
  }
  Ok(())
}

fn process_join_chunk(
  chunk_entries: &[(String, Vec<Option<TableEntry>>)],
  local_slices: &mut [SeriesMutSlice<'_>],
  base_obj: &str,
  base_ctx: &JoinBaseContext,
  base_pk_idx: usize,
  base_spec: &TableJoinSpec<'_>,
  other_specs: &[TableJoinSpec<'_>],
  bars: usize,
) -> Result<()> {
  for (local_i, (obj_name, slots)) in chunk_entries.iter().enumerate() {
    let start = local_i * bars;
    let end = start + bars;

    local_slices[base_pk_idx].copy_range_from_series(start, end, &base_ctx.pk_col.data)?;

    if obj_name == base_obj {
      let src_cols = base_spec
        .schema
        .columns()
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != base_spec.schema_pk_idx);
      for (&dst_col, (_, col)) in base_spec.dst_cols.iter().zip(src_cols) {
        local_slices[dst_col].copy_range_from_series(start, end, &col.data)?;
      }
    } else {
      let base_entry = slots.get(base_spec.table_id).and_then(|o| o.as_ref());
      write_table_to_slices(local_slices, start, end, base_spec, base_entry, base_ctx)?;
    }

    for spec in other_specs {
      let entry = slots.get(spec.table_id).and_then(|o| o.as_ref());
      write_table_to_slices(local_slices, start, end, spec, entry, base_ctx)?;
    }
  }
  Ok(())
}

impl MsdConnection {
  fn new(url: &str, token: &str, compress: bool) -> Result<Self> {
    let builder = ClientBuilder::new()
      .user_agent("msd-client")
      .tcp_nodelay(true)
      .no_proxy()
      .zstd(compress);

    let url = url.trim_end_matches('/').to_string();

    let builder = if !token.is_empty() {
      let mut header = HeaderMap::new();
      header.insert(
        AUTHORIZATION,
        HeaderValue::from_str(format!("Bearer {}", token).as_str()).unwrap(),
      );
      builder.default_headers(header)
    } else {
      builder
    };

    let conn = builder.build().map(|client| Self { client, url })?;
    Ok(conn)
  }

  fn query_join(
    &self,
    sql: String,
    base_table: String,
    fill: HashMap<String, FillMethod>,
    base_obj: String,
  ) -> Result<(Table, Vec<String>, usize)> {
    let default_fill = fill.get("*").copied().unwrap_or_default();
    let base_fill = fill.get(&base_table).copied().unwrap_or(default_fill);

    let (tx, mut rx) = mpsc::channel(16);
    let base_ctx_lock = Arc::new(OnceLock::<Arc<JoinBaseContext>>::new());

    let conn = self.clone();
    let base_obj_stream = base_obj.clone();
    let base_table_stream = base_table.clone();
    let fill_arc = Arc::new(fill);
    let fill_stream = Arc::clone(&fill_arc);
    let base_ctx_stream = Arc::clone(&base_ctx_lock);

    get_runtime().spawn(async move {
      if let Err(err) = query_msd_join_stream(
        conn,
        sql,
        base_obj_stream,
        base_table_stream,
        fill_stream,
        default_fill,
        base_ctx_stream,
        &tx,
      )
      .await
      {
        let _ = tx.send(Err(err)).await;
      }
    });

    let mut table_names: Vec<Arc<str>> = Vec::new();
    let mut table_id_by_name: HashMap<Arc<str>, usize> = HashMap::new();
    let mut obj_id_by_name: HashMap<String, usize> = HashMap::new();
    let mut obj_entries: Vec<(String, Vec<Option<TableEntry>>)> = Vec::new();

    while let Some(res) = rx.blocking_recv() {
      let batch = res?;
      for item in batch {
        if item.obj.is_empty() || item.table_name.is_empty() {
          continue;
        }
        let table_id = if let Some(&id) = table_id_by_name.get(&item.table_name) {
          id
        } else {
          let id = table_names.len();
          table_id_by_name.insert(Arc::clone(&item.table_name), id);
          table_names.push(item.table_name);
          id
        };

        let obj_id = if let Some(&id) = obj_id_by_name.get(&item.obj) {
          id
        } else {
          let id = obj_entries.len();
          obj_id_by_name.insert(item.obj.clone(), id);
          obj_entries.push((item.obj, Vec::with_capacity(table_names.len().max(1))));
          id
        };

        let slots = &mut obj_entries[obj_id].1;
        if slots.len() <= table_id {
          slots.resize_with(table_id + 1, || None);
        }
        slots[table_id] = Some(TableEntry {
          table: item.table,
          plan: item.plan,
        });
      }
    }

    let Some(&base_obj_idx) = obj_id_by_name.get(&base_obj) else {
      bail!("base_obj {} not found", base_obj);
    };
    let Some(&base_table_id) = table_id_by_name.get(base_table.as_str()) else {
      bail!("base_table {} not found in {}", base_table, base_obj);
    };
    if obj_entries[base_obj_idx]
      .1
      .get(base_table_id)
      .and_then(|o| o.as_ref())
      .is_none()
    {
      bail!("base_table {} not found in {}", base_table, base_obj);
    }

    drop(obj_id_by_name);
    drop(table_id_by_name);

    obj_entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let base_obj_idx = obj_entries
      .binary_search_by(|e| e.0.as_str().cmp(base_obj.as_str()))
      .expect("base_obj must exist after sort");

    let base_slots = &obj_entries[base_obj_idx].1;
    let base = &base_slots[base_table_id]
      .as_ref()
      .expect("base_table must exist")
      .table;

    if base.columns().is_empty() {
      bail!("base_table {} in {} has no columns", base_table, base_obj);
    }

    let pk_idx = base.pk_column();
    let base_ctx = if let Some(ctx) = base_ctx_lock.get() {
      Arc::clone(ctx)
    } else {
      let pk_col = base.columns()[pk_idx].clone();
      let pk_strictly_increasing = pk_col.data.is_strictly_increasing();
      Arc::new(JoinBaseContext {
        pk_col,
        pk_strictly_increasing,
      })
    };

    let bars = base_ctx.pk_col.data.len();
    let num_objs = obj_entries.len();
    let total_rows = num_objs * bars;

    let mut other_table_ids: Vec<usize> = base_slots
      .iter()
      .enumerate()
      .filter_map(|(tid, slot)| (tid != base_table_id && slot.is_some()).then_some(tid))
      .collect();
    other_table_ids.sort_unstable_by(|&a, &b| table_names[a].cmp(&table_names[b]));

    let total_col_count = base.column_count()
      + other_table_ids
        .iter()
        .map(|&tid| {
          let t = &base_slots[tid].as_ref().unwrap().table;
          t.column_count().saturating_sub(usize::from(!t.columns().is_empty()))
        })
        .sum::<usize>();

    let mut result_cols = Vec::with_capacity(total_col_count);
    let mut base_dst_cols = Vec::with_capacity(base.column_count().saturating_sub(1));
    for (idx, col) in base.columns().iter().enumerate() {
      if idx != pk_idx {
        base_dst_cols.push(result_cols.len());
      }
      result_cols.push(col.with_data(Series::new(col.kind, total_rows)));
    }

    let base_spec = TableJoinSpec {
      table_id: base_table_id,
      fill: base_fill,
      schema: base,
      schema_pk_idx: pk_idx,
      dst_cols: base_dst_cols,
    };

    let mut other_specs = Vec::with_capacity(other_table_ids.len());
    for tid in other_table_ids {
      let t_schema = &base_slots[tid].as_ref().unwrap().table;
      let other_pk_idx = t_schema.pk_column();
      let method = fill_arc
        .get(table_names[tid].as_ref())
        .copied()
        .unwrap_or(default_fill);
      let mut dst_cols = Vec::with_capacity(t_schema.column_count().saturating_sub(1));
      for (idx, col) in t_schema.columns().iter().enumerate() {
        if idx != other_pk_idx {
          dst_cols.push(result_cols.len());
          result_cols.push(col.with_data(Series::new(col.kind, total_rows)));
        }
      }
      other_specs.push(TableJoinSpec {
        table_id: tid,
        fill: method,
        schema: t_schema,
        schema_pk_idx: other_pk_idx,
        dst_cols,
      });
    }

    if total_rows > 0 {
      let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(num_objs);
      let chunk_objs_len = (num_objs + num_workers - 1) / num_workers;
      let num_chunks = (num_objs + chunk_objs_len - 1) / chunk_objs_len;
      let chunk_rows = chunk_objs_len * bars;

      let mut worker_cols: Vec<Vec<SeriesMutSlice<'_>>> = (0..num_chunks)
        .map(|_| Vec::with_capacity(result_cols.len()))
        .collect();
      for col in &mut result_cols {
        for (w_idx, slice) in col
          .data
          .chunks_mut(chunk_rows, num_chunks)
          .into_iter()
          .enumerate()
        {
          worker_cols[w_idx].push(slice);
        }
      }

      if num_chunks == 1 {
        let mut slices = worker_cols.into_iter().next().unwrap();
        process_join_chunk(
          &obj_entries,
          &mut slices,
          &base_obj,
          &base_ctx,
          pk_idx,
          &base_spec,
          &other_specs,
          bars,
        )?;
      } else {
        std::thread::scope(|s| -> Result<()> {
          let mut handles = Vec::with_capacity(num_chunks);
          let base_obj_ref = base_obj.as_str();
          let base_ctx_ref = &*base_ctx;
          let base_spec_ref = &base_spec;
          let other_specs_ref = &other_specs;

          for (chunk_entries, mut local_slices) in obj_entries
            .chunks(chunk_objs_len)
            .zip(worker_cols.into_iter())
          {
            handles.push(s.spawn(move || {
              process_join_chunk(
                chunk_entries,
                &mut local_slices,
                base_obj_ref,
                base_ctx_ref,
                pk_idx,
                base_spec_ref,
                other_specs_ref,
                bars,
              )
            }));
          }

          for handle in handles {
            handle
              .join()
              .map_err(|_| anyhow!("Join worker thread panicked"))??;
          }
          Ok(())
        })?;
      }
    }

    let objs: Vec<String> = obj_entries
      .iter_mut()
      .map(|(obj, _)| std::mem::take(obj))
      .collect();
    defer_drop_join_entries(obj_entries);

    Ok((Table::from_columns(result_cols), objs, bars))
  }

  fn snapshot(
    &self,
    tables: Vec<String>,
    objects: Vec<String>,
    date: Option<&str>,
  ) -> Result<Table> {
    let sql = self.snapshot_sql(&tables, &objects, date);
    let full = self.async_query(sql)?;
    let mut objs = full.keys().cloned().collect::<Vec<_>>();

    objs.sort();

    let pk_meta = HashMap::from([("primary_key".to_string(), Variant::Bool(true))]);
    let mut result_cols = vec![
      Field::new_with_data("obj", DataType::String, objs.clone().into()).with_metadata(pk_meta),
    ];

    for t in &tables {
      let t_name = t.split_once(':').map_or(t.as_str(), |(name, _)| name);
      let Some(t_schema) = full.values().find_map(|m| m.get(t_name)) else {
        continue;
      };

      let mut table_cols: Vec<Field> = t_schema
        .columns()
        .iter()
        .map(|col| col.to_empty_with_capacity(objs.len()))
        .collect();

      for obj in &objs {
        if let Some(obj_table) = full[obj].get(t_name)
          && obj_table.row_count() > 0
        {
          for (dst, src) in table_cols.iter_mut().zip(obj_table.columns().iter()) {
            extend_series_with_fill(&mut dst.data, &src.data, &[0], FillMethod::Nan);
          }
        } else {
          for (dst, src) in table_cols.iter_mut().zip(t_schema.columns().iter()) {
            extend_series_with_fill(&mut dst.data, &src.data, &[usize::MAX], FillMethod::Nan);
          }
        }
      }

      result_cols.extend(table_cols);
    }

    Ok(Table::from_columns(result_cols))
  }

  fn async_query(&self, sql: String) -> Result<GroupedTables> {
    let batches = self.async_query_batches(sql)?;
    let total: usize = batches.iter().map(|b| b.len()).sum();
    let mut m = HashMap::with_capacity(total);
    for batch in batches {
      for (obj, table_name, table) in batch {
        if !obj.is_empty() && !table_name.is_empty() {
          let mm = m.entry(obj).or_insert_with(|| HashMap::with_capacity(1));
          mm.insert(table_name.as_ref().to_owned(), table);
        }
      }
    }
    Ok(m)
  }

  fn async_query_batches(&self, sql: String) -> Result<Vec<FrameBatch>> {
    let (tx, rx) = oneshot::channel();

    let conn = self.clone();
    get_runtime().spawn(async {
      let result = query_msd_batches(conn, sql).await;
      let _ = tx.send(result);
    });

    if let Ok(res) = rx.blocking_recv() {
      res
    } else {
      bail!("internal error to query")
    }
  }

  fn query_map<'py, F>(&self, py: Python<'py>, sql: String, mut handler: F) -> Result<()>
  where
    F: FnMut((String, Arc<str>, Table)) -> Result<()>,
  {
    let (tx, mut rx) = mpsc::channel(16);

    let conn = self.clone();
    get_runtime().spawn(async move {
      if let Err(err) = query_msd_stream(conn, sql, &tx).await {
        let _ = tx.send(Err(err)).await;
      }
    });

    while let Some(res) = py.detach(|| rx.blocking_recv()) {
      let batch = res?;
      for item in batch {
        handler(item)?;
      }
    }

    Ok(())
  }

  async fn async_import_csv(
    &self,
    table: &str,
    body: reqwest::Body,
    header: bool,
    delimiter: Option<&str>,
  ) -> Result<serde_json::Value> {
    let skip = if header { 1 } else { 0 };
    let mut url = format!("{}/table/{}?skip={}", self.url, table, skip);
    if let Some(d) = delimiter {
      url.push_str(&format!("&delimiter={}", d));
    }
    put_import(self.clone(), url, "text/csv", body).await
  }

  async fn async_import_table(
    &self,
    table: &str,
    body: reqwest::Body,
  ) -> Result<serde_json::Value> {
    let url = format!("{}/table/{}", self.url, table);
    put_import(self.clone(), url, "application/x-msd-table-frame", body).await
  }

  fn import_csv(
    &self,
    table: &str,
    body: impl Into<reqwest::Body>,
    header: bool,
    delimiter: Option<&str>,
  ) -> Result<serde_json::Value> {
    let (tx, rx) = oneshot::channel();
    let conn = self.clone();
    let table = table.to_string();
    let delimiter = delimiter.map(|s| s.to_string());
    let body = body.into();
    get_runtime().spawn(async move {
      let result = conn
        .async_import_csv(&table, body, header, delimiter.as_deref())
        .await;
      let _ = tx.send(result);
    });
    if let Ok(res) = rx.blocking_recv() {
      res
    } else {
      bail!("internal error during import")
    }
  }

  fn import_table(&self, table: &str, body: impl Into<reqwest::Body>) -> Result<serde_json::Value> {
    let (tx, rx) = oneshot::channel();
    let conn = self.clone();
    let table = table.to_string();
    let body = body.into();
    get_runtime().spawn(async move {
      let result = conn.async_import_table(&table, body).await;
      let _ = tx.send(result);
    });
    if let Ok(res) = rx.blocking_recv() {
      res
    } else {
      bail!("internal error during import")
    }
  }

  fn snapshot_sql(
    &self,
    tables: &Vec<String>,
    objects: &Vec<String>,
    date: Option<&str>,
  ) -> String {
    let objects_where = if objects.is_empty() {
      "".to_string()
    } else {
      let mut sql = " WHERE obj IN (".to_string();
      for (i, obj) in objects.iter().enumerate() {
        if i > 0 {
          sql.push_str(",");
        }
        sql.push('\'');
        sql.push_str(obj);
        sql.push('\'');
      }
      sql.push_str(") ");
      sql
    };

    let mut sql = String::new();
    for table in tables.iter() {
      let (table, fields) = table.split_once(':').unwrap_or((&table, "*"));

      let (table, ts_filter) = table.split_once('@').unwrap_or((table, ""));

      sql.push_str("SELECT ");
      sql.push_str(fields);
      sql.push_str(" FROM ");
      sql.push_str(table);
      sql.push_str(&objects_where);
      if let Some(date) = date
        && !date.is_empty()
      {
        sql.push_str(" AND ts <= '");
        sql.push_str(date);
        sql.push('\'');
      }
      if !ts_filter.is_empty() {
        sql.push_str(" AND ");
        sql.push_str(ts_filter);
      }
      sql.push_str(" LIMIT -1;");
    }
    sql
  }
}

const STREAM_BATCH_MAX_FRAMES: usize = 128;
const STREAM_BATCH_MAX_BYTES: usize = 512 * 1024;

fn scan_complete_frames(buf: &[u8], scan_pos: &mut usize, frames_in_buf: &mut usize) -> Result<()> {
  while buf.len() - *scan_pos >= 8 {
    let (header_size, data_size) = match check_table_frame(&buf[*scan_pos..*scan_pos + 8]) {
      Ok(size) => size,
      Err(err) => {
        bail!("invalid table frame: {}", err);
      }
    };
    let frame_end = *scan_pos + header_size + data_size;
    if buf.len() < frame_end {
      break;
    }
    *scan_pos = frame_end;
    *frames_in_buf += 1;
  }
  Ok(())
}

fn take_ready_batch(
  buf: &mut Vec<u8>,
  scan_pos: &mut usize,
  frames_in_buf: &mut usize,
) -> Option<(Vec<u8>, usize)> {
  if *frames_in_buf < STREAM_BATCH_MAX_FRAMES && *scan_pos < STREAM_BATCH_MAX_BYTES {
    return None;
  }
  let rem = buf.len() - *scan_pos;
  let mut next_buf = Vec::with_capacity(rem.max(128 * 1024));
  if *scan_pos < buf.len() {
    next_buf.extend_from_slice(&buf[*scan_pos..]);
    buf.truncate(*scan_pos);
  }
  let batch_buf = std::mem::replace(buf, next_buf);
  let count = *frames_in_buf;
  *scan_pos = 0;
  *frames_in_buf = 0;
  Some((batch_buf, count))
}

async fn query_msd_batches(conn: MsdConnection, sql: String) -> Result<Vec<FrameBatch>> {
  let url = format!("{}/query", conn.url);

  let req = conn
    .client
    .post(&url)
    .json(&serde_json::json!({ "query": sql }));

  let mut resp = req.send().await?;

  let mut buf = Vec::with_capacity(128 * 1024);
  let mut scan_pos = 0usize;
  let mut frames_in_buf = 0usize;
  let mut tasks = Vec::new();

  while let Some(chunk) = resp.chunk().await? {
    buf.extend_from_slice(&chunk);
    scan_complete_frames(&buf, &mut scan_pos, &mut frames_in_buf)?;

    if let Some((batch_buf, count)) = take_ready_batch(&mut buf, &mut scan_pos, &mut frames_in_buf) {
      tasks.push(tokio::spawn(async move {
        unpack_frame_batch(&batch_buf, count)
      }));
    }
  }

  if scan_pos != buf.len() {
    bail!("unexpected EOF in table frame");
  }

  let tail_batch = if frames_in_buf > 0 {
    Some(unpack_frame_batch(&buf, frames_in_buf)?)
  } else {
    None
  };

  let mut batches = Vec::with_capacity(tasks.len() + usize::from(tail_batch.is_some()));
  for task in tasks {
    let batch = task.await??;
    batches.push(batch);
  }
  if let Some(tail) = tail_batch {
    batches.push(tail);
  }

  Ok(batches)
}

async fn stream_query_batches<T, F>(
  conn: MsdConnection,
  sql: String,
  tx: &mpsc::Sender<Result<T>>,
  mut unpack_fn: F,
) -> Result<()>
where
  T: Send + 'static,
  F: FnMut(Vec<u8>, usize) -> tokio::task::JoinHandle<Result<T>>,
{
  let url = format!("{}/query", conn.url);

  let req = conn
    .client
    .post(&url)
    .json(&serde_json::json!({ "query": sql }));

  let mut resp = req.send().await?;

  let mut buf = Vec::with_capacity(128 * 1024);
  let mut scan_pos = 0usize;
  let mut frames_in_buf = 0usize;
  let mut tasks = std::collections::VecDeque::with_capacity(16);

  while let Some(chunk) = resp.chunk().await? {
    buf.extend_from_slice(&chunk);
    scan_complete_frames(&buf, &mut scan_pos, &mut frames_in_buf)?;

    if let Some((batch_buf, count)) = take_ready_batch(&mut buf, &mut scan_pos, &mut frames_in_buf) {
      tasks.push_back(unpack_fn(batch_buf, count));

      if tasks.len() >= 8
        && let Some(task) = tasks.pop_front()
      {
        let batch = task.await??;
        if tx.send(Ok(batch)).await.is_err() {
          return Ok(());
        }
      }
    }
  }

  if scan_pos != buf.len() {
    bail!("unexpected EOF in table frame");
  }

  for task in tasks {
    let batch = task.await??;
    if tx.send(Ok(batch)).await.is_err() {
      return Ok(());
    }
  }

  if frames_in_buf > 0 {
    let tail = unpack_fn(buf, frames_in_buf).await??;
    let _ = tx.send(Ok(tail)).await;
  }

  Ok(())
}

async fn query_msd_stream(
  conn: MsdConnection,
  sql: String,
  tx: &mpsc::Sender<Result<FrameBatch>>,
) -> Result<()> {
  stream_query_batches(conn, sql, tx, |batch_buf, count| {
    tokio::spawn(async move { Ok(unpack_frame_batch(&batch_buf, count)?) })
  })
  .await
}

fn unpack_and_plan_join_batch(
  batch_buf: &[u8],
  count: usize,
  base_obj: &str,
  base_table: &str,
  fill: &HashMap<String, FillMethod>,
  default_fill: FillMethod,
  base_ctx: &OnceLock<Arc<JoinBaseContext>>,
) -> Result<Vec<JoinBatchItem>> {
  let frames = unpack_frame_batch(batch_buf, count)?;

  let mut cached_ctx = base_ctx.get().cloned();
  if cached_ctx.is_none() {
    for (obj, table_name, table) in &frames {
      if obj == base_obj && table_name.as_ref() == base_table && !table.columns().is_empty() {
        let pk_idx = table.pk_column();
        let pk_col = table.columns()[pk_idx].clone();
        let pk_strictly_increasing = pk_col.data.is_strictly_increasing();
        let ctx = Arc::new(JoinBaseContext {
          pk_col,
          pk_strictly_increasing,
        });
        let _ = base_ctx.set(ctx);
        cached_ctx = base_ctx.get().cloned();
        break;
      }
    }
  }

  let mut fill_cache: Option<(Arc<str>, FillMethod)> = None;
  let mut items = Vec::with_capacity(frames.len());
  for (obj, table_name, table) in frames {
    let plan = if let Some(ctx) = cached_ctx.as_ref()
      && !(obj == base_obj && table_name.as_ref() == base_table)
      && !table.columns().is_empty()
    {
      let t_pk = &table.columns()[table.pk_column()];
      if t_pk.kind == ctx.pk_col.kind {
        let method = resolve_fill(fill, default_fill, &table_name, &mut fill_cache);
        Some(compute_join_plan(
          &ctx.pk_col,
          t_pk,
          ctx.pk_strictly_increasing,
          method,
        )?)
      } else {
        None
      }
    } else {
      None
    };
    items.push(JoinBatchItem {
      obj,
      table_name,
      table,
      plan,
    });
  }

  Ok(items)
}

#[allow(clippy::too_many_arguments)]
async fn query_msd_join_stream(
  conn: MsdConnection,
  sql: String,
  base_obj: String,
  base_table: String,
  fill: Arc<HashMap<String, FillMethod>>,
  default_fill: FillMethod,
  base_ctx: Arc<OnceLock<Arc<JoinBaseContext>>>,
  tx: &mpsc::Sender<Result<Vec<JoinBatchItem>>>,
) -> Result<()> {
  let base_obj_arc: Arc<str> = Arc::from(base_obj.as_str());
  let base_table_arc: Arc<str> = Arc::from(base_table.as_str());

  stream_query_batches(conn, sql, tx, |batch_buf, count| {
    let base_obj = Arc::clone(&base_obj_arc);
    let base_table = Arc::clone(&base_table_arc);
    let fill = Arc::clone(&fill);
    let base_ctx = Arc::clone(&base_ctx);
    tokio::spawn(async move {
      unpack_and_plan_join_batch(
        &batch_buf,
        count,
        &base_obj,
        &base_table,
        &fill,
        default_fill,
        &base_ctx,
      )
    })
  })
  .await
}

async fn put_import(
  conn: MsdConnection,
  url: String,
  content_type: &'static str,
  body: reqwest::Body,
) -> Result<serde_json::Value> {
  let req = conn
    .client
    .put(&url)
    .header(reqwest::header::CONTENT_TYPE, content_type)
    .body(body);

  let resp = req.send().await?;
  if !resp.status().is_success() {
    let status = resp.status();
    let txt = resp.text().await.unwrap_or_default();
    bail!("Import failed: {} - {}", status, txt);
  }
  let txt = resp.text().await.unwrap_or_default();
  if let Ok(json) = serde_json::from_str::<serde_json::Value>(&txt) {
    Ok(json)
  } else {
    Ok(serde_json::json!({ "response": txt }))
  }
}

fn json_to_py<'py>(py: Python<'py>, value: serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
  match value {
    serde_json::Value::Null => Ok(PyNone::get(py).to_owned().into_any()),
    serde_json::Value::Bool(b) => Ok(PyBool::new(py, b).to_owned().into_any()),
    serde_json::Value::Number(n) => {
      if let Some(i) = n.as_i64() {
        Ok(PyInt::new(py, i).into_any())
      } else if let Some(u) = n.as_u64() {
        Ok(PyInt::new(py, u).into_any())
      } else if let Some(f) = n.as_f64() {
        Ok(PyFloat::new(py, f).into_any())
      } else {
        Ok(PyNone::get(py).to_owned().into_any())
      }
    }
    serde_json::Value::String(s) => Ok(PyString::new(py, &s).into_any()),
    serde_json::Value::Array(arr) => {
      let list = PyList::empty(py);
      for item in arr {
        list.append(json_to_py(py, item)?)?;
      }
      Ok(list.into_any())
    }
    serde_json::Value::Object(obj) => {
      let dict = PyDict::new(py);
      for (k, v) in obj {
        dict.set_item(k, json_to_py(py, v)?)?;
      }
      Ok(dict.into_any())
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use msd_table::pack_table_frame;
  use tokio::io::{AsyncReadExt, AsyncWriteExt};
  use tokio::net::TcpListener;

  #[test]
  fn test_import_csv() {
    unsafe {
      std::env::set_var("no_proxy", "127.0.0.1,localhost");
      std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
    }

    let (url, handle) = get_runtime().block_on(async {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let port = listener.local_addr().unwrap().port();
      let url = format!("http://127.0.0.1:{}", port);

      let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut req_buf = vec![0u8; 1024];
        let n = socket.read(&mut req_buf).await.unwrap();
        let req_str = String::from_utf8_lossy(&req_buf[..n]);
        assert!(req_str.starts_with("PUT /table/my_table?skip=1"));
        assert!(req_str.contains("content-type: text/csv"));

        let resp_body = r#"{"total_rows":5,"time_used_ms":10,"rows_per_sec":500}"#;
        let resp = format!(
          "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
          resp_body.len(),
          resp_body
        );
        socket.write_all(resp.as_bytes()).await.unwrap();
      });

      (url, handle)
    });

    let conn = MsdConnection::new(&url, "", false).unwrap();
    let csv_data = b"col1,col2\n1,2\n3,4\n5,6\n7,8\n9,10\n".to_vec();
    let res = conn.import_csv("my_table", csv_data, true, None).unwrap();
    assert_eq!(res["total_rows"], 5);
    assert_eq!(res["time_used_ms"], 10);
    assert_eq!(res["rows_per_sec"], 500);

    get_runtime().block_on(handle).unwrap();
  }

  #[test]
  fn test_import_table() {
    unsafe {
      std::env::set_var("no_proxy", "127.0.0.1,localhost");
      std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
    }

    let (url, handle) = get_runtime().block_on(async {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let port = listener.local_addr().unwrap().port();
      let url = format!("http://127.0.0.1:{}", port);

      let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut req_buf = vec![0u8; 1024];
        let n = socket.read(&mut req_buf).await.unwrap();
        let req_str = String::from_utf8_lossy(&req_buf[..n]);
        assert!(req_str.starts_with("PUT /table/my_table"));
        assert!(req_str.contains("content-type: application/x-msd-table-frame"));

        let resp_body = r#"{"total_rows":10,"time_used_ms":15,"rows_per_sec":667}"#;
        let resp = format!(
          "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
          resp_body.len(),
          resp_body
        );
        socket.write_all(resp.as_bytes()).await.unwrap();
      });

      (url, handle)
    });

    let conn = MsdConnection::new(&url, "", false).unwrap();
    let mut t = Table::from_columns(vec![Field::new_with_data(
      "col1",
      DataType::Int64,
      vec![1i64, 2i64].into(),
    )]);
    let _ = t.set_table_meta("table", "my_table".into());
    let frame = pack_table_frame(&t);
    let res = conn.import_table("my_table", frame).unwrap();
    assert_eq!(res["total_rows"], 10);
    assert_eq!(res["time_used_ms"], 15);
    assert_eq!(res["rows_per_sec"], 667);

    get_runtime().block_on(handle).unwrap();
  }

  #[test]
  fn test_snapshot_sql() {
    let conn = MsdConnection::new("http://127.0.0.1:8080", "", false).unwrap();
    let tables = vec![
      "stock_kline_1d".to_string(),
      "stock_financial@ts.month=12:f001,f002".to_string(),
    ];
    let objects = vec!["SH600000".to_string(), "SH600001".to_string()];

    let sql_no_date = conn.snapshot_sql(&tables, &objects, None);
    assert_eq!(
      sql_no_date,
      "SELECT * FROM stock_kline_1d WHERE obj IN ('SH600000','SH600001')  LIMIT -1;SELECT f001,f002 FROM stock_financial WHERE obj IN ('SH600000','SH600001')  AND ts.month=12 LIMIT -1;"
    );

    let sql_with_date = conn.snapshot_sql(&tables, &objects, Some("2025-12-31"));
    assert_eq!(
      sql_with_date,
      "SELECT * FROM stock_kline_1d WHERE obj IN ('SH600000','SH600001')  AND ts <= '2025-12-31' LIMIT -1;SELECT f001,f002 FROM stock_financial WHERE obj IN ('SH600000','SH600001')  AND ts <= '2025-12-31' AND ts.month=12 LIMIT -1;"
    );
  }

  #[test]
  fn test_concat_batches_single_table() {
    let t1 = Table::from_columns(vec![Field::new_with_data(
      "val",
      DataType::Int64,
      vec![10i64, 20i64].into(),
    )]);
    let t2 = Table::from_columns(vec![Field::new_with_data(
      "val",
      DataType::Int64,
      vec![30i64, 40i64, 50i64].into(),
    )]);

    let batch = vec![
      ("SH600000".to_string(), Arc::from("quote"), t1),
      ("SZ000001".to_string(), Arc::from("quote"), t2),
    ];

    let result = concat_batches(vec![batch]).unwrap();
    assert_eq!(result.len(), 1);

    let (table_name, table, ranges) = &result[0];
    assert_eq!(table_name.as_ref(), "quote");
    assert_eq!(table.row_count(), 5);
    assert_eq!(ranges.len(), 2);
    assert_eq!(ranges[0], ("SH600000".to_string(), 0, 2));
    assert_eq!(ranges[1], ("SZ000001".to_string(), 2, 5));

    // Verify concatenated cell values
    assert_eq!(
      table.get_cell(0, 0),
      Some(msd_table::VariantRef::Int64(&10))
    );
    assert_eq!(
      table.get_cell(1, 0),
      Some(msd_table::VariantRef::Int64(&20))
    );
    assert_eq!(
      table.get_cell(2, 0),
      Some(msd_table::VariantRef::Int64(&30))
    );
    assert_eq!(
      table.get_cell(3, 0),
      Some(msd_table::VariantRef::Int64(&40))
    );
    assert_eq!(
      table.get_cell(4, 0),
      Some(msd_table::VariantRef::Int64(&50))
    );
  }

  #[test]
  fn test_concat_batches_multi_tables() {
    let t1 = Table::from_columns(vec![Field::new_with_data(
      "val",
      DataType::Int64,
      vec![10i64].into(),
    )]);
    let t2 = Table::from_columns(vec![Field::new_with_data(
      "col_str",
      DataType::String,
      vec!["abc".to_string(), "def".to_string()].into(),
    )]);
    let t3 = Table::from_columns(vec![Field::new_with_data(
      "val",
      DataType::Int64,
      vec![20i64, 30i64].into(),
    )]);

    let batch = vec![
      ("SH600000".to_string(), Arc::from("quote"), t1),
      ("SH600000".to_string(), Arc::from("financial"), t2),
      ("SZ000001".to_string(), Arc::from("quote"), t3),
    ];

    let result = concat_batches(vec![batch]).unwrap();
    assert_eq!(result.len(), 2);

    let (tname0, table0, ranges0) = &result[0];
    assert_eq!(tname0.as_ref(), "quote");
    assert_eq!(table0.row_count(), 3);
    assert_eq!(ranges0.len(), 2);
    assert_eq!(ranges0[0], ("SH600000".to_string(), 0, 1));
    assert_eq!(ranges0[1], ("SZ000001".to_string(), 1, 3));

    let (tname1, table1, ranges1) = &result[1];
    assert_eq!(tname1.as_ref(), "financial");
    assert_eq!(table1.row_count(), 2);
    assert_eq!(ranges1.len(), 1);
    assert_eq!(ranges1[0], ("SH600000".to_string(), 0, 2));
  }

  #[test]
  fn test_concat_batches_empty() {
    let result = concat_batches(vec![]).unwrap();
    assert!(result.is_empty());

    let result2 = concat_batches(vec![vec![]]).unwrap();
    assert!(result2.is_empty());
  }

  fn make_test_frame(obj: &str, table_name: &str, ts: Vec<i64>, vals: Vec<f64>) -> Vec<u8> {
    let pk_meta = HashMap::from([("primary_key".to_string(), Variant::Bool(true))]);
    let mut t = Table::from_columns(vec![
      Field::new_with_data("ts", DataType::DateTime, ts.into()).with_metadata(pk_meta),
      Field::new_with_data("val", DataType::Float64, vals.into()),
    ]);
    let _ = t.set_table_meta("table", table_name.into());
    let _ = t.set_table_meta("obj", obj.into());
    pack_table_frame(&t)
  }

  fn serve_frames_once(payload: Vec<u8>, chunk_size: usize) -> (String, tokio::task::JoinHandle<()>) {
    unsafe {
      std::env::set_var("no_proxy", "127.0.0.1,localhost");
      std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
    }

    get_runtime().block_on(async move {
      let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
      let port = listener.local_addr().unwrap().port();
      let url = format!("http://127.0.0.1:{}", port);

      let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut req_buf = vec![0u8; 4096];
        let _ = socket.read(&mut req_buf).await.unwrap();

        let header = format!(
          "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-msd-table-frame\r\n\r\n",
          payload.len()
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        for piece in payload.chunks(chunk_size) {
          socket.write_all(piece).await.unwrap();
        }
      });

      (url, handle)
    })
  }

  fn build_join_test_frames(base_first: bool) -> Vec<u8> {
    let base_ts = vec![100i64, 200, 300, 400];
    let mut frames: Vec<Vec<u8>> = Vec::new();

    if base_first {
      frames.push(make_test_frame("SH600000", "kline", base_ts.clone(), vec![1.0, 2.0, 3.0, 4.0]));
      frames.push(make_test_frame("SH600000", "shares", vec![50, 250], vec![10.0, 20.0]));
    }

    // 18 objects across multiple workers, mixed exact-match, partial ts, missing base_table, missing shares
    for i in 1..=18 {
      let obj = format!("SH6000{:02}", i);
      if i == 5 {
        // Missing kline entirely, only has shares
        frames.push(make_test_frame(&obj, "shares", vec![150], vec![55.0]));
        continue;
      }
      if i % 3 == 0 {
        // Missing ts=200 in kline
        frames.push(make_test_frame(&obj, "kline", vec![100, 300, 400], vec![10.0, 30.0, 40.0]));
      } else {
        // Exact match with base_obj
        let base_v = (i as f64) * 100.0;
        frames.push(make_test_frame(
          &obj,
          "kline",
          base_ts.clone(),
          vec![base_v + 1.0, base_v + 2.0, base_v + 3.0, base_v + 4.0],
        ));
      }
      if i != 7 {
        frames.push(make_test_frame(&obj, "shares", vec![150, 350], vec![i as f64, (i * 2) as f64]));
      }
    }

    if !base_first {
      frames.push(make_test_frame("SH600000", "shares", vec![50, 250], vec![10.0, 20.0]));
      frames.push(make_test_frame("SH600000", "kline", base_ts, vec![1.0, 2.0, 3.0, 4.0]));
    }

    frames.into_iter().flatten().collect()
  }

  #[test]
  fn test_query_join_streaming_base_first_and_base_last() {
    for base_first in [true, false] {
      let payload = build_join_test_frames(base_first);
      let (url, handle) = serve_frames_once(payload, 97);

      let conn = MsdConnection::new(&url, "", false).unwrap();
      let fill = HashMap::from([("shares".to_string(), FillMethod::Backward)]);
      let (table, objs, bars) = conn
        .query_join(
          "SELECT * FROM kline; SELECT * FROM shares;".to_string(),
          "kline".to_string(),
          fill,
          "SH600000".to_string(),
        )
        .unwrap();

      get_runtime().block_on(handle).unwrap();

      assert_eq!(bars, 4);
      assert_eq!(objs.len(), 19);
      assert_eq!(objs[0], "SH600000");
      assert_eq!(objs[18], "SH600018");
      assert_eq!(table.row_count(), 19 * 4);
      assert_eq!(table.column_count(), 3);

      // SH600000 (rows 0..4): exact kline [1,2,3,4], backward shares [10,10,20,20]
      let ts_col = table.columns()[0].data.get_datetime().unwrap();
      let kline_val = table.columns()[1].data.get_float64().unwrap();
      let shares_val = table.columns()[2].data.get_float64().unwrap();

      assert_eq!(&ts_col[0..4], &[100, 200, 300, 400]);
      assert_eq!(&kline_val[0..4], &[1.0, 2.0, 3.0, 4.0]);
      assert_eq!(&shares_val[0..4], &[10.0, 10.0, 20.0, 20.0]);

      // SH600003 (obj_idx = 3, rows 12..16): kline missing ts=200 (default FillMethod::Zero => 0.0)
      let r3 = 3 * 4;
      assert_eq!(&ts_col[r3..r3 + 4], &[100, 200, 300, 400]);
      assert_eq!(&kline_val[r3..r3 + 4], &[10.0, 0.0, 30.0, 40.0]);
      // shares at [150, 350] joined backward onto [100, 200, 300, 400] => [NaN, 3.0, 3.0, 6.0]
      assert!(shares_val[r3].is_nan());
      assert_eq!(&shares_val[r3 + 1..r3 + 4], &[3.0, 3.0, 6.0]);

      // SH600005 (obj_idx = 5, rows 20..24): missing kline entirely => [0,0,0,0], shares=[NaN,55,55,55]
      let r5 = 5 * 4;
      assert_eq!(&kline_val[r5..r5 + 4], &[0.0, 0.0, 0.0, 0.0]);
      assert!(shares_val[r5].is_nan());
      assert_eq!(&shares_val[r5 + 1..r5 + 4], &[55.0, 55.0, 55.0]);

      // SH600007 (obj_idx = 7, rows 28..32): exact match kline, missing shares => all NaN
      let r7 = 7 * 4;
      assert_eq!(&kline_val[r7..r7 + 4], &[701.0, 702.0, 703.0, 704.0]);
      assert!(shares_val[r7..r7 + 4].iter().all(|v| v.is_nan()));
    }
  }
}
