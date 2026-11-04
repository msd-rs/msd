use std::collections::HashMap;
use std::sync::Arc;

use msd_table::DataType;
use msd_table::Field;
use msd_table::FillMethod;
use msd_table::Result;
use msd_table::Table;
use msd_table::Variant;
use msd_table::bail;
use msd_table::check_table_frame;
use msd_table::compute_joined_indices;
use msd_table::extend_series_with_fill;
use msd_table::unpack_frame_batch;
use pyo3::exceptions::PyTypeError;
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
  fn query_map<'py>(&self, py: Python<'py>, sql: String, handler: Bound<'py, PyAny>) -> PyResult<()> {
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

#[derive(Debug, Clone)]
struct MsdConnection {
  client: Client,
  url: String,
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
    let full = self.async_query(sql)?;
    let Some(base_obj_tables) = full.get(&base_obj) else {
      bail!("base_obj {} not found", base_obj);
    };
    let Some(base) = base_obj_tables.get(&base_table) else {
      bail!("base_table {} not found in {}", base_table, base_obj);
    };

    let mut objs = full.keys().cloned().collect::<Vec<_>>();
    objs.sort();

    let mut other_tables = base_obj_tables
      .keys()
      .filter(|&k| k != &base_table)
      .cloned()
      .collect::<Vec<_>>();
    other_tables.sort();

    let pk_idx = base.pk_column();
    let base_pk_col = &base.columns()[pk_idx];
    let bars = base_pk_col.data.len();
    let total_rows = objs.len() * bars;

    let default_fill = fill.get("*").copied().unwrap_or_default();
    let base_fill = fill.get(&base_table).copied().unwrap_or(default_fill);

    let mut result_cols = Vec::new();
    for col in base.columns() {
      result_cols.push(col.to_empty_with_capacity(total_rows));
    }
    for t_name in &other_tables {
      let t_schema = &base_obj_tables[t_name];
      let other_pk_idx = t_schema.pk_column();
      for (idx, col) in t_schema.columns().iter().enumerate() {
        if idx != other_pk_idx {
          result_cols.push(col.to_empty_with_capacity(total_rows));
        }
      }
    }

    let mut indices = Vec::with_capacity(bars);
    let empty_indices = vec![usize::MAX; bars];

    for obj in &objs {
      let obj_tables = &full[obj];
      result_cols[pk_idx].data.extend(&base_pk_col.data, false)?;

      if obj == &base_obj {
        for (idx, col) in base.columns().iter().enumerate() {
          if idx != pk_idx {
            result_cols[idx].data.extend(&col.data, false)?;
          }
        }
      } else if let Some(t) = obj_tables.get(&base_table)
        && !t.columns().is_empty()
      {
        let t_pk = &t.columns()[t.pk_column()];
        compute_joined_indices(base_pk_col, t_pk, base_fill, &mut indices)?;
        for (idx, col) in t.columns().iter().enumerate() {
          if idx != pk_idx {
            extend_series_with_fill(&mut result_cols[idx].data, &col.data, &indices, base_fill);
          }
        }
      } else {
        for (idx, col) in base.columns().iter().enumerate() {
          if idx != pk_idx {
            extend_series_with_fill(
              &mut result_cols[idx].data,
              &col.data,
              &empty_indices,
              base_fill,
            );
          }
        }
      }

      let mut col_offset = base.column_count();
      for t_name in &other_tables {
        let method = fill.get(t_name).copied().unwrap_or(default_fill);
        let t_schema = &base_obj_tables[t_name];
        let other_pk_idx = t_schema.pk_column();

        if let Some(t) = obj_tables.get(t_name)
          && !t.columns().is_empty()
        {
          let t_pk = &t.columns()[other_pk_idx];
          compute_joined_indices(base_pk_col, t_pk, method, &mut indices)?;
          for (idx, col) in t.columns().iter().enumerate() {
            if idx == other_pk_idx {
              continue;
            }
            extend_series_with_fill(&mut result_cols[col_offset].data, &col.data, &indices, method);
            col_offset += 1;
          }
        } else {
          for (idx, col) in t_schema.columns().iter().enumerate() {
            if idx == other_pk_idx {
              continue;
            }
            extend_series_with_fill(
              &mut result_cols[col_offset].data,
              &col.data,
              &empty_indices,
              method,
            );
            col_offset += 1;
          }
        }
      }
    }

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
          let mm = m
            .entry(obj)
            .or_insert_with(|| HashMap::with_capacity(1));
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

    while buf.len() - scan_pos >= 8 {
      let (header_size, data_size) = match check_table_frame(&buf[scan_pos..scan_pos + 8]) {
        Ok(size) => size,
        Err(err) => {
          bail!("invalid table frame: {}", err);
        }
      };
      let frame_end = scan_pos + header_size + data_size;
      if buf.len() < frame_end {
        break;
      }
      scan_pos = frame_end;
      frames_in_buf += 1;
    }

    if frames_in_buf >= 128 || scan_pos >= 64 * 1024 {
      let mut next_buf = Vec::with_capacity(128 * 1024);
      if scan_pos < buf.len() {
        next_buf.extend_from_slice(&buf[scan_pos..]);
        buf.truncate(scan_pos);
      }
      let batch_buf = std::mem::replace(&mut buf, next_buf);
      let count = frames_in_buf;
      scan_pos = 0;
      frames_in_buf = 0;

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

async fn query_msd_stream(
  conn: MsdConnection,
  sql: String,
  tx: &mpsc::Sender<Result<FrameBatch>>,
) -> Result<()> {
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

    while buf.len() - scan_pos >= 8 {
      let (header_size, data_size) = match check_table_frame(&buf[scan_pos..scan_pos + 8]) {
        Ok(size) => size,
        Err(err) => {
          bail!("invalid table frame: {}", err);
        }
      };
      let frame_end = scan_pos + header_size + data_size;
      if buf.len() < frame_end {
        break;
      }
      scan_pos = frame_end;
      frames_in_buf += 1;
    }

    if frames_in_buf >= 128 || scan_pos >= 64 * 1024 {
      let mut next_buf = Vec::with_capacity(128 * 1024);
      if scan_pos < buf.len() {
        next_buf.extend_from_slice(&buf[scan_pos..]);
        buf.truncate(scan_pos);
      }
      let batch_buf = std::mem::replace(&mut buf, next_buf);
      let count = frames_in_buf;
      scan_pos = 0;
      frames_in_buf = 0;

      tasks.push_back(tokio::spawn(async move {
        unpack_frame_batch(&batch_buf, count)
      }));

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
    let tail = unpack_frame_batch(&buf, frames_in_buf)?;
    let _ = tx.send(Ok(tail)).await;
  }

  Ok(())
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
  use tokio::io::AsyncWriteExt;
  use tokio::net::TcpListener;

  #[test]
  fn test_query_map() {
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
        let _ = socket.read(&mut req_buf).await.unwrap();

        let mut t1 = Table::from_columns(vec![Field::new_with_data(
          "col1",
          DataType::Int64,
          vec![1i64, 2i64].into(),
        )]);
        let _ = t1.set_table_meta("table", "table1".into());
        let _ = t1.set_table_meta("obj", "obj1".into());
        let frame1 = pack_table_frame(&t1);

        let mut t2 = Table::from_columns(vec![Field::new_with_data(
          "col2",
          DataType::String,
          vec!["hello".to_string(), "world".to_string()].into(),
        )]);
        let _ = t2.set_table_meta("table", "table2".into());
        let _ = t2.set_table_meta("obj", "obj2".into());
        let frame2 = pack_table_frame(&t2);

        let mut body = frame1;
        body.extend_from_slice(&frame2);

        let resp = format!(
          "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
          body.len()
        );
        socket.write_all(resp.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
      });

      (url, handle)
    });

    let conn = MsdConnection::new(&url, "", false).unwrap();
    let mut received = Vec::new();
    conn
      .query_map("SELECT 1;".to_string(), |t| {
        received.push((
          t.get_table_meta("obj")
            .and_then(|v| v.get_string())
            .cloned()
            .unwrap(),
          t.get_table_meta("table")
            .and_then(|v| v.get_string())
            .cloned()
            .unwrap(),
        ));
        Ok(())
      })
      .unwrap();

    assert_eq!(received.len(), 2);
    assert_eq!(received[0], ("obj1".to_string(), "table1".to_string()));
    assert_eq!(received[1], ("obj2".to_string(), "table2".to_string()));

    get_runtime().block_on(handle).unwrap();
  }

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
}
