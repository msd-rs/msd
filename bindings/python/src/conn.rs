use std::collections::HashMap;

use msd_table::DataType;
use msd_table::Field;
use msd_table::FillMethod;
use msd_table::Result;
use msd_table::Table;
use msd_table::Variant;
use msd_table::bail;
use msd_table::check_table_frame;
use msd_table::unpack_table_frame;
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
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

use crate::py_table::table_to_py_dict;
use crate::py_table::table_to_py_list;
use crate::runtime::get_runtime;

// grouped table {obj: {table_name: table}}
type GroupedTables = HashMap<String, HashMap<String, Table>>;
type GroupedPyTables = HashMap<String, HashMap<String, PyTable>>;

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
  fn query(&self, sql: String) -> PyResult<GroupedPyTables> {
    let m = self.inner.async_query(sql)?;

    let g = m
      .into_iter()
      .map(|(obj, tables)| {
        let tables = tables
          .into_iter()
          .map(|(name, table)| (name, PyTable { inner: table }))
          .collect();
        (obj, tables)
      })
      .collect::<HashMap<_, _>>();
    Ok(g)
  }

  /// query and join the result to one table
  /// the join result will
  /// - rows: count_of_objects * base_table.rows
  /// - columns: sum columns of join_asof for base_obj's tables
  /// return error when base_obj or base_table not found
  ///
  /// fill is map of table name and fill method, it can have a "*" as key for any table not found in map
  #[pyo3(signature = (sql, base_table, fill, base_obj))]
  fn query_join(
    &self,
    sql: String,
    base_table: String,
    fill: HashMap<String, String>,
    base_obj: String,
  ) -> PyResult<(PyTable, Vec<String>, usize)> {
    let fill = fill
      .into_iter()
      .map(|(k, v)| (k, FillMethod::from(v)))
      .collect::<HashMap<String, FillMethod>>();
    let (table, objs, bars) = self.inner.query_join(sql, base_table, fill, base_obj)?;
    Ok((PyTable { inner: table }, objs, bars))
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
  #[pyo3(signature = (tables, objects))]
  fn snapshot(&self, tables: Vec<String>, objects: Vec<String>) -> PyResult<PyTable> {
    let table = self.inner.snapshot(tables, objects)?;
    Ok(PyTable { inner: table })
  }

  /// query and call handler for each table
  #[pyo3(signature = (sql, handler))]
  fn query_map(&self, sql: String, handler: Bound<'_, PyAny>) -> PyResult<()> {
    if !handler.is_callable() {
      return Err(PyTypeError::new_err("handler must be callable"));
    }
    self.inner.query_map(sql, |table| {
      handler.call1((PyTable { inner: table },))?;
      Ok(())
    })?;
    Ok(())
  }
}

#[pyclass(name = "Table")]
pub struct PyTable {
  inner: Table,
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
    self
      .inner
      .get_table_meta("table")
      .and_then(|v| v.get_string())
      .map(|s| s.clone())
      .unwrap_or_default()
  }
  #[getter]
  fn obj(&self) -> String {
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
    let builder = ClientBuilder::new().user_agent("msd-client").zstd(compress);

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
    let base_pk_table = Table::from_columns(vec![base.columns()[pk_idx].clone()]);
    let base_empty = base.to_empty();
    let default_fill = fill.get("*").copied().unwrap_or_default();
    let base_fill = fill.get(&base_table).copied().unwrap_or(default_fill);

    let mut result: Option<Table> = None;

    let bars = base_pk_table.row_count();

    for obj in &objs {
      let obj_tables = &full[obj];
      let mut obj_joined = if obj == &base_obj {
        base.clone()
      } else if let Some(t) = obj_tables.get(&base_table) {
        base_pk_table.join_asof(t, base_fill)?
      } else {
        base_pk_table.join_asof(&base_empty, base_fill)?
      };

      for t_name in &other_tables {
        let method = fill.get(t_name).copied().unwrap_or(default_fill);
        if let Some(t) = obj_tables.get(t_name) {
          obj_joined = obj_joined.join_asof(t, method)?;
        } else {
          let empty_t = base_obj_tables[t_name].to_empty();
          obj_joined = obj_joined.join_asof(&empty_t, method)?;
        }
      }

      if let Some(ref mut res) = result {
        res.v_concat(&obj_joined)?;
      } else {
        result = Some(obj_joined);
      }
    }

    Ok((result.unwrap_or_default(), objs, bars))
  }

  fn snapshot(&self, tables: Vec<String>, objects: Vec<String>) -> Result<Table> {
    let sql = self.snapshot_sql(&tables, &objects);
    let full = self.async_query(sql)?;
    let mut objs = full.keys().cloned().collect::<Vec<_>>();

    objs.sort();

    let pk_meta = HashMap::from([("primary_key".to_string(), Variant::Bool(true))]);
    let mut result = Table::from_columns(vec![
      Field::new_with_data("obj", DataType::String, objs.clone().into())
        .with_metadata(pk_meta.clone()),
    ]);

    for t in &tables {
      let t_name = t.split_once(':').map_or(t.as_str(), |(name, _)| name);
      let Some(mut sub_table) = full
        .values()
        .find_map(|m| m.get(t_name))
        .map(|t| t.to_empty())
      else {
        continue;
      };

      let mut sub_objs = Vec::with_capacity(objs.len());
      for obj in &objs {
        if let Some(obj_table) = full[obj].get(t_name)
          && obj_table.row_count() > 0
        {
          sub_table.extend(obj_table, false)?;
          sub_objs.push(obj.clone());
        }
      }

      sub_table.insert_column(
        0,
        Field::new_with_data("obj", DataType::String, sub_objs.into())
          .with_metadata(pk_meta.clone()),
      );

      result = result.join_asof(&sub_table, FillMethod::Nan)?;
    }

    Ok(result)
  }

  fn async_query(&self, sql: String) -> Result<GroupedTables> {
    let (tx, rx) = oneshot::channel();

    let conn = self.clone();
    get_runtime().spawn(async {
      let result = query_msd(conn, sql).await;
      let _ = tx.send(result);
    });

    if let Ok(res) = rx.blocking_recv() {
      res
    } else {
      bail!("internal error to query")
    }
  }

  fn query_map<F>(&self, sql: String, mut handler: F) -> Result<()>
  where
    F: FnMut(Table) -> Result<()>,
  {
    let (tx, mut rx) = mpsc::channel(32);

    let conn = self.clone();
    get_runtime().spawn(async move {
      if let Err(err) = query_msd_stream(conn, sql, &tx).await {
        let _ = tx.send(Err(err)).await;
      }
    });

    while let Some(res) = rx.blocking_recv() {
      let table = res?;
      handler(table)?;
    }

    Ok(())
  }

  fn snapshot_sql(&self, tables: &Vec<String>, objects: &Vec<String>) -> String {
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

      sql.push_str("SELECT ");
      sql.push_str(fields);
      sql.push_str(" FROM ");
      sql.push_str(table);
      sql.push_str(&objects_where);
      sql.push_str(" LIMIT -1;");
    }
    sql
  }
}

async fn query_msd(conn: MsdConnection, sql: String) -> Result<GroupedTables> {
  let url = format!("{}/query", conn.url);

  let req = conn
    .client
    .post(&url)
    .json(&serde_json::json!({ "query": sql }));

  let resp = req.send().await?;

  let stream = resp.bytes_stream();
  let stream_reader = tokio_util::io::StreamReader::new(
    stream.map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
  );

  let mut rd = tokio::io::BufReader::with_capacity(64 * 1024, stream_reader);

  let mut buf = Vec::with_capacity(8 * 1024);

  let mut m = HashMap::new();
  buf.resize(8, 0);
  while rd.read_exact(&mut buf).await.is_ok() {
    let (header_size, data_size) = match check_table_frame(&buf) {
      Ok(size) => size,
      Err(err) => {
        bail!("invalid table frame: {}", err);
      }
    };

    buf.resize(header_size + data_size, 0);
    rd.read_exact(&mut buf[header_size..]).await?;
    let table = unpack_table_frame(&buf, false)?;

    let obj = table.get_table_meta("obj").and_then(|v| v.get_string());
    let table_name = table.get_table_meta("table").and_then(|v| v.get_string());

    if let Some((obj, table_name)) = obj.zip(table_name) {
      let mm = m.entry(obj.clone()).or_insert(HashMap::new());
      mm.insert(table_name.to_string(), table);
    }

    buf.clear();
    buf.resize(header_size, 0);
  }

  Ok(m)
}

async fn query_msd_stream(
  conn: MsdConnection,
  sql: String,
  tx: &mpsc::Sender<Result<Table>>,
) -> Result<()> {
  let url = format!("{}/query", conn.url);

  let req = conn
    .client
    .post(&url)
    .json(&serde_json::json!({ "query": sql }));

  let resp = req.send().await?;

  let stream = resp.bytes_stream();
  let stream_reader = tokio_util::io::StreamReader::new(
    stream.map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
  );

  let mut rd = tokio::io::BufReader::with_capacity(64 * 1024, stream_reader);

  let mut buf = Vec::with_capacity(8 * 1024);

  buf.resize(8, 0);
  while rd.read_exact(&mut buf).await.is_ok() {
    let (header_size, data_size) = match check_table_frame(&buf) {
      Ok(size) => size,
      Err(err) => {
        bail!("invalid table frame: {}", err);
      }
    };

    buf.resize(header_size + data_size, 0);
    rd.read_exact(&mut buf[header_size..]).await?;
    let table = unpack_table_frame(&buf, false)?;

    if tx.send(Ok(table)).await.is_err() {
      return Ok(());
    }

    buf.clear();
    buf.resize(header_size, 0);
  }

  Ok(())
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
          t.get_table_meta("obj").and_then(|v| v.get_string()).cloned().unwrap(),
          t.get_table_meta("table").and_then(|v| v.get_string()).cloned().unwrap(),
        ));
        Ok(())
      })
      .unwrap();

    assert_eq!(received.len(), 2);
    assert_eq!(received[0], ("obj1".to_string(), "table1".to_string()));
    assert_eq!(received[1], ("obj2".to_string(), "table2".to_string()));

    get_runtime().block_on(handle).unwrap();
  }
}
