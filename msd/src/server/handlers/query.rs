use std::collections::HashMap;

use super::is_msd_client;
use crate::server::DBState;
use axum::{
  Json,
  body::{Body, HttpBody},
  extract::State,
  http::{HeaderMap, Response},
  response::IntoResponse,
};
use axum_streams::{StreamBodyAs, StreamBodyAsOptions, StreamingFormat};
use futures::{StreamExt, stream::BoxStream};
use http_body::Frame;
use msd_db::{errors::DbError, request::MsdRequest};
use msd_request::{
  ListObjectsRequest, QueryRequest, RequestKey, SqlRequest, pack_table_frame, sql_to_request,
};
use msd_table::{Table, Variant, table};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio_stream::{self as stream};
use tracing::{debug, info, warn};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataRequest {
  pub query: String,
  pub only_schema: Option<bool>,
}

#[derive(Debug)]
struct TableFrameBody {
  set: JoinSet<Result<Table, DbError>>,
}

impl TableFrameBody {
  fn new(db: DBState, requests: Vec<SqlRequest>) -> Self {
    let mut set = JoinSet::new();
    for req in requests {
      set.spawn(handle_sql_request(db.clone(), req));
    }
    Self { set }
  }
}

impl HttpBody for TableFrameBody {
  type Data = axum::body::Bytes;
  type Error = axum::Error;

  fn poll_frame(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
    let this = self.get_mut();
    let res = this.set.try_join_next();
    match res {
      None => {
        if this.set.is_empty() {
          std::task::Poll::Ready(None)
        } else {
          cx.waker().wake_by_ref();
          std::task::Poll::Pending
        }
      }
      Some(Ok(Ok(table))) => {
        let frame = Frame::data(axum::body::Bytes::from(pack_table_frame("", &table)));
        std::task::Poll::Ready(Some(Ok(frame)))
      }
      Some(Ok(Err(e))) => std::task::Poll::Ready(Some(Err(axum::Error::new(e)))),
      Some(Err(e)) => std::task::Poll::Ready(Some(Err(axum::Error::new(e)))),
    }
  }
}
impl IntoResponse for TableFrameBody {
  fn into_response(self) -> Response<Body> {
    let mut resp = Response::new(Body::new(self));
    resp.headers_mut().insert(
      axum::http::header::CONTENT_TYPE,
      axum::http::header::HeaderValue::from_static("application/x-msd-table-frame"),
    );
    resp
  }
}
pub async fn handle_data(
  State(db): State<DBState>,
  headers: HeaderMap,
  Json(body): Json<DataRequest>,
) -> Result<impl IntoResponse, (axum::http::StatusCode, String)> {
  let requests = sql_to_request(&body.query).map_err(|e| {
    (
      axum::http::StatusCode::BAD_REQUEST,
      format!("SQL parse error: {}", e),
    )
  })?;

  let requests = flatten_requests_by_object(db.clone(), requests);

  debug!(count = requests.len(), "start to handle requests");

  if is_msd_client(&headers) {
    let body = TableFrameBody::new(db.clone(), requests);
    Ok(body.into_response())
  } else {
    let s = stream::iter(requests)
      .then(move |r| handle_sql_request(db.clone(), r))
      .map(|r| r.map_err(|e| axum::Error::new(e)));

    Ok(StreamBodyAs::new(TableNdJsonFormat {}, s).into_response())
  }
}

async fn handle_sql_request(db: DBState, req: SqlRequest) -> Result<Table, DbError> {
  debug!("Handling SQL request: {:?}", req);
  let res = match req {
    SqlRequest::Query(query_req) => handle_query(db, query_req).await,
    SqlRequest::CreateTable(name, table) => handle_create_table(db, name, table).await,
    SqlRequest::Schema(name) => handle_schema(db, name).await,
    _ => Err(DbError::UnsupportedRequestType),
  };
  match res {
    Ok(table) => Ok(table),
    Err(e) => {
      debug!(%e, "Failed to handle SQL request");
      Err(e)
    }
  }
}

fn flatten_requests_by_object(db: DBState, requests: Vec<SqlRequest>) -> Vec<SqlRequest> {
  requests
    .into_iter()
    .flat_map(|r| match r {
      SqlRequest::Query(query_req) => {
        if is_list_objects(&query_req) {
          return vec![SqlRequest::Query(query_req)];
        }
        let objects = if query_req
          .objects
          .as_ref()
          .map(|s| s.is_empty())
          .unwrap_or(true)
        {
          // if no specific objects, use obj in key
          matched_objects(db.clone(), &query_req.table, &query_req.obj)
        } else {
          // if specific objects, use them
          let objects = query_req.objects.as_ref().unwrap();
          objects
            .iter()
            .map(|obj| matched_objects(db.clone(), &query_req.table, &obj))
            .flatten()
            .collect()
        };
        if objects.is_empty() {
          return vec![];
        }
        objects
          .into_iter()
          .map(|obj| {
            let mut query_req = query_req.clone();
            query_req.key.obj = obj;
            SqlRequest::Query(query_req)
          })
          .collect()
      }
      SqlRequest::Insert(insert_req) => {
        let sub_requests = match db.get_schema(&insert_req.table) {
          Ok(schema) => insert_req.to_table(&schema).unwrap_or_default(),
          Err(e) => {
            warn!(%e, table = &insert_req.table, "Failed to get schema");
            vec![]
          }
        };
        sub_requests
          .into_iter()
          .map(|req| SqlRequest::Insert(req))
          .collect()
      }
      _ => vec![r],
    })
    .collect()
}

fn matched_objects(db: DBState, table: &str, pattern: &str) -> Vec<String> {
  if pattern.is_empty() || pattern.contains(|c| c == '*' || c == '?') {
    let req = ListObjectsRequest {
      key: RequestKey::new(table, pattern),
    };
    match db.matched_objects(&req) {
      Ok(objects) => objects.into_iter().collect(),
      Err(e) => {
        warn!(%e, table, pattern, "Failed to get matched objects");
        vec![]
      }
    }
  } else {
    vec![pattern.to_string()]
  }
}

fn is_list_objects(req: &QueryRequest) -> bool {
  req
    .fields
    .as_ref()
    .map(|f| f.len() == 1 && f[0] == "obj")
    .unwrap_or(false)
}

async fn handle_schema(db: DBState, name: String) -> Result<Table, DbError> {
  db.get_schema(&name)
}

async fn handle_query(db: DBState, req: QueryRequest) -> Result<Table, DbError> {
  if is_list_objects(&req) {
    let req = ListObjectsRequest { key: req.key };
    let mut objs = db.matched_objects(&req)?.into_iter().collect::<Vec<_>>();
    objs.sort();
    return Ok(table!({name : "obj", kind : string, data : objs}));
  }

  let obj = req.key.obj.clone();
  let table = req.key.table.clone();
  let (msd_req, resp_rx) = MsdRequest::query(req);
  db.request(msd_req).await.map_err(|e| e)?;
  resp_rx.await.map_err(|e| e)?.map(|t| {
    t.with_metadata(HashMap::from([
      ("obj".into(), Variant::String(obj)),
      ("table".into(), Variant::String(table)),
    ]))
  })
}

async fn handle_create_table(db: DBState, name: String, table: Table) -> Result<Table, DbError> {
  info!(name, ?table, "Creating table");
  let msd_req = MsdRequest::create_table(name, table);
  db.request(msd_req).await.map_err(|e| e)?;
  Ok(Table::default())
}

struct TableNdJsonFormat {}

impl StreamingFormat<Table> for TableNdJsonFormat {
  fn to_bytes_stream<'a, 'b>(
    &'a self,
    stream: BoxStream<'b, Result<Table, axum::Error>>,
    _: &'a StreamBodyAsOptions,
  ) -> BoxStream<'b, Result<axum::body::Bytes, axum::Error>> {
    Box::pin({
      stream.map(|obj_res| match obj_res {
        // ignore error
        Err(_) => Ok(axum::body::Bytes::default()),
        Ok(table) => {
          let mut package = serde_json::to_vec(&table).unwrap();
          package.push(b'\n');
          Ok(axum::body::Bytes::from(package))
        }
      })
    })
  }

  fn http_response_headers(&self, _: &StreamBodyAsOptions) -> Option<HeaderMap> {
    let mut header_map = HeaderMap::new();
    header_map.insert(
      axum::http::header::CONTENT_TYPE,
      axum::http::header::HeaderValue::from_static("application/x-ndjson"),
    );
    Some(header_map)
  }
}
