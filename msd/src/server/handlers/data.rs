use super::is_msd_client;
use crate::server::DBState;
use axum::{Json, extract::State, http::HeaderMap, response::IntoResponse};
use axum_streams::{StreamBodyAs, StreamBodyAsOptions, StreamingFormat};
use futures::{StreamExt, stream::BoxStream};
use msd_db::{errors::DbError, request::MsdRequest};
use msd_request::{
  ListObjectsRequest, QueryRequest, RequestKey, SqlRequest, pack_table_frame, sql_to_request,
};
use msd_table::Table;
use serde::{Deserialize, Serialize};
use tokio_stream::{self as stream};
use tracing::{debug, warn};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataRequest {
  pub query: String,
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

  let s = stream::iter(requests)
    .then(move |r| handle_sql_request(db.clone(), r))
    .map(|r| r.map_err(|e| axum::Error::new(e)));

  if is_msd_client(&headers) {
    Ok(StreamBodyAs::new(TableFrameFormat {}, s))
  } else {
    Ok(StreamBodyAs::new(TableNdJsonFormat {}, s))
  }
}

async fn handle_sql_request(db: DBState, req: SqlRequest) -> Result<Table, DbError> {
  debug!("Handling SQL request: {:?}", req);
  let res = match req {
    SqlRequest::Query(query_req) => handle_query(db, query_req).await,
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

async fn handle_query(db: DBState, req: QueryRequest) -> Result<Table, DbError> {
  let (msd_req, resp_rx) = MsdRequest::query(req);
  db.request(msd_req).await.map_err(|e| e)?;
  resp_rx.await.map_err(|e| e)?
}

struct TableFrameFormat {}

impl StreamingFormat<Table> for TableFrameFormat {
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
          let package = pack_table_frame("", &table);
          Ok(axum::body::Bytes::from(package))
        }
      })
    })
  }

  fn http_response_headers(&self, _: &StreamBodyAsOptions) -> Option<HeaderMap> {
    let mut header_map = HeaderMap::new();
    header_map.insert(
      axum::http::header::CONTENT_TYPE,
      axum::http::header::HeaderValue::from_static("application/x-msd-table-frame"),
    );
    Some(header_map)
  }
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
