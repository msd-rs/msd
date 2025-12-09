use crate::server::DBState;
use axum::{Json, extract::State, http::HeaderMap, response::IntoResponse};
use axum_streams::StreamBodyAs;
use msd_db::{errors::DbError, request::MsdRequest};
use msd_request::{ListObjectsRequest, QueryRequest, RequestKey, SqlRequest, sql_to_request};
use msd_table::Table;
use serde::{Deserialize, Serialize};
use tokio_stream::{self as stream, StreamExt};
use tracing::{debug, warn};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataRequest {
  pub query: String,
}

pub async fn handle_data(
  State(db): State<DBState>,
  Json(body): Json<DataRequest>,
) -> Result<(HeaderMap, impl IntoResponse), (axum::http::StatusCode, String)> {
  let reqs = sql_to_request(&body.query).map_err(|e| {
    (
      axum::http::StatusCode::BAD_REQUEST,
      format!("SQL parse error: {}", e),
    )
  })?;

  let reqs = flatten_requests_by_object(db.clone(), reqs);

  let s = stream::iter(reqs)
    .then(move |r| handle_sql_request(db.clone(), r))
    .map(|r| r.map_err(|e| axum::Error::new(e)));

  let mut headers = HeaderMap::new();
  headers.insert(
    axum::http::header::CONTENT_TYPE,
    "application/x-ndjson".parse().unwrap(),
  );
  Ok((headers, StreamBodyAs::json_nl_with_errors(s)))
}

async fn handle_sql_request(db: DBState, req: SqlRequest) -> Result<Table, DbError> {
  debug!("Handling SQL request: {:?}", req);
  match req {
    SqlRequest::Query(query_req) => handle_query(db, query_req).await,
    _ => Err(DbError::UnsupportedRequestType),
  }
}

fn flatten_requests_by_object(db: DBState, reqs: Vec<SqlRequest>) -> Vec<SqlRequest> {
  reqs
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
        let sub_reqs = match db.get_schema(&insert_req.table) {
          Ok(schema) => insert_req.to_table(&schema).unwrap_or_default(),
          Err(e) => {
            warn!(%e, table = &insert_req.table, "Failed to get schema");
            vec![]
          }
        };
        sub_reqs
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
      Ok(objects) => objects,
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
