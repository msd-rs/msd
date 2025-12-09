use super::DBState;
use axum::{Json, extract::State, http::HeaderMap, response::IntoResponse};
use axum_streams::StreamBodyAs;
use msd_db::{errors::DbError, request::MsdRequest};
use msd_request::{SqlRequest, sql_to_request};
use msd_table::Table;
use serde::{Deserialize, Serialize};
use tokio_stream::{self as stream, StreamExt};
use tracing::debug;

#[derive(Debug, Serialize, Deserialize)]
pub struct DataRequest {
  pub query: String,
}

pub async fn handle_data(
  State(db): State<DBState>,
  Json(body): Json<DataRequest>,
) -> Result<(HeaderMap, impl IntoResponse), (axum::http::StatusCode, String)> {
  let req = sql_to_request(&body.query).map_err(|e| {
    (
      axum::http::StatusCode::BAD_REQUEST,
      format!("SQL parse error: {}", e),
    )
  })?;

  let s = stream::iter(req)
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
    SqlRequest::Insert(insert_req) => {
      let (msd_req, resp_rx) = MsdRequest::insert(insert_req);
      db.request(msd_req).await.map_err(|e| e)?;
      resp_rx.await.map_err(|e| e)?
    }
    SqlRequest::Query(query_req) => {
      let (msd_req, resp_rx) = MsdRequest::query(query_req);
      db.request(msd_req).await.map_err(|e| e)?;
      resp_rx.await.map_err(|e| e)?
    }
    _ => Err(DbError::UnsupportedRequestType),
  }
}
