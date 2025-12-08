//! Request module containing different types of requests that can be sent to the database workers.
//!

use std::{collections::HashMap, hash::Hash, ops::Deref};

pub use msd_request::*;
use msd_table::Table;
use tokio::sync::oneshot;

use crate::errors::DbError;

/// A Request to be processed by a database worker.
///
/// Call the associated `build_*` methods to create requests along with their response channels.
#[derive(Debug)]
pub enum Request {
  Insert {
    req: InsertRequest,
    resp_tx: RequestSender<InsertResponse>,
  },
  Query {
    req: QueryRequest,
    resp_tx: RequestSender<QueryResponse>,
  },
  ListObjects {
    req: ListObjectsRequest,
    resp_tx: RequestSender<ListObjectsResponse>,
  },

  Broadcast(Broadcast),
}

impl Clone for Request {
  fn clone(&self) -> Self {
    match self {
      Request::Broadcast(msg) => Request::Broadcast(msg.clone()),
      _ => panic!("Only Broadcast requests can be cloned"),
    }
  }
}

impl Request {
  pub fn insert(req: InsertRequest) -> (Self, RequestReceiver<InsertResponse>) {
    let (req, resp_tx, resp_rx) = req.to_request();
    (Request::Insert { req, resp_tx }, resp_rx)
  }
  pub fn query(req: QueryRequest) -> (Self, RequestReceiver<QueryResponse>) {
    let (req, resp_tx, resp_rx) = req.to_request();
    (Request::Query { req, resp_tx }, resp_rx)
  }

  pub fn create_table<S: Into<String>>(name: S, table: Table) -> Self {
    Request::Broadcast(Broadcast::CreateTable(name.into(), table))
  }

  pub fn drop_table<S: Into<String>>(name: S) -> Self {
    Request::Broadcast(Broadcast::DropTable(name.into()))
  }

  pub fn update_schema(schema_map: HashMap<String, Table>) -> Self {
    Request::Broadcast(Broadcast::UpdateSchema(schema_map))
  }
}

impl Deref for Request {
  type Target = RequestKey;
  fn deref(&self) -> &Self::Target {
    match self {
      Request::Insert { req, .. } => &req.key,
      Request::Query { req, .. } => &req.key,
      Request::ListObjects { req, .. } => &req.key,
      Request::Broadcast(_) => broadcast_key(),
    }
  }
}

pub type RequestSender<T> = oneshot::Sender<Result<T, DbError>>;
pub type RequestReceiver<T> = oneshot::Receiver<Result<T, DbError>>;

pub trait DbRequest: Deref<Target = RequestKey> + Hash + Sized + Send {
  type Response;

  fn to_request(
    self,
  ) -> (
    Self,
    RequestSender<Self::Response>,
    RequestReceiver<Self::Response>,
  ) {
    let (resp_tx, resp_rx) = oneshot::channel();
    (self, resp_tx, resp_rx)
  }
}

impl DbRequest for InsertRequest {
  type Response = InsertResponse;
}

impl DbRequest for QueryRequest {
  type Response = QueryResponse;
}
