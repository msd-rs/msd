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
pub enum MsdRequest {
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
  Delete {
    req: DeleteRequest,
    resp_tx: RequestSender<DeleteResponse>,
  },

  Broadcast(Broadcast),
}

impl Clone for MsdRequest {
  fn clone(&self) -> Self {
    match self {
      MsdRequest::Broadcast(msg) => MsdRequest::Broadcast(msg.clone()),
      _ => panic!("Only Broadcast requests can be cloned"),
    }
  }
}

impl MsdRequest {
  pub fn insert(req: InsertRequest) -> (Self, RequestReceiver<InsertResponse>) {
    let (req, resp_tx, resp_rx) = req.to_request();
    (MsdRequest::Insert { req, resp_tx }, resp_rx)
  }
  pub fn query(req: QueryRequest) -> (Self, RequestReceiver<QueryResponse>) {
    let (req, resp_tx, resp_rx) = req.to_request();
    (MsdRequest::Query { req, resp_tx }, resp_rx)
  }

  pub fn delete(req: DeleteRequest) -> (Self, RequestReceiver<DeleteResponse>) {
    let (req, resp_tx, resp_rx) = req.to_request();
    (MsdRequest::Delete { req, resp_tx }, resp_rx)
  }

  pub fn create_table<S: Into<String>>(name: S, table: Table) -> Self {
    MsdRequest::Broadcast(Broadcast::CreateTable(name.into(), table))
  }

  pub fn drop_table<S: Into<String>>(name: S) -> Self {
    MsdRequest::Broadcast(Broadcast::DropTable(name.into()))
  }

  pub fn update_schema(schema_map: HashMap<String, Table>) -> Self {
    MsdRequest::Broadcast(Broadcast::UpdateSchema(schema_map))
  }
}

impl Deref for MsdRequest {
  type Target = RequestKey;
  fn deref(&self) -> &Self::Target {
    match self {
      MsdRequest::Insert { req, .. } => &req.key,
      MsdRequest::Query { req, .. } => &req.key,
      MsdRequest::ListObjects { req, .. } => &req.key,
      MsdRequest::Delete { req, .. } => &req.key,
      MsdRequest::Broadcast(_) => broadcast_key(),
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

impl DbRequest for DeleteRequest {
  type Response = DeleteResponse;
}
