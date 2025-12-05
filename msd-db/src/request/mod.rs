//! Request module containing different types of requests that can be sent to the database workers.
//!

mod base;
mod broadcast;
mod insert;
mod query;

use std::{collections::HashMap, ops::Deref};

pub use base::*;
pub use broadcast::*;
pub use insert::*;
use msd_table::Table;
pub use query::*;

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

  pub fn create_table(name: String, table: Table) -> Self {
    Request::Broadcast(Broadcast::CreateTable(name, table))
  }

  pub fn drop_table(name: String) -> Self {
    Request::Broadcast(Broadcast::DropTable(name))
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
      Request::Broadcast(_) => broadcast_key(),
    }
  }
}
