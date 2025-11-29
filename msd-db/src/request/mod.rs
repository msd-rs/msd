//! Request module containing different types of requests that can be sent to the database workers.
//!

mod base;
mod broadcast;
mod insert;
mod query;

pub use base::*;
pub use broadcast::*;
pub use insert::*;
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

impl Request {
  pub fn build_insert(req: InsertRequest) -> (Self, RequestReceiver<InsertResponse>) {
    let (req, resp_tx, resp_rx) = req.to_request();
    (Request::Insert { req, resp_tx }, resp_rx)
  }
  pub fn build_query(req: QueryRequest) -> (Self, RequestReceiver<QueryResponse>) {
    let (req, resp_tx, resp_rx) = req.to_request();
    (Request::Query { req, resp_tx }, resp_rx)
  }
  /// Build a broadcast request, cloning the broadcast message, since it will be sent to multiple workers.
  pub fn build_broadcast(message: &Broadcast) -> Self {
    Request::Broadcast(message.clone())
  }
}
