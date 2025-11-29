use tokio::sync::oneshot;

mod base;
mod broadcast;
mod insert;
mod query;

pub use base::*;
pub use broadcast::*;
pub use insert::*;
pub use query::*;

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
