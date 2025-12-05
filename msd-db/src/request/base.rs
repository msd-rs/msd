use std::{fmt::Display, hash::Hash, ops::Deref, sync::OnceLock};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::errors::DbError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestKey {
  pub table: String,
  pub obj: String,
}

pub(crate) fn broadcast_key() -> &'static RequestKey {
  static BROADCAST_KEY: OnceLock<RequestKey> = OnceLock::new();
  BROADCAST_KEY.get_or_init(|| RequestKey {
    table: "__broadcast__".into(),
    obj: "__broadcast__".into(),
  })
}

impl RequestKey {
  pub fn new(table: String, obj: String) -> Self {
    Self { table, obj }
  }
  pub fn is_broadcast(&self) -> bool {
    self == broadcast_key()
  }
}

impl Display for RequestKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:{}", self.table, self.obj)
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
