use std::{fmt::Display, hash::Hash, ops::Deref};

use msd_table::{Table, Variant};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::errors::DbError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestKey {
  pub table: String,
  pub obj: String,
}

impl Display for RequestKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:{}", self.table, self.obj)
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsertData {
  Table(Table),
  Row(Vec<Variant>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
  #[serde(flatten)]
  pub key: RequestKey,
  pub data: InsertData,
}

impl Hash for InsertRequest {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

impl Deref for InsertRequest {
  type Target = RequestKey;

  fn deref(&self) -> &Self::Target {
    &self.key
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
  #[serde(flatten)]
  pub key: RequestKey,
}

impl Deref for QueryRequest {
  type Target = RequestKey;

  fn deref(&self) -> &Self::Target {
    &self.key
  }
}

impl Hash for QueryRequest {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

#[derive(Debug)]
pub enum Request {
  Insert {
    req: InsertRequest,
    resp_tx: oneshot::Sender<Result<(), DbError>>,
  },
  Query {
    req: QueryRequest,
    resp_tx: oneshot::Sender<Result<Table, DbError>>,
  },
}

impl Request {
  pub fn insert(req: InsertRequest) -> (Self, oneshot::Receiver<Result<(), DbError>>) {
    let (resp_tx, resp_rx) = oneshot::channel();
    (Request::Insert { req, resp_tx }, resp_rx)
  }
  pub fn query(req: QueryRequest) -> (Self, oneshot::Receiver<Result<Table, DbError>>) {
    let (resp_tx, resp_rx) = oneshot::channel();
    (Request::Query { req, resp_tx }, resp_rx)
  }
}
