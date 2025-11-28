use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::request::Request;

#[derive(Debug, Error)]
pub enum DBError {
  #[error("Invalid key format: {0:?}")]
  InvalidKeyFormat(Vec<u8>),
  #[error("Request dispatch failed")]
  RequestDispatchFailed(#[from] mpsc::error::SendError<Request>),
  #[error("Request receive failed")]
  RequestReceiveFailed(#[from] oneshot::error::RecvError),
  #[error("Table Error")]
  TableError(#[from] msd_table::TableError),
  #[error("Store Error")]
  StoreError(#[from] msd_store::StoreError),
  #[error("Table not found {0} {1}")]
  TableNotFound(String, String),
}
