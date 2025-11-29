use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::request::{Request, RequestKey};

#[derive(Debug, Error)]
pub enum DbError {
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
  #[error("Serialization Error")]
  SerializationError(#[from] bincode::Error),
  #[error("Not found {0}")]
  NotFound(RequestKey),
  #[error("Chunk missing for {0} at seq {1}")]
  ChunkMissing(RequestKey, u32),
}
