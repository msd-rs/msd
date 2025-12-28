// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! Error types for MsdDb.

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::request::{MsdRequest, RequestKey};

#[derive(Debug, Error)]
pub enum DbError {
  #[error("Invalid key format: {0:?}")]
  InvalidKeyFormat(Vec<u8>),
  #[error("Request dispatch failed")]
  RequestDispatchFailed(#[from] mpsc::error::SendError<MsdRequest>),
  #[error("Request receive failed")]
  RequestReceiveFailed(#[from] oneshot::error::RecvError),
  #[error("Table Error")]
  TableError(#[from] msd_table::TableError),
  #[error("Key Pattern Error")]
  KeyPatternError(#[from] wildcard::WildcardError),
  #[error("Request Error")]
  RequestError(#[from] msd_request::RequestError),
  #[error("Store Error")]
  StoreError(#[from] msd_store::StoreError),
  #[error("Encode Error")]
  BinaryEncodeError(#[from] bincode::error::EncodeError),
  #[error("Decode Error")]
  BinaryDecodeError(#[from] bincode::error::DecodeError),
  #[error("Not found {0}")]
  NotFound(RequestKey),
  #[error("Chunk missing for {0} at seq {1}")]
  ChunkMissing(RequestKey, u32),
  #[error("Table not found: {0}")]
  TableNotFound(String),
  #[error("Invalid agg type for field: {0}")]
  InvalidAgg(String),
  #[error("Cache not found for key: {0:?}")]
  CacheNotFound(RequestKey),
  #[error("Internal error: {0}")]
  InternalError(String),
  #[error("Invalid table schema: {0}")]
  InvalidTableSchema(String),
  #[error("Unsupported operation")]
  UnsupportedRequestType,
  #[error("Chan format error: {0}")]
  ChanFormatError(String),
}
