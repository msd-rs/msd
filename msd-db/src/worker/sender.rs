use std::hash::{Hash, Hasher};

use msd_request::{Broadcast, RequestKey};
use rustc_hash::FxHasher;
use tokio::sync::mpsc;

use crate::{errors::DbError, request::MsdRequest};

#[derive(Clone)]
pub struct WorkSender {
  workers: Vec<mpsc::Sender<MsdRequest>>,
}

impl WorkSender {
  pub fn new(workers: Vec<mpsc::Sender<MsdRequest>>) -> Self {
    Self { workers }
  }

  pub fn add(&mut self, worker: mpsc::Sender<MsdRequest>) {
    self.workers.push(worker);
  }

  fn get_worker(&self, key: &RequestKey) -> &mpsc::Sender<MsdRequest> {
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let index = (hash as usize) % self.workers.len();
    &self.workers[index]
  }

  pub fn send(&self, req: MsdRequest) -> Result<(), DbError> {
    let worker = self.get_worker(&req);
    worker
      .try_send(req)
      .map_err(|_| DbError::InternalError("Failed to send request".to_string()))?;
    Ok(())
  }

  pub fn broadcast(&self, req: MsdRequest) -> Result<(), DbError> {
    for worker in &self.workers {
      worker
        .try_send(req.clone())
        .map_err(|_| DbError::InternalError("Failed to send request".to_string()))?;
    }
    Ok(())
  }

  pub async fn close(&self) {
    let tasks = self
      .workers
      .iter()
      .map(|worker| worker.send(MsdRequest::Broadcast(Broadcast::Shutdown)));
    futures::future::join_all(tasks).await;
    for worker in &self.workers {
      worker.closed().await;
    }
  }
}
