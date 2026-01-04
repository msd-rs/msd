// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! Background flusher for MsdDb.

use crate::request::{Broadcast, MsdRequest};
use tokio::{select, sync::mpsc};
use tracing::{debug, info};

pub(crate) async fn bg_flush(
  interval: i64,
  workers: Vec<mpsc::Sender<MsdRequest>>,
  mut rx: mpsc::Receiver<Broadcast>,
) {
  let mut timer = tokio::time::interval(tokio::time::Duration::from_micros(interval as u64 / 2));

  loop {
    select! {
      _ = timer.tick() => {
        debug!("invoking flush");
        for worker in &workers {
          worker.send(MsdRequest::Broadcast(Broadcast::Flush)).await;
        }
      }
      msg = rx.recv() => {
        match msg {
          Some(Broadcast::Shutdown) => {
            info!("flusher: received shutdown broadcast");
            break;
          }
          _ => { /* ignore other broadcast messages */ }
        }
      }
    }
  }
  info!("flusher: shutdown");
}
