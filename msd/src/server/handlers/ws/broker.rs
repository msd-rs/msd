use std::collections::HashMap;

use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::error;

use super::message::Message;

type Subscriber = mpsc::Sender<Arc<Message>>;

#[derive(Debug)]
pub struct Broker {
  pub subscribes: RwLock<HashMap<String, Subscriber>>,
}

impl Broker {
  pub fn new() -> Self {
    Self {
      subscribes: RwLock::new(HashMap::new()),
    }
  }

  pub fn add(&self, name: &str, sender: Subscriber) {
    match self.subscribes.write() {
      Ok(mut subscribes) => {
        subscribes.insert(name.to_string(), sender);
      }
      Err(e) => {
        error!(%e, name, "add subscriber failed");
      }
    }
  }

  pub fn remove(&self, name: &str) {
    match self.subscribes.write() {
      Ok(mut subscribes) => {
        subscribes.remove(name);
      }
      Err(e) => {
        error!(%e, name, "remove subscriber failed");
      }
    }
  }

  pub async fn broadcast(&self, message: Arc<Message>) {
    match self.subscribes.read() {
      Ok(subscribes) => {
        for (_, sender) in subscribes.iter() {
          let _ = sender.send(message.clone()).await;
        }
      }
      Err(e) => {
        error!(%e, "broadcast failed");
      }
    }
  }
}
