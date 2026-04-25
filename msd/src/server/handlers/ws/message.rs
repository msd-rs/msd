use axum::extract::ws::{self, Utf8Bytes};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Subscribe {
  pub table: String,
  pub objs: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Notify {
  pub table: String,
  pub obj: String,
  pub min_ts: i64,
  pub max_ts: i64,
  pub count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
  pub status: i32,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub msg: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
  Subscribe(Subscribe),
  Unsubscribe(Subscribe),
  #[serde(skip)]
  Notify(Notify, Utf8Bytes),
  Status(Status),
}

impl TryFrom<ws::Message> for Message {
  type Error = anyhow::Error;

  fn try_from(value: ws::Message) -> Result<Self, Self::Error> {
    match value {
      ws::Message::Text(text) => serde_json::from_str(&text).map_err(|e| e.into()),
      ws::Message::Binary(bytes) => serde_json::from_slice(&bytes).map_err(|e| e.into()),
      _ => Err(anyhow::anyhow!("Invalid message type")),
    }
  }
}

impl Message {
  pub fn build_notify(table: &str, obj: &str, min_ts: i64, max_ts: i64, count: usize) -> Self {
    let notify = Notify {
      table: table.to_string(),
      obj: obj.to_string(),
      min_ts,
      max_ts,
      count,
    };
    let json_bytes = serde_json::to_vec(&notify)
      .map_err(|e| e.to_string())
      .and_then(|b| Utf8Bytes::try_from(b).map_err(|e| e.to_string()))
      .unwrap_or_default();
    Message::Notify(notify, json_bytes)
  }
}
