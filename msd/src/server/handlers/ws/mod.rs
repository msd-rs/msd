mod broker;
mod filter;
mod message;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{self, WebSocket};
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::Response;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::server::AppStateRef;
use crate::server::handlers::ws::filter::Filter;

pub use broker::Broker;
pub use message::Message;

pub async fn handle_ws(
  ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
  headers: HeaderMap,
  ws: WebSocketUpgrade,
  State(state): State<AppStateRef>,
) -> Response {
  let ip = headers
    .get("x-forwarded-for")
    .map_or(remote_addr.to_string(), |value| {
      value.to_str().unwrap().to_string()
    });

  ws.on_upgrade(|socket| handle_socket(socket, state, ip))
}

async fn handle_socket(socket: WebSocket, state: AppStateRef, ip: String) {
  let name = format!("sub-{}", ip);
  debug!("{} connected", name);

  let (ws_sender, mut ws_receiver) = socket.split();
  let (msg_sender, msg_rx) = mpsc::channel::<Arc<Message>>(128);

  state.broker.add(&name, msg_sender.clone());

  // handle event, this is main logic for websocket, handle both user request and system broadcast
  tokio::spawn(handle_msg(name.clone(), msg_rx, ws_sender));

  // keep receive message from websocket, and send it to event channel
  while let Some(msg) = ws_receiver.next().await {
    match msg {
      Ok(msg) => match Message::try_from(msg) {
        Ok(msg) => {
          let _ = msg_sender.send(Arc::new(msg)).await;
        }
        Err(e) => {
          error!("{} failed to convert message: {:?}", name, e);
        }
      },
      Err(e) => {
        error!("{} received error: {:?}", name, e);
        break;
      }
    }
  }

  state.broker.remove(&name);
  debug!(name, "ws handler quit");
}

async fn handle_msg(
  name: String,
  mut ev_rx: mpsc::Receiver<Arc<Message>>,
  mut ws_sender: SplitSink<WebSocket, ws::Message>,
) {
  let mut filter = Filter::new();

  while let Some(msg) = ev_rx.recv().await {
    let msg = msg.as_ref();

    debug!(name, msg = ?msg, "received message");
    match msg {
      Message::Subscribe(sub) => {
        filter.subscribe(sub);
      }
      Message::Unsubscribe(sub) => {
        filter.unsubscribe(sub);
      }
      Message::Notify(notify, json_bytes) => {
        if !filter.is_allowed(notify) {
          debug!(name, msg = ?msg, "received unallowed message");
          continue;
        }
        match ws_sender.send(ws::Message::Text(json_bytes.clone())).await {
          Err(err) => {
            error!(name, ?err, "failed to send message");
            break;
          }
          Ok(_) => {}
        }
      }
      _ => {
        debug!(name, msg = ?msg, "received unhandled message");
      }
    }
  }

  debug!(name, "ws msg handler quit");
}
