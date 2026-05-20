# Subscribe Data Change

MSD-RS provides a real-time WebSocket subscription interface that allows clients to subscribe to and receive notifications about data updates.

## WebSocket Endpoint

The WebSocket subscription endpoint is located at:
```text
ws://<server_address>:<port>/ws
```
For example, if your server is running locally on port 8080: `ws://127.0.0.1:8080/ws`.

---

## Client Request Messages (Client -> Server)

To control your subscription, send JSON-formatted messages over the WebSocket connection.

### 1. Subscribe to Data Changes

To subscribe to updates on specific tables and objects, send a `Subscribe` message:

```json
{
  "Subscribe": {
    "table": "kline1m",
    "objs": ["SH600000", "SZ000001"]
  }
}
```

#### Wildcard Suffix Matching
Both the table name and the object names support trailing wildcard matching using the `*` suffix:

- **Subscribe to all tables and all objects:**
  ```json
  {
    "Subscribe": {
      "table": "*",
      "objs": ["*"]
    }
  }
  ```
- **Subscribe to all tables starting with `kline` for Shanghai (SH) stocks:**
  ```json
  {
    "Subscribe": {
      "table": "kline*",
      "objs": ["SH*"]
    }
  }
  ```

#### Hierarchical Exclusions
Subscriptions are managed using a Trie-based hierarchical filter. This allows you to subscribe to a broad range of events and exclude specific subsets. For example:
1. First, subscribe to all tables and all objects: `table: "*", objs: ["*"]`
2. Next, unsubscribe from trades of Shanghai stocks by sending an `Unsubscribe` message for `table: "trade", objs: ["SH*"]`.

---

### 2. Unsubscribe from Data Changes

To cancel subscriptions, send an `Unsubscribe` message containing the patterns you want to remove:

```json
{
  "Unsubscribe": {
    "table": "kline1m",
    "objs": ["SZ000001"]
  }
}
```

---

## Server Notification Messages (Server -> Client)

When data is inserted or updated in the database, the server checks active connections. If a connection has a subscription matching the updated table and object, the server sends a JSON `Notify` message text frame over the WebSocket:

```json
{
  "table": "kline1m",
  "obj": "SH600000",
  "min_ts": 1672537200000000,
  "max_ts": 1672537300000000,
  "count": 10
}
```

### Fields:
- `table`: The name of the table that received new/updated data.
- `obj`: The specific object partition key (e.g., stock symbol).
- `min_ts`: The minimum timestamp (in microseconds) of the updated batch of records.
- `max_ts`: The maximum timestamp (in microseconds) of the updated batch of records.
- `count`: The number of rows modified/inserted in the update.

---

## Implementation Details (`msd::server::handle::ws`)

The WebSocket handling is implemented in the server workspace handlers at `msd/src/server/handlers/ws/`.

### 1. Connection Upgrade & Lifecycle
- The Axum handler `handle_ws` in [mod.rs](file:///home/jia/repo/msd-rs2/msd/src/server/handlers/ws/mod.rs) captures the client's connection info and upgrades the protocol.
- In `handle_socket`, the WebSocket stream is split into a writer (`ws_sender`) and a reader (`ws_receiver`).
- A client-specific `mpsc::channel` is created, and its sender is registered globally in the `AppState`'s `Broker` with a subscriber key of format `sub-<remote_ip>`.
- When the socket is closed or disconnected, the connection key is automatically removed from the `Broker`.

### 2. Concurrent Processing
- Two tasks handle connection logic concurrently:
  - **Read Loop**: Listens for text/binary messages from the client. Incoming payloads are parsed into `Message` variants (like `Subscribe` or `Unsubscribe`) and dispatched to the internal handler.
  - **Write Task (`handle_msg`)**: Manages the subscription `Filter` state for the client. When a database `Notify` notification is broadcasted by the global `Broker`, the write task matches it against the client's Trie `Filter`. If allowed, it serializes the notification to JSON and pushes it as a text frame to the client.

### 3. Subscription Filtering Trie
- The matching logic is located in [filter.rs](file:///home/jia/repo/msd-rs2/msd/src/server/handlers/ws/filter.rs).
- It uses a custom double-trie structure (`TableTrie` containing nested `ObjTrie`s) to record and evaluate subscriptions. This ensures fast, prefix-capable matches for table and object filters, maintaining low latency even with many subscriptions.
