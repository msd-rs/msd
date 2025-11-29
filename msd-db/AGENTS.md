# High Level Design of MSD-DB

For a time-series data, it's should have the basic definition as below:

- **TableName**: The name of the time-series data table, e.g. `temperature_sensors`, `stock_kline`.
- **ObjectName**: the owner name of the data, for example, hostname, device id, user id, stock symbol, etc.
- **Timestamp**: the time when the data is generated or collected.
- **Value Columns**: the actual data fields, which can be of various types (integer, float, string, etc.). e.g.
  - For a temperature sensor, columns can be `temperature`, `humidity`, `pressure`.
  - For a stock market data, columns can be `open`, `close`, `high`, `low`, `volume`.
- **Schema**: the structure that defines the data types and organization of the value columns, also with metadata like units, descriptions, etc.

This time-series data in MSD-DB is called as `DbTable`j

MSD-DB store the `DbTable` in a partitioned by `ObjectName` then partitioned by `Time` way. Because the underlying storage engine is `MsdStore`, which is a key-value store, the key is designed as below:

```
Key = DataKey | IndexKey
DataKey = ObjectName + SequenceNumber
SequenceNumber = Hex(-COUNT_OF_CHUNKS_BEFORE - 1) # to ensure lexicographical order
IndexKey = ObjectName + "00000000"
```

The `DataKey` is used to store the actual time-series data points, while the `IndexKey` is used to store metadata about the `ObjectName` for quick lookups.

The `DataValue` contains the serialized data table for the corresponding `ObjectName`, while the `IndexValue` contains metadata such as the number of chunks, time range, and other relevant information for efficient querying and management of the time-series data.

## Update Process

When new data points arrive for a specific `ObjectName`, the update process involves the following steps:

1. `MsdDB` keep a in-memory buffer for each `ObjectName` as last chunk. so that new data points can be appended to this buffer.
2. When the buffer reaches a certain size or time threshold, it is serialized into a `DataValue` and stored in `MsdStore` with a new `DataKey`.
3. The `IndexValue` for the `ObjectName` is updated to reflect the new chunk, including updating the count of chunks and the time range.
4. The updated `IndexValue` is then stored back in `MsdStore` with the corresponding `IndexKey`.

### Update Kind

The `DbTable` may have a metadata field called `round_ts` to indicate the time granularity for data aggregation. When updating data points, the timestamp of each data point is rounded according to the `round_ts` value. then the rounded timestamp is used to determine how to store or aggregate the data points in the `DbTable`.

- **Append**: The rounded timestamp is newer than the latest timestamp in the existing data. The new data points are simply appended to the end of the `DbTable`.
- **Update**: The rounded timestamp is same as an latest timestamp in the existing data. The existing data points at that timestamp are updated with the new values. Because the data is rounded, the update is done on the aggregated data for that time interval. Each filed can have its own aggregation method, such as sum, average, max, min, etc.
- **Insert**: The rounded timestamp is older than the latest timestamp in the existing data but does not exist in the data. The new data points are inserted into the appropriate position in the `DbTable` based on the rounded timestamp. This may require shifting existing data points to accommodate the new entries.
- **Ignore**: The `DbTable` may also have a metadata field called `ignore_older_than` to specify a cutoff timestamp. If the rounded timestamp of the new data points is older than this cutoff, the update operation is ignored, and the data points are not stored in the `DbTable`. This helps to prevent stale or irrelevant data from being added to the time-series database.


# Implementation Details

The `MsdDB` is implemented in Rust as a library crate named `msd-db`. It provides a high-level API for managing time-series data tables, including functions for creating, updating, querying, and deleting `DbTable` entries.

The `msd-db` crate depends on other crates in the `msd-rs2` workspace, including `msd-store` for low-level key-value storage and `msd-table` for data table management.

## Struct Definitions

### MsdDb

`MsdDb` is the main entry point for the database. It manages the storage engine and the worker pool.

```rust
pub struct MsdDb<S: MsdStore> {
    store: Arc<S>,
    workers: Vec<Sender<Request>>,
}

impl<S: MsdStore> MsdDb<S> {
    pub fn new(store: S, worker_count: usize) -> Self { ... }
    pub async fn insert(&self, table: &str, obj: &str, data: Table) -> Result<()> { ... }
    pub async fn query(&self, table: &str, obj: &str, range: TimeRange) -> Result<Table> { ... }
}
```

### Worker and Request

To improve performance, `msd-db` rarely use locking mechanisms. Instead, it use channel-based communication and background worker tasks to handle concurrent operations safely and efficiently. Each worker holds the in-memory state, caches, and buffers for processing data updates and queries.

When a client makes a request, the client is dispatched to the appropriate worker via channels by hashing the `ObjectName`. Workers process the requests asynchronously, and workers count is configurable based on the system's CPU cores and expected workload. This request-response model use a one-shot channel for each request to ensure that responses are sent back to the correct client without blocking other operations.

```rust
enum Request {
    Insert {
        table: String,
        obj: String,
        data: Table,
        resp: Sender<Result<()>>,
    },
    Query {
        table: String,
        obj: String,
        range: TimeRange,
        resp: Sender<Result<Table>>,
    },
}

struct Worker<S: MsdStore> {
    id: usize,
    store: Arc<S>,
    // buffers: HashMap<(String, String), Table>, // (table, obj) -> buffer
}
```

## Storage Key Design

As mentioned in the high-level design:

```
Key = DataKey | IndexKey
DataKey = ObjectName + SequenceNumber
IndexKey = ObjectName + "00000000"
```

Note: The `TableName` is not part of the key because each table has its own separate store space.

## Data Flow

1.  **Insert**:
    - Client calls `MsdDb::insert`.
    - `MsdDb` hashes `(table, obj)` to select a worker.
    - Sends `Request::Insert` to the worker.
    - Worker appends data to the in-memory buffer.
    - If buffer exceeds limit, worker flushes to `MsdStore`.
    - Worker sends response back to client.

2.  **Query**:
    - Client calls `MsdDb::query`.
    - `MsdDb` hashes `(table, obj)` to select a worker.
    - Sends `Request::Query` to the worker.
    - Worker reads from in-memory buffer and `MsdStore`.
    - Merges results and sends back to client.
