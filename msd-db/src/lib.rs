//!  MSD database module.
//!
//! # High Level Design of MSD-DB

//! For a time-series data, it's should have the basic definition as below:
//!
//! - **TableName**: The name of the time-series data table, e.g. `temperature_sensors`, `stock_kline`.
//! - **ObjectName**: the owner name of the data, for example, hostname, device id, user id, stock symbol, etc.
//! - **Timestamp**: the time when the data is generated or collected.
//! - **Value Columns**: the actual data fields, which can be of various types (integer, float, string, etc.). e.g.
//!   - For a temperature sensor, columns can be `temperature`, `humidity`, `pressure`.
//!   - For a stock market data, columns can be `open`, `close`, `high`, `low`, `volume`.
//! - **Schema**: the structure that defines the data types and organization of the value columns, also with metadata like units, descriptions, etc.
//!
//! This time-series data in MSD-DB is called as `DbTable`j
//!
//! MSD-DB store the `DbTable` in a partitioned by `ObjectName` then partitioned by `Time` way. Because the underlying storage engine is `MsdStore`, which is a key-value store, the key is designed as below:
//!
//! ```
//! Key = DataKey | IndexKey
//! DataKey = ObjectName + SequenceNumber
//! SequenceNumber = Hex(-COUNT_OF_CHUNKS_BEFORE - 1) # to ensure lexicographical order
//! IndexKey = ObjectName + "00000000"
//! ```
//!
//! The `DataKey` is used to store the actual time-series data points, while the `IndexKey` is used to store metadata about the `ObjectName` for quick lookups.
//!
//! The `DataValue` contains the serialized data table for the corresponding `ObjectName`, while the `IndexValue` contains metadata such as the number of chunks, time range, and other relevant information for efficient querying and management of the time-series data.
//!
//! ## Update Process
//!
//! When new data points arrive for a specific `ObjectName`, the update process involves the following steps:
//!
//! 1. `MsdDB` keep a in-memory buffer for each `ObjectName` as last chunk. so that new data points can be appended to this buffer.
//! 2. When the buffer reaches a certain size or time threshold, it is serialized into a `DataValue` and stored in `MsdStore` with a new `DataKey`.
//! 3. The `IndexValue` for the `ObjectName` is updated to reflect the new chunk, including updating the count of chunks and the time range.
//! 4. The updated `IndexValue` is then stored back in `MsdStore` with the corresponding `IndexKey`.
//!
//! ### Update Kind
//!
//! The `DbTable` may have a metadata field called `round_ts` to indicate the time granularity for data aggregation. When updating data points, the timestamp of each data point is rounded according to the `round_ts` value. then the rounded timestamp is used to determine how to store or aggregate the data points in the `DbTable`.
//!
//! - **Append**: The rounded timestamp is newer than the latest timestamp in the existing data. The new data points are simply appended to the end of the `DbTable`.
//! - **Update**: The rounded timestamp is same as an latest timestamp in the existing data. The existing data points at that timestamp are updated with the new values. Because the data is rounded, the update is done on the aggregated data for that time interval. Each filed can have its own aggregation method, such as sum, average, max, min, etc.
//! - **Insert**: The rounded timestamp is older than the latest timestamp in the existing data but does not exist in the data. The new data points are inserted into the appropriate position in the `DbTable` based on the rounded timestamp. This may require shifting existing data points to accommodate the new entries.
//! - **Ignore**: The `DbTable` may also have a metadata field called `ignore_older_than` to specify a cutoff timestamp. If the rounded timestamp of the new data points is older than this cutoff, the update operation is ignored, and the data points are not stored in the `DbTable`. This helps to prevent stale or irrelevant data from being added to the time-series database.

pub mod db;
pub mod errors;
pub mod index;
pub mod keys;
pub mod request;
pub mod worker;
pub use db::MsdDb;
mod serde;
pub use serde::DbBinary;
