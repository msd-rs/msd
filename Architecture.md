# Software Architecture Description

## 1. Introduction

**MSD (Micro Strategy Daemon)** is a time-series database optimized for financial quantitative analysis. It abandons the traditional "heavy query, light transport" architecture, prioritizing "data retrieval bandwidth" and "integration with analysis tools" as core design metrics.

### Core Philosophy: Solving the "Last Mile" from "Data" to "Analysis"

In financial quantitative research, researchers' time is often wasted waiting for data to load from the database into Python memory.

**Traditional Pain Points**: General-purpose databases (like MySQL, InfluxDB) bottleneck on the Python side during the Binary -> Objects -> DataFrame conversion. Even if the underlying C++ engine is fast, parsing byte streams row-by-row and encapsulating them as Python objects consumes significant CPU and causes frequent memory allocations.

**MSD's Solution**: "Compute-Ready" Delivery. MSD stores and transmits data in a binary format completely consistent with NumPy's memory layout. Query results map directly to numpy.ndarray, achieving **Zero-Parsing** loading from disk to DataFrame.


### Global Design
MSD adopts a **Layered Architecture**:
1.  **Interface Layer**: Exposes services via HTTP API and provides interactive Shell tools.
2.  **Core Logic Layer**: `msd-db` is responsible for data organization, index maintenance, and read/write flow control.
3.  **Data Structure Layer**: `msd-table` defines the columnar storage memory structure (similar to Apache Arrow), optimizing analysis calculation performance.
4.  **Storage Layer**: `msd-store` abstracts the underlying storage engine, currently defaulting to **RocksDB** as the persistent Key-Value storage.

Core data model concepts include:
-   **TableName**: Table name (e.g., `kline`).
-   **ObjectName**: Data ownership object (e.g., `SH600519`).
-   **Timestamp**: Timestamp.
-   **Value Columns**: Actual data columns.

The data storage strategy is deeply partitioned based on `ObjectName` and `Timestamp`; the underlying Key design ensures physical aggregation and sequential access of data.

Python Binding is a first-class citizen of MSD, providing a high-performance interface for interacting with MSD, and seamless integration with analysis tools like NumPy, Pandas, Polars.

### Non-Goals

- Complex SQL Queries: Including advanced operations like Join, Window, Aggregate, Group By. These functions can be implemented via more efficient, specialized Python libraries.
- High Availability Service: MSD aims to provide a high-performance, easy-to-use, maintenance-free time-series database for independent quantitative work, making it easy to run on workstations or personal computers to serve localhost or LAN users, without pursuing high availability. However, being written in Rust, it already possesses high availability at the single-machine level.
- User Accounts: MSD does not support user accounts but does support access authentication.

---

## 2. The Main Project: `msd`

The `msd` crate is the entry point for the entire project, compiling to generate the executable file `msd`. It mainly contains three subcommand functions:

### 2.1 MSD Server (`msd server`)
This is the server-side process of the database, responsible for handling external requests.
*   **Functions**:
    *   Start HTTP server (based on `axum`).
    *   Maintain database instance lifecycle (start/stop RocksDB).
    *   Handle concurrent read/write requests.
*   **API**:
    *   `POST /query`: Execute query statements. Supported SQL operations include:
        *   **SQL Operations**:
            *   **Query Data**: `SELECT ...` (supports `WHERE`, `ORDER BY`, `LIMIT`)
            *   **Create Table**: `CREATE TABLE ...` (supports defining column types and metadata)
            *   **Insert Data**: `INSERT INTO ...` (supports `VALUES` and `COPY` modes)
            *   **Delete Data**: `DELETE FROM ...` (delete by object or time range)
            *   **Drop Table**: `DROP TABLE ...`
            *   **Get Schema**: `DESCRIBE <table>`
            *   **List Object**: `SELECT obj FROM <table>` 
        *   **Recommended Query Methods**:
            *   **Python SDK**: Use `pymsd.query` (synchronous) or `pymsd.async_query` (asynchronous).
            *   **CLI Tool**: Use `msd shell` for interactive execution.
        *   **Response Formats**:
            *   **Binary Format** (when User-Agent contains `msd-client`):
                *   Returns `application/x-msd-table-frame` binary stream.
                *   Python SDK automatically handles and parses it into a Generator, where each item is an `(object_name, date_table)` tuple.
            *   **NDJSON Format** (Default):
                *   Returns `application/x-ndjson` (Newline Delimited JSON).
                *   Each line is a complete JSON object, representing a table data block.
    *   `PUT /table/{table_name}`: Data write interface.
        *   **Recommended Write Methods**:
            *   **Python SDK**: Use `pymsd.import_csv` or `pymsd.import_dataframes` (located in `msd.update` module) for high-performance writing.
            *   **CLI Tool**: Use `.import` command in `msd shell` to import CSV files.
        *   **Underlying Protocols**:
            *   **CSV Format** (Default):
                *   Can directly send CSV text data.
                *   **First column** must be `obj` (object name), subsequent columns correspond to table structure.
                *   Supports `?skip=N` parameter to skip the first N lines (e.g., Header).
            *   **Binary Format**:
                *   Requires setting Header `Content-Type: application/x-msd-table-frame`.
                *   Sends MSD's custom `TableFrame` binary frame stream, which has higher performance.
                *   **Construction Method**: Recommended to use Python binding library `pymsd` to generate.
                    ```python
                    import msd
                    # df can be pandas.DataFrame, polars.DataFrame or list of (name, array)
                    df_list : Iterator[Tuple[str, DataFrame]] = [(name, df)]
                    msd.import_dataframes(baseURL, table_name, df_list)
                    ```
*   **Features**:
    *   **Authentication**: Supports JWT Token authentication (`auth-token`) and role-based permission control (`read`, `write`, `admin`).
    *   **Pre Aggregation**: Do aggregation when data updating, for example, a `kline` can be updated through `snapshot` or `tick` datafeed, the OLHCVA data is aggregated automatically. 
    *   **Chain Updates**: When a table is updated, it can trigger updates in dependent tables, for example, a `snapshot` can trigger updates in `kline` tables by configuration. With this feature and pre-aggregation, it is very easy to connect a exchange datafeed.
    *   **Single Binary Deployment**: The server and client are compiled into a single binary file without other dependencies, making it easy to deploy and manage.
    *   **High Performance**: In can handle high write/read throughput, For a typical hardware 8C 16G NVMe SSD, it can handle about 8M rows/second insert and 10M rows/second query. see [bench-of-msd](https://cnb.cool/elsejj/bench-of-msd) for more details.
    *   **AI Friendly**: It's built-in MCP tool, help AI to understand the data catalog and schema, then write correct analysis programs for your research.

### 2.2 MSD Shell (`msd shell`)
This is the built-in interactive command-line client for managing and querying the database.
*   **Function**: Provides a SQL interactive environment (REPL).
*   **Supported Commands**:
    *   SQL Statements: Input SQL directly for queries, ending with a semicolon `;`.
    *   `.server <url>`: Set the server address to connect to.
    *   `.import <file> <table> [skip]`: Import CSV file into the specified table.
    *   `.dump <table> [file]`: Export table data to CSV format.
    *   `.schema <table>`: View table structure.
    *   `.rows <num>`: Set the limit for displayed rows.
    *   `.output [file]`: Redirect output to a file.
    *   `.help`, `.exit`: Help and exit.

### 2.3 MSD Token (`msd token`)
Used to generate JWT authentication Tokens required for accessing the server.
*   **Usage**: Specify key (`-a`), role (`-r`), and expiration time (`-e`) to generate a Token string.


## 3. SubProjects

The project is organized using Cargo Workspace, containing multiple core crates with divided responsibilities as follows:

### 3.1 `msd-db` (Core Database Engine)
*   **Purpose**: Implements the core logic of the time-series database.
*   **Design**:
    *   **Data Model**: Defines `DbTable`, managing Schema and metadata.
    *   **Storage Layout**:
        *   **DataKey**: `ObjectName` + `SequenceNumber` (time-based chunking), used to store actual time-series data blocks.
        *   **IndexKey**: `ObjectName` + fixed suffix, used to store object metadata (indexes).
    *   **Write Flow**: `In-Memory Buffer` -> `Serialize` -> `RocksDB`.
    *   **Update Strategy**: Supports Append, Update (aggregate update), Insert, and Ignore (ignore old data).

### 3.2 `msd-table` (In-Memory Data Table Structure)
*   **Purpose**: Provides efficient columnar data structures for in-memory calculation and serialization.
*   **Design**:
    *   Although the description mentions "based on apache arrow", it actually implements a lightweight columnar storage structure.
    *   Contains structures like `Table`, `Series` (column), `Field` (field).
    *   Supports multiple data types (`D64` (double), `D128` (decimal), `Timestamp`, `String`, etc.).
    *   Responsible for binary serialization and CSV parsing.

### 3.3 `msd-store` (Storage Abstraction Layer)
*   **Purpose**: Decouples database logic from underlying physical storage.
*   **Design**:
    *   Defines `MsdStore` trait, containing standard KV operation interfaces like `get`, `put`, `delete`, `scan` (prefix_with).
    *   **`RocksDbStore`**: Concrete implementation based on RocksDB, providing high-performance local SSD/HDD storage capabilities.

### 3.4 `msd-request` (Protocol and Models)
*   **Purpose**: Defines data models for communication between client and server.
*   **Design**:
    *   Contains struct definitions for query requests (`Query`), aggregation operations (`Agg`), filter conditions (`Filter`), etc.
    *   Defines `TableFrame` binary protocol for efficient table data transmission.
    *   Acts as the contract between `msd` (client/server) and `msd-db`.

### 3.5 `msd-db-viewer` (Debugging Tool)
*   **Purpose**: Developer tool used to directly view underlying RocksDB data without starting the server.
*   **Functions**:
    *   Can directly read RocksDB files (`CURRENT`, SSTable, etc.).
    *   Browse raw Schema, Index, and Data blocks by table name (`table`) and key (`key`).
    *   Output in JSON format for easy debugging of storage layer issues.

### 3.6 `bindings` (Multi-language Bindings)
*   **Purpose**: Enables other programming languages to conveniently call MSD functions or interact with the MSD server.
*   **Python Binding (`bindings/python`)**:
    *   **Crate Name**: `pymsd`.
    *   **Technology**: Built based on `pyo3` and `numpy`.
    *   **Function**: Allows Python programs to directly manipulate MSD data structures, suitable for data analysis and scientific computing scenarios.
    *   **Features**: The main function of the Python binding is to support directly converting MSD data tables into `pandas.DataFrame`, `polars.DataFrame`, `numpy.ndarray`.
*   **TypeScript Binding (`bindings/typescript`)**:
    *   **Technology**: Built using `bun`, supporting the generation of ESM modules for Browser and Node.js environments.
    *   **Function**: Provides a type-safe JS/TS client library, facilitating interaction between Web frontend or Node.js backend applications and MSD.
