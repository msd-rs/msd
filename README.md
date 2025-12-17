
# MSD-RS

MSD-RS is a **high-performance time-series database** built on top of [RocksDB](https://rocksdb.org/), written in pure Rust. It is engineered to handle massive amounts of time-series data with speed and efficiency.

## Purpose & Domain

The primary goal of MSD-RS is to provide a robust storage and query engine for high-frequency time-series data. 

**Most Suitable Domain:**

- **Quantitative Finance:** Storing and analyzing market data (ticks, candles, quotes).
  - **Zero-Latency OHLCV:** The **Data Pre-aggregation** feature eliminates the need to scan millions of ticks to build K-lines (candles), allowing for instant access to any timeframe (1m, 1d, etc.) as data streams in.
  - **Reactive Data Pipelines:** **Chain Updates** allow new market data to automatically propagate through dependent tables, updating indicators or derived strategies in real-time.
  - **High-Frequency Ready:** Optimized for the high write throughput and low latency reads required by algorithmic trading and market analysis.

## Key Features

- **🚀 High Performance:** Leveraging Rust's safety and speed, coupled with the proven performance of RocksDB for persistent storage.
- **⚡ Data Pre-aggregation:**
  - Instead of performing costly aggregations at query time, `msd` computes aggregations incrementally as data is updated.
  - Ideally suited for financial data where raw ticks are voluminous; the system maintains real-time aggregated states (like `Sum`, `Min`, `Max`, `Avg`, `First`, `Uniq`) for derived datasets (e.g., 1-minute or 1-day K-lines), ensuring instant query responses without scanning raw history.
  - *See `agg_state.rs` for implementation details.*
- **🔗 Chain Updates (Chan):**
  - Configurable "Chain" logic allows updates in one table to automatically propagate to and update independent dependent tables.
  - For example, an update to a `snapshot` table can trigger updates to `kline1m` and `kline1d` tables, filtering and transforming data on the fly (e.g., `ChangedIf` logic).
  - This capability enables complex, event-driven data pipelines entirely within the database engine.
  - *See `chan.rs` for implementation details.*
- **📦 Structured Data:** Based on strongly typed `Table` and `Series` structures, ensuring data integrity and efficient columnar processing.
- **🔌 Client-Server Architecture (HTTP Protocol):**
  - **Server:** Built with `axum`, capable of handling high concurrency request processing.
  - **Universality:** Uses standard HTTP for communication, making it accessible from **any** programming language or tool (curl, Postman, browser) without needing custom drivers.
  - **Simplicity & Debugging:** Requests and responses are easy to inspect and debug.
  - **Shell:** Integrated interactive shell for querying and managing the database.
- **🐍 Python Bindings:** Seamless integration with Python ecosystem for data science and analysis workflows.
  - **Zero-Copy NumPy Transformation:** Leveraging Rust's memory safety and PyO3, the bindings allow for near instant transformation of `msd` tables into NumPy arrays (using `from_vec` / `from_slice`), enabling ultra-fast data analysis without serialization overhead.
- **Binary & CSV Support:** Efficient binary serialization for internal storage and optimized CSV support for bulk data ingestion.
- **🛠️ Single Binary Deployment:**
  - **Zero Dependencies:** compiled as a single, self-contained executable with all dependencies (including RocksDB) statically linked.
  - **Simplified Ops:** No complex installation scripts, library conflicts, or container orchestrations required—just copy the binary and run.
  - **Easy Upgrades:** Updating the database is as simple as replacing the executable file.

## Usage Guide

### prerequisites

Ensure you have the latest Rust toolchain installed.

### Build the Project

```bash
cargo build --release
```

### Running the Server

Start the MSD server to accept connections and requests.

```bash
# Run with default settings (listens on 127.0.0.1:50510)
cargo run --release -p msd -- server

# Custom configuration
cargo run --release -p msd -- server --listen 0.0.0.0:8080 --workers 16 --db ./my_data
```

**Server Options:**
- `-l, --listen <ADDR>`: Address to listen on (default: `127.0.0.1:50510`).
- `-w, --workers <NUM>`: Number of worker threads (default: `8`).
- `--db <PATH>`: Path to the database directory (default: `./msd_db`).
- `-a, --auth-token <TOKEN>`: Optional authentication token.

### Interactive Shell

Connect to a running server and interact with it using the built-in shell.

```bash
cargo run --release -p msd -- shell
```

**Shell Options:**
- `-s, --server <URL>`: Server URL to connect to (default: `http://127.0.0.1:50510`).
- `[COMMAND]`: Optional command to run directly (non-interactive mode).


### Running Tests

Run the comprehensive test suite across all workspace members.

```bash
cargo test --workspace
```

## Project Structure

- **`msd`**: Main binary (entry point for Server & Shell).
- **`msd-db`**: Core database logic, request handling, and worker pool.
- **`msd-store`**: Storage layer abstraction (RocksDB implementation).
- **`msd-table`**: High-performance dataframe/table structures.
- **`msd-request`**: Protocol definitions for requests and responses.
- **`bindings/python`**: Python client bindings.
- **`bindings/typescript`**: TypeScript client bindings.
