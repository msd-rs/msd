# Project Guide

## Project Overview
`msd-rs2` is a high performance time series database built on top of RocksDB. It is designed to store and query time series data in a fast and efficient manner. 

## Project Structure
The project is organized as a Cargo workspace with the following members:

- **`msd`**: The main binary crate acting as the entry point for the application.
- **`msd-store`**: A library crate responsible for the storage layer, currently implementing a RocksDB-based store.
- **`msd-table`**: A library crate handling data tables 
  - `Table`: A data structure for storing tabular data
  - `Series`: A data structure for storing a data series
  - `Variant`: A data structure for storing data with any type, it is used to interact with `Series` and `Table`. `Variant` has a set of API convent between native types. There also 

## Technical Stack
- **Language**: Rust (Edition 2024)
- **Core Dependencies**:
    - `rocksdb`: For persistent storage (in `msd-store`).
    - `serde`, `serde_json`: For serialization and deserialization.
    - `tracing`: For logging and instrumentation.
    - `anyhow`, `thiserror`: For error handling.
    - `time`: For time handling.

## Development Workflow

### Build
To build the entire workspace:
```bash
cargo build
```

### Test
To run tests across all crates in the workspace:
```bash
cargo test
```

### Running the Application
To run the main binary:
```bash
cargo run -p msd
```

## Key Conventions
- **Error Handling**: Uses `thiserror` for library errors and `anyhow` for application-level error handling.
- **Logging**: Uses `tracing` for structured logging.
