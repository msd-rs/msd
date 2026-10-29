# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.1.21] - 2026-10-29

### Added

- Support for configuring maximum retained daily log files via `--log-max-days` CLI argument or `MSD_LOG_MAX_DAYS` environment variable.
- Native high-performance HTTP client `MsdConnection` in Python bindings using Tokio and Reqwest:
  - `query`: executes queries and parses streamed table frames directly in Rust into `{obj: {table_name: Table}}`.
  - `query_join`: queries and performs as-of joins across multiple tables directly in Rust.
  - `snapshot`: captures latest snapshots of multiple tables across objects with support for single objects, wildcards (`*`), and KV table lookups (`@table:key`).
- Native Python `Table` wrapper offering fast cell access and zero-copy conversion to NumPy data via `to_dict()` and `to_list()`.
- Table operations in `msd-table`: horizontal concatenation (`h_concat`), vertical concatenation (`v_concat`), and `join_asof` with configurable fill methods (`Zero`, `Nan`, `Backward`, `Forward`).
- Worker batch query execution (`BatchQuery` / `query_batch`) in `msd-db`, batching large object query sets by worker to improve throughput and concurrency.
- `Server-Timing` HTTP response header reporting database query duration (`db`), serialization duration (`enc`), and total processing time (`total`).
- Streaming table serialization utilities `write_table_to` and `write_table_v2_to` in `msd-table`.

### Changed

- Server query handler now streams and encodes results progressively instead of buffering all tables in memory.
- Shell query CLI displays `Server-Timing` metrics and optimizes multi-table streaming display with single header rendering and buffered printing.
- Upgraded Python binding dependencies `pyo3` and `numpy` to `0.30`.
- Updated default Docker container listen port to `20510`.

## [0.1.20] - 2026-10-23

### Added

- Support for IP authentication whitelist via `--auth-whitelist` CLI argument or `MSD_AUTH_WHITELIST` environment variable to bypass token auth for trusted IPs and CIDR blocks.
- Shell query support for authentication token via `--token` CLI argument or `MSD_AUTH_TOKEN` environment variable.
- Automatic decompression support in CLI `import` for gzip (`.gz`), zstd (`.zstd`), and brotli (`.br`) compressed files (both CSV and `.tbl` formats) from local paths, HTTP URLs, and stdin.

### Changed

- Renamed server JWT signing key configuration from `auth_token` / `MSD_AUTH_TOKEN` to `auth_key` / `MSD_AUTH_KEY`.
- Migrated table and object identifiers to `SmolStr` across `msd-db`, `msd-request`, and `msd-table` to reduce heap allocations.
- Configured HTTP server response compression to focus on Zstandard (`zstd`).
- Added `#[repr(u8)]` attribute to `DataType`.

### Fixed

- Fixed HTTP request decompression by configuring `RequestDecompressionLayer` on the router.

## [0.1.19] - 2026-09-21

### Added

- obj filter now support read content from other kv table, for example:
    - `stock_block` is a kv table that mapping block name to obj list, there is a block named '出海概念'
    - `select * from stock_kline_1d where obj = '@stock_block:出海概念' limit -1;` will return all stocks in that block
    - This feature alleviates the burden of building large WHERE clauses.

## [0.1.18] - 2026-09-21

### Fix

- A python sdk syntax error

## [0.1.17] - 2026-09-20

### Added

- Native join for `MsdTable` in Python bindings via Rust implementation (`aligned_index`) for high-performance time-series alignment.
- New `load_concat` and `build_sql` APIs in Python `MsdClient` with `Aligner` utility.
- Table Frame binary serialization format v2 (`table_frame_v2`), optimized for simpler and faster deserialization in JavaScript/TypeScript.
- Full Table Frame binary v2 support in TypeScript SDK.
- CLI `convert` command to convert tables between CSV, JSON, JSONL, and TableFrame (v1/v2) formats.
- CLI `import` command now supports importing both CSV and binary table frame (`.tbl`) files from file paths, stdin, or HTTP URLs.
- Support for `truncate` option before import / insert requests to clear existing table data prior to inserting.
- TypeScript SDK support for static file mock queries (JSON, binary v1, binary v2).

### Changed

- Import handler now allows input rows to have extra columns.
- Python: Improved concatenation performance in Polars DataFrame adaptor using Polars `LazyFrame` (`pl.collect_all` and vertical concat).
- Moved `table_frame` module from `msd-request` to `msd-table`.
- Upgraded `rocksdb` and other workspace dependencies (including `rmcp`, `pyo3`).

### Fixed

- TypeScript SDK: Fix text decoding in binary v2 format to use subarray slice.

### Removed

- Removed `D128` (`rust_decimal::Decimal`) data type from `msd-table`.


## [0.1.16] - 2026-07-22

### Fix

- core: list objects with in operator issue
- python: `load` now can pass '=2026-01-01' as `end` to include 2026-01-01 data


## [0.1.15] - 2026-07-18

### Added

- `limit` in SQL Query now support negative number, which return last n rows instead top n rows for positive number, the common SQL 
``` sql
SELECT * FROM (
    SELECT * 
    FROM your_table 
    ORDER BY ts DESC 
    LIMIT 10
) AS subquery
ORDER BY ts ASC;
```
can be simplify to 
``` sql
SELECT * FROM your_table ORDER BY ts ASC LIMIT -10;
```

### Fix

- Fix performance regression of large object query.




## [0.1.14] - 2026-07-15

### Fix

- Fix serde issues in table

## [0.1.13] - 2026-07-10

### Added

- Support for multiple channel workers/senders to improve message processing throughput.
- Custom delimiter configuration option when importing CSV files.
- Support for `Decimal` type in SQL parsing.

### Changed

- Query worker now returns an empty table instead of an error when the requested object is not found.
- Optimized performance of clearing/deleting table contents.
- Upgraded workspace crate dependencies (including `axum`, `sqlparser`, and others).
- Fixed compiler and binding warnings in Python and TypeScript bindings.

### Fixed

- Fixed import failure for Key-Value (KV) engine tables.

## [0.1.12] - 2026-05-21

### Added

- Key-value (KV) storage engine mode support (`WITH (engine='kv')`) for simple point lookup/mapping tables, bypassing background workers, caches, and partitions.
- Integration tests for KV engine mode verifying SQL table creation, inserts, deletes, point queries, and wildcard operations.
- Reference documentation for KV engine mode in the SQL interface guide.
- Developer design/architecture documentation.
- Column unit attribute support.
- Channel update notifications count in `Notify`.
- Broker configuration in application state.
- Chinese (`README.cn.md`) translation.

### Changed

- Enhanced channel notification/trigger updates implementation.
- Improved CSV parser to support empty line handling.
- Improved `Variant` parser to support empty value handling.
- Optimized concatenation functionality in python binding.
- Added `pre_join_hook` during table load.
- Upgraded workspace crate dependencies and bumped version configurations.

### Removed

## [0.1.11] - 2026-01-08

### Added

- Python doc

### Changed

- Fix insert order


## [0.1.10] - 2026-01-07

First github release