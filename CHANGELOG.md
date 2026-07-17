# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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