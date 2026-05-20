# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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