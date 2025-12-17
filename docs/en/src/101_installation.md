# Installation

## Prerequisites

Ensure you have the latest Rust toolchain installed.

## Build from Source

Clone the repository and build the project using Cargo:

```bash
cargo build --release
```

This will produce a single binary `msd` in `target/release/`.

## Docker

You can also build a Docker image using the provided `Dockerfile`:

```bash
docker build -t msd-rs .
```
