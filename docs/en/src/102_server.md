# Server

Start the MSD server to accept connections and requests.

## Basic Usage

```bash
# Run with default settings (listens on 127.0.0.1:50510)
cargo run --release -p msd -- server
```

## Custom Configuration

```bash
cargo run --release -p msd -- server --listen 0.0.0.0:8080 --workers 16 --db ./my_data
```

## Server Options

- `-l, --listen <ADDR>`: Address to listen on (default: `127.0.0.1:50510`).
- `-w, --workers <NUM>`: Number of worker threads (default: `8`).
- `--db <PATH>`: Path to the database directory (default: `./msd_db`).
- `-a, --auth-token <TOKEN>`: Optional authentication token.
