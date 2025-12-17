# Client

MSD-RS uses a standard HTTP protocol for communication, making it accessible from any programming language or tool (curl, Postman, browser).

## Interactive Shell

Connect to a running server and interact with it using the built-in shell.

```bash
cargo run --release -p msd -- shell
```

### Shell Options

- `-s, --server <URL>`: Server URL to connect to (default: `http://127.0.0.1:50510`).
- `[COMMAND]`: Optional command to run directly (non-interactive mode).

## HTTP Protocol

The server exposes an HTTP API. You can send requests using standard HTTP methods. The default port is `50510`.

Example using `curl`:

```bash
curl -X POST http://127.0.0.1:50510/query -d "SELECT * FROM my_table"
```
