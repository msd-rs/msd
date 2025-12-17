# Rust

You can use the `msd-request` crate to interact with the database programmatically from Rust applications.

## Example

```rust
use msd_request::{MsdClient, QueryRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MsdClient::new("http://127.0.0.1:50510");
    let response = client.query("SELECT * FROM my_table").await?;
    println!("{:?}", response);
    Ok(())
}
```
