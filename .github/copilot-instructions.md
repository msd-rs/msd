This project named `msd` is a Rust application that implements a high-performance database system.

There are several background information that can help you understand the code better:

- The project use cargo workspaces to manage multiple crates.
  - `msd` is the main crate that contains the entry point of the application.
  - `msd-store` is a library crate that implements low-level storage engine
    - the trait `MsdStore` is the main interface of the storage engine
    - currently, the `RocksDbStore` is the only implementation of `MsdStore`, which uses RocksDB as the underlying storage engine
  - `msd-table` is a library crate that implements the table, `table` is a logical unit of data that can be queried and manipulated
    
