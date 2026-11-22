# Introduction

This is the Python binding for [msd](https://github.com/msd-rs/msd-app). `msd` is a high-performance financial time series database.

It provides a native Rust client `MsdConnection`, which handles streaming binary table frames directly via Tokio and Reqwest with zero-copy NumPy conversion, native joins, snapshots, and fast data imports.

# Installation

```bash
pip install pymsd
```

# Native Client: MsdConnection

`MsdConnection` is implemented in Rust. It eliminates Python HTTP overhead, streams and parses binary table frames natively, and returns lightweight `Table` objects with zero-copy conversion to NumPy ndarrays.

```python
import pymsd

# Initialize connection (supports optional bearer token and zstd compression)
conn = pymsd.MsdConnection("http://localhost:50510", token="", compress=True)
```

## Table Representation

Queries through `MsdConnection` return native `Table` instances:
- **Metadata**: `table.name`, `table.obj`, `table.rows`, `table.columns`, `table.header`.
- **Cell access**: `table[row, col]` returns typed Python values.
- **Zero-copy export to NumPy** (transfers memory buffer and resets the `Table` instance):
  - `table.to_dict()`: returns `{column_name: np.ndarray}`
  - `table.to_list()`: returns `[(column_name, np.ndarray), ...]`
  - Convert directly to pandas or polars DataFrames:
    ```python
    import pandas as pd
    import polars as pl

    df_pd = pd.DataFrame(table.to_dict())
    df_pl = pl.DataFrame(table.to_dict())
    ```

## Querying Data

### 1. Grouped Query (`query`)
Executes SQL queries and groups the results into a 2-level dictionary: `{obj: {table_name: Table}}`.

```python
sql = "SELECT * FROM kline WHERE obj = 'SH600000' LIMIT -100;"
results = conn.query(sql)

for obj, tables in results.items():
  for table_name, table in tables.items():
    print(obj, table_name, table.rows, table.columns)
```

### 2. Streaming Query with Callback (`query_map`)
Streams each table frame progressively as it arrives from the server and invokes a callback handler. This keeps memory usage minimal when processing large query result sets.

```python
def on_table(table: pymsd.Table):
  print(f"Received table: {table.obj}.{table.name} with {table.rows} rows")

conn.query_map("SELECT * FROM kline WHERE obj IN ('SH600000', 'SZ000001');", on_table)
```

### 3. Native As-Of Join (`query_join`)
Aligns and joins multiple time-series tables across multiple objects directly in Rust.

```python
sql = """
SELECT * FROM stock_kline_1d WHERE obj = 'SH600*' LIMIT -1000;
SELECT * FROM stock_shares WHERE obj = 'SH600*';
"""
# Aligns stock_shares to stock_kline_1d using backward fill, based on SH600000 timeline
table, objs, bars = conn.query_join(
  sql=sql,
  base_table="stock_kline_1d",
  fill={"stock_shares": "backward", "*": "nan"},
  base_obj="SH600000",
)
print(f"Joined {len(objs)} objects, each {bars} rows. Total rows: {table.rows}")
```

### 4. Multi-Table Snapshot (`snapshot`)
Takes the latest snapshot across multiple tables for a set of objects and joins them by object into a single table.

Supports:
- **Objects**: single code (`'SH600000'`), wildcard pattern (`'SH600*'`), or block lookup (`'@stock_block:全部A股'`).
- **Field selection**: `'table_name: field1, field2'`.
- **Timestamp feature filtering**: `'table_name@ts.month=12: field1, field2'`.

```python
table = conn.snapshot(
  tables=[
    "stock_kline_1d",
    "stock_shares: total_shares",
    "stock_capital_flow",
    "stock_financial@ts.month=12: f001, f002", # only take the annal report
  ],
  objects=["@stock_block:全部A股"],
)
print("Snapshot shape:", table.rows, table.columns)
```

## Importing Data

`MsdConnection` provides high-throughput data import methods.

### 1. Import CSV (`import_csv`)
Import CSV data from raw `bytes`, a file path (`str` or `Path`), or a binary file-like stream object.

```python
# From file path or Path object
res = conn.import_csv("my_table", "data.csv", header=True, delimiter=",")

# From file-like object with .read()
with open("data.csv", "rb") as f:
  res = conn.import_csv("my_table", f, header=True)
print("Import result:", res)
```

### 2. Import Table Frame (`import_table`)
Import native Table Frame binary format from a `Table` instance, raw `bytes`, a file path, or a binary file-like stream object.

```python
# Import from an existing Table instance
res = conn.import_table("target_table", table)

# Import from a .tbl file
res = conn.import_table("target_table", "data.tbl")
```

### 3. Import DataFrames (`import_dataframes` / `import_dataframe`)
Stream pandas or polars DataFrames into `msd`:

```python
import pandas as pd

df = pd.DataFrame({
  "ts": [1700000000000000, 1700000060000000],
  "close": [10.5, 10.8],
})

# Supports a single (obj, df) tuple, list/iterator of (obj, df), or pre-packed bytes
data = ("SH600000", df)
res = conn.import_dataframes("stock_kline", data, decimal_fields={"close": 2})
```

# Performance

For a test node, `MsdConnection` can query about 7.4M rows across 1846 different symbols in under 1 second. See [test_query.py](./tests/test_query.py) for more details.