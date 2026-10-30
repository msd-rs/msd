# Introduction

This is the Python binding for [msd](https://github.com/msd-rs/msd-app). `msd` is a high-performance financial time series database.

For high level usage, it provides `MsdClient` class, which uses `requests` and a DataFrame (pandas or polars) as the data format.

For high-performance applications, it provides a native Rust client `MsdConnection`, which handles streaming binary table frames directly via Tokio and Reqwest with zero-copy NumPy conversion, native joins, snapshots, and fast data imports.

# Installation

```bash
pip install pymsd
```

# Native Client: MsdConnection

`MsdConnection` is the recommended high-performance client implemented in Rust. It eliminates Python HTTP overhead, streams and parses binary table frames natively, and returns lightweight `Table` objects with zero-copy conversion to NumPy ndarrays.

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

# High Level Usage (DataFrame Adaptor)

1. install `requests` by `pip install requests` 
2. choose a DataFrame library, `pandas` or `polars`, and install it by `pip install pandas` or `pip install polars` 
3. create a `MsdClient` instance by `pymsd.create_msd_pandas` or `pymsd.create_msd_polars` with the url of msd server
4. use the `MsdClient.load` method to query data from msd


# Low Level Usage

The transport layer is based on HTTP, and the data format can be JSON or Binary. The Binary format is more efficient and recommended for non-browser clients, binary format is parsed by `pymsd._msd` native library.

If you prefer to use a custom HTTP client (e.g. `requests` or `aiohttp`) rather than the native `MsdConnection`, this package provides `parse_reader` and `parse_reader_async` functions to parse binary table frame responses directly. With these functions, you can use any HTTP request library to query data from `msd`. For example, you can use `requests` for synchronous requests, and `aiohttp` for asynchronous requests.

It also provides `pymsd.query` and `pymsd.query_async` functions to query data from `msd`, which just demonstrate how to use `pymsd.parse_reader` and `pymsd.parse_reader_async`. When your want use these functions, your should install `requests` or `aiohttp` manually.


# Performance

The performance of `parse_reader` and `parse_reader_async` is just same as the Rust based client, with about 1~2% overhead. For a test node, it can query about 6M rows of 1800 different symbols in about 1 second. The following table is the result of `pytest `.

```python
RESULT_OBJECTS = 1846
RESULT_ROWS = 7438514
SQL_TO_TEST = "select * from kline where obj='SH60*'"
```

| Name (time in ms) | Min | Max | Mean | StdDev | Median | IQR | Outliers | OPS | Rounds | Iterations |
|---|---|---|---|---|---|---|---|---|---|---|
| test_query_many_ndarray | 972.8022 (1.0) | 978.1467 (1.0) | 976.1578 (1.0) | 2.1991 (1.0) | 975.8558 (1.0) | 3.0612 (1.0) | 1;0 | 1.0244 (1.0) | 5 | 1 |
| test_query_many_dataframe | 972.8057 (1.00) | 987.1984 (1.01) | 980.0452 (1.00) | 6.8454 (3.11) | 980.4594 (1.00) | 13.2980 (4.34) | 2;0 | 1.0204 (1.00) | 5 | 1 |
| test_query_many_polars | 973.1088 (1.00) | 995.1073 (1.02) | 982.3909 (1.01) | 9.5757 (4.35) | 979.7399 (1.00) | 16.7033 (5.46) | 1;0 | 1.0179 (0.99) | 5 | 1 |
| test_query_concat_polars | 991.4861 (1.02) | 999.8344 (1.02) | 994.1573 (1.02) | 3.3793 (1.54) | 993.7752 (1.02) | 3.8383 (1.25) | 1;0 | 1.0059 (0.98) | 5 | 1 |
| test_query_concat_pandas | 1,161.1306 (1.19) | 1,186.2676 (1.21) | 1,172.4941 (1.20) | 11.3836 (5.18) | 1,167.3264 (1.20) | 20.0729 (6.56) | 1;0 | 0.8529 (0.83) | 5 | 1 |

see the [test_query.py](./tests/test_query.py) for more details.