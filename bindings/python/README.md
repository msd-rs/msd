# Introduction

This is the Python binding for [msd](https://github.com/msd-rs/msd-rs2). `msd` is a high-performance financial time series database.

The transport layer is based on HTTP, and the data format can be JSON or Binary. The Binary format is more efficient and recommended for non-browser clients.

Because of the HTTP request library is very common, this package does not provide a client, instead it provides `parse_reader` and `parse_reader_async` functions to parse the HTTP response. With these functions, you can use any HTTP request library to query data from `msd`. For example, you can use `requests` for synchronous requests, and `aiohttp` for asynchronous requests.

It also provides `msd_query_requests` and `msd_query_aiohttp` functions to query data from `msd`, which just demonstrate how to use `parse_reader` and `parse_reader_async`. When your want use these functions, your should install `requests` or `aiohttp` manually.

# Installation

```bash
pip install msd
```


# Basic Usage

1. Choose a HTTP request library, which should support stream reading Response(have a `read` method). For example, `requests.Response.raw` or `aiohttp.ClientResponse.content`   
2. Set the user agent to `MSD_USER_AGENT`
3. Provide response stream reader to `parse_reader` or `parse_reader_async`

# Performance

The performance of `parse_reader` and `parse_reader_async` is just same as the Rust based client, with about 1~2% overhead. For a test node, it can query about 6M rows of 1800 different symbols in about 1 second. The following table is the result of `pytest `.

```python
RESULT_OBJECTS = 1789
RESULT_ROWS = 6245835
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