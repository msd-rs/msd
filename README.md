
# MSD

[English](./README.md) | [简体中文](./README.cn.md)

MSD (Micro Strategy Daemon) is a **AI Friendly**, **High-Performance** time-series database built on top of [RocksDB](https://rocksdb.org/), written in pure Rust. It is engineered to handle massive amounts of time-series data with speed and efficiency.


## Purpose & Domain

The primary goal of MSD is to provide a easy-to-use, AI-aware time-series database for quantitative finance, help your do financial analysis and trading strategy development in 'one person studio' with AI assistance.


## Key Features

- **🤖 AI Friendly:** 
  - **MCP Integration:** The built-in [MCP](https://modelcontextprotocol.io/docs/getting-started/intro) service allows AI to understand the data, write correct data queries and analysis code. 
  - **Simple API:** The API is designed to be simple, let AI write correct code easily.

- **🛠️ Easy Deployment:**
  - **Zero Dependencies:** compiled as a single, self-contained executable with all dependencies (including RocksDB) statically linked.
  - **Simplified Ops:** No complex installation scripts, library conflicts, or container orchestrations required—just copy the binary and run.
  - **Easy Upgrades:** Updating the database is as simple as replacing the executable file.

- **🐍 Python First:** Python is the first citizen language for MSD bindings. Both performance and ease of use are prioritized.
  - **Zero-Copy NumPy Transformation:** Leveraging Rust's memory safety and PyO3, the bindings allow for near instant transformation of `msd` tables into NumPy arrays (using `from_vec` / `from_slice`), enabling ultra-fast data analysis without serialization overhead.
  - **Easy integration with pandas/polars:** The bindings provide easy integration with popular data analysis libraries, such as pandas and polars, API return grouped, joined, aggregated DataFrames directly, no need to write complex SQL queries.

- **🚀 High Performance:** We know that prepare data is a time-consuming process, and know there are huge data read in  'write-restart-verify' iterations when doing data analysis. In a typical workstation hardware(8C 16G SSD), MSD can
  - Insert total 100M OHLCVA rows within 10K stocks less than 20s at 5M/s speed
  - Query total 100M OHLCVA rows within 10K stocks less than 10s at 10M/s speed
  - See [bench-of-msd](https://cnb.cool/elsejj/bench-of-msd) for more details
  

## Usage Guide

### prerequisites

Download the pre-built binary from [releases](https://github.com/msd-rs/msd-app/releases) with your platform, unpack it to any `PATH` aware location.


### Running the Server

Start the MSD server to accept connections and requests.

```bash
# Run with default settings (listens on 127.0.0.1:50510)
msd server

# Custom configuration
msd server --listen 0.0.0.0:8080 --workers 16 --db ./my_data

# Help
msd server --help
```

### Interactive Shell

Connect to a running server and interact with it using the built-in shell.

```bash
# Run with default settings (connects to 127.0.0.1:50510)
msd shell

# Custom configuration
msd shell -s http://192.168.1.100:50510

# Execute a command without entering the shell
msd shell "select * from stock_kline_1d where obj='SH600000' limit 10"

# Help
msd shell --help

```

### Python Bindings

1. Install the bindings
```bash
pip install pymsd
```
2. Install your favorite data analysis library and http client library, for example pandas and requests
```bash
pip install pandas requests
```

3. Use the bindings
```python
import pymsd

MSD_URL = "http://127.0.0.1:50510"
client = pymsd.create_msd_pandas(MSD_URL)

data = client.load(
  objs=["SH600000", "SZ000001"],
  tables=["stock_kline_1d", "stock_dividend", "stock_shares"],
  start="2025-01-01",
  join={"stock_dividend": "zero", "*": "backward"},
)

sh600000 = data["SH600000"]
print(sh600000[(sh600000["dividend"] > 0) | (sh600000.index < 5) | (sh600000.index > 240)])

# results:
# - result is a pandas DataFrame as stock_kline_1d date order
# - stock_dividend is joined by zero, can be do compute meaningful, natural in next step
# - stock_shares is joined by backward, can be do compute meaningful, natural in next step

"""
            ts   open   high    low  close       volume  ...  transfers  dividend  rightShare  rightPrice         total      tradable
0   2025-01-02  11.73  11.77  11.39  11.43  181959699.0  ...        0.0      0.00         0.0         0.0  1.940557e+10  1.940557e+10
1   2025-01-03  11.44  11.54  11.36  11.38  115468044.0  ...        0.0      0.00         0.0         0.0  1.940557e+10  1.940557e+10
2   2025-01-06  11.38  11.48  11.22  11.44  108553630.0  ...        0.0      0.00         0.0         0.0  1.940557e+10  1.940557e+10
3   2025-01-07  11.42  11.53  11.37  11.51   74786288.0  ...        0.0      0.00         0.0         0.0  1.940557e+10  1.940557e+10
4   2025-01-08  11.50  11.63  11.40  11.50  106238601.0  ...        0.0      0.00         0.0         0.0  1.940557e+10  1.940557e+10
104 2025-06-12  11.56  11.70  11.52  11.68  129022000.0  ...        0.0      3.62         0.0         0.0  1.940557e+10  1.940557e+10
187 2025-10-15  11.34  11.42  11.26  11.40  127106100.0  ...        0.0      2.36         0.0         0.0  1.940560e+10  1.940560e+10
241 2025-12-30  11.53  11.56  11.45  11.48   58258400.0  ...        0.0      0.00         0.0         0.0  1.940560e+10  1.940560e+10
242 2025-12-31  11.48  11.49  11.40  11.41   59062000.0  ...        0.0      0.00         0.0         0.0  1.940560e+10  1.940560e+10
243 2026-01-05  11.42  11.51  11.41  11.50   87549100.0  ...        0.0      0.00         0.0         0.0  1.940560e+10  1.940560e+10
244 2026-01-06  11.50  11.68  11.48  11.67  130464800.0  ...        0.0      0.00         0.0         0.0  1.940560e+10  1.940560e+10

[11 rows x 14 columns]
"""
```
