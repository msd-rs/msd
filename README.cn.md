# MSD

[English](./README.md) | [简体中文](./README.cn.md)

MSD (Micro Strategy Daemon) 是一个 **AI 友好**、**高性能** 的时序数据库，基于 [RocksDB](https://rocksdb.org/) 构建，使用纯 Rust 编写。它专为高速、高效地处理海量时序数据而设计。


## 目标与领域

MSD 的主要目标是为量化金融提供一个易于使用、具备 AI 感知能力的时序数据库，帮助在 AI 的辅助下通过“个人工作室”完成金融分析和交易策略开发。


## 核心特性

- **🤖 AI 友好：** 
  - **MCP 集成：** 内置 [MCP](https://modelcontextprotocol.io/docs/getting-started/intro) 服务，允许 AI 理解数据，编写正确的数据查询和分析代码。 
  - **简单的 API：** API 设计简洁，让 AI 能够轻松编写出正确的代码。

- **🛠️ 易于部署：**
  - **零依赖：** 编译为单个自包含的可执行文件，所有依赖（包括 RocksDB）均已静态链接。
  - **极简运维：** 无需复杂的安装脚本、库冲突解决或容器编排——只需复制二进制文件即可运行。
  - **轻松升级：** 更新数据库只需替换可执行文件。

- **🐍 Python 优先：** Python 是 MSD 绑定的第一公民语言。性能和易用性均被视为重中之重。
  - **零拷贝 NumPy 转换：** 利用 Rust 的内存安全特性和 PyO3，绑定允许将 `msd` 表近乎瞬时地转换为 NumPy 数组（通过 `from_vec` / `from_slice`），实现超快的数据分析而无需序列化开销。
  - **与 pandas/polars 轻松集成：** 绑定提供了与 pandas 和 polars 等流行数据分析库的轻松集成，API 直接返回分组、连接、聚合后的 DataFrame，无需编写复杂的 SQL 查询。

- **🚀 高性能：** 我们深知准备数据是一个耗时的过程，也知道在进行数据分析的“编写-重启-验证”迭代中存在海量的数据读取。在典型的个人工作站硬件（8C 16G SSD）上，MSD 可以：
  - 在 20 秒内插入 1 万只股票的总计 1 亿行 OHLCVA 数据，速度达 500 万行/秒。
  - 在 10 秒内查询 1 万只股票的总计 1 亿行 OHLCVA 数据，速度达 1000 万行/秒。
  - 更多详情请参阅 [bench-of-msd](https://cnb.cool/elsejj/bench-of-msd)。
  

## 使用指南

### 前提条件

从 [releases](https://github.com/msd-rs/msd-app/releases) 下载适用于你平台的预编译二进制文件，并将其解压到任何已加入 `PATH` 的位置。


### 运行服务器

启动 MSD 服务器以接收连接和请求。

```bash
# 使用默认设置运行（监听 127.0.0.1:50510）
msd server

# 自定义配置
msd server --listen 0.0.0.0:8080 --workers 16 --db ./my_data

# 帮助信息
msd server --help
```

### 交互式 Shell

连接到正在运行的服务器并使用内置 shell 进行交互。

```bash
# 使用默认设置运行（连接到 127.0.0.1:50510）
msd shell

# 自定义配置
msd shell -s http://192.168.1.100:50510

# 直接执行命令而不进入 shell
msd shell "select * from stock_kline_1d where obj='SH600000' limit 10"

# 帮助信息
msd shell --help

```

### Python 绑定

1. 安装绑定
```bash
pip install pymsd
```
2. 安装你喜欢的数据分析库和 HTTP 客户端库，例如 pandas 和 requests
```bash
pip install pandas requests
```

3. 使用绑定
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

# 结果：
# - 结果是一个按 stock_kline_1d 日期排序的 pandas DataFrame
# - stock_dividend 通过 zero 方式连接，方便在下一步进行有意义、自然的计算
# - stock_shares 通过 backward 方式连接，方便在下一步进行有意义、自然的计算

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
