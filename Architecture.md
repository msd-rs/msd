# 软件架构描述 (Software Architecture Description)

## 1. 简介 (Introduction)

**MSD (Micro Strategy Daemon)** 是一款专为金融量化分析优化的时序数据库。它放弃了传统数据库“重查询、轻传输”的架构，转而将 “数据检索带宽” 与 “分析工具集成度” 作为核心设计指标。

### 核心哲学：解决从“数据”到“分析”的最后一公里

在金融量化研究中，研究员的时间往往浪费在等待数据从数据库加载到 Python 内存的过程中。

传统痛点：通用数据库（如 MySQL, InfluxDB）在 Python 侧的瓶颈在于 Binary -> Objects -> DataFrame 的转换。即使底层 C++ 引擎再快，逐行解析字节流并封装为 Python 对象的过程也会消耗大量 CPU 并产生频繁的内存分配。

MSD 的解法：“计算就绪”的整装配送。MSD 将数据以与 NumPy 内存布局完全一致的二进制格式存储与传输。查询结果直接映射为 numpy.ndarray，实现从磁盘到 DataFrame 的**零解析（Zero-Parsing）**加载。


### 全局设计
MSD 采用 **分层架构**：
1.  **接口层 (Interface Layer)**:通过 HTTP API 暴露服务，并提供交互式 Shell 工具。
2.  **核心逻辑层 (Core Logic Layer)**: `msd-db` 负责数据的组织、索引维护和读写流程控制。
3.  **数据结构层 (Data Structure Layer)**: `msd-table` 定义了列式存储的内存结构（类似 Apache Arrow），优化了分析计算性能。
4.  **存储层 (Storage Layer)**: `msd-store` 抽象了底层存储引擎，目前默认使用 **RocksDB** 作为持久化 Key-Value 存储。

数据模型核心概念包括：
-   **TableName**: 表名（如 `sensors`）。
-   **ObjectName**: 数据归属对象（如 `device_001`）。
-   **Timestamp**: 时间戳。
-   **Value Columns**: 实际数据列。

数据存储策略是基于 `ObjectName` 和 `Timestamp` 的双重分区，底层 Key 设计保证了数据的物理聚集和顺序访问。

Python Binding 是 MSD 的一等公民，它提供了高性能的与 MSD 的交互接口，以及与 NumPy, Pandas, Polars 等分析工具的无缝集成。

### 非设计目标

- 复杂SQL查询: 包括 Join, Window, Aggregate , Group By 等高级操作. 这些功能可通过更加高效, 专业的 Python 相关库实现。
- 高可用服务: Msd 旨在为量化独立工作提供一个高性能, 易用, 免运维的时序数据库, 使其可以轻易的运行于工作站或个人电脑, 服务主机用户或局域网用户 并不追求高可用性。但由于其用 Rust 编写，在单机层面已经具备了很高的可用性。
- 用户账号: Msd 不支持用户账户, 但支持访问鉴权.

---

## 2. 主项目 (The Main Project): `msd`

`msd` crate 是整个项目的入口，编译后生成可执行文件 `msd`。它主要包含三个子命令功能：

### 2.1 MSD Server (`msd server`)
这是数据库的服务端进程，负责处理外部请求。
*   **功能**:
    *   启动 HTTP 服务器（基于 `axum`）。
    *   维护数据库实例的生命周期（启动/关闭 RocksDB）。
    *   处理并发读写请求。
*   **对外 API**:
    *   `POST /query`: 执行查询语句，支持的 SQL 操作包括：
        *   **SQL 操作**:
            *   **Query Data**: `SELECT ...` (支持 `WHERE`, `ORDER BY`, `LIMIT`)
            *   **Create Table**: `CREATE TABLE ...` (支持定义列类型和元数据)
            *   **Insert Data**: `INSERT INTO ...` (支持 `VALUES` 和 `COPY` 模式)
            *   **Delete Data**: `DELETE FROM ...` (按对象或时间范围删除)
            *   **Drop Table**: `DROP TABLE ...`
            *   **Get Schema**: `DESCRIBE <table>`
            *   **List Object**: `SELECT obj FROM <table>` (优化查询)
        *   **推荐查询方式**:
            *   **Python SDK**: 使用 `pymsd.query` (同步) 或 `pymsd.async_query` (异步)。
            *   **CLI 工具**: 使用 `msd shell` 交互执行。
        *   **响应格式**:
            *   **Binary 格式** (当 User-Agent 包含 `msd-client` 时):
                *   返回 `application/x-msd-table-frame` 二进制流。
                *   Python SDK 会自动处理并解析为 Generator，每项为 `(object_name, date_table)` 元组。
            *   **NDJSON 格式** (默认):
                *   返回 `application/x-ndjson` (Newline Delimited JSON)。
                *   每一行是一个完整的 JSON 对象，代表一个表数据块。
    *   `PUT /table/{table_name}`: 数据写入接口。
        *   **推荐写入方式**:
            *   **Python SDK**: 使用 `pymsd.import_csv` 或 `pymsd.import_dataframes` (位于 `msd.update` 模块) 进行高效写入。
            *   **CLI 工具**: 使用 `msd shell` 的 `.import` 命令导入 CSV 文件。
        *   **底层协议**:
            *   **CSV 格式** (默认):
                *   可以直接发送 CSV 文本数据。
                *   **首列**必须是 `obj` (对象名)，后续列对应表结构。
                *   支持 `?skip=N` 参数跳过前 N 行 (如 Header)。
            *   **Binary 格式**:
                *   需设置 Header `Content-Type: application/x-msd-table-frame`。
                *   发送 MSD 自定义的 `TableFrame` 二进制帧流，性能更高。
                *   **构建方式**: 推荐使用 Python 绑定库 `pymsd` 生成。
                    ```python
                    import msd
                    # df can be pandas.DataFrame, polars.DataFrame or list of (name, array)
                    binary_data = msd.pack_dataframe("object_name", df)
                    requests.put(url, data=binary_data, headers={"Content-Type": "application/x-msd-table-frame"})
                    ```
*   **特性**:
    *   **鉴权**: 支持 JWT Token 认证 (`auth-token`) 和基于角色的权限控制 (`read`, `write`, `admin`)。
    *   **压缩**: 支持多种压缩算法 (gzip, zstd, brotli, deflate) 以减少网络传输开销。
    *   **可观测性**: 集成了 `pprof` 用于性能分析。

### 2.2 MSD Shell (`msd shell`)
这是自带的交互式命令行客户端，用于管理和查询数据库。
*   **功能**: 提供一个 SQL 交互环境 (REPL)。
*   **支持的命令**:
    *   SQL 语句: 直接输入 SQL 进行查询，以分号 `;` 结尾。
    *   `.server <url>`: 设置连接的服务器地址。
    *   `.import <file> <table> [skip]`: 将 CSV 文件导入到指定表。
    *   `.dump <table> [file]`: 将表数据导出为 CSV 格式。
    *   `.schema <table>`: 查看表结构。
    *   `.rows <num>`: 设置显示的行数限制。
    *   `.output [file]`: 将输出重定向到文件。
    *   `.help`, `.exit`: 帮助和退出。

### 2.3 MSD Token (`msd token`)
用于生成访问服务器所需的 JWT 认证 Token。
*   **用法**: 指定密钥 (`-a`)、角色 (`-r`) 和过期时间 (`-e`) 生成 Token 字符串。

---

## 3. 子项目 (SubProjects)

项目采用 Cargo Workspace 组织，包含多个核心库 (crates)，职责划分如下：

### 3.1 `msd-db` (核心数据库引擎)
*   **目的**: 实现时序数据库的核心逻辑。
*   **设计**:
    *   **数据模型**: 定义了 `DbTable`，管理模式 (Schema) 和元数据。
    *   **存储布局**:
        *   **DataKey**: `ObjectName` + `SequenceNumber` (基于时间分块)，用于存储实际的时序数据块。
        *   **IndexKey**: `ObjectName` + 固定后缀，用于存储对象的元数据（索引）。
    *   **写入流程**: `In-Memory Buffer` -> `Serialize` -> `RocksDB`。
    *   **更新策略**: 支持 Append（追加）、Update（聚合更新）、Insert（插入）和 Ignore（忽略旧数据）。

### 3.2 `msd-table` (内存数据表结构)
*   **目的**: 提供高效的列式数据结构，用于内存中的计算和序列化。
*   **设计**:
    *   尽管描述提到 "based on apache arrow"，实际上实现了一套轻量级的列式存储结构。
    *   包含 `Table`, `Series` (列), `Field` (字段) 等结构。
    *   支持多种数据类型 (`D64` (double), `D128` (decimal), `Timestamp`, `String` 等)。
    *   负责数据的二进制序列化和 CSV 解析。

### 3.3 `msd-store` (存储抽象层)
*   **目的**: 解耦数据库逻辑与底层物理存储。
*   **设计**:
    *   定义了 `MsdStore` trait，包含 `get`, `put`, `delete`, `scan` (prefix_with) 等标准 KV 操作接口。
    *   **`RocksDbStore`**: 基于 RocksDB 的具体实现，提供了高性能的本地 SSD/HDD 存储能力。

### 3.4 `msd-request` (协议与模型)
*   **目的**: 定义客户端与服务端通信的数据模型。
*   **设计**:
    *   包含查询请求 (`Query`)、聚合操作 (`Agg`)、过滤条件 (`Filter`) 等结构体定义。
    *   定义了 `TableFrame` 二进制协议，用于高效传输表数据。
    *   作为 `msd` (client/server) 和 `msd-db` 之间的契约。

### 3.5 `msd-db-viewer` (调试工具)
*   **目的**: 开发者工具，用于直接查看底层 RocksDB 数据，无需启动服务器。
*   **功能**:
    *   可以直接读取 RocksDB 文件 (`CURRENT`, SSTable 等)。
    *   按表名 (`table`) 和键 (`key`) 浏览原始的 Schema、Index 和 Data 数据块。
    *   输出为 JSON 格式，方便调试存储层问题。

### 3.6 `bindings` (多语言绑定)
*   **目的**: 让其他编程语言能够方便地调用 MSD 的功能或与 MSD 服务器交互。
*   **Python 绑定 (`bindings/python`)**:
    *   ** crate 名**: `pymsd`.
    *   **技术**: 基于 `pyo3` 和 `numpy` 构建。
    *   **功能**: 允许 Python 程序直接操作 MSD 的数据结构，适用于数据分析和科学计算场景。
    *   **特性**: Python 绑定的主要功能是支持将 MSD 数据表直接转换为 `pandas.DataFrame`, `polars.DataFrame`, `numpy.ndarray`。
*   **TypeScript 绑定 (`bindings/typescript`)**:
    *   **技术**: 使用 `bun` 构建，支持生成浏览器和 Node.js 环境的 ESM 模块。
    *   **功能**: 提供类型安全的 JS/TS 客户端库，方便 Web 前端或 Node.js 后端应用与 MSD 交互。
