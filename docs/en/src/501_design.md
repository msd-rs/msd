# MSD 系统架构概述

## 1. 项目简介

MSD (Micro Strategy Daemon) 是一个专为量化金融设计的高性能时序数据库，使用纯 Rust 编写，基于 RocksDB 构建。设计目标是解决量化研究中"从数据到分析"的最后一公里问题——即将数据从数据库高效加载到 Python DataFrame 的过程。

### 核心设计理念

传统数据库（如 MySQL、InfluxDB）的瓶颈在 Python 侧：Binary -> Objects -> DataFrame 的转换过程消耗大量 CPU 和内存。MSD 的解决方案是 **"Compute-Ready"交付**——以与 NumPy 内存布局完全一致的二进制格式存储和传输数据，查询结果直接映射为 `numpy.ndarray`，实现磁盘到 DataFrame 的零解析加载。

### 非目标

- 复杂 SQL 查询（JOIN、Window、GROUP BY 等——应由更高效的 Python 库实现）
- 高可用集群（面向单机工作站/个人电脑场景）
- 用户账户系统（支持 JWT 认证但不支持用户管理）

---

## 2. 分层架构

MSD 采用清晰的分层架构，各层之间通过 trait 解耦：

```
┌──────────────────────────────────────┐
│      接口层 (msd)                     │
│  HTTP Server / Shell / Token         │
│  Axum 路由、JWT 认证、MCP 集成        │
├──────────────────────────────────────┤
│      协议层 (msd-request)             │
│  请求/响应类型、SQL 解析器、           │
│  TableFrame 二进制协议                │
├──────────────────────────────────────┤
│      核心引擎层 (msd-db)              │
│  MsdDb -> Worker 池 ->               │
│  Cache + Index + AggState + Chan     │
├──────────────────────────────────────┤
│      数据结构层 (msd-table)           │
│  Table / Series / Variant / Field    │
│  列式存储、CSV 解析、聚合更新器        │
├──────────────────────────────────────┤
│      存储抽象层 (msd-store)           │
│  MsdStore trait -> RocksDbStore      │
├──────────────────────────────────────┤
│      RocksDB (持久化引擎)             │
└──────────────────────────────────────┘
```

---

## 3. Cargo Workspace 成员

项目使用 Cargo Workspace 管理，包含以下 crate：

| Crate | 类型 | 用途 |
|-------|------|------|
| `msd` | binary | 主入口：服务器、交互式Shell、JWT令牌生成 |
| `msd-store` | library | 存储抽象层：定义 MsdStore trait，实现 RocksDB 后端 |
| `msd-table` | library | 内存数据表：列式存储结构 Table/Series/Variant |
| `msd-db` | library | 核心引擎：Worker 池、缓存、索引、聚合、链更新 |
| `msd-request` | library | 协议类型：请求/响应模型、SQL 解析、二进制帧格式 |
| `msd-db-viewer` | binary | 调试工具：直接读取 RocksDB 文件并导出 JSON |
| `bindings/python` | library | Python 绑定（PyO3）：零拷贝转 NumPy/Pandas/Polars |
| `bindings/typescript` | library | TypeScript 绑定（bun） |

### crate 间依赖关系

```
msd (binary)
  ├── msd-db
  │     ├── msd-store
  │     ├── msd-table
  │     └── msd-request
  ├── msd-store
  ├── msd-table
  └── msd-request

msd-db-viewer (binary)
  ├── msd-db
  ├── msd-store
  ├── msd-table
  └── msd-request

bindings/python
  ├── msd-table
  └── msd-request
```

---

## 4. 核心数据模型

### 4.1 基本概念

- **TableName (表名)**: 时序数据表名称，如 `kline`、`snapshot`
- **ObjectName (对象名)**: 数据归属对象，如 `SH600519`（股票代码）、设备ID 等
- **Timestamp (时间戳)**: 数据生成时间，Unix 微秒时间戳
- **Value Columns (值列)**: 实际数据字段，如 `open`、`close`、`high`、`low`、`volume`

### 4.2 存储键设计

底层使用 RocksDB 的 KV 存储，键设计如下：

```
DataKey   = ObjectName + "." + SequenceNumber
SequenceNumber = Hex(-COUNT_OF_CHUNKS_BEFORE - 1)  // 保证字典序逆序排列
IndexKey  = "\x00\x00\x00\x00" + "." + ObjectName
```

**关键性质**：由于 SequenceNumber 取负编码，后来的 chunk 在字典序中更小。因此对同一个 ObjectName：
- IndexKey 总是排在所有 DataKey 之前
- DataKey 按时间从新到旧排列

### 4.3 数据分块 (Chunk)

每个 ObjectName 的数据被切分为多个 Chunk，每个 Chunk ：
- 包含连续时间范围内的数据行（默认 200 行/chunk）
- 在 RocksDB 中作为独立的 KV 对存储
- 最新未满的 Chunk 缓存在内存中（cache.cached）

### 4.4 索引项 (IndexItem)

每个 Chunk 对应一个 IndexItem，存储在索引值中：

```rust
struct IndexItem {
    start: i64,   // chunk 中最小时戳（包含）
    end: i64,     // chunk 中最大时间戳（不包含）
    count: u64,   // chunk 中数据行数
}
```

所有 IndexItem 组成 Vec<IndexItem>，序列化后存在 IndexKey 下。查询时通过索引快速定位需要扫描的 Chunk。

---

## 5. 数据流概览

### 5.1 查询流程

```
HTTP POST /query (SQL)
  -> sql_to_request() 解析
  -> 按 objects 展开为多个 QueryRequest
  -> FxHash 路由到 Worker
  -> Worker 检查 CacheMap
    -> 从 Index 定位相关 Chunk
    -> 从 RocksDB + Cache 加载 Chunk
    -> 按时间范围/字段/limit 过滤
    -> 合并结果
  -> TableFrame/NDJSON 流式返回
```

### 5.2 写入流程

```
HTTP PUT /table/{name} (CSV/Binary)
  -> CSV 零拷贝解析(simd_csv) 或 TableFrame 解码
  -> 按 ObjectName 分组
  -> FxHash 路由到 8 个 Worker
  -> Worker 处理插入:
    1. 时间戳舍入 (round_ts)
    2. 判断更新类型 (Append/Update/Insert/Ignore)
    3. 聚合更新 (AggState)
    4. Chunk 满了则轮转 (rotate)
    5. 链更新传播 (Chan)
```

### 5.3 后台刷新流程

```
Flusher 定时器触发 Broadcast::Flush
  -> 各 Worker 收到 Flush
    -> 检查 last_changed + refresh_interval >= now
    -> flush_index() 持久化索引
    -> flush_chunk() 持久化当前 Chunk
```

---

## 6. 关键技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Rust (Edition 2024) |
| Web 框架 | axum (0.8) |
| 异步运行时 | tokio (1) |
| 存储引擎 | RocksDB (via rust-rocksdb) |
| 序列化 | serde + serde_json + bincode |
| 日志 | tracing + tracing-subscriber |
| CLI | clap (4) + rustyline |
| Python 绑定 | PyO3 + numpy |
| 压缩 | LZ4 + Zstd |
| 哈希 | FxHash (rustc-hash) |
| 内存分配 | jemalloc |

---

## 7. 项目文件布局

```
msd-rs2/
├── Cargo.toml              # Workspace 清单
├── Cargo.lock
├── Makefile                 # 构建自动化
├── Dockerfile
├── README.md / README.cn.md
├── Architecture.md          # 英文架构文档
├── AGENTS.md                # Claude Code 指南
├── .env                     # 开发环境变量
├── .cargo/config.toml       # Cargo 构建配置
│
├── msd/                     # 主 binary crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs          # 入口点
│       ├── app_config.rs    # CLI 参数定义
│       ├── logging.rs       # 日志配置
│       ├── token.rs         # JWT 令牌生成
│       ├── server/          # HTTP 服务器
│       │   ├── mod.rs       # 路由 + AppState
│       │   └── handlers/
│       │       ├── mod.rs
│       │       ├── query.rs     # POST /query
│       │       ├── import.rs    # PUT /table/{name}
│       │       ├── mcp.rs       # GET /mcp
│       │       ├── permission.rs # JWT 认证授权
│       │       └── ws/          # WebSocket
│       └── shell/           # 交互式 Shell
│
├── msd-store/               # 存储抽象层
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs           # MsdStore trait
│       ├── store_rocksdb/   # RocksDB 实现
│       └── errors.rs
│
├── msd-table/               # 内存数据表
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── table/           # Table/Field/Series
│       ├── variant/         # Variant 类型 + 运算
│       ├── d64.rs           # Decimal64 类型
│       ├── date.rs          # 日期时间工具
│       ├── serde/           # CSV 序列化
│       └── updater/         # 聚合更新器
│
├── msd-db/                  # 核心数据库引擎
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs           # 模块文档 + 数据模型
│       ├── db/mod.rs        # MsdDb 实现
│       ├── request.rs       # MsdRequest 枚举
│       ├── index.rs         # IndexItem
│       ├── serde.rs         # DbBinary 序列化
│       ├── errors.rs
│       └── worker/          # Worker 实现
│           ├── mod.rs       # Worker 主逻辑
│           ├── cache.rs     # CacheMap
│           ├── insert.rs    # 插入处理
│           ├── query.rs     # 查询处理
│           ├── delete.rs    # 删除处理
│           ├── init.rs      # 缓存初始化
│           ├── agg_state.rs # 聚合状态机
│           ├── chan.rs      # 链更新机制
│           └── flusher.rs   # 后台刷新
│
├── msd-request/             # 协议类型
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── base.rs          # RequestKey, DateRange
│       ├── keys.rs          # Key 编码/解码
│       ├── query.rs         # QueryRequest
│       ├── insert.rs        # InsertRequest + InsertData
│       ├── delete.rs        # DeleteRequest
│       ├── agg.rs           # AggStateId 枚举
│       ├── broadcast.rs     # Broadcast 枚举
│       ├── table_frame.rs   # TableFrame 二进制协议
│       ├── errors.rs
│       └── sql/             # SQL 解析器
│
├── msd-db-viewer/           # 调试工具
│   └── src/main.rs
│
├── bindings/
│   ├── python/              # Python 绑定 (PyO3)
│   └── typescript/          # TypeScript 绑定
│
├── tests/                   # 集成测试
│   ├── sql/                 # SQL 测试脚本
│   ├── data/                # 测试用 CSV 数据
│   └── scripts/             # Python 演示脚本
│
└── docs/en/                 # 英文文档
```
