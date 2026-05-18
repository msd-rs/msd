# 模块功能详解

## 1. msd — 主入口 Crate

### 1.1 概述

`msd` 是整个项目的二进制入口，编译生成 `msd` 可执行文件。提供三个子命令：`server`、`shell`、`token`。

### 1.2 入口点 (main.rs)

文件：`msd/src/main.rs`

启动流程：
1. 加载 `.env` 环境变量 (`dotenvy::dotenv_override`)
2. 解析 CLI 参数 (`MsdOptions::parse()`)
3. 配置日志系统 (`setup_logging`)
4. 非 MSVC 平台初始化 jemalloc 全局分配器和 pprof 性能分析
5. 设置默认时区
6. 根据子命令分派到 `server::run()` / `shell::run()` / `token::run()`

```rust
#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv_override().ok();
    let main_options = app_config();
    let _logging_guards = setup_logging();
    // ...
    match &main_options.command {
        MsdCommands::Server(options) => server::run(options).await?,
        MsdCommands::Shell(options) => shell::run(options).await?,
        MsdCommands::Token(options) => token::run(options).await?,
    }
    Ok(())
}
```

### 1.3 配置系统 (app_config.rs)

文件：`msd/src/app_config.rs`

使用 `clap` derive 模式定义 CLI 参数。配置通过全局单例获取：

```rust
pub fn app_config() -> &'static MsdOptions {
    static APP_CONFIG: OnceLock<MsdOptions> = OnceLock::new();
    APP_CONFIG.get_or_init(|| MsdOptions::parse())
}
```

主要配置项（均支持环境变量）：

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| `--db` | `MSD_DB_PATH` | `./msd_db` | 数据库目录 |
| `--log-dir` | `MSD_LOG_PATH` | stdout | 日志目录 |
| `--tz` | `MSD_TZ` | 本地时区 | 时区偏移 |
| `--listen` | `MSD_LISTEN_ADDR` | `127.0.0.1:50510` | 服务器监听地址 |
| `--workers` | `MSD_WORKERS` | `8` | Worker 线程数 |
| `--auth-token` | `MSD_AUTH_TOKEN` | 无 | JWT 认证密钥 |
| `--bg-flush-interval` | - | `5m` | 后台刷新间隔 |
| `--pid` | `MSD_PID_FILE` | 无 | PID 文件路径 |
| `-P` | - | `read` | 默认公共权限 |

### 1.4 HTTP 服务器 (server/mod.rs)

文件：`msd/src/server/mod.rs`

基于 `axum` 框架的 HTTP 服务器。

**AppState 结构**：
```rust
pub struct AppState {
    pub db: MsdDb<RocksDbStore>,
    pub broker: Broker,  // WebSocket 发布订阅
}
```

**路由注册**：
| 方法 | 路径 | 处理器 | 功能 |
|------|------|--------|------|
| POST | `/query` | `handlers::handle_data` | SQL 查询 |
| PUT | `/table/{table_name}` | `handlers::handle_table` | 数据导入 |
| ANY | `/ws` | `handlers::handle_ws` | WebSocket 连接 |
| GET | `/mcp` | `handlers::mcp_service` | MCP 协议服务 |

**中间件栈**：Decompression -> CORS -> Compression

**启动流程**：
1. 绑定 TCP 监听
2. 创建 RocksDbStore 实例
3. 创建 MsdDb 实例（含 N 个 Worker）
4. 构建 AppState（db + broker）
5. 构建 Axum Router
6. 启动服务（带优雅关闭）

### 1.5 查询处理器 (handlers/query.rs)

文件：`msd/src/server/handlers/query.rs`

处理 `POST /query` 请求：

1. 解析 SQL 文本 -> `SqlRequest` 列表
2. 对每个 SqlRequest，展开 objects（通配符匹配）
3. 将展开后的请求逐个发送到 MsdDb
4. 根据 User-Agent 选择响应格式：
   - `msd-client`：二进制 TableFrame 流
   - 其他：NDJSON (Newline Delimited JSON)

### 1.6 导入处理器 (handlers/import.rs)

文件：`msd/src/server/handlers/import.rs`

处理 `PUT /table/{table_name}` 请求：

1. 读取请求体（CSV 文本或 TableFrame 二进制）
2. CSV 使用 `simd_csv` 零拷贝解析
3. 使用 `JoinSet` 并行跨 8 个 Worker 分派数据
4. 按 ObjectName 分组，每组发送为一个 InsertRequest

### 1.7 权限系统 (handlers/permission.rs)

文件：`msd/src/server/handlers/permission.rs`

三层权限模型（位掩码）：
- `read` (1): 查询权限
- `write` (2): 写入权限
- `admin` (4): 管理权限

认证方式：HTTP `Authorization: Bearer <token>` 头。本地请求（127.0.0.1）无 Token 时自动授权。

### 1.8 MCP 集成 (handlers/mcp.rs)

文件：`msd/src/server/handlers/mcp.rs`

实现 Model Context Protocol，提供：
- `list_tables`: 列出所有表及 schema
- `get_table`: 获取指定表的详细结构
- SQL/Python 教程提示（帮助 AI 理解数据）

### 1.9 WebSocket (handlers/ws/)

提供实时发布-订阅机制：

- `broker.rs`: 发布-订阅代理，管理订阅者通道，广播消息
- `message.rs`: 消息类型定义 (Subscribe/Unsubscribe/Notify/Status)
- `filter.rs`: 基于表的过滤器
- `mod.rs`: WebSocket 升级 + 消息循环处理

### 1.10 交互式 Shell (shell/)

文件：`msd/src/shell/mod.rs`

基于 `rustyline` 的 REPL 客户端。

支持的命令：
- `.server <url>` - 设置服务器地址
- `.import <file> <table> [skip]` - 导入 CSV 文件
- `.dump <table> [file]` - 导出表到 CSV
- `.schema <table>` - 查看表结构
- `.rows <num>` - 设置显示行数限制
- `.output [file]` - 重定向输出
- `.help` / `.exit` - 帮助/退出
- 直接输入 SQL 语句（以 `;` 结束）

---

## 2. msd-store — 存储抽象层

### 2.1 概述

定义 `MsdStore` trait，解耦数据库逻辑与物理存储。当前唯一实现是基于 RocksDB 的 `RocksDbStore`。

### 2.2 MsdStore Trait

文件：`msd-store/src/lib.rs`

```rust
pub trait MsdStore {
    fn get<K: AsRef<[u8]>>(&self, key: K, table: &str) -> Result<Option<Vec<u8>>, StoreError>;
    fn get_next<K: AsRef<[u8]>>(&self, key: K, table: &str, buf: Option<(Vec<u8>, Vec<u8>)>) -> Result<Option<(Vec<u8>, Vec<u8>)>, StoreError>;
    fn put<K: AsRef<[u8]>, V: Into<Vec<u8>>>(&self, key: K, value: V, table: &str, ttl: Option<u64>) -> Result<(), StoreError>;
    fn delete<K: AsRef<[u8]>>(&self, key: K, table: &str) -> Result<(), StoreError>;
    fn prefix_with<K: AsRef<[u8]>, F: FnMut(&[u8], &[u8]) -> bool>(&self, start_from: K, prefix: Option<usize>, table: &str, rev: bool, f: F) -> Result<(), StoreError>;
    fn new_table(&self, name: &str) -> Result<bool, StoreError>;
    fn drop_table(&self, name: &str) -> Result<(), StoreError>;
    fn list_tables(&self) -> Result<Vec<String>, StoreError>;
    fn remove_expired(&self) -> Result<(), StoreError>;
}
```

**设计要点**：
- `table` 参数对应 RocksDB 的 Column Family（列族）
- `prefix_with` 支持前缀扫描和反向扫描，传入闭包逐 KV 处理
- `get_next` 获取字典序中的下一个 KV 对
- TTL 支持过期数据自动清理

### 2.3 RocksDB 实现 (store_rocksdb/)

每个 MSD 表映射为一个 RocksDB Column Family。额外有一个 `__TTL__` CF 用于管理过期。

压缩策略：Level 0-5 使用 LZ4，Level 6+ 使用 Zstd。

---

## 3. msd-table — 内存数据表

### 3.1 概述

提供高效的列式数据结构，类似 Apache Arrow 但更轻量。专为时序数据的快速读取和序列化设计。

### 3.2 Table (表)

核心数据结构。列式存储，每列是一个 `Series`。

```rust
pub struct Table {
    columns: Vec<Field>,    // 列定义 (schema)
    data: Vec<Series>,      // 列数据
    pk_index: usize,        // 主键列索引
    metadata: ...,          // 表级元数据
}
```

主要方法：
- `push_row(Vec<Variant>)` - 追加一行
- `sort_by_pk(descending)` - 按主键排序
- `group_by(col_index)` - 按列值分组
- `extend_filtered(table, f)` - 过滤扩展
- `retain_columns_by(f)` - 保留指定列
- `get_table_meta(key)` / `set_table_meta(key, value)` - 元数据操作
- `get_field_meta(key)` / `set_field_meta(field, key, value)` - 字段元数据

### 3.3 Series (列)

单列数据，支持多种类型的数组存储。提供按索引读写、slab copy 等高效操作。

### 3.4 Variant (变体类型)

所有数据值的统一表示。支持类型：

| 类型 | 对应 Rust 类型 | 说明 |
|------|---------------|------|
| Null | - | 空值 |
| Bool | bool | 布尔 |
| Int32 / Int64 | i32 / i64 | 有符号整数 |
| UInt32 / UInt64 | u32 / u64 | 无符号整数 |
| Float32 / Float64 | f32 / f64 | 浮点数 |
| Decimal64 | D64 | 定点小数（8字节） |
| Decimal128 | D128 (rust_decimal) | 定点小数（16字节） |
| DateTime | i64 (微秒时间戳) | 日期时间 |
| String | String | 字符串 |
| Binary | Vec<u8> | 二进制 |

Variant 提供类型转换 (`cast`)、算术运算 (`ops`) 等 API。

### 3.5 Field (列定义)

```rust
pub struct Field {
    pub name: String,       // 列名
    pub kind: DataType,     // 数据类型
    pub index: usize,       // 列索引
    pub metadata: ...,      // 列级元数据 (如 agg, desc)
}
```

### 3.6 RowsTable (行存格式)

用于外部数据输入的中间格式，导入时会转换为列存 Table。

### 3.7 日期时间工具 (date.rs)

- `now()` - 当前微秒时间戳
- `parse_datetime(str)` - 解析日期字符串
- `parse_unit(str)` - 解析时间单位 (如 "5m", "1d")
- `round_ts_with_tz(ts, unit, tz)` - 时间戳舍入
- `parse_duration(str)` - 解析 Duration 字符串

### 3.8 Updater (更新器)

实现 `Updater` trait，用于数据聚合更新：

- `First` - 保留第一个值
- `Period` - 指定周期内聚合
- `Previous` - 保留前一个值
- 无状态更新器：直接映射函数

---

## 4. msd-db — 核心数据库引擎

### 4.1 概述

实现时序数据库的核心逻辑：Worker 管理、缓存、索引、聚合和链更新。

### 4.2 MsdDb (数据库实例)

文件：`msd-db/src/db/mod.rs`

```rust
pub struct MsdDb<S: MsdStore> {
    store: Arc<S>,
    workers: Vec<mpsc::Sender<MsdRequest>>,
    schemas: Arc<RwLock<HashMap<String, Table>>>,
    objects: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    flusher: mpsc::Sender<Broadcast>,
}
```

**创建流程** (`MsdDb::new`):
1. 初始化 N 个 Worker，每个有独立的 mpsc channel（容量 200,000）
2. 初始化 schema 和 objects 缓存
3. 如果 refresh_interval > 0，启动后台刷新任务
4. 从 RocksDB 加载已持久化的 schema
5. 对每个已存在的表，扫描 objects
6. 将 schema 广播到所有 Worker

**请求路由** (`MsdDb::request`):
- 广播请求 (Broadcast)：发送到所有 Worker
- 普通请求：通过 FxHash 取模路由到指定 Worker
- 特殊处理：
  - `ListObjects`：直接查询 objects 缓存
  - `CreateTable`：先创建表再广播
  - `Delete`：先更新 objects 缓存，再发送到 Worker

**FxHash 路由**：
```rust
fn get_worker(&self, key: &RequestKey) -> &mpsc::Sender<MsdRequest> {
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let index = (hash as usize) % self.workers.len();
    &self.workers[index]
}
```

### 4.3 MsdRequest (请求枚举)

文件：`msd-db/src/request.rs`

```rust
pub enum MsdRequest {
    Insert { req: InsertRequest, resp_tx: RequestSender<InsertResponse> },
    Query { req: QueryRequest, resp_tx: RequestSender<QueryResponse> },
    ListObjects { req, resp_tx },
    Delete { req: DeleteRequest, resp_tx: RequestSender<DeleteResponse> },
    Comment { table, field, desc },
    Broadcast(Broadcast),
}
```

Broadcast 变体：
```rust
pub enum Broadcast {
    UpdateSchema(HashMap<String, Table>),
    CreateTable(String, Table),
    DropTable(String),
    Flush,
    Shutdown,
}
```

### 4.4 Worker (工作线程)

文件：`msd-db/src/worker/mod.rs`

```rust
pub struct Worker<S: MsdStore> {
    pub id: usize,
    pub store: Arc<S>,
    pub cache: CacheMap,          // FxHashMap<RequestKey, CacheValue>
    pub schema: HashMap<String, Table>,
    pub tx: mpsc::Sender<MsdRequest>,
    pub refresh_interval: i64,
}
```

**run 循环**：从 mpsc channel 接收请求并处理。每个 Worker 独立运行在 tokio task 中。

处理分发：
- `Insert` -> `handle_insert()`
- `Query` -> `handle_query()`
- `Delete` -> `handle_delete()`
- `Broadcast(Shutdown)` -> `handle_shutdown()` + 退出循环
- `Broadcast(other)` -> `handle_broadcast()`

### 4.5 CacheMap (缓存)

文件：`msd-db/src/worker/cache.rs`

```rust
pub struct CacheValue {
    pub cached: Table,              // 当前(最新) chunk 的完整数据
    pub index: Vec<IndexItem>,      // 所有 chunk 的索引
    pub state: Vec<Option<AggState>>, // 每列的聚合状态
    pub chan: Option<Chan>,         // 链更新配置
    pub last_changed: i64,         // 最后修改时间戳
}

pub type CacheMap = FxHashMap<RequestKey, CacheValue>;
```

缓存以 `(table_name, object_name)` 为 key，即每个 Worker 管理的不同 object 有独立的缓存。

### 4.6 插入处理 (worker/insert.rs)

**更新类型判断** (基于时间戳舍入后的 pk):
- **Append**: 舍入后时间戳 > 缓存最新时间戳 -> 追加新行，重置聚合状态
- **Update**: 舍入后时间戳 == 缓存最新时间戳 -> 更新最后一行，应用聚合
- **Insert**: 理论上可在中间插入（实际中较罕见）
- **Ignore**: 时间戳 < `ignore_older_than` 阈值 -> 跳过

**Chunk 轮转**：当 `cache.cached.row_count() >= chunk_size`，序列化当前 chunk 到 RocksDB，创建新的空 chunk。

**聚合更新流程**：
1. 获取当前行的聚合状态（正更新同一时间戳）
2. 对每个列，如果有 AggState，调用 `agg_state.update(&cell_value)`
3. 获取聚合结果 `agg_state.get()` 更新缓存中的值

### 4.7 聚合状态机 (worker/agg_state.rs)

支持聚合类型（在 Field metadata 中通过 `agg` 键配置）：

| 聚合 | AggStateId | 说明 |
|------|-----------|------|
| `sum` | Sum | 求和 |
| `count` | Count | 计数 |
| `min` | Min | 最小值 |
| `max` | Max | 最大值 |
| `avg` | Avg | 平均值 |
| `first` | First | 第一个值 |
| `prev` | Prev | 前一值 |
| `uniq_count` | UniqCount | 唯一值计数 |
| `diff_prev` | DiffPrev | 与前一值的差 |
| `diff_first` | DiffFirst | 与第一个值的差 |

### 4.8 查询处理 (worker/query.rs)

查询流程：
1. 确保缓存已初始化（从 RocksDB 加载 index + last chunk）
2. 从 index 找出与 DateRange 重叠的所有 chunk
3. 过滤字段（如果指定了 fields）
4. 按降序/升序遍历 chunks（先从 RocksDB 加载，最后合并缓存 chunk）
5. 逐行过滤：时间范围 + limit
6. 返回合并后的 Table，附加 `obj` 和 `table` 元数据

### 4.9 链更新机制 (worker/chan.rs)

允许一张表（如 `snapshot`）在更新时自动触发另一张表（如 `kline`）的更新。

**Chan 字符串格式**：
```
target_table1,target_table2:field1,field2,changed_if(field3,field1),field4
```

- `target_table1,target_table2`：目标表名（逗号分隔）
- 冒号后：字段映射规则
- `field1`：直接复制 field1
- `changed_if(field3,field1)`：如果 field3 变化则发 field3，否则发 field1

**ChanItem 类型**：
- `Copy { id }`：直接复制源行第 id 列
- `ChangedIf { id, no_change_id, prev }`：如果该列值变化则发送 id 列，否则发送 no_change_id 列

**执行流程**：
1. Insert 时，如果表有 chan 配置，构建目标表的行
2. 通过 Worker 的 tx channel 向自己发 InsertRequest
3. 目标表收到后按正常 Insert 流程处理（可能再触发链更新）

### 4.10 后台刷新 (worker/flusher.rs)

定时触发 `Broadcast::Flush`，各 Worker 检查 `last_changed + refresh_interval >= now`，将脏缓存刷到 RocksDB。

Shutdown 时立即刷新所有缓存。

---

## 5. msd-request — 协议类型

### 5.1 概述

客户端-服务器通信的共享类型层。所有 crate 都依赖此 crate。

### 5.2 核心类型

**RequestKey** (`base.rs`):
```rust
pub struct RequestKey {
    pub table: String,  // 表名
    pub obj: String,    // 对象名
}
```

**DateRange** (`base.rs`):
```rust
pub struct DateRange {
    pub start: Option<(i64, bool)>,  // (timestamp, inclusive)
    pub end: Option<(i64, bool)>,    // (timestamp, inclusive)
}
```

**QueryRequest** (`query.rs`):
```rust
pub struct QueryRequest {
    pub key: RequestKey,
    pub fields: Option<Vec<String>>,
    pub date_range: DateRange,
    pub ascending: Option<bool>,
    pub limit: Option<usize>,
    pub objects: Option<Vec<String>>,  // 用于展开多个 object
}
```

**InsertData** (`insert.rs`):
```rust
pub enum InsertData {
    Rows(RowsTable),     // 行存格式
    Columns(Vec<Series>), // 列存格式
    Csv(String),         // CSV 字符串
    Table(Table),        // 已转换的 Table
}
```

### 5.3 Key 编码 (keys.rs)

DataKey 编码确保字典序与时间序相反（最新的 chunk 排在前面）：

```rust
pub fn new_data(obj: &str, seq: u32) -> Self {
    // key = obj + "." + (-(seq as i64) - 1).to_be_bytes()
    // seq=0 -> 全FF, seq=1 -> 全FE, ..., 保证 seq 越大键越小
}
```

IndexKey 总是以 `\x00\x00\x00\x00.` 开头，保证在字典序中排在所有 DataKey 前面。

### 5.4 TableFrame 二进制协议 (table_frame.rs)

高效的表数据传输格式：

```
+----------+----------+--------------+------+--------+
| MAGIC(2) | VERSION  | FRAME_SIZE   | DATA | CRC32  |
|          | (2)      | (4)          | (N)  | (4)    |
+----------+----------+--------------+------+--------+
```

- MAGIC: `0x4D 0x53` ("MS")
- VERSION: `0x0001`
- DATA: bincode 序列化的 Table
- CRC32: 数据完整性校验

### 5.5 SQL 解析器 (sql/)

基于 `sqlparser` 库，自定义 `MsdSqlDialect`。支持：

- `SELECT [fields] FROM table [WHERE ...] [ORDER BY ...] [LIMIT n]`
- `CREATE TABLE name (col type [meta...], ...) [WITH (meta=value, ...)]`
- `INSERT INTO table VALUES (...)`
- `INSERT INTO table COPY <csv>`
- `DELETE FROM table [WHERE obj = '...'] [WHERE ts ...]`
- `DROP TABLE name`
- `DESCRIBE table`
- `COMMENT ON TABLE/COLUMN table[.col] IS 'desc'`

---

## 6. msd-db-viewer — 调试工具

直接读取 RocksDB 目录，将 Schema/Index/Data 块转储为 JSON。调试存储层无需启动服务器。

```bash
cargo run -p msd-db-viewer -- <db_path> [table] [key]
```

---

## 7. bindings — 语言绑定

### 7.1 Python 绑定

使用 PyO3 编译 Rust 扩展 (`_msd`)，结合 Python 层 (`pymsd`)：

**Rust 侧**：
- `py_table.rs`：包装 `Table`，零拷贝暴露内部 Series 为 NumPy 数组

**Python 侧关键 API**：
- `pymsd.query(base_url, sql)` - 同步查询
- `pymsd.async_query(base_url, sql)` - 异步查询
- `pymsd.import_csv(base_url, table, csv_data)` - CSV 导入
- `pymsd.import_dataframes(base_url, table, df_iter)` - DataFrame 导入
- `create_msd_pandas()` / `create_msd_polars()` - 创建 DataFrame 工厂
- `Client` (easy.py) - 高级易用客户端

### 7.2 TypeScript 绑定

使用 `bun` 构建，生成 ESM 模块供浏览器和 Node.js 使用。
