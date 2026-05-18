# 核心数据流详解

本文档深入分析 MSD 中几个核心数据流的实现细节，帮助开发者理解代码执行路径。

---

## 1. 查询数据流

### 1.1 端到端流程

```
Client                    HTTP Server                MsdDb                    Worker
  |                           |                         |                        |
  |-- POST /query (SQL) ---->|                         |                        |
  |                           |                         |                        |
  |                    sql_to_request()                 |                        |
  |                    (解析 SQL 文本)                   |                        |
  |                           |                         |                        |
  |                    按 objects 展开                   |                        |
  |                    (通配符->具体对象列表)              |                        |
  |                           |                         |                        |
  |                           |-- request(Query) ------>|                        |
  |                           |                         |                        |
  |                           |                         |-- get_worker(key)      |
  |                           |                         |   (FxHash 取模)       |
  |                           |                         |                        |
  |                           |                         |-- try_send(req) ------>|
  |                           |                         |                        |
  |                           |                         |              ensure_cache_initialized
  |                           |                         |              (加载 index + last chunk)
  |                           |                         |                        |
  |                           |                         |              index 扫描找重叠 chunk
  |                           |                         |                        |
  |                           |                         |              RocksDB prefix_with
  |                           |                         |              (逐个加载历史 chunk)
  |                           |                         |                        |
  |                           |                         |              过滤字段 + 时间范围 + limit
  |                           |                         |                        |
  |                           |                         |              合并结果 Table
  |                           |                         |                        |
  |                           |                   <-----|-- resp_tx.send(table)  |
  |                           |                         |                        |
  |                    序列化响应                         |                        |
  |                    (NDJSON 或 TableFrame)            |                        |
  |                           |                         |                        |
  |  <-- 流式响应 -------------|                         |                        |
```

### 1.2 关键步骤详解

#### 步骤 1：SQL 解析

文件：`msd-request/src/sql/mod.rs`

函数 `sql_to_request(input: &str) -> Result<Vec<SqlRequest>>` 将 SQL 文本解析为请求列表。

```rust
// 示例 SQL -> SqlRequest 映射
"SELECT * FROM kline WHERE obj = 'SH600519' LIMIT 100"
-> SqlRequest::Query(QueryRequest {
    key: RequestKey { table: "kline", obj: "SH600519" },
    fields: None,
    date_range: DateRange::default(),
    ascending: None,
    limit: Some(100),
    objects: None,
})
```

#### 步骤 2：Objects 展开

文件：`msd/src/server/handlers/query.rs`

如果 `QueryRequest.objects` 不为空（即 SQL 中指定了 `obj = 'xxx'` 或通配符），将其展开为多个单 object 请求。

```rust
// 伪代码
for req in sql_requests {
    let objects = db.matched_objects(&req.key.table, pattern)?;
    for obj in objects {
        let single_req = QueryRequest {
            key: RequestKey { table, obj },
            ..req.clone()
        };
        dispatch(db, single_req);
    }
}
```

#### 步骤 3：Index 扫描定位 Chunk

文件：`msd-db/src/worker/query.rs`

```rust
// 从 index 找出与 DateRange 有交集的 chunk 序号
let (first_query_seq, last_query_seq) = index
    .iter()
    .enumerate()
    .filter_map(|(idx, item)| {
        if item.overlap(&req.date_range) { Some(idx) }
        else { None }
    })
    .fold((index.len(), 0), |(first, last), idx| {
        (first.min(idx), last.max(idx))
    });
```

#### 步骤 4：RocksDB 前缀扫描

利用键设计特性——同一 object 的所有 chunk 在字典序中相邻——使用 `prefix_with` 高效扫描：

```rust
self.store.prefix_with(
    start_key,       // 从最新（或最旧）chunk 开始
    Some(prefix_len),// 限定前缀：obj + '.'
    &table,
    !descending,     // 控制扫描方向
    |k, v| {
        // 解析 key 获取 seq
        // 检查是否超出查询范围
        // 反序列化 Table，过滤行
        // 合并到 result Table
        true  // 继续扫描
    },
)?;
```

#### 步骤 5：最后 Chunk 合并

`index.len() - 1` 位置的 chunk 在缓存中，跳过 RocksDB 读取，直接使用 `cache.cached`。此 chunk 的处理取决于排序方向：
- 降序时在扫描前处理（最新数据优先）
- 升序时在扫描后处理

### 1.3 响应序列化

文件：`msd/src/server/handlers/query.rs`

两种响应格式：

**NDJSON** (默认)：
```json
{"obj":"SH600519","table":"kline","data":{...}}
{"obj":"SZ000001","table":"kline","data":{...}}
```

**TableFrame** (Client 标识为 `msd-client` 时)：
```rust
pack_table_frame(&table) -> Vec<u8>
// MAGIC(2) + VERSION(2) + SIZE(4) + bincode(Table) + CRC32(4)
```

---

## 2. 写入数据流

### 2.1 端到端流程

```
Client                    HTTP Server                 MsdDb                    Worker
  |                           |                         |                        |
  |-- PUT /table/kline ----->|                         |                        |
  |   (CSV 或 TableFrame)     |                         |                        |
  |                           |                         |                        |
  |                    读取 body 内容                    |                        |
  |                    CSV: simd_csv 零拷贝解析           |                        |
  |                    Binary: unpack_table_frame()     |                        |
  |                           |                         |                        |
  |                    parse_csv_to_table()              |                        |
  |                    (第一列为 obj)                    |                        |
  |                           |                         |                        |
  |                    table.group_by(0)                 |                        |
  |                    (按 obj 分组)                     |                        |
  |                           |                         |                        |
  |                    JoinSet 并行分发 (8 任务)          |                        |
  |                           |                         |                        |
  |                           |-- for each obj --------->|                        |
  |                           |   InsertRequest         |                        |
  |                           |                         |                        |
  |                           |                         |-- get_worker(obj.hash)  |
  |                           |                         |                        |
  |                           |                         |   更新 objects 缓存     |
  |                           |                         |                        |
  |                           |                         |-- try_send(req) ------>|
  |                           |                         |                        |
  |                           |                         |              handle_insert()
  |                           |                         |              (详见下方插入算法)
  |                           |                         |                        |
  |                           |                   <-----|-- resp_tx.send(())     |
  |                           |                         |                        |
  |  <-- 204 No Content ------|                         |                        |
```

### 2.2 插入算法详解

文件：`msd-db/src/worker/insert.rs`

```
handle_insert(req) {
    1. ensure_cache_initialized(&req.key)
       // 如果首次访问该 object，从 RocksDB 加载 index + last chunk
    
    2. on_insert_existing(req) {
        a. 获取 schema (表结构)
        b. 获取主键列索引 (pk_col)
        c. 解析 round_unit (从元数据，如 "5m")
        d. 解析 chunk_size (从元数据，默认 200)
        
        e. 获取或创建缓存项：
           cache = CacheValue {
               cached: schema.to_empty(),  // 空表
               index: vec![IndexItem::default()],  // 初始索引
               state: AggState::table_states(schema), // 聚合状态
               chan: Chan::try_from(schema).ok(),     // 链更新配置
               last_changed: 0,
           }
        
        f. 逐行处理：
           for each row in incoming {
               raw_pk = row[pk_col].get_datetime()
               pk = round_ts_with_tz(raw_pk, round_unit, tz)
               
               if pk < cached_last_pk => skip  // 忽略旧数据
               
               if pk == cached_last_pk && cached_row_count > 0 {
                   // UPDATE: 更新最后一行
                   for each col (except pk) {
                       agg_state.update(&cell_value)  // 更新聚合
                       cell = agg_state.get()         // 取聚合结果
                   }
               } else {
                   // APPEND: 追加新行
                   if cache.cached.row_count() >= chunk_size {
                       // ROTATE: chunk 满了，持久化并创建新 chunk
                       seq = index.len() - 1
                       flush_chunk(store, &key, old_chunk, seq)
                       index.push(IndexItem::default())
                       cache.cached = schema.to_empty()
                   }
                   
                   // 重置聚合状态
                   for state in cache.state { state.reset() }
                   
                   // 应用 chan 转换（如配置了链更新）
                   if let Some(chan_table) = chan.apply(&new_row) {
                       push to chan_table
                   }
                   
                   // 写入缓存
                   cache.cached.push_row(new_row)
                   
                   // 更新索引
                   index.last_mut().count += 1
                   index.last_mut().end = pk
               }
           }
        
        g. 将新产生的 chunk 持久化到 RocksDB
        h. 更新 cache.last_changed 时间戳
    }
    
    3. if chan_table exists {
        on_insert_chan(&key, chan_table) {
            // 向目标表发送 InsertRequest
            for target_table in chan.tables() {
                let chan_req = InsertRequest {
                    key: RequestKey { table: target_table, obj },
                    data: InsertData::Table(chan_table.clone()),
                };
                self.tx.try_send(MsdRequest::insert(chan_req));
            }
        }
    }
}
```

### 2.3 时间戳舍入

`round_ts_with_tz(ts, round_unit, tz_offset)` 将时间戳舍入到指定的时间粒度。

```rust
// 示例
let ts = 1710000030_000000;  // 微秒
let unit = (5, b'm');         // 5 分钟
let tz = offset_hours(8);     // UTC+8

// round_ts_with_tz(ts, &unit, tz)
// -> 1710000000_000000  (舍入到 5 分钟边界)
```

### 2.4 聚合更新示例

假设 `kline` 表的字段配置：
```sql
CREATE TABLE kline (
    ts DATETIME,
    open FLOAT64 agg=first,
    high FLOAT64 agg=max,
    low FLOAT64 agg=min,
    close FLOAT64 agg=first,
    volume FLOAT64 agg=sum
) WITH (round='1d')
```

当插入两条同一天的 `snapshot` 数据时：
```
Row1: ts=2024-01-01 10:00, open=10.0, high=10.5, low=9.8, close=10.3, volume=1000
Row2: ts=2024-01-01 14:00, open=10.4, high=10.8, low=10.1, close=10.6, volume=2000
```

两条数据的 ts 都会 round 到 `2024-01-01 00:00`（同一天）。

Row1 被 APPEND（新时间戳）：
- open=10.0, high=10.5, low=9.8, close=10.3, volume=1000

Row2 触发 UPDATE（同一天）：
- open: first 保持 10.0（不变）
- high: max(10.5, 10.8) = 10.8
- low: min(9.8, 10.1) = 9.8
- close: first 保持 10.3
- volume: sum(1000, 2000) = 3000

最终结果：一条 kline 日线记录。

---

## 3. 链更新数据流

### 3.1 配置示例

```sql
-- snapshot 表（原始快照数据）
CREATE TABLE snapshot (
    ts DATETIME, open FLOAT64, high FLOAT64,
    low FLOAT64, close FLOAT64, volume FLOAT64
) WITH (chan='kline:changed_if(open,close),changed_if(high,close),changed_if(low,close),close')

-- kline 表（日线聚合）
CREATE TABLE kline (
    ts DATETIME,
    open FLOAT64 agg=first,
    high FLOAT64 agg=max,
    low FLOAT64 agg=min,
    close FLOAT64 agg=first
) WITH (round='1d')
```

### 3.2 执行流程

```
snapshot 插入一行数据
        |
        v
Worker.handle_insert()
        |
        v
on_insert_existing()
    |-- 处理 snapshot 自身的数据追加/更新
    |-- 同时构建 chan_table（通过 Chan::apply）
    |       |
    |       |-- 对于 open 列：changed_if(open, close)
    |       |   -> 如果 open 变化发 open，否则发 close
    |       |-- 对于 high 列：changed_if(high, close)
    |       |-- 对于 low 列：changed_if(low, close)
    |       |-- 对于 close 列：直接复制
    |       |
    |       |--> chan_table 积累行数据
    |
    v
on_insert_chan()
    |-- 向 Worker 自身重新发送 InsertRequest
    |   key = RequestKey { table: "kline", obj: same_obj }
    |   data = InsertData::Table(chan_table)
    |
    v
Worker 再次处理（这次是 kline 表）
    |-- kline 按 round='1d' 进行聚合
    |-- 继续往上可能再触发链更新（如果 kline 还有 chan）
```

### 3.3 ChanItem::ChangedIf 的工作原理

```rust
ChangedIf { id, no_change_id, prev }
```

- `prev` 记录该字段的上一值
- 当新行中 `id` 列的值 != `prev` 时，发送 `id` 列的值（表示"变化了"）
- 当值相等时，发送 `no_change_id` 列的值（表示"没变"）

这说明 ChangedIf 依赖列值变化检测。例如 K 线场景：
- 如果 `open` 在这个快照周期内变化了，发送新的 open 值
- 如果 `open` 没变，发送 `close` 值替代

---

## 4. 后台刷新与关闭数据流

### 4.1 定时刷新

```
Flusher Task (tokio::spawn)
    |
    |-- sleep(refresh_interval)
    |-- 向 flusher channel 发送 Broadcast::Flush
    |-- 向所有 Worker 发送 Broadcast::Flush
    |
    v
Worker.handle_broadcast(Flush)
    |
    |-- for each (key, cache_item) in cache:
    |      if cache_item.last_changed + refresh_interval >= now:
    |          flush_index(store, key, &cache_item.index)
    |          flush_chunk(store, key, &cache_item.cached, seq)
    |          cache_item.last_changed = now
```

### 4.2 优雅关闭

```
shutdown_signal (SIGINT/SIGTERM)
    |
    v
CancellationToken.cancel()
    |
    v
axum graceful_shutdown
    |
    v
MsdDb::shutdown()
    |-- 向 flusher 发送 Broadcast::Shutdown
    |-- 向所有 Worker 发送 Broadcast::Shutdown
    |
    v
Worker.handle_shutdown()
    |-- for each (key, cache_item) in cache:
    |      flush_index(store, key, &cache_item.index)
    |      flush_chunk(store, key, &cache_item.cached, seq)
    |-- 关闭 rx channel
    |-- 退出 run() 循环
```

---

## 5. 缓存初始化流程

文件：`msd-db/src/worker/init.rs`

当 Worker 首次收到某个 object 的请求时：

```
ensure_cache_initialized(key)
    |
    |-- 检查 cache 是否已存在该 key
    |-- 如果存在 -> 直接返回
    |-- 如果不存在:
    |       |
    |       |-- 从 RocksDB 读取该 object 的 index
    |       |   (Key::new_index(obj) -> get)
    |       |
    |       |-- 反序列化 index (Vec<IndexItem>)
    |       |
    |       |-- 如果有 index 项:
    |       |     读取最后一个 chunk 的数据
    |       |     (Key::new_data(obj, index.len()-1) -> get)
    |       |     反序列化为 Table -> 作为 cache.cached
    |       |
    |       |-- 创建 CacheValue 并插入 cache
    |       |-- 返回结果
```

---

## 6. 并发模型

### 6.1 Worker 隔离

```
              MsdDb
                |
     +----------+---------+
     |          |         |
  Worker0    Worker1   ... Worker7
     |          |         |
  CacheMap   CacheMap   CacheMap
  (独立)     (独立)     (独立)
```

- 每个 Worker 有独立的 CacheMap，无锁竞争
- 同一个 object 总是路由到同一个 Worker（FxHash 确定性）
- Worker 之间通过 mpsc channel 通信（链更新场景）
- Schema 变更通过 Broadcast 同步到所有 Worker

### 6.2 锁策略

| 锁 | 类型 | 竞争 |
|----|------|------|
| `MsdDb.schemas` | RwLock | 低（仅在 DDL 时写） |
| `MsdDb.objects` | RwLock | 中（每次 Insert 都写） |
| Worker.CacheMap | 无锁 | 无（单线程访问） |

---

## 7. 数据完整性

### 7.1 写入保证

- Insert 数据先写缓存（内存），后异步刷盘
- 后台定时刷新 + 关闭时强制刷新
- TableFrame 协议提供 CRC32 校验

### 7.2 数据一致性问题

当前设计存在以下风险点（已知限制）：
- 后台刷新间隔内如果进程崩溃，未刷盘的缓存数据会丢失
- 可通过减小 `refresh_interval` 降低风险（如设为 1m）
- RocksDB 自身提供 WAL 保护已刷盘的数据
