# `/query` 查询链路性能优化记录

- **日期时间**: 2026-11-04 14:49 (+08:00)
- **基准 Commit ID**: `b7c8726def7b0eca19dc05813a82e07fba336ddc` (`feat: enable multi ts field filter in SQL`)
- **分支**: `query_opt`

---

## 1. 背景与复现用例

针对 HTTP `/query` 处理器（`msd/src/server/handlers/query.rs` 中的 `handle_data`）及底层 `msd-db` / `msd-table` / `msd-store` 查询执行路径进行性能分析与优化。

### 基准测试命令

启动服务：
```bash
cargo run -r --bin msd server
```

压测请求：
```bash
curl -v -w "total: %{time_total}s" http://127.0.0.1:50510/query \
  -H 'user-agent: msd-client' \
  -H 'content-type: application/json' \
  -o /dev/null \
  -d '{"query":"select f001,f007,f095, f130,f203 from stock_financial limit -8"}'
```

### 数据集特征
- 表名：`stock_financial`
- 标的数量（Objects）：`5,228` 个
- 表总列数：`167` 列（`ts` 主键列 + `166` 个财务指标列）
- 查询投影列数：`6` 列（`ts`, `f001`, `f007`, `f095`, `f130`, `f203`）
- 查询行数：每个标的取最新 `8` 行（`limit -8`）
- 响应体大小：约 `2.31 MB`（`application/x-msd-table-frame` 二进制帧流）

---

## 2. 性能瓶颈分析（Profiling）

通过在 `handle_data`、`Worker::run` 及 `Worker::handle_query` 中注入分段计时埋点，定位到以下 5 个核心性能瓶颈：

### 2.1 列裁剪（Column Projection）时机过晚
在原 `Worker::handle_query` 中，针对全部 5,228 个标的的每一次查询：
1. 先调用 `result = cache.cached.to_empty()`，克隆全部 **167 个 `Field`**（包含每个字段的 `name: String` 与 `metadata: Option<HashMap<String, Variant>>`）。
2. 在 `collect_rows_from_table` 中，对每行遍历全部 **167 列**，逐单元格执行 `src.data.get(r) -> VariantRef -> to_variant() -> dst.data.push(Variant)`。
3. 在收集完所有行之后，才通过 `filter_table_columns` 丢弃未请求的 **161 列**。
- **影响**：单次请求产生约 `5,228 * 8 * 167 ≈ 700 万` 次单元格访问及约 `350 万` 次无效的堆内存分配/释放。

### 2.2 1,823 个标的在每次热查询中触发 RocksDB 历史分片扫描与全量反序列化
- `CacheValue.cached` 仅缓存每个标的的最后一个分片（`seq = index.len() - 1`）。
- 在 `stock_financial` 中，有 **1,823 个标的** 的最新分片行数不足 8 行（1~7 行），导致每次热查询这 1,823 个标的都会触发 `need_store_scan = true` 去读取倒数第二个分片（`last_seq - 1`）。
- 原扫描逻辑每次调用 `self.store.prefix_with(...)` 创建并销毁 RocksDB C++ `DBIterator`，且 `RocksDbStore::new` 中块缓存 `Cache::new_lru_cache(1024 * 8)` 被误配置为 **8 KB**（而非 256 MB），导致频繁块缓存未命中。
- 读取到历史分片字节后，调用 `DbBinary::from_bytes::<Table>(v)` 全量反序列化 **167 列**，随后丢弃其中 161 列。

### 2.3 冷启动 `ensure_cache_initialized` 全量解码与冗余状态计算
- 冷启动加载缓存时，原 `ensure_cache_initialized` 立即将最新分片的 **167 列** 全部反序列化为 `Table`，并计算仅在 `insert` 时才会使用的 `AggState::table_states(&table)` 和 `Chan::parse_from_table(&table)`。
- 同时将只读加载的缓存项 `last_changed` 设为 `now()`，导致未发生写入的缓存项在定时 Flush 和 Shutdown 时被无意义地重序列化写回 RocksDB。

### 2.4 批量查询中的 `Table` 重复分配与跨任务打包开销
- 原 `BatchQuery` 中，Worker 为 5,228 个标的分别构造独立的 `Table` 实例（重复分配 5,228 份 `Field` 向量与 `Table.metadata` `HashMap`），将 `Vec<Table>` 通过通道发送回 HTTP 层。
- `TableFrameBody` 为每个批次派生 `tokio::spawn` 任务（存入 `JoinSet`），逐个将 `Table` 序列化为 frame 字节后再销毁。

### 2.5 请求展开阶段的 Schema 克隆与 `HashSet` 构造
- `flatten_requests_by_object` 调用 `state.db.get_schema(&query_req.table)` 克隆整个 167 列的 `Table` 结构仅用于检查 `.is_kv()`。
- `matched_objects` 将内存中的标的集合先全量克隆并哈希插入到临时 `HashSet<SmolStr>`，随后立刻 `.into_iter().collect::<Vec<_>>()` 转为 `Vec`。

---

## 3. 具体优化方案与改动文件

### 3.1 存储层优化（`msd-store`）
- **文件**:
  - `msd-store/src/lib.rs`
  - `msd-store/src/store_rocksdb/mod.rs`
- **改动**:
  1. 将 RocksDB LRU Block Cache 从 `1024 * 8`（8 KB）调整为 `256 * 1024 * 1024`（256 MB）。
  2. 在 `MsdStore` trait 中新增 `get_with` 方法，并在 `RocksDbStore` 中基于 `db.get_pinned_cf` 实现零拷贝切片访问，避免读取时额外的 `Vec<u8>` 拷贝。

### 3.2 表结构、按需列延迟解码与批量帧打包（`msd-table`）
- **文件**:
  - `msd-table/src/table/series.rs`
  - `msd-table/src/table/field.rs`
  - `msd-table/src/table/table.rs`
  - `msd-table/src/table_frame.rs`
  - `msd-table/src/errors.rs`
  - `msd-table/src/lib.rs`
- **改动**:
  1. **类型化批量行拷贝**：在 `Series` 中新增 `with_capacity`、`clear`、`extend_range` 与 `extend_by_indices`，支持直接在同类型底层 `Vec<T>` 上按范围或索引切片拷贝数据，彻底消除逐单元格 `VariantRef -> Variant` 转换。
  2. **`LazyChunk` 按需列解码**：新增 `LazyChunk` 结构，保存分片的原始 bincode 字节流 `Box<[u8]>`。首次访问时通过零分配扫描记录每一列 `Series` 的字节偏移量 `series_offsets: Vec<u32>`；查询某列时仅解码该列的 `Series` 并缓存在 `cols: Vec<Option<Series>>` 中，未查询的列完全不解码、不分配内存。
  3. **`decode_projected_series` 单次投影解码**：针对历史分片扫描，直接从借用的 `&[u8]` 切片中跳过未请求列、仅解码目标投影列，并在达到最大请求列索引后提前终止扫描。
  4. **`BatchFramePacker` 预编码打包器**：针对同一批次的查询结果，预先编码一次 `Table.version`、列数、各投影列的 `(name, kind, metadata)` 头部以及 `Table.metadata` 的固定前缀；每个标的打包时只需 `memcpy` 列头并编码对应的 `Series` 数据、标的名称 `obj` 与 CRC32。

### 3.3 数据库 Worker 与缓存层优化（`msd-db`）
- **文件**:
  - `msd-db/src/worker/cache.rs`
  - `msd-db/src/worker/init.rs`
  - `msd-db/src/worker/insert.rs`
  - `msd-db/src/worker/query.rs`
  - `msd-db/src/worker/mod.rs`
  - `msd-db/src/request.rs`
  - `msd-db/src/db/mod.rs`
- **改动**:
  1. **惰性缓存初始化与写时物化**：`ensure_cache_initialized` 将最新分片存为 `CacheValue.lazy_last: Option<LazyChunk>`（`last_changed = 0`），不解码未访问列、不预计算 `AggState` 和 `Chan`。当 `handle_insert` 触发写入时，调用 `CacheValue::materialize_for_write` 按需解码完整 `Table` 并初始化聚合与通道状态；`Broadcast::Flush` 与 `handle_shutdown` 仅刷盘真正发生过修改的缓存项。
  2. **次新分片缓存（`lazy_prev`）与索引直查**：在 `CacheValue` 中新增 `lazy_prev: Option<(u32, LazyChunk)>` 缓存倒数第二个分片（`index.len() - 2`），并在历史分片回溯时直接按内存 `cache.index` 的序列号 `seq` 进行点查（取代 `prefix_with` 迭代器扫描），使 `limit -8` 热查询实现 **0 次 RocksDB 读取、0 次 bincode 解码**。
  3. **前置列裁剪、范围快路径与原地反转**：
     - 查询开始时计算投影列索引 `proj_cols`。
     - 当无额外时间过滤且分片位于查询时间范围内时，直接按所需行数计算 `[start..end]` 切片范围并调用 `Series::extend_range`。
     - `is_last_mode`（`limit < 0`）收集完成后直接对各列调用 `Series::reverse()` 原地反转，替代 `sort_by_pk` 排序。
  4. **Worker 内置帧打包（`BatchQueryFrame`）**：新增 `MsdRequest::BatchQueryFrame` 与 `MsdDb::query_batch_frames`，Worker 在批次内复用同一组 `Vec<Series>` 缓冲与 `BatchFramePacker`，直接生成打包好的帧字节流 `Vec<u8>`。
  5. **元数据与标的匹配无锁/少拷贝路径**：公开 `MsdDb::is_kv_table`，新增 `MsdDb::matched_objects_vec` 直接返回 `Vec<SmolStr>`。

### 3.4 HTTP 查询处理器优化（`msd`）
- **文件**:
  - `msd/src/server/handlers/query.rs`
- **改动**:
  1. `flatten_requests_by_object` 改用 `state.db.is_kv_table` 与 `state.db.matched_objects_vec`。
  2. `TableFrameBody` 直接持有并轮询 `query_batch_frames` 返回的 `Vec<RequestReceiver<Vec<u8>>>`，省去为每个分块派生 Tokio 任务的调度开销。

### 3.5 Python 客户端绑定优化（`bindings/python` & `msd-table`）
- **文件**:
  - `msd-table/src/table_frame.rs`
  - `msd-table/src/table/table.rs`
  - `bindings/python/src/conn.rs`
  - `bindings/python/src/py_table.rs`
  - `bindings/python/src/lib.rs`
  - `bindings/python/pymsd/query.py`
  - `bindings/python/pymsd/_msd.pyi`
- **改动**:
  1. **免 `HashMap` 元数据解包（`unpack_table_frame_meta` / `unpack_frame_batch`）**：利用 `PosSliceReader` 与 `SkipVariant` 直接从二进制流中提取 `obj` 与 `table` 字符串（同批次内复用 `Arc<str>` 共享表名），跳过 `Table.metadata` 和第 0 列主键 `Field.metadata` 的 `HashMap` 分配（单次查询省去数万次 `HashMap` 与 `String` 堆分配）。
  2. **流式分块并行解包（`query_msd_batches` / `query_msd_stream`）**：移除逐帧 `BufReader::read_exact` 循环，直接基于 `resp.chunk().await` 扫描完整帧边界并按批次派生到 Tokio 多线程运行时并行解包，使 `conn.query`、`conn.query_map`、`conn.query_join`、`conn.snapshot` 与 `pymsd.query` 的客户端解包均与服务端流式发送完全重叠。
  3. **释放 GIL 与直接构建 `PyDict`（`PyMsdConnection::query`）**：在网络请求与解包阶段通过 `py.detach` 释放 GIL；移除两层 Rust 中间 `HashMap`（`GroupedTables` / `GroupedPyTables`），直接在 Python 侧构建结果字典并缓存复用内层字典的 `table_name` `PyString` 键。
  4. **异步后台释放 `PyTable`（`Drop for PyTable`）**：当 Python 侧覆盖或销毁大规模查询结果字典时，将非空 `Table` 移交后台线程释放，避免在持有 GIL 的主线程同步执行数万次 `free()`。
  5. **优化其余 `query_*` 变种（`query_map` / `query_join` / `snapshot` / `pymsd.query`）**：
     - `conn.query_map`：`query_msd_stream` 改为按批次并行解包并通过有界通道流式传递 `FrameBatch`，等待批次时通过 `py.detach` 释放 GIL。
     - `conn.query_join`：预分配最终行数 `objs.len() * bars` 的结果列，使用 `compute_joined_indices` 与 `extend_series_with_fill` 直接追加至目标列，消除每个标的对象构造中间 `Table`、`join_asof` 克隆已有列及 `v_concat` 多次扩容拷贝。
     - `conn.snapshot`：预分配 `objs.len()` 容量并直接按标的顺序填充各表首行或 `NaN` 缺失值，省去构造中间 `sub_table` 与基于字符串主键的 `join_asof`。
     - `pymsd.query`（生成器接口）：新增 `MsdConnection.query_raw` 方法并复用连接实例，替代原 Python `requests` + 逐帧 `r.read(8)` / `parse_table_frame` 循环。

---

## 4. 优化前后性能对比

在相同环境下执行 `curl` 与 `uv run pytest -s tests/test_conn.py` 测试：

| 测试场景 | 优化前 | 优化后 | 性能提升 |
| :--- | :---: | :---: | :---: |
| **冷启动首次查询 (`curl` Cold Cache)** | `0.1129s` (~112.9 ms) | **`0.0701s - 0.0730s`** (~70.1 ms) | **~1.6x** |
| **热缓存查询 (`curl` Warm Cache)** | `0.0681s - 0.1039s` (~68.1 - 103.9 ms) | **`0.00355s`** (**3.55 ms**) | **~19x - 29x** |
| **热查询吞吐速率 (`curl`)** | ~23 - 35 MB/s | **~652.8 MB/s** | **~19x - 28x** |
| **Python `conn.query` (`test_financial`, 5,228 标的)** | `0.015s - 0.017s` (~15 - 17 ms) | **`0.003s - 0.004s`** (**3 - 4 ms**) | **~4x - 5x** |
| **Python `conn.query_map` (`test_query_map`, 875 标的 × 1,000 行)** | `0.0221s` (~22.1 ms) | **`0.00998s`** (**9.98 ms**) | **~2.2x** |
| **Python `pymsd.query` (`test_query_many_ndarray`, 16,728 帧)** | `0.1052s` (~105.2 ms) | **`0.0524s`** (**52.4 ms**) | **~2.0x** |
| **Python `conn.snapshot` (`test_query_many_conn`, 5,576 标的 × 3 表)** | `0.0263s` (~26.3 ms) | **`0.0163s`** (**16.3 ms**) | **~1.6x** |
| **Python `conn.query_join` (`test_query_kline`, 875,000 行 × 12 列)** | `0.0546s` (~54.6 ms) | **`0.0423s`** (**42.3 ms**) | **~1.3x** |


