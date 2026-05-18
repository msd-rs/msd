# 开发指南

## 1. 环境准备

### 1.1 系统依赖

```bash
# Ubuntu/Debian
sudo apt install build-essential pkg-config \
    libssl-dev libclang-dev llvm-dev \
    librocksdb-dev

# 安装 Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# 安装 mold 链接器（推荐，加速编译）
sudo apt install mold
```

### 1.2 克隆项目

```bash
git clone <repo-url> msd-rs2
cd msd-rs2
```

### 1.3 配置环境变量

项目根目录有 `.env` 文件作为模板：

```bash
# .env
MSD_DB_PATH=./msd_db
RUST_LOG=info
MSD_WORKERS=8
MSD_TZ=+08:00
```

### 1.4 IDE 配置

推荐使用 VS Code + rust-analyzer。项目已有 `.vscode/settings.json`。

---

## 2. 构建与运行

### 2.1 编译配置

`.cargo/config.toml` 中的构建优化：

```toml
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "target-feature=+sse4.2,+avx2"]
linker = "clang"
[target.x86_64-unknown-linux-gnu.rustflags]
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
[build]
target-dir = "/tmp/msd-rs2"
```

构建产物输出到 `/tmp/msd-rs2` 以减少 SSD 写入。

### 2.2 常用构建命令

```bash
# Debug 构建
cargo build

# Release 构建
cargo build --release

# 仅构建 msd 二进制
cargo build -p msd

# 运行
cargo run -p msd -- server

# 运行服务器（自定义参数）
cargo run -p msd -- server --listen 0.0.0.0:50510 --workers 16

# 启动 Shell
cargo run -p msd -- shell --server http://127.0.0.1:50510

# 生成 Token
cargo run -p msd -- token -a your-secret-key -r admin -e 365
```

### 2.3 Makefile 快捷命令

```bash
make build      # Release 构建
make test       # 运行所有测试
make python     # 构建 Python 绑定
make release    # 发布构建（全面优化）
```

---

## 3. 测试

### 3.1 运行测试

```bash
# 全部测试
cargo test

# 指定 crate
cargo test -p msd-db
cargo test -p msd-request
cargo test -p msd-table

# 具体测试
cargo test -p msd-request sql::tests

# 显示输出
cargo test -- --nocapture
```

### 3.2 测试结构

| 位置 | 类型 | 说明 |
|------|------|------|
| `msd-db/tests/` | 集成测试 | 数据库操作流程测试 |
| `msd-table/tests/` | 集成测试 | CSV 解析、Series 操作 |
| `msd-request/src/sql/tests.rs` | 单元测试 | SQL 解析器测试 |
| `msd/tests/` | 集成测试 | RocksDB 存储测试 |
| `tests/sql/` | SQL 脚本 | 完整 SQL 场景测试 |
| `tests/scripts/` | Python 脚本 | 端到端功能演示 |

### 3.3 测试数据

测试用 CSV 数据在 `tests/data/` 目录：
- kline 数据（日线行情）
- dividend 数据（分红）
- shares 数据（股本）
- financials 数据（财务数据）

---

## 4. 代码规范

### 4.1 格式化

```bash
cargo fmt --all
```

使用 `rustfmt.toml` 配置（2 空格缩进）。

### 4.2 Lint

```bash
cargo clippy --all -- -D warnings
```

### 4.3 版权头

每个源文件需要包含：

```rust
// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only
```

### 4.4 文档注释

关键结构体和函数应有 doc comment (`///`)。`msd-db/src/lib.rs` 包含模块级设计文档，修改核心逻辑时需同步更新。

### 4.5 错误处理规范

- 库 crate 使用 `thiserror` 定义错误类型
- 应用层使用 `anyhow::Result`
- 不要对可预见的内部错误 panic（使用 `Result`）
- `MsdRequest` 中对不会被 Worker 处理的消息使用 `unreachable!` / `panic!`

---

## 5. 添加新功能指南

### 5.1 添加新的数据类型

1. **msd-table/src/variant/mod.rs**：在 `Variant` 枚举中添加新变体
2. 实现 `cast.rs` 中的类型转换规则
3. 实现 `ops.rs` 中的算术运算（如需要）
4. 更新 `serde/` 中的序列化逻辑
5. 更新 `datatype.rs` 中的 `DataType` 枚举
6. 添加测试验证序列化往返

### 5.2 添加新的聚合类型

1. **msd-request/src/agg.rs**：在 `AggStateId` 枚举中添加新 ID
2. **msd-db/src/worker/agg_state.rs**：
   - 在 `AggState` 枚举中添加新变体
   - 实现 `update()` 中的更新逻辑
   - 实现 `get()` 中的取值逻辑
   - 实现 `reset()` 中的重置逻辑
   - 更新 `From<AggStateId>` 和 `id()` 方法
3. 更新 SQL 解析器以识别新聚合名称

### 5.3 添加新的 HTTP 端点

1. **msd/src/server/mod.rs**：添加路由
   ```rust
   .route("/new-path", get(handlers::handle_new))
   ```
2. **msd/src/server/handlers/**：创建处理器文件
3. 如需要认证，在 `permission.rs` 中添加权限检查
4. 如涉及数据库操作，定义对应的 `MsdRequest` 变体
5. 在 Worker 中添加处理逻辑

### 5.4 添加新的 MsdRequest 类型

1. **msd-request/src/**：定义新请求类型及其响应类型
2. **msd-db/src/request.rs**：在 `MsdRequest` 枚举中添加新变体
3. **msd-db/src/worker/mod.rs**：在 `run()` 方法的 match 分支中处理
4. **msd-db/src/db/mod.rs**：在 `request()` 方法中添加路由逻辑
5. 如需数据交换，添加对应的 `RequestSender/Receiver`

### 5.5 添加新的 SQL 语法

1. **msd-request/src/sql/mod.rs**：
   - 在 `SqlRequest` 或相关枚举中添加变体
   - 在 `sql_to_request()` 中添加解析分支
2. **msd-request/src/sql/msd_dialect.rs**：定义自定义 SQL 方言（如需要）
3. **msd-request/src/sql/tests.rs**：添加测试用例
4. 在对应的 Handler 中添加处理逻辑

### 5.6 添加新的存储后端

1. 创建新模块（如 `msd-store/src/store_xxx/`）
2. 实现 `MsdStore` trait
3. 在 `msd-store/src/lib.rs` 中导出
4. 通过 feature flag 控制编译：
   ```toml
   [features]
   rocksdb = []
   xxx = []
   ```

---

## 6. 调试技巧

### 6.1 日志配置

通过环境变量控制日志级别：
```bash
# 全局 info，msd-db 模块 debug
RUST_LOG=info,msd_db=debug cargo run -p msd -- server

# 查看 trace 级别日志（非常详细）
RUST_LOG=trace cargo run -p msd -- server
```

### 6.2 使用 msd-db-viewer 调试存储层

```bash
# 列出所有表名和 key
cargo run -p msd-db-viewer -- ./msd_db

# 查看特定表的 Schema
cargo run -p msd-db-viewer -- ./msd_db kline

# 查看特定 key 的 Index 和 Data
cargo run -p msd-db-viewer -- ./msd_db kline SH600519
```

### 6.3 使用 Shell 进行交互式调试

```bash
cargo run -p msd -- shell
# 在 Shell 中
.server http://127.0.0.1:50510
.schema kline
SELECT * FROM kline LIMIT 10;
.import test.csv kline
.dump kline test_out.csv
```

### 6.4 性能分析

```bash
# 启用 pprof 生成火焰图
cargo run -p msd -- server --pprof flamegraph
# 退出后生成 flamegraph.pprof.svg
```

---

## 7. 常见开发任务

### 7.1 修改聚合规则

相关文件：
- `msd-db/src/worker/agg_state.rs` — 聚合状态机
- `msd-db/src/worker/insert.rs` — `on_insert_existing()` 方法
- `msd-request/src/agg.rs` — AggStateId 定义

### 7.2 修改存储键布局

相关文件：
- `msd-request/src/keys.rs` — Key 编码/解码
- `msd-db/src/lib.rs` — 键布局注释
- `msd-db/src/worker/init.rs` — 缓存初始化
- `msd-db/src/worker/query.rs` — 查询中的键操作

注意：修改键布局需要数据迁移方案。

### 7.3 优化查询性能

切入点：
1. **索引扫描** (`worker/query.rs`): 减少需要扫描的 chunk 数量
2. **列过滤** (`filter_table_columns`): 只反序列化需要的列
3. **chunk_size**: 调整默认 chunk 大小（默认 200 行）
4. **缓存命中率**: 检查 cache 初始化逻辑 (`worker/init.rs`)

### 7.4 优化写入性能

切入点：
1. **CSV 解析**: 已使用 `simd_csv` 零拷贝解析
2. **Worker 并行度**: 增加 Worker 数量
3. **chunk_size**: 更大的 chunk 减少序列化次数（但增加内存）
4. **后台刷新间隔**: 增大间隔减少 IO 次数（增加数据丢失风险）

### 7.5 添加表元数据支持

表元数据存储在 Table 的 metadata 中，通过：
- `table.get_table_meta(key)` - 读取
- `table.set_table_meta(key, value)` - 设置

列元数据：
- `field.get_metadata(key)` - 读取
- `table.set_field_meta(field, key, value)` - 设置

已支持的元数据键：
- `round`：时间聚合粒度（如 `"5m"`）
- `chunkSize`：每 chunk 行数
- `chan`：链更新配置
- `agg`：聚合方法（用于列）
- `desc`：字段/表描述
- `ignore_older_than`：旧数据截止时间戳

---

## 8. 发布流程

### 8.1 版本号管理

所有 crate 版本在各自的 `Cargo.toml` 中定义。Workspace 依赖版本在根 `Cargo.toml` 的 `[workspace.dependencies]` 中统一管理。

### 8.2 构建 Release

```bash
make release
# 或
cargo build --release
```

### 8.3 Docker 构建

```bash
docker build -t msd:latest .
```

### 8.4 CI/CD

项目使用 CNB (cnb.cool) 作为 CI/CD 平台，配置在 `.cnb.yml` 中。

流程包括：
- 构建（cargo build --release）
- Docker 镜像推送
- PyPI 包上传（Python 绑定）

---

## 9. 注意事项

### 9.1 数据安全性

- **不要在生产环境中使用 `rm -rf msd_db`**：这会永久删除所有数据
- 备份：直接复制 `msd_db` 目录即可（RocksDB 自包含）
- 修改存储键布局前，先做数据迁移方案

### 9.2 并发安全

- `MsdDb.schemas` 和 `MsdDb.objects` 使用 `RwLock`，注意避免死锁
- 每个 Worker 拥有独立的缓存（CacheMap），无需锁同步
- Worker 通过 mpsc channel 通信，天然线程安全
- 向 Worker 发送请求使用 `try_send`（非阻塞）

### 9.3 内存管理

- 缓存无上限，数据量大时需关注内存使用
- 每个 Chunk 默认 200 行，`chunkSize` 元数据可调整
- jemalloc 减少内存碎片

### 9.4 向后兼容

- Table 序列化使用 bincode，添加新字段时需考虑兼容性
- Channel 容量（200,000）在负载极高时可能成为瓶颈
