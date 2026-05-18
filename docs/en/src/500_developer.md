# MSD 开发指导文档

欢迎加入 MSD 项目！本文档集旨在帮助新成员快速了解项目架构、各模块功能，并开始进行开发。

## 文档导航

### [01-系统架构概述](./01-系统架构概述.md)
阅读时间约 20 分钟。涵盖：
- 项目简介与设计理念
- 分层架构图
- Cargo Workspace 成员及依赖关系
- 核心数据模型（TableName/ObjectName/Timestamp/Chunk/Index）
- 存储键设计原理
- 数据流概览
- 技术栈
- 项目文件布局

**建议首先阅读此文档。**

### [02-模块功能详解](./02-模块功能详解.md)
阅读时间约 45 分钟。涵盖：
- `msd` — HTTP 服务器、Shell、Token、权限、MCP、WebSocket
- `msd-store` — MsdStore trait 与 RocksDB 实现
- `msd-table` — Table/Series/Variant/Field 数据结构
- `msd-db` — MsdDb/Worker/CacheMap/聚合状态/链更新/后台刷新
- `msd-request` — 请求类型/SQL 解析/Key 编码/TableFrame 协议
- `msd-db-viewer` — 调试工具
- `bindings` — Python/TypeScript 绑定

**了解每个 crate 的职责和内部结构时参考。**

### [03-开发指南](./03-开发指南.md)
阅读时间约 30 分钟。涵盖：
- 环境准备与构建命令
- 测试结构与运行方法
- 代码规范（格式/Lint/版权头/错误处理）
- 添加新功能的分步指南
  - 新数据类型/新聚合/新 HTTP 端点/新 MsdRequest/新 SQL 语法
- 调试技巧（日志/db-viewer/Shell/pprof）
- 常见开发任务速查
- 发布流程

**开始写代码前必读。**

### [04-核心数据流详解](./04-核心数据流详解.md)
阅读时间约 35 分钟。涵盖：
- 查询完整流程（SQL 解析 -> 展开 -> Worker 扫描 -> 过滤合并 -> 响应）
- 写入完整流程（CSV 解析 -> 分组 -> Worker 插入 -> 聚合更新 -> 链更新）
- 时间戳舍入与聚合更新示例
- 链更新机制详解
- 后台刷新与关闭流程
- 缓存初始化流程
- 并发模型与锁策略

**深入理解代码执行路径时参考。**

### [05-API参考](./05-API参考.md)
阅读时间约 20 分钟。涵盖：
- HTTP API（POST /query、PUT /table、GET /ws、GET /mcp）
- TableFrame 二进制协议格式
- SQL 语法参考（SELECT/CREATE TABLE/INSERT/DELETE/DESCRIBE/COMMENT）
- Python SDK API
- JWT 认证与权限
- 环境变量参考

**编写客户端或理解接口协议时参考。**

## 快速链接

- **架构设计文档（英文）**: 项目根目录 `Architecture.md`
- **README（中文）**: 项目根目录 `README.md` (英文)、`README.cn.md` (中文)
- **模块内部文档**: `msd-db/src/lib.rs`（数据模型和键布局）、`msd-db/AGENTS.md`
- **MCP 教程**: `msd/src/server/handlers/mcp_sql_guide.md`、`mcp_python_guide.md`

## 建议的阅读顺序

1. 先读 `01-系统架构概述` — 建立全局认知
2. 再读 `02-模块功能详解` — 了解每个 crate
3. 然后读 `04-核心数据流详解` — 理解代码执行路径
4. 接着读 `03-开发指南` — 动手开发
5. 需要时翻阅 `05-API参考` — 接口速查
