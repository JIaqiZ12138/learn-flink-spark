# learn-flink 源码研读笔记

> 对应引擎：Apache Flink 1.20

本目录只放**文档**，不放代码。代码实战见同模块 `src/main/java/`。

> **图不放文档目录**：所有 `.drawio` 源文件与 `.png` 导出统一归档在 [`assets/`](./assets/)，
> 正文以图片形式内嵌引用。文档目录里只有 `.md`。

## 分层索引

| 分类 | 说明 | 主题数 | 进度 |
|---|---|---|---|
| [`flink-core/`](./flink-core/) | 引擎本体：一条记录从提交到执行的完整生命周期。 | 7 | 3/7 |
| [`flink-sql/`](./flink-sql/) | SQL 层：从一条 SQL 文本到执行计划。 | 4 | 0/4 |
| [`flink-cdc/`](./flink-cdc/) | 变更数据捕获：把数据库的 binlog 变成 Flink 数据流。 | 6 | 0/6 |
| [`flink-agent/`](./flink-agent/) | 事件驱动的 AI Agent 框架：让 Agent 跑在 Flink 流处理之上。 | 5 | 0/5 |

---

## Flink Core（内核）

引擎本体：一条记录从提交到执行的完整生命周期。

> 详细说明（模块组成、每个文件的作用、源码入口链接）见 [`flink-core/README.md`](./flink-core/README.md)。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| [`job-submit/`](./flink-core/job-submit/) | 任务提交 | CLI / REST / Application 三种入口 → JobGraph → ExecutionGraph | ✅ 已完成 |
| [`scheduling/`](./flink-core/scheduling/) | 作业调度 | 调度策略、Slot 分配、ResourceManager、failover | ✅ 已完成 |
| `memory/` | 内存管理 | MemoryManager、NetworkBufferPool、托管内存模型 | ⬜ 待开始 |
| `rpc/` | 组件通信 | RPC 框架（Pekko）、Akka 到 Pekko 的迁移、组件间协议 | ⬜ 待开始 |
| [`datastream/`](./flink-core/datastream/) | DataStream API 算子 | Source / Operator / Sink 三层算子、函数族谱、算子链、类型推断、分区器、连接器 | ✅ 已完成 |
| `window-watermark/` | 窗口和水位线 | WindowAssigner、Trigger、Evictor、Watermark 传播 | ⬜ 待开始 |
| `state-fault-tolerance/` | 状态与容错 | KeyedState / OperatorState、StateBackend、Checkpoint、Savepoint | ⬜ 待开始 |

> FlinkSQL 不在 Flink Core 下，它是独立的主题域 [`flink-sql/`](./flink-sql/)。


## Flink SQL

SQL 层：从一条 SQL 文本到执行计划。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| `parser/` | SQL 解析 | Calcite Parser、SqlNode、DDL/DML 解析 | ⬜ 待开始 |
| `optimizer/` | SQL 优化器 | 逻辑计划、规则优化、代价模型、物理计划生成 | ⬜ 待开始 |
| `join/` | Join 算子 | Regular / Interval / Lookup Join、双流 Join 的状态与水位线 | ⬜ 待开始 |
| `connectors/` | 外部集成 | Table Connector、Catalog、格式（JSON/Avro/Parquet） | ⬜ 待开始 |


## Flink CDC

变更数据捕获：把数据库的 binlog 变成 Flink 数据流。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| `architecture/` | 架构总览 | Source → Debezium/自研增量快照 → DataChangeEvent → Flink | ⬜ 待开始 |
| `snapshot/` | 增量快照算法 | 无锁快照、chunk 切分、并行快照 | ⬜ 待开始 |
| `source-connector/` | Source 实现 | FLIP-27 Source 接口、Enumerator / Reader 拆分 | ⬜ 待开始 |
| `schema-evolution/` | Schema 演进 | 表结构变更的捕获与下游兼容 | ⬜ 待开始 |
| `exactly-once/` | 断点续传与容错 | offset 持久化、重启恢复、Exactly-Once 语义 | ⬜ 待开始 |
| `pipeline-connector/` | 整库同步 | Pipeline Connector：源库到目标库的整库/整表同步 | ⬜ 待开始 |


## Flink Agents

事件驱动的 AI Agent 框架：让 Agent 跑在 Flink 流处理之上。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| `architecture/` | 架构总览 | Agent / Event / Action / Channel / Runner 核心模型 | ⬜ 待开始 |
| `execution-environment/` | 执行环境 | AgentsExecutionEnvironment 与 Flink 作业的映射 | ⬜ 待开始 |
| `chat-model/` | LLM 接入 | ChatModel / ChatSetup，模型调用与流式返回 | ⬜ 待开始 |
| `tool-calling/` | 工具调用 | Tool 定义、参数绑定、调用结果回填 | ⬜ 待开始 |
| `memory/` | 记忆与状态 | Agent 记忆、会话状态与 Flink State 的结合 | ⬜ 待开始 |

