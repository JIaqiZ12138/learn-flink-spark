# learn-spark 源码研读笔记

> 对应引擎：Apache Spark 3.5

本目录只放**文档**，不放代码。代码实战见同模块 `src/main/java/`。

## 分层索引

| 分类 | 说明 | 主题数 | 进度 |
|---|---|---|---|
| [`spark-core/`](./spark-core/) | 引擎本体：RDD 血缘、Stage 切分、Shuffle 与内存。 | 7 | 0/7 |
| [`spark-sql/`](./spark-sql/) | SQL 层：Catalyst 优化器与全阶段代码生成。 | 6 | 0/6 |
| [`spark-streaming/`](./spark-streaming/) | 流处理：微批模型与结构化流。 | 4 | 0/4 |
| [`spark-mllib/`](./spark-mllib/) | 机器学习库（后续可选）。 | 2 | 0/2 |

---

## Spark Core（内核）

引擎本体：RDD 血缘、Stage 切分、Shuffle 与内存。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| `job-submit/` | 任务提交 | spark-submit → SparkContext → Driver / Executor 的角色划分 | ⬜ 待开始 |
| `rdd-lineage/` | RDD 与血缘 | RDD 抽象、窄/宽依赖、血缘重算容错 | ⬜ 待开始 |
| `scheduler/` | 作业调度 | DAGScheduler 切 Stage、TaskScheduler、推测执行 | ⬜ 待开始 |
| `shuffle/` | Shuffle 机制 | SortShuffleManager、ShuffleWriter/Reader、溢出与合并 | ⬜ 待开始 |
| `memory/` | 内存管理 | 统一内存模型、Execution/Storage 内存、GC 调优 | ⬜ 待开始 |
| `storage/` | 存储体系 | BlockManager、缓存与持久化级别、广播变量 | ⬜ 待开始 |
| `rpc/` | 组件通信 | Netty RPC、Driver/Executor 心跳与消息协议 | ⬜ 待开始 |


## Spark SQL

SQL 层：Catalyst 优化器与全阶段代码生成。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| `parser/` | SQL 解析 | ANTLR 语法、Unresolved Logical Plan | ⬜ 待开始 |
| `catalyst/` | Catalyst 优化器 | Analyzer / Optimizer / 规则与策略、谓词下推、列裁剪 | ⬜ 待开始 |
| `planner/` | 物理计划 | Physical Plan、WholeStageCodegen、Tungsten | ⬜ 待开始 |
| `join/` | Join 算子 | Broadcast / SortMerge / ShuffledHash Join 的选择与代价 | ⬜ 待开始 |
| `aqe/` | 自适应执行 | AQE：动态合并分区、动态切换 Join 策略、倾斜处理 | ⬜ 待开始 |
| `datasource/` | 外部集成 | DataSource V2、Catalog、格式（Parquet/ORC/JSON） | ⬜ 待开始 |


## Spark Streaming

流处理：微批模型与结构化流。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| `structured-streaming/` | 结构化流 | Structured Streaming 的增量执行模型 | ⬜ 待开始 |
| `watermark-state/` | 水印与状态 | 事件时间、水印、有状态算子与状态存储 | ⬜ 待开始 |
| `exactly-once/` | Exactly-Once | source/sink 的端到端精确一次语义 | ⬜ 待开始 |
| `micro-batch/` | 微批与连续处理 | MicroBatch 与 Continuous Processing 的取舍 | ⬜ 待开始 |


## Spark MLlib

机器学习库（后续可选）。

| 目录 | 主题 | 关注点 | 状态 |
|---|---|---|---|
| `pipelines/` | ML Pipeline | Transformer / Estimator / Pipeline 抽象 | ⬜ 待开始 |
| `algorithms/` | 常用算法 | 分类、回归、聚类、协同过滤 | ⬜ 待开始 |

