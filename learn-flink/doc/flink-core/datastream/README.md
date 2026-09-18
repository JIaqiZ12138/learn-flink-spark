# DataStream API 算子与连接器（Flink 1.20.4）

> **对应引擎**：Apache Flink 1.20.4 　**源码基线**：仓库根目录 `flink-1.20-source/`
> **配套代码**：`learn-flink/src/main/java/com/jiaqiz/flink/`（每个算子都有可运行 demo）
> **配套图**：[`datastream-api-architecture.drawio`](../../assets/datastream-api-architecture.drawio) · [`function-operator-mapping.drawio`](../../assets/function-operator-mapping.drawio) · [`source-sink-architecture.drawio`](../../assets/source-sink-architecture.drawio)（源文件与 PNG 都在 `doc/assets/`）
> **原始清单**：[`todolist`](./todolist)

本主题回答一个问题：**你在 DataStream API 里调用的每一个算子，在 Flink 内部到底是什么、跑在哪里、状态放在哪。**

---

## 一、文档地图

| # | 文档 | 覆盖内容 | 对应 todolist |
|---|---|---|---|
| 0 | [`datastream-api-architecture.md`](./datastream-api-architecture.md) | **总体架构**：API 分层、Source/Operator/Sink 三大部分、函数族谱、算子链、类型推断、并行度与 KeyGroup | 1 |
| 1 | [`basic-functions.md`](./basic-functions.md) | `MapFunction` / `FlatMapFunction` / `FilterFunction` 与 `StreamMap` / `StreamFlatMap` / `StreamFilter` | 2 |
| 2 | [`process-functions.md`](./process-functions.md) | `ProcessFunction` / `KeyedProcessFunction` / `CoProcessFunction` / `ProcessWindowFunction` / `ProcessAllWindowFunction` / `ProcessJoinFunction` / 广播 Process 家族 + 定时器 + 侧输出 | 3 |
| 3 | [`window-functions.md`](./window-functions.md) | `WindowFunction` / `AllWindowFunction` / `ProcessWindowFunction` + `WindowAssigner` / `Trigger` / `Evictor` + `WindowOperator` | 4 |
| 4 | [`rich-functions.md`](./rich-functions.md) | `RichFunction` 家族、`AbstractRichFunction`、`RuntimeContext`、累加器与指标、状态访问 | 5 |
| 5 | [`async-io.md`](./async-io.md) | `AsyncFunction` / `RichAsyncFunction` / `AsyncDataStream` / `AsyncWaitOperator` | 6 |
| 6 | [`join-functions.md`](./join-functions.md) | `JoinFunction` / `FlatJoinFunction` / `CoGroupFunction` / `ProcessJoinFunction` + `JoinedStreams` / `CoGroupedStreams` / `IntervalJoin` | 7 |
| 7 | [`keyby-and-partitioners.md`](./keyby-and-partitioners.md) | `KeySelector` / `KeyedStream` / KeyGroup 机制 + `StreamPartitioner` 全家族（forward / rebalance / rescale / shuffle / broadcast / global / custom） | 8 |
| 8 | [`sources-and-sinks.md`](./sources-and-sinks.md) | FLIP-27 `Source`/`SplitEnumerator`/`SourceReader` 与旧 `SourceFunction`；Sink V2 `Sink`/`SinkWriter`/`Committer` 与旧 `SinkFunction`；`FileSink` / `print()` / datagen | source/sink |
| 9 | [`connectors.md`](./connectors.md) | JDBC / Kafka / HBase / MySQL 四类外部系统的 source·sink：依赖坐标、最小代码、可验证边界 | source/sink |

---

## 二、算子总表：从 API 到运行时

> 「底层算子」指真正执行 `processElement` 的类，它们分布在**两个包**里（这点很容易记错）：
> ① `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/` —— **UDF 类算子**：`StreamMap`、`StreamFlatMap`、`StreamFilter`、`ProcessOperator`、`KeyedProcessOperator`、`co/CoProcessOperator`、`co/CoBroadcastWith*Operator`、`co/IntervalJoinOperator`、`async/AsyncWaitOperator`、`SourceOperator` …
> ② `flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/` —— **基础与运行时算子**：`windowing/WindowOperator`、`windowing/EvictingWindowOperator`、`sink/SinkWriterOperator`、`sink/CommitterOperator`、`TimestampsAndWatermarksOperator` …
> 「demo」列是可运行代码的落点，运行方式统一为：
> `mvn -o -pl learn-flink exec:exec -Dmain.class=<全限定类名>`

### 2.1 基础算子（`operators/`）

| API | 函数接口 | 底层算子 | demo |
|---|---|---|---|
| `DataStream.map(...)` | `MapFunction` | `StreamMap` | `operators/BasicFunctionsDemo` |
| `DataStream.flatMap(...)` | `FlatMapFunction` | `StreamFlatMap` | `operators/BasicFunctionsDemo` |
| `DataStream.filter(...)` | `FilterFunction` | `StreamFilter` | `operators/BasicFunctionsDemo` |
| `DataStream` 的对象结构 | — | — | `operators/DataStreamStructureDemo` |

### 2.2 ProcessFunction 家族（`process/`）

| API | 函数接口 | 底层算子 | demo |
|---|---|---|---|
| `DataStream.process(...)` | `ProcessFunction` | `ProcessOperator` | `process/ProcessFunctionDemo` |
| `KeyedStream.process(...)` | `KeyedProcessFunction` | `KeyedProcessOperator` | `process/ProcessFunctionDemo` |
| `ConnectedStreams.process(...)` | `CoProcessFunction` / `KeyedCoProcessFunction` | `CoProcessOperator` / `KeyedCoProcessOperator` | `process/CoProcessFunctionDemo` |
| `BroadcastConnectedStream.process(...)` | `BroadcastProcessFunction` / `KeyedBroadcastProcessFunction` | 广播算子 | `process/BroadcastProcessFunctionDemo` |
| `WindowedStream.process(...)` | `ProcessWindowFunction` / `ProcessAllWindowFunction` | `WindowOperator` | `window/WindowFunctionsDemo` |

### 2.3 窗口（`window/`）

| API | 函数接口 | 底层算子 | demo |
|---|---|---|---|
| `.apply(WindowFunction)` / `.apply(AllWindowFunction)` | `WindowFunction` / `AllWindowFunction` | `WindowOperator` | `window/WindowFunctionsDemo` |
| `.process(ProcessWindowFunction)` | `ProcessWindowFunction` | `WindowOperator` | `window/WindowFunctionsDemo` |
| `.reduce(...)` / `.aggregate(...)` | `ReduceFunction` / `AggregateFunction` | `WindowOperator`（增量聚合） | `window/WindowAggregateDemo` |
| `.window(...)` / `.windowAll(...)` | `WindowAssigner` / `Trigger` / `Evictor` | `WindowOperator` / `EvictingWindowOperator` | `window/WindowAssignersDemo` |

### 2.4 富函数（`rich/`）

| API | 函数接口 | 说明 | demo |
|---|---|---|---|
| 任意算子的 `Rich*` 版本 | `RichFunction` → `AbstractRichFunction` | `open`/`close`/`RuntimeContext` | `rich/RichFunctionDemo` |

### 2.5 异步 I/O（`async/`）

| API | 函数接口 | 底层算子 | demo |
|---|---|---|---|
| `AsyncDataStream.orderedWait(...)` | `AsyncFunction` | `AsyncWaitOperator`（有序队列） | `async/AsyncFunctionDemo` |
| `AsyncDataStream.unorderedWait(...)` | `AsyncFunction` | `AsyncWaitOperator`（无序队列） | `async/AsyncFunctionDemo` |
| — | `RichAsyncFunction` | 同上 | `async/RichAsyncFunctionDemo` |

### 2.6 Join（`join/`）

| API | 函数接口 | 底层算子 | demo |
|---|---|---|---|
| `.join(...).apply(JoinFunction)` | `JoinFunction` / `FlatJoinFunction` | 窗口 + coGroup 路径 | `join/WindowJoinDemo` |
| `.coGroup(...).apply(CoGroupFunction)` | `CoGroupFunction` | `WindowOperator` | `join/CoGroupDemo` |
| `KeyedStream.intervalJoin(...).process(...)` | `ProcessJoinFunction` | `IntervalJoinOperator` | `join/IntervalJoinDemo` |

### 2.7 keyBy 与分区（`keyed/`、`partitioner/`）

| API | 关键接口 | 底层机制 | demo |
|---|---|---|---|
| `keyBy(...)` | `KeySelector` | `KeyGroupStreamPartitioner` + KeyGroup | `keyed/KeyedStreamDemo` |
| `rebalance()/rescale()/shuffle()/broadcast()/forward()/global()` | `StreamPartitioner` 各实现 | `ChannelSelector` 选 channel | `partitioner/PartitionerDemo` |
| `partitionCustom(...)` | `Partitioner` | `CustomPartitionerWrapper` | `partitioner/PartitionerDemo` |

### 2.8 Source / Sink（`source/`、`sink/`）

| API | 接口 | 底层算子 | demo |
|---|---|---|---|
| `env.fromSource(Source, ...)` | FLIP-27 `Source` / `SplitEnumerator` / `SourceReader` | `SourceOperator` | `source/CustomSourceDemo` |
| `env.addSource(SourceFunction)` | `SourceFunction` / `RichParallelSourceFunction` | 源算子（旧 API） | `source/LegacySourceFunctionDemo` |
| `.sinkTo(Sink)` | Sink V2 `Sink` / `SinkWriter` | Sink 算子 | `sink/CustomSinkDemo` |
| `.addSink(SinkFunction)` | `SinkFunction` / `RichSinkFunction` | `StreamSink` | `sink/LegacySinkFunctionDemo` |
| `.print()` | `PrintSinkFunction` | `StreamSink` | 各 demo 通用 |
| `FileSink`（`flink-connector-files`） | Sink V2（含两阶段提交） | 文件 Sink | `sink/CustomSinkDemo` |
| JDBC（自写 + 官方连接器） | `RichSinkFunction` / `RichSourceFunction` / `JdbcSink` | — | `source/JdbcSourceSinkDemo` |
| Kafka | `KafkaSource` / `KafkaSink` | — | `source/KafkaConnectorDemo`（仅编译） |

---

## 三、怎么读这套文档

1. **先看总体架构**：[`datastream-api-architecture.md`](./datastream-api-architecture.md) + 算子类层次架构图（§1.2）→ 先建立"抽象算子 → 具体实现"的层次感，再看 §1.1 的"API 对象 → Transformation → StreamGraph → 运行时算子"五层旅程；
2. **再按需读子文档**：每个子文档都是「类清单 → 源码实现 → 易错点 → demo 与真实输出」四段式；
3. **每个结论都能落到代码**：子文档里的 demo 都能用 `mvn -o -pl learn-flink exec:exec -Dmain.class=...` 跑起来，文档里的输出是**真实运行结果**（未实跑的会显式标注）。

> 📌 **输出里的可变数值**：文档引用的 demo 输出都是**真实运行结果**，但其中**耗时、墙钟时间、随机分区落点、多并行度下的输出交错顺序**每次运行都会不同。各子文档在对应小节都加了提示。凡涉及"确定性"的结论，本文档主题都用**多次运行比对**来验证（例如 `PartitionerDemo` 跑了 **4 遍**，并把 4 遍的映射全部列出）。

### 读源码引用时的两条约定

| 约定 | 说明 |
|---|---|
| **路径缩写** | 部分子文档用 `SJ/` 表示 `flink-streaming-java/src/main/java/org/apache/flink/streaming/`、`CORE/` 表示 `flink-core/src/main/java/org/apache/flink/api/common/functions`、`RT/` 表示 `flink-runtime/src/main/java/org/apache/flink/runtime/`。每个子文档头部都会重复声明它自己用的缩写 |
| ⚠️ **同名文件陷阱** | 仓库里存在**同名不同模块**的文件，只写文件名会有歧义。目前已确认的：`WindowOperator.java`、`WindowOperatorBuilder.java`（**流处理版**在 `SJ/runtime/operators/windowing/`，**Table SQL 版**在 `flink-table/flink-table-runtime/.../groupwindow/operator/`）。**本主题文档中提到这两个类时一律指流处理版**，行号也只对 `SJ/runtime/operators/windowing/` 下的文件有效 |

> 📌 本文档主题下所有 `SomeFile.java#Lnnn` 形式的行号都**已用脚本逐条核对**（文件存在 + 行号在文件长度内），共 349 处引用，0 处失效。

> ⚠️ **未实跑的部分会明确标注**：Kafka 需要 broker、HBase 需要集群、MySQL 需要实例，本机（无外网、Docker 未启动）无法启动这些外部系统。文档里对这些 demo 会写清"仅编译验证 / 需要什么条件才能跑"。核心算子（基础算子 / Process / 窗口 / 富函数 / Async / Join / Partitioner / Source·Sink 自写实现）**全部本地真跑**并附真实输出。

---

## 四、环境与运行方式

| 项 | 要求 |
|---|---|
| JDK | **17**（`maven.compiler.release=11`，源码可用 JDK 17 编译运行） |
| 运行单个 demo | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.<pkg>.<Demo>` |
| 日志 | 已默认 `WARN`（见 `learn-flink/src/main/resources/log4j2.properties`），需要看链路细节时临时改 `rootLogger.level` |
| 外部依赖 | Kafka 连接器 `3.3.0-1.20`、SQLite 驱动 `3.44.0.0` 已加入 pom；JDBC/ HBase 官方连接器需联网获取（见 [`connectors.md`](./connectors.md)） |
