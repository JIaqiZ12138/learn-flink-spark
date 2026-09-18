# Flink Core（内核）

> **对应引擎**：Apache Flink 1.20.4 　**源码基线**：仓库根目录 `flink-1.20-source/`

引擎本体：**一条记录从提交到执行、再到容错的完整生命周期**。

`doc/flink-core/` 是 `learn-flink/doc/` 下的第一个主题域，对应 Flink 源码里**除 SQL / CDC / Agent 之外的全部引擎代码**。

---

## 一、Flink Core 的组成

"内核"不是一个 Maven 模块，而是下面这组模块的集合。它们的依赖方向是单向的：上层可以依赖下层，反之不行。

### 1.1 模块清单

| 模块 | 一句话职责 | 关键入口类 |
|---|---|---|
| `flink-annotations/` | API 稳定性注解：定义"哪些类是对用户的承诺" | <a href="../../../flink-1.20-source/flink-annotations/src/main/java/org/apache/flink/annotation/Public.java#L38" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">Public</code></a> |
| `flink-core-api/` | 三大 API 共享的接口层：函数、状态、类型 | <a href="../../../flink-1.20-source/flink-core-api/src/main/java/org/apache/flink/api/common/functions/Function.java#L30" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">Function</code></a>、<a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/api/common/state/KeyedStateStore.java#L25" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">KeyedStateStore</code></a> |
| `flink-core/` | 核心实现：类型系统、序列化、内存段、配置项、`JobGraph` | <a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/api/common/typeutils/TypeSerializer.java#L59" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">TypeSerializer</code></a>、<a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java#L70" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">MemorySegment</code></a>、<a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/api/common/ExecutionConfig.java#L81" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">ExecutionConfig</code></a> |
| `flink-runtime/` | 分布式运行时：JobManager、TaskManager、调度、网络栈、状态与 Checkpoint | <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/ExecutionGraph.java#L89" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">ExecutionGraph</code></a>（**接口**，实现见 `DefaultExecutionGraph`）、<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/SchedulerBase.java#L141" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">SchedulerBase</code></a>、<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointCoordinator.java#L102" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">CheckpointCoordinator</code></a> |
| `flink-streaming-java/` | DataStream API 与流处理算子：`StreamGraph`、算子链、`StreamTask` | <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L130" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">DataStream</code></a>、<a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java#L199" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StreamTask</code></a>、<a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/tasks/OperatorChain.java#L109" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">OperatorChain</code></a> |
| `flink-rpc/` | 组件间 RPC：接口层 + Pekko 实现 | <a href="../../../flink-1.20-source/flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcService.java#L34" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">RpcService</code></a>、<a href="../../../flink-1.20-source/flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcEndpoint.java#L95" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">RpcEndpoint</code></a>、<a href="../../../flink-1.20-source/flink-rpc/flink-rpc-akka/src/main/java/org/apache/flink/runtime/rpc/pekko/PekkoRpcService.java#L87" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">PekkoRpcService</code></a> |
| `flink-clients/` | CLI 与客户端侧提交链路 | <a href="../../../flink-1.20-source/flink-clients/src/main/java/org/apache/flink/client/cli/CliFrontend.java#L92" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">CliFrontend</code></a> |
| `flink-state-backends/` | 状态后端实现：RocksDB / heap / ForSt | <a href="../../../flink-1.20-source/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/contrib/streaming/state/EmbeddedRocksDBStateBackend.java#L97" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">EmbeddedRocksDBStateBackend</code></a>、<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/HeapKeyedStateBackend.java#L80" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">HeapKeyedStateBackend</code></a> |
| `flink-metrics/` | 指标采集与上报（JMX / Prometheus / …） | `flink-metrics/flink-metrics-core/`（待补充笔记） |

### 1.2 一条记录穿过的五层

```text
① API 层          flink-streaming-java     DataStream → StreamGraph
        ↓
② 客户端层        flink-clients            CliFrontend → JobGraph
        ↓
③ 运行时编排层    flink-runtime            Dispatcher / ResourceManager / JobMaster → ExecutionGraph
        ↓
④ Task 执行层     flink-streaming-java     StreamTask + OperatorChain
        ↓
⑤ 状态与容错层    flink-runtime + flink-state-backends    StateBackend / Checkpoint
```

还有两样东西**横穿**③④⑤，所以它们不属于某一层：

- **RPC**（`flink-rpc/`）：<a href="../../../flink-1.20-source/flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcEndpoint.java#L95" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">RpcEndpoint</code></a> 是所有分布式组件的基类，<a href="../../../flink-1.20-source/flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcEndpoint.java#L229" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">onStart()</code></a> 是每个组件的启动钩子
- **内存与网络缓冲**（`flink-core/` + `flink-runtime/`）：<a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java#L70" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">MemorySegment</code></a> 是最小分配单位，网络栈在它之上做缓冲池

---

## 二、主题地图

| 主题目录 | 关注点 | 状态 |
|---|---|---|
| [`job-submit/`](job-submit/) | 任务提交：CLI / REST / Application 入口 → `JobGraph` → `ExecutionGraph` | ✅ 已完成 |
| [`scheduling/`](scheduling/) | 作业调度：调度策略、Slot 分配、ResourceManager、failover | ✅ 已完成 |
| [`memory/`](memory/) | 内存管理：MemoryManager、NetworkBufferPool、托管内存模型 | ⬜ 待开始 |
| [`rpc/`](rpc/) | 组件通信：RPC 框架（Pekko）、Akka 到 Pekko 的迁移、组件间协议 | ⬜ 待开始 |
| [`datastream/`](datastream/) | DataStream API 算子：Source / Operator / Sink 三层算子、函数族谱、算子链、类型推断、分区器、连接器 | ✅ 已完成 |
| [`window-watermark/`](window-watermark/) | 窗口和水位线：WindowAssigner、Trigger、Evictor、Watermark 传播 | ⬜ 待开始 |
| [`state-fault-tolerance/`](state-fault-tolerance/) | 状态与容错：KeyedState / OperatorState、StateBackend、Checkpoint、Savepoint | ⬜ 待开始 |

> FlinkSQL 不在本目录下，它有自己的主题域：[`flink-sql/`](../flink-sql/)。

### 2.1 `job-submit/` — 任务提交 ✅

**关注点**：`bin/flink run` 敲下去之后，到第一个 Task 在 TaskManager 上跑起来，中间发生了什么。

**源码入口**：<a href="../../../flink-1.20-source/flink-clients/src/main/java/org/apache/flink/client/cli/CliFrontend.java#L92" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">CliFrontend</code></a> → <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/graph/StreamGraphGenerator.java#L310" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StreamGraphGenerator.generate()</code></a> → <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/graph/StreamingJobGraphGenerator.java#L139" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StreamingJobGraphGenerator.createJobGraph()</code></a> → <a href="../../../flink-1.20-source/flink-yarn/src/main/java/org/apache/flink/yarn/YarnClusterDescriptor.java#L894" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">YarnClusterDescriptor.startAppMaster()</code></a> → <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/entrypoint/ClusterEntrypoint.java#L836" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">ClusterEntrypoint.runClusterEntrypoint()</code></a> → <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMaster.java#L170" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">JobMaster</code></a>

**已完成的文档**：

| 文档 | 讲什么 |
|---|---|
| [`job-submission-yarn-per-job.md`](job-submit/job-submission-yarn-per-job.md) | 主线：提交流程的**界限**（从哪到哪、涉及几个 JVM）→ **核心源码逐段解析**（客户端 / YARN / AM / Dispatcher / JobMaster / TaskManager）→ **总结**。含 201 个可点击源码链接与 51 段真实抽取的源码 |
| [`three-graphs-wordcount.md`](job-submit/three-graphs-wordcount.md) | 用最小 WordCount 对照 `StreamGraph` / `JobGraph` / `ExecutionGraph` 三张图的差异，附真实运行输出 |
| [`job-submission-all-modes.md`](job-submit/job-submission-all-modes.md) | 早期笔记：五条提交入口、session 模式的 REST 提交链路、`JobSubmitHandler` 分支 |
| [`job-submission-flow.drawio`](../assets/job-submission-flow.drawio) · [`.png`](../assets/job-submission-flow.png) | 全流程架构图（drawio 源文件 + 导出图片） |
| [`three-graphs-comparison.drawio`](../assets/three-graphs-comparison.drawio) · [`.png`](../assets/three-graphs-comparison.png) | 三张图并排对照图 |

### 2.2 `scheduling/` — 作业调度 ✅

**关注点**：`ExecutionGraph` 建好之后，谁决定"哪个 `ExecutionVertex` 在什么时候、被部署到哪个 slot 上"。

**源码入口**：<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/SchedulerBase.java#L141" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">SchedulerBase</code></a>（调度器公共骨架）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/DefaultScheduler.java#L88" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">DefaultScheduler</code></a>（默认实现，**继承 SchedulerBase**）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/strategy/SchedulingStrategy.java#L32" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">SchedulingStrategy</code></a>（策略接口）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/strategy/PipelinedRegionSchedulingStrategy.java#L52" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">PipelinedRegionSchedulingStrategy</code></a>（按 region 调度）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/slotmanager/SlotManager.java#L47" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">SlotManager</code></a>（slot 供需撮合）

**关键动作**：<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/DefaultScheduler.java#L503" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">allocateSlotsAndDeploy()</code></a> —— 申请 slot 并下发 Task

> ⚠️ 两个易错点：
> ① <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/adaptive/AdaptiveScheduler.java#L172" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">AdaptiveScheduler</code></a> **不继承** `SchedulerBase`，它直接实现 `SchedulerNG` 并自带一套状态机；真正复用骨架的是批模式的 `AdaptiveBatchScheduler`。
> ② `SlotPool` **不在调度器里**，由 JobMaster 的 `SlotPoolService` 持有；调度器只拿到 <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/ExecutionSlotAllocator.java#L28" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">ExecutionSlotAllocator</code></a> → `PhysicalSlotProviderImpl` → `SlotPool`。

**想弄清楚的问题**：slot 共享组怎么影响调度？failover 时是重启整个 region 还是单个 vertex？自适应调度器在什么条件下触发 rescale？

**已完成的文档**：

| 文档 | 讲什么 |
|---|---|
| [`job-scheduling-and-execution.md`](scheduling/job-scheduling-and-execution.md) | 主线：调度与执行的**界限**（起点 `JobMaster.startScheduling()`、终点用户代码执行、闭环是重调度）→ **12 节源码解析**（`SchedulerBase` / `DefaultScheduler` / `PipelinedRegionSchedulingStrategy` / `DefaultExecutionDeployer` / `Execution` 状态机 / 逻辑 slot 与物理 slot / `DeclarativeSlotPoolBridge` / `FineGrainedSlotManager` / `ActiveResourceManager` + `YarnResourceManagerDriver` / `TaskExecutor` / `Task.run()` / failover / 自适应调度）→ **总结**。含 118 个可点击源码链接 |
| [`job-scheduling-flow.drawio`](../assets/job-scheduling-flow.drawio) · [`.png`](../assets/job-scheduling-flow.png) | 总览架构图：JobMaster 调度栈 / ResourceManager / TaskManager 三栏 + 失败重调度回路 |
| [`slot-allocation-deploy-flow.drawio`](../assets/slot-allocation-deploy-flow.drawio) · [`.png`](../assets/slot-allocation-deploy-flow.png) | 时序图：一次 `allocateSlotsAndDeploy()` 的完整往返（JM 声明需求 → RM 撮合 → TM 推回槽位 → 绑定 → `submitTask`） |

### 2.3 `memory/` — 内存管理 ⬜

**关注点**：TaskManager 的堆内/堆外内存怎么切分，算子拿到的 `MemorySegment` 从哪来，网络缓冲区怎么在算子之间流转。

**源码入口**：<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/memory/MemoryManager.java#L60" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">MemoryManager</code></a>（托管内存分配）→ <a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java#L70" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">MemorySegment</code></a>（最小分配单位，替代 `byte[]`）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/NetworkBufferPool.java#L63" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">NetworkBufferPool</code></a>（TM 级缓冲池）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LocalBufferPool.java#L71" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">LocalBufferPool</code></a>（算子级子池）

**关键动作**：<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/memory/MemoryManager.java#L193" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">MemoryManager.allocatePages()</code></a>、<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/NetworkBufferPool.java#L453" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">NetworkBufferPool.createBufferPool()</code></a>

**配置侧**：<a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/configuration/TaskManagerOptions.java#L42" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">TaskManagerOptions</code></a>、<a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/configuration/JobManagerOptions.java#L41" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">JobManagerOptions</code></a>

### 2.4 `rpc/` — 组件通信 ⬜

**关注点**：Dispatcher、ResourceManager、JobMaster、TaskExecutor 分处不同 JVM / 线程，它们之间怎么发消息、怎么保证"每个组件单线程"。

**源码入口**：<a href="../../../flink-1.20-source/flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcService.java#L34" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">RpcService</code></a>（服务）→ <a href="../../../flink-1.20-source/flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcEndpoint.java#L95" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">RpcEndpoint</code></a>（端点基类）→ <a href="../../../flink-1.20-source/flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcSystem.java#L35" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">RpcSystem</code></a>（加载实现）→ <a href="../../../flink-1.20-source/flink-rpc/flink-rpc-akka/src/main/java/org/apache/flink/runtime/rpc/pekko/PekkoRpcService.java#L87" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">PekkoRpcService</code></a> / <a href="../../../flink-1.20-source/flink-rpc/flink-rpc-akka/src/main/java/org/apache/flink/runtime/rpc/pekko/PekkoRpcActor.java#L86" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">PekkoRpcActor</code></a>（Pekko 实现）

**想弄清楚的问题**：Akka 到 Pekko 的迁移是怎么做到对上层透明的？`mainThreadExecutor` 为什么能保证组件不被并发调用？

### 2.5 `datastream/` — DataStream API 算子 ✅

**关注点**：你在 DataStream API 里调用的每一个算子（`map` / `filter` / `process` / `window` / `join` / `keyBy` / 各种 Sink），在 Flink 内部**到底是什么类、跑在哪、状态放在哪**；以及 Source / Operator / Sink 三大部分各自的抽象与生命周期。

**文档结构**：1 篇总体架构（[`datastream-api-architecture.md`](datastream/datastream-api-architecture.md)）+ 9 篇按算子大类拆分的子文档 + 3 张架构图 + **22 个可运行 demo**（大部分本地 MiniCluster 实跑验证）。

**源码入口**：<a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L130" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">DataStream</code></a>（API 层句柄）→ <a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/api/dag/Transformation.java#L111" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">Transformation</code></a>（逻辑图元数据）→ <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/graph/StreamGraphGenerator.java#L310" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StreamGraphGenerator.generate()</code></a> → <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/graph/StreamingJobGraphGenerator.java#L245" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StreamingJobGraphGenerator.createJobGraph()</code></a>（算子链）→ <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/StreamOperator.java#L47" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StreamOperator</code></a>（运行时算子）→ <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/AbstractUdfStreamOperator.java#L50" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">AbstractUdfStreamOperator</code></a>（UDF 包装）→ <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java#L637" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StreamTask.processInput()</code></a>（驱动主循环）

**一个必须记住的坑**：运行时算子在**两个包**里 ——
① `streaming/api/operators/`：UDF 类算子（`StreamMap` / `StreamFlatMap` / `StreamFilter` / `ProcessOperator` / `KeyedProcessOperator` / `co/*` / `async/AsyncWaitOperator` / `SourceOperator`）
② `streaming/runtime/operators/`：基础与运行时算子（`windowing/WindowOperator` / `windowing/EvictingWindowOperator` / `sink/SinkWriterOperator` / `sink/CommitterOperator` / `TimestampsAndWatermarksOperator`）

**想弄清楚的问题**：为什么 `map → filter → print` 只生成 1 个 `JobVertex`？lambda 里返回 `Tuple2` 为什么必须写 `.returns(...)`？`keyBy` 之后 `JobVertex` 为什么变多？`key → subtask` 到底怎么算的？

### 2.6 `window-watermark/` — 窗口与水位线 ⬜

**关注点**：时间语义怎么定、窗口什么时候触发、迟到数据怎么办、水位线怎么在链路里传播。

**源码入口**：<a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/api/common/eventtime/WatermarkStrategy.java#L56" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WatermarkStrategy</code></a>（策略）→ <a href="../../../flink-1.20-source/flink-core/src/main/java/org/apache/flink/api/common/eventtime/WatermarkGenerator.java#L32" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WatermarkGenerator</code></a>（生成器）→ <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/TimestampsAndWatermarksOperator.java#L51" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">TimestampsAndWatermarksOperator</code></a>（运行时算子）→ <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/assigners/WindowAssigner.java#L44" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowAssigner</code></a> / <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/Trigger.java#L56" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">Trigger</code></a> / <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/evictors/Evictor.java#L44" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">Evictor</code></a>（三件套）→ <a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperator.java#L101" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowOperator</code></a>（窗口算子本体）

**关键动作**：<a href="../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperator.java#L281" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowOperator.processElement()</code></a>

**想弄清楚的问题**：水位线是"插入流中的特殊记录"还是"带外信号"？多输入算子怎么取所有输入的最小值？

### 2.7 `state-fault-tolerance/` — 状态与容错 ⬜

**关注点**：KeyedState / OperatorState 存在哪、怎么读写，Checkpoint 怎么在不停机的前提下拍出一致快照，Savepoint 又有什么不同。

**源码入口**：<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/state/StateBackend.java#L81" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">StateBackend</code></a>（后端抽象）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/state/KeyedStateBackend.java#L36" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">KeyedStateBackend</code></a>（键控状态）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/HeapKeyedStateBackend.java#L80" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">HeapKeyedStateBackend</code></a> / <a href="../../../flink-1.20-source/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/contrib/streaming/state/EmbeddedRocksDBStateBackend.java#L97" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">EmbeddedRocksDBStateBackend</code></a>（两种实现）

**容错链路**：<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointCoordinator.java#L102" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">CheckpointCoordinator</code></a> → <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointCoordinator.java#L572" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">triggerCheckpoint()</code></a> → <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/api/CheckpointBarrier.java#L45" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">CheckpointBarrier</code></a>（随数据流动的屏障）→ <a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/state/SnapshotStrategy.java#L41" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">SnapshotStrategy</code></a>（快照策略）

**关键动作**：<a href="../../../flink-1.20-source/flink-runtime/src/main/java/org/apache/flink/runtime/state/KeyedStateBackend.java#L108" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">getOrCreateKeyedState()</code></a>

**想弄清楚的问题**：对齐（aligned）和非对齐（unaligned）检查点的差别？barrier 是怎么"追上"上游数据的？RocksDB 增量检查点为什么能只传增量？

---

## 三、目录索引

### 3.1 `doc/flink-core/` 下的目录

| 目录 | 主题 | 状态 | 备注 |
|---|---|---|---|
| [`job-submit/`](job-submit/) | 任务提交 | ✅ 已完成 | 含 3 篇文档 + 2 张图 |
| [`scheduling/`](scheduling/) | 作业调度 | ✅ 已完成 | 1 篇正文 + 2 张图 |
| [`memory/`](memory/) | 内存管理 | ⬜ 待开始 | 同上 |
| [`rpc/`](rpc/) | 组件通信 | ⬜ 待开始 | 同上 |
| [`datastream/`](datastream/) | DataStream API 算子 | ✅ 已完成 | 1 篇总览 + 9 篇子文档 + 3 张图 + 22 个可运行 demo |
| [`window-watermark/`](window-watermark/) | 窗口与水位线 | ⬜ 待开始 | 同上 |
| [`state-fault-tolerance/`](state-fault-tolerance/) | 状态与容错 | ⬜ 待开始 | 同上 |
| [`../assets/`](../assets/) | 图与 drawio 源文件 | — | **所有主题共用**：图不放文档目录，统一归档到这里 |

### 3.2 `job-submit/` 下的每个文件

| 文件 | 类型 | 作用 |
|---|---|---|
| [`job-submission-yarn-per-job.md`](job-submit/job-submission-yarn-per-job.md) | 正文 · 148 KB | **主线文档**。严格分三部分：① 提交流程的界限（从哪一步到哪一步、三个 JVM 边界）② 核心源码解析（客户端 → YARN → AM → Dispatcher/ResourceManager → JobMaster → TaskManager，共 10 节）③ 总结。文中蓝色类名/方法名可点击跳源码 |
| [`three-graphs-wordcount.md`](job-submit/three-graphs-wordcount.md) | 正文 · 24 KB | 以一个最小 WordCount 为例，对照 `StreamGraph` / `JobGraph` / `ExecutionGraph`：各在哪生成、由谁持有、节点粒度差多少。附 MiniCluster 真实运行输出与打印 API |
| [`job-submission-all-modes.md`](job-submit/job-submission-all-modes.md) | 早期笔记 · 58 KB | 主线文档的前身。覆盖五条提交入口、"全部模式"对照、session 模式走 REST 的链路（`WebMonitorEndpoint` → `JobSubmitHandler`）。**与主线文档的分工**：这里看"所有模式横向对比"，主线文档看"YARN per-job 纵向深挖" |
| [`job-submission-flow.drawio`](../assets/job-submission-flow.drawio) | 图 · 源文件 | 全流程架构图。风格约定：**曲边方框**=对象/组件，**方框**=方法调用 |
| [`job-submission-flow.png`](../assets/job-submission-flow.png) | 图 · 导出 | 上面那张图的 PNG 导出，供文档内嵌和快速预览 |
| [`three-graphs-comparison.drawio`](../assets/three-graphs-comparison.drawio) | 图 · 源文件 | StreamGraph → JobGraph → ExecutionGraph 并排对照 |
| [`three-graphs-comparison.png`](../assets/three-graphs-comparison.png) | 图 · 导出 | 上面那张图的 PNG 导出 |
| [`README.md`](README.md) | 导航 | 本文件 |

### 3.3 `scheduling/` 下的每个文件

| 文件 | 类型 | 作用 |
|---|---|---|
| [`job-scheduling-and-execution.md`](scheduling/job-scheduling-and-execution.md) | 正文 · 174 KB | 调度与执行主线：① 界限（起点 / 终点 / 闭环 / 三层结构 + 版本号机制）② 12 节源码解析（`SchedulerNG`/`SchedulerBase`/`DefaultScheduler` → `PipelinedRegionSchedulingStrategy` → `DefaultExecutionDeployer` → `Execution` 状态机 → 逻辑 slot 与物理 slot → `SlotPool`/`DeclarativeSlotPoolBridge` → `FineGrainedSlotManager` → `ActiveResourceManager`/`YarnResourceManagerDriver` → `TaskExecutor` → `Task.run()` → failover → 自适应调度）③ 总结 |
| [`job-scheduling-flow.drawio`](../assets/job-scheduling-flow.drawio) · [`.png`](../assets/job-scheduling-flow.png) | 图 · 源文件 + 导出 | 调度与执行总览架构图：**① JobMaster 调度栈 / ② ResourceManager / ③ TaskManager** 三栏 + **④ 失败与重调度回路**，底部附图例 |
| [`slot-allocation-deploy-flow.drawio`](../assets/slot-allocation-deploy-flow.drawio) · [`.png`](../assets/slot-allocation-deploy-flow.png) | 图 · 源文件 + 导出 | Slot 申请与 Task 部署时序图：5 条泳道（JobMaster 主线程 / SlotPool / ResourceManager / YARN / TaskManager）× 16 步，标出三套 RPC 的方向、两次 all-or-nothing、两处版本校验 |

### 3.4 `datastream/` 下的每个文件

| 文件 | 类型 | 作用 |
|---|---|---|
| [`README.md`](datastream/README.md) | 导航 | 文档地图 + **算子总表**（API → 函数接口 → 底层算子 → demo 四列对照），共 8 张映射表 |
| [`datastream-api-architecture.md`](datastream/datastream-api-architecture.md) | 正文 · 总览 | 五层架构（API 层 → Transformation DAG → StreamGraph → JobGraph → 运行时算子）+ Source/Operator/Sink 三大支柱 + 全算子映射表 + 算子链 / 类型推断 / 并行度与 KeyGroup 三大机制 |
| [`basic-functions.md`](datastream/basic-functions.md) | 正文 · 子文档 | `MapFunction` / `FlatMapFunction` / `FilterFunction` 的契约与 `StreamMap` / `StreamFlatMap` / `StreamFilter` 实现；`TimestampedCollector` 单一 `reuse` 陷阱；类型推断两条失败路径 |
| [`process-functions.md`](datastream/process-functions.md) | 正文 · 子文档 | `ProcessFunction` 家族四条支线；定时器机制（`InternalTimerServiceImpl` 按键组优先队列 + watermark 推进触发）；侧输出；广播状态 |
| [`window-functions.md`](datastream/window-functions.md) | 正文 · 子文档 | `WindowFunction` / `AllWindowFunction` / `ProcessWindowFunction`；`WindowAssigner` / `Trigger` / `Evictor`；`WindowOperator` vs `EvictingWindowOperator`；增量聚合 |
| [`rich-functions.md`](datastream/rich-functions.md) | 正文 · 子文档 | `RichFunction` 家族、`AbstractRichFunction`、生命周期时序（`initializeState` 早于 `open`）、`RuntimeContext`、累加器与指标 |
| [`async-io.md`](datastream/async-io.md) | 正文 · 子文档 | `AsyncFunction` / `RichAsyncFunction`、`AsyncWaitOperator` 两种队列、`capacity` 与 timeout、checkpoint 重放语义（at-least-once） |
| [`join-functions.md`](datastream/join-functions.md) | 正文 · 子文档 | `JoinFunction` / `FlatJoinFunction` / `CoGroupFunction` / `ProcessJoinFunction`；`JoinedStreams` 被改写成 `CoGroupedStreams`；`IntervalJoinOperator` 双 MapState + 事件时间 |
| [`keyby-and-partitioners.md`](datastream/keyby-and-partitioners.md) | 正文 · 子文档 | `key → keyGroup → subtask` 三跳公式与实测；八个 `StreamPartitioner` 的分布对照（跑两遍区分确定性/随机性） |
| [`sources-and-sinks.md`](datastream/sources-and-sinks.md) | 正文 · 子文档 | FLIP-27 `Source`/`SplitEnumerator`/`SourceReader` 三段式协议 vs 旧 `SourceFunction`；Sink V2 `Sink`/`SinkWriter`/`Committer` 两阶段提交 vs 旧 `SinkFunction` |
| [`connectors.md`](datastream/connectors.md) | 正文 · 子文档 | JDBC / Kafka / HBase / MySQL 四类连接器：依赖坐标、完整代码、**可验证边界**（HBase 在 1.20 无官方连接器） |
| [`datastream-api-architecture.drawio`](../assets/datastream-api-architecture.drawio) · [`.png`](../assets/datastream-api-architecture.png) | 图 · 源文件 + 导出 | DataStream API **算子类层次图**：`StreamOperator` 接口 → `AbstractStreamOperator` → `AbstractUdfStreamOperator` → 具体算子（继承/实现关系，2880×1282） |
| [`function-operator-mapping.drawio`](../assets/function-operator-mapping.drawio) · [`.png`](../assets/function-operator-mapping.png) | 图 · 源文件 + 导出 | 函数接口 → 底层算子映射图（3987×2392） |
| [`source-sink-architecture.drawio`](../assets/source-sink-architecture.drawio) · [`.png`](../assets/source-sink-architecture.png) | 图 · 源文件 + 导出 | Source / Sink 抽象与生命周期对照图（3810×1723） |

**配套代码**：`learn-flink/src/main/java/com/jiaqiz/flink/` 下的 `operators/`、`process/`、`window/`、`rich/`、`async/`、`join/`、`keyed/`、`partitioner/`、`source/`、`sink/` 共 22 个 demo，运行方式统一为
`mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.<pkg>.<Demo>`。

> **图的维护方式**：图统一放在 `doc/assets/`，与文档分离。改完 `.drawio` 后重新导出同名 `.png`，文档里的图片会自动更新。
> 导出命令：`drawio --export --format png --scale 2 --border 10 --output x.png x.drawio`
>
> ⚠️ **导出注意两点**：
> ① **单边像素必须 < 4096**。部分 Markdown 渲染器（Chromium/Electron 系）对超过 4096 px 的图片**不显示**，表现为"图片加载不出来"。老图（3883 / 3635 px）能显示、新图（4875 / 4955 px，`--scale 2`）不能，就是这个原因。大图请用 `--scale 1.5`（本目录两张新图即 3660×2130、3720×2509）。
> ② 若本机已开着 draw.io 桌面版，直接调 CLI 会连到那个实例并**挂住**，需用隔离的用户目录：
> `drawio --user-data-dir=/tmp/drawio-cli-profile --no-sandbox --disable-gpu --export --format png --scale 1.5 --border 10 --output x.png x.drawio`
>
> **自检命令**（确认导出结果没超限）：`sips -g pixelWidth -g pixelHeight x.png`

---

## 四、怎么读

### 4.1 推荐顺序

1. 先看 **全流程架构图**（`job-submission-flow.png`），建立"客户端 → YARN → AM → JobMaster → TaskManager"的空间感
2. 再读 **`job-submission-yarn-per-job.md` 的第一部分**，把流程的"界限"钉死：从哪一步开始、到哪一步结束、跨了几个 JVM
3. 然后按 2.1 → 2.10 **逐节读源码解析**，每节都配了真实源码，遇到蓝色链接就点进 `flink-1.20-source/` 看上下文
4. 读累了看 **`three-graphs-wordcount.md`**，用最小的例子把三张图的差异一次性对齐
5. 最后跑一遍 `learn-flink/src/main/java/com/jiaqiz/flink/sample/GraphComparisonJob.java`，亲手把三张图打印出来
6. 进入调度主题：先看 **`scheduling/job-scheduling-flow.png`**（三栏总览），再读 **`job-scheduling-and-execution.md`**，配 **`slot-allocation-deploy-flow.png`** 理解"一次 `allocateSlotsAndDeploy()` 到底走了几趟 RPC"
7. 进入算子主题：先看 **`datastream/datastream-api-architecture.png`**（算子类层次：`StreamOperator` → 具体算子），再读 **`datastream/datastream-api-architecture.md`** 建立"API 对象 → Transformation → StreamGraph → 运行时算子"的空间感，最后按算子大类挑子文档；每个算子都跑一遍 `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.<pkg>.<Demo>` 对照真实输出

### 4.2 环境要求

| 项 | 要求 | 说明 |
|---|---|---|
| JDK | **17** | Flink 1.20 官方支持 8 / 11 / 17；IDEA 里需确认 Project SDK 与 Language level 都是 17 |
| Maven | **3.8.6** | Flink 的 enforcer **精确要求 3.8.6**，用系统更新的 Maven 会被拒绝。构建 `flink-1.20-source` 请用其自带的 `./mvnw` |
| 源码 | 仓库根目录 `flink-1.20-source/` | 已带本地中文注释，蓝色链接直接指向这里 |

### 4.3 写新笔记时的约定

| 约定 | 做法 |
|---|---|
| 源码链接 | 统一的**蓝色可点击**样式，类名/方法名都能点，指向 `flink-1.20-source/` 的具体行 |
| 代码块 | 从源码**实际抽取**，不手写伪代码 |
| 文件名 | 用中文，与同目录的图保持同名对应 |
| 图 | 源文件与导出都放 `doc/assets/`，不放文档目录；正文以图片形式内嵌 |

> 上层索引见 [`doc/README.md`](../README.md)；仓库总览见 [根 `README.md`](../../../README.md)。

