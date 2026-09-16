# Flink Core（内核）

引擎本体：一条记录从提交到执行的完整生命周期。

## 主题清单

| 主题 | 关注点 | 状态 |
|---|---|---|
| 任务提交 | CLI / REST / Application 三种入口 → JobGraph → ExecutionGraph | ✅ 已完成 |
| 作业调度 | 调度策略、Slot 分配、ResourceManager、failover | ⬜ 待开始 |
| 内存管理 | MemoryManager、NetworkBufferPool、托管内存模型 | ⬜ 待开始 |
| 组件通信 | RPC 框架（Pekko）、Akka 到 Pekko 的迁移、组件间协议 | ⬜ 待开始 |
| 处理函数 | StreamOperator、OperatorChain、UDF 生命周期 | ⬜ 待开始 |
| 窗口和水位线 | WindowAssigner、Trigger、Evictor、Watermark 传播 | ⬜ 待开始 |
| 状态与容错 | KeyedState / OperatorState、StateBackend、Checkpoint、Savepoint | ⬜ 待开始 |
| FlinkSQL | Table API 与 DataStream 的衔接、Planner 装载、运行时算子 | ⬜ 待开始 |

> 每个主题一个子目录，笔记写进对应目录即可。
