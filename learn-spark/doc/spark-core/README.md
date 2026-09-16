# Spark Core（内核）

引擎本体：RDD 血缘、Stage 切分、Shuffle 与内存。

## 主题清单

| 主题 | 关注点 | 状态 |
|---|---|---|
| 任务提交 | spark-submit → SparkContext → Driver / Executor 的角色划分 | ⬜ 待开始 |
| RDD 与血缘 | RDD 抽象、窄/宽依赖、血缘重算容错 | ⬜ 待开始 |
| 作业调度 | DAGScheduler 切 Stage、TaskScheduler、推测执行 | ⬜ 待开始 |
| Shuffle 机制 | SortShuffleManager、ShuffleWriter/Reader、溢出与合并 | ⬜ 待开始 |
| 内存管理 | 统一内存模型、Execution/Storage 内存、GC 调优 | ⬜ 待开始 |
| 存储体系 | BlockManager、缓存与持久化级别、广播变量 | ⬜ 待开始 |
| 组件通信 | Netty RPC、Driver/Executor 心跳与消息协议 | ⬜ 待开始 |

> 每个主题一个子目录，笔记写进对应目录即可。
