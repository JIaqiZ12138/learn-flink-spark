# Flink CDC

变更数据捕获：把数据库的 binlog 变成 Flink 数据流。

## 主题清单

| 主题 | 关注点 | 状态 |
|---|---|---|
| 架构总览 | Source → Debezium/自研增量快照 → DataChangeEvent → Flink | ⬜ 待开始 |
| 增量快照算法 | 无锁快照、chunk 切分、并行快照 | ⬜ 待开始 |
| Source 实现 | FLIP-27 Source 接口、Enumerator / Reader 拆分 | ⬜ 待开始 |
| Schema 演进 | 表结构变更的捕获与下游兼容 | ⬜ 待开始 |
| 断点续传与容错 | offset 持久化、重启恢复、Exactly-Once 语义 | ⬜ 待开始 |
| 整库同步 | Pipeline Connector：源库到目标库的整库/整表同步 | ⬜ 待开始 |

> 每个主题一个子目录，笔记写进对应目录即可。
