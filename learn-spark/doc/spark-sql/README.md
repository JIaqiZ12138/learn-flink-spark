# Spark SQL

SQL 层：Catalyst 优化器与全阶段代码生成。

## 主题清单

| 主题 | 关注点 | 状态 |
|---|---|---|
| SQL 解析 | ANTLR 语法、Unresolved Logical Plan | ⬜ 待开始 |
| Catalyst 优化器 | Analyzer / Optimizer / 规则与策略、谓词下推、列裁剪 | ⬜ 待开始 |
| 物理计划 | Physical Plan、WholeStageCodegen、Tungsten | ⬜ 待开始 |
| Join 算子 | Broadcast / SortMerge / ShuffledHash Join 的选择与代价 | ⬜ 待开始 |
| 自适应执行 | AQE：动态合并分区、动态切换 Join 策略、倾斜处理 | ⬜ 待开始 |
| 外部集成 | DataSource V2、Catalog、格式（Parquet/ORC/JSON） | ⬜ 待开始 |

> 每个主题一个子目录，笔记写进对应目录即可。
