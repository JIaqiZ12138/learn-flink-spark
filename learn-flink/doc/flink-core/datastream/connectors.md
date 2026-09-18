# 外部连接器：JDBC / Kafka / HBase / MySQL

> **代码基线**：Apache Flink 1.20.4　**运行环境**：JDK 17 + 本机 Maven 缓存（**无外网、无 Docker**）
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/source/`、`.../sink/`
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)

---

# 一、先看结论表：每个连接器"能验证到什么程度"

| 连接器 | 依赖坐标 | 本机是否缓存 | 1.20 是否可用 | 编译验证 | 本地实跑 | 本文档标注 |
|---|---|---|---|---|---|---|
| **Kafka** | `org.apache.flink:flink-connector-kafka:3.3.0-1.20` | ✅ 已缓存 | ✅（可升 `3.4.0-1.20`） | ✅ 已实测通过 | ❌ 需 broker（本机无 Docker） | 编译已验证 / **运行未验证** |
| **JDBC** | `org.apache.flink:flink-connector-jdbc:3.3.0-1.20` | ❌ 只有 `3.1.1-1.17` | ✅（1.20 官方版本就是 3.3.0-1.20） | 未实测（本机无 jar） | — | **未验证** |
| ↑ JDBC 替代路线 | 原生 JDBC + `org.xerial:sqlite-jdbc:3.44.0.0` | ✅ | ✅ | ✅ | ✅ **已真跑通** | 实跑已验证 |
| **HBase** | `flink-connector-hbase-2.2:4.0.0-1.19`（**1.20 无官方版**） | ❌ 完全没有 | ⚠️ 跨版本（该版基于 Flink 1.18.1 构建） | ❌ 无 jar | ❌ 需集群 | **未验证** |
| **MySQL（JDBC 路线）** | 同 JDBC + `mysql:mysql-connector-java:8.0.28` | ✅ 驱动已缓存 | 见 JDBC 行 | 可编译 | ❌ 需实例 | 未验证 |
| **MySQL（CDC 路线）** | `org.apache.flink:flink-connector-mysql-cdc:3.5.0` | ✅ 已缓存（+ `flink-connector-debezium:3.5.0`） | ✅ 其 parent pom 基于 **Flink 1.20.1** | ✅ | ❌ 需 MySQL（ROW binlog） | 编译可验证 / **运行未验证** |

> **为什么把验证边界写这么细**：连接器文档最容易"看起来都对、实际跑不起来"。上表每一行的"编译/实跑"都是本机实测结论，不是推测。

---

# 二、⚠️ 全局陷阱：官方连接器的依赖是 `provided`

官方连接器（kafka / jdbc / hbase）在 pom 里把 **`flink-connector-base` 声明为 `<scope>provided</scope>`**，因此它**不会**被传递到你的工程里。而 `KafkaSink` 的 `setDeliveryGuarantee(...)` 需要 `org.apache.flink.connector.base.DeliveryGuarantee`（就在这个 jar 里），**不显式引入就编译不过**（报"程序包 org.apache.flink.connector.base 不存在"）。

本仓库已在根 pom 里显式声明（注意版本：本机 m2 只有 1.20.1 / 1.20.3，没有 1.20.4）：

```xml
<!-- pom.xml -->
<flink.connector.kafka.version>3.3.0-1.20</flink.connector.kafka.version>
<flink.connector.base.version>1.20.3</flink.connector.base.version>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>${flink.connector.kafka.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-base</artifactId>
    <version>${flink.connector.base.version}</version>
</dependency>
```

---

# 三、Kafka

## 3.1 坐标与类名（javap 核实，注意**新包名**）

| 用途 | 类全名 |
|---|---|
| Source | `org.apache.flink.connector.kafka.source.KafkaSource` |
| Sink | `org.apache.flink.connector.kafka.sink.KafkaSink` |
| 序列化 | `org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema`（含 `builder()`） |
| 起始位点 | `org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer` |

> ⚠️ **不要用 `org.apache.flink.streaming.connectors.kafka.*`** —— 那是 1.13 时代的老包，1.20 里只剩一个 Table 内部类。

## 3.2 最小用法（本仓库的 demo 即此写法，**已编译通过**）

```java
// Source
KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("demo-topic")
        .setGroupId("learn-flink-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

DataStream<String> stream = env.fromSource(
        source, WatermarkStrategy.noWatermarks(), "kafka-source");
```

```java
// Sink（至少一次）
KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                        .setTopic("demo-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)   // ← 需要 flink-connector-base
        .build();

stream.sinkTo(sink);
```

**精确一次（EOS）**：`setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)` + `setTransactionalIdPrefix("learn-flink-")`，并要求 Kafka 侧开启事务相关配置。

| builder 方法 | 说明 |
|---|---|
| `setBootstrapServers` / `setTopics(String...)` / `setGroupId` | 基本连接与消费组 |
| `setStartingOffsets(OffsetsInitializer)` | `earliest()` / `latest()` / `committedOffsets()` / `timestamp(...)` |
| `setBounded(OffsetsInitializer)` / `setUnbounded(...)` | 有界/无界消费（有界模式便于测试跑完） |
| `setValueOnlyDeserializer` | 只反序列化 value |
| `setRecordSerializer` / `setDeliveryGuarantee` / `setTransactionalIdPrefix` | Sink 侧 |

## 3.3 运行前置条件（本机未验证）

需要一个 Kafka broker。若本机有 Docker，可这样起（**本机 Docker 未安装，未执行**）：

```yaml
# docker-compose.yml（示例，未验证）
services:
  kafka:
    image: apache/kafka:3.7.0
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

---

# 四、JDBC

## 4.1 官方连接器：1.20 对应 `3.3.0-1.20`，**它自带 `JdbcSource`**

| 用途 | 类全名 | 备注 |
|---|---|---|
| Sink | `org.apache.flink.connector.jdbc.JdbcSink`（`sink(...)` 静态工厂） | 配 `JdbcExecutionOptions` / `JdbcConnectionOptions` / `JdbcStatementBuilder` |
| **Source** | `org.apache.flink.connector.jdbc.source.JdbcSource` + `JdbcSourceBuilder` | builder：`setSql` / `setResultExtractor` / `setUsername` / `setPassword` / `setDriverName` / `setDBUrl` / `setTypeInformation` / `build` |
| 结果抽取 | `org.apache.flink.connector.jdbc.source.ResultExtractor<T>` | `T extract(ResultSet)` |

> ⚠️ **1.20 的官方版本号是 `3.3.0-1.20`**（1.20 线只有这一个）。本机缓存里的 `3.1.1-1.17` 是给 Flink 1.17 编译的：**不支持 1.20，而且没有 `JdbcSource`**（那版只能自写 `RichSourceFunction`）。
>
> ⚠️ 因此本仓库**没有**把 `flink-connector-jdbc` 纳入默认构建（本机无外网、拿不到 3.3.0-1.20）。根 pom 里保留了注释块，联网后取消注释即可。

```xml
<!-- 联网后启用（根 pom 已留注释块） -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-jdbc</artifactId>
    <version>3.3.0-1.20</version>
</dependency>
```

## 4.2 可跑替代路线：原生 JDBC + SQLite（本仓库 demo 走这条，**已真跑通**）

思路：Sink 用 `RichSinkFunction`、Source 用 `RichSourceFunction`，内部直接用 `java.sql`。这样**不依赖任何 Flink 连接器**，且能在本地完整验证"写入 → 读回"闭环。

**本机实测结果**（`source/JdbcSourceSinkDemo`）：先跑 sink 作业写入 3 行，再跑 source 作业读回，`RUN_EXIT=0`、读回 3 行。

**踩到并解决的两个真实坑**（写 demo 时必看）：

| # | 现象 | 原因与解决 |
|---|---|---|
| 1 | `IllegalStateException: Detected an UNBOUNDED source with the 'execution.runtime-mode' set to 'BATCH'` | `RichSourceFunction` 被 Flink 视为 **UNBOUNDED**；1.20 下若 `execution.runtime-mode=BATCH` 直接拒绝。→ 用 STREAMING 模式（默认） |
| 2 | `org.sqlite.SQLiteException: [SQLITE_BUSY] The database file is locked` | source 的 `ResultSet` 持有读锁，与 sink 写同一文件冲突。→ **读写用不同 db 文件**，或同文件开 `PRAGMA journal_mode=WAL` |

另外：JDK 17 下用 `java` 直接跑需 `--add-opens java.base/java.util=ALL-UNNAMED`（以及 `java.lang` / `java.lang.invoke` / `java.nio` / `sun.nio.ch`）。

## 4.3 MySQL 走 JDBC 路线

驱动已缓存：`mysql:mysql-connector-java:8.0.28`（另有 `8.0.27`、`com.mysql:mysql-connector-j:8.1.0`）。

```java
// 只需换 URL 与驱动类名，代码与 SQLite 版一致
Class.forName("com.mysql.cj.jdbc.Driver");
Connection conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/demo?useSSL=false&serverTimezone=UTC", "user", "pass");
```

Sink 侧若用官方连接器：

```java
stream.addSink(JdbcSink.sink(
        "INSERT INTO words (word, cnt) VALUES (?, ?)",
        (ps, w) -> { ps.setString(1, w.f0); ps.setInt(2, w.f1); },
        JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/demo")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("user").withPassword("pass")
                .build()));
```

> 需要一个 MySQL 实例才能实跑；本机无实例 → **未验证**。

---

# 五、HBase（**1.20 没有官方连接器**，务必注意）

| 事实 | 依据 |
|---|---|
| `flink-connector-hbase-2.2` 的版本**止于 `4.0.0-1.19`**，没有 1.20 版 | Maven Central metadata 实测 |
| `flink-connector-hbase-1.4` 止于 `3.0.0-1.17` | 同上 |
| `4.0.0-1.19` 的 parent pom 里 `<flink.version>1.18.1</flink.version>` | 跨版本使用有风险 |
| 本机 m2 **完全没有** hbase 连接器 | `ls ~/.m2/repository/org/apache/flink/ \| grep -i hbase` 为空 |
| **不存在** `HBaseSource`、**不存在** `HBaseConnectionOptions` | 两个 jar 内 grep 均无 |

**实际 API 形态**（与常见博客里的写法不同）：

| 方向 | 真实类 | 说明 |
|---|---|---|
| Sink | `org.apache.flink.connector.hbase.sink.HBaseSinkFunction<T> extends RichSinkFunction<T>` | 构造器 `(String tableName, org.apache.hadoop.conf.Configuration, HBaseMutationConverter<T>, long flushMaxSize, long flushMaxRows, long flushIntervalMs)` |
| Sink 转换器 | `HBaseMutationConverter<T>` | `void open()` + `Mutation convertToMutation(T)` |
| Source | `org.apache.flink.connector.hbase2.source.HBaseRowDataInputFormat(...)` | **InputFormat 路线，不是 FLIP-27 Source**；配 `env.createInput(...)` |
| Schema | `HBaseTableSchema` | `addColumn(family, qualifier, Class)` / `setRowKey(name, Class)` |
| 配置 | `HBaseConnectorOptions`（Table 用）、`HBaseWriteOptions` | — |

**验证边界**：本机连编译都做不到（无 jar），且 1.20 无官方发行版 → 本文档该部分**全部标注"未验证"**。要在 1.20 上用 HBase，实际选择只有：
1. 用 `4.0.0-1.19` 跨版本（**风险自担**，可能有 API/依赖冲突）；
2. 等官方发布 1.20 版；
3. 自写 `RichSinkFunction` / `RichSourceFunction`（或 FLIP-27 `Source`）+ HBase Client API —— 与 §4.2 的 JDBC 自写路线同构，是当前最可控的做法。

---

# 六、MySQL（CDC 路线）

## 6.1 坐标纠正（很重要）

| 说法 | 事实 |
|---|---|
| `com.ververica:flink-connector-mysql-cdc` | 那是 **CDC 2.x 时代**的老坐标；`com.ververica` 目录下**没有** cdc |
| 正确坐标 | **`org.apache.flink:flink-connector-mysql-cdc:3.5.0`**（本机已缓存） |
| 1.20 兼容性 | ✅ 其 parent pom 实测 `<flink.major.version>1.20</flink.major.version>`、`<flink.version>1.20.1</flink.version>`，与 1.20.4 同线可用（Central 另有 `3.6.0-1.20`） |

## 6.2 最小用法

```java
MySqlSource<String> source = MySqlSource.<String>builder()
        .hostname("localhost")
        .port(3306)
        .databaseList("demo")            // 也可用正则
        .tableList("demo.orders")        // 也可用正则
        .username("cdc")
        .password("cdc")
        .deserializer(new JsonDebeziumDeserializationSchema())   // CDC 自带接口
        .startupOptions(StartupOptions.initial())
        .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc").print();
```

| 要点 | 说明 |
|---|---|
| 类名 | `org.apache.flink.cdc.connectors.mysql.source.MySqlSource` / `MySqlSourceBuilder` |
| `deserializer(...)` 参数类型 | **`org.apache.flink.cdc.debezium.DebeziumDeserializationSchema<T>`**（CDC 自己的接口），**不是** Flink 的 `DeserializationSchema` |
| JSON 反序列化器 | `JsonDebeziumDeserializationSchema` 在 `org.apache.flink:flink-connector-debezium:3.5.0`（已缓存） |
| `StartupOptions` | `initial()` / `snapshot()` / `earliest()` / `latest()` / `timestamp(long)` / `specificOffset(...)` |

## 6.3 服务端与权限前置条件（**未实测**，通行配置）

```sql
-- 账号权限
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
```

```ini
# my.cnf
binlog_format     = ROW
binlog_row_image  = FULL
server-id         = 223344        # 必须唯一
```

> 本机无 MySQL 实例（Docker 未安装）→ **运行未验证**；本仓库未把 mysql-cdc 纳入默认构建（依赖较重），根 pom 里留了注释块。

---

# 七、如果只能记住三件事

| # | 结论 |
|---|---|
| 1 | **官方连接器的 `flink-connector-base` 是 provided**，用 `DeliveryGuarantee` 等类必须自己加依赖 —— 这是最容易卡住的编译错误 |
| 2 | **HBase 在 Flink 1.20 没有官方连接器**（止于 `4.0.0-1.19`，且基于 1.18.1）；网上大量"1.20 + HBase"的示例其实跑不通 |
| 3 | 想在本地把"外部系统读写"跑通，**自写 `RichSourceFunction`/`RichSinkFunction`（或 FLIP-27 Source / Sink V2）+ 嵌入式数据库（SQLite）**是最可靠的路线，本仓库的 `source/JdbcSourceSinkDemo` 就是这么验证的 |

---

# 八、配套 demo 与真实输出

| demo | 覆盖 | 验证级别 | 运行 |
|---|---|---|---|
| `source/JdbcSourceSinkDemo` | 原生 JDBC + SQLite：写入 → 读回闭环 | ✅ **实跑通过** | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.source.JdbcSourceSinkDemo` |
| `source/KafkaConnectorDemo` | `KafkaSource` / `KafkaSink` 完整配置 | ✅ 编译通过 / ❌ 未运行（需 broker） | `... -Dmain.class=com.jiaqiz.flink.source.KafkaConnectorDemo` |
| Kafka / HBase / MySQL 官方连接器代码 | 见本文档 §3 / §5 / §6 | 文档级（未验证） | — |

## 8.1 验证级别的诚实划分

本主题涉及的四个外部系统，**只有 SQLite 一个能在本机形成真闭环**，其余三个都受限于"没有实例 / 没有连接器"。分级如下：

| 外部系统 | 依赖能否解析 | 代码能否编译 | 能否真跑 | 原因 |
|---|---|---|---|---|
| **SQLite / JDBC** | ✅ | ✅ | ✅ **真跑通过** | SQLite 是嵌入式文件库，无需服务端；`org.xerial:sqlite-jdbc:3.44.0.0` 已在 pom |
| **Kafka** | ✅（`flink-connector-kafka:3.3.0-1.20` 已引入） | ✅ | ❌ 未跑 | 本机无 broker；demo 默认走"跳过真实执行"分支 |
| **HBase** | ❌ | ❌ | ❌ | **Flink 1.20 没有官方 HBase 连接器**（止于 `4.0.0-1.19`，且面向 1.18.1）；且本机 Maven 离线，无法获取 |
| **MySQL（JDBC 直连）** | ⚠️ 连接器坐标已注释在根 pom | ⚠️ | ❌ | Maven 离线无法拉取 `flink-connector-jdbc:3.3.0-1.20`；本机也无 MySQL 实例 |
| **MySQL CDC** | ⚠️ 坐标已注释（`org.apache.flink:flink-connector-mysql-cdc:3.5.0`） | ⚠️ | ❌ | 同上 + 需要开启 binlog 的实例 |

---

## 8.2 `source/JdbcSourceSinkDemo` —— ✅ 真跑通过（唯一端到端闭环）

```
[main] 已删除旧文件 /tmp/flink-jdbc-demo.db
[main] ===== 作业 1：JDBC Sink 写入 jdbc:sqlite:/tmp/flink-jdbc-demo.db =====
[jdbc-sink subtask#0] open(): 驱动已加载、连接已建立、表 demo_records 就绪
[jdbc-sink] executeBatch() 提交 3 条（累计 3 条）
[jdbc-sink] executeBatch() 提交 2 条（累计 5 条）
[jdbc-sink] close(): 尾批已刷 + commit()，本次共写入 5 条
[main] 作业 1 结束：env.execute() 返回意味着 open/invoke/close 已全部走完，数据已经 commit 落盘
[main] ===== 作业 2：JDBC Source 从 jdbc:sqlite:/tmp/flink-jdbc-demo.db 读回 =====
[jdbc-source] open(): sqlite 驱动已加载
row{id=1, content=hello-flink, created_at=1789727065220}
row{id=2, content=hello-spark, created_at=1789727065220}
row{id=3, content=hello-jdbc, created_at=1789727065220}
row{id=4, content=hello-sqlite, created_at=1789727065222}
row{id=5, content=hello-sink-v2, created_at=1789727065222}
[jdbc-source] run(): SELECT 结束，共读回 5 行
[main] 直接查库 SELECT COUNT(*) = 5
[main] 闭环校验 OK：写入 5 条，读回 5 条
```

**为什么这个 demo 值得单独当"标准范式"**：它给出了在本机验证任何外部系统连接器的通用套路 ——

```
自写 RichSinkFunction / RichSourceFunction（或 FLIP-27 Source / Sink V2）
        + 嵌入式/可本地启动的目标系统
        + 写入后直连数据库 SELECT COUNT(*) 做第三方对账
```

三个细节都是 JDBC sink 的高频事故点，本 demo 都处理了：

| 细节 | 代码里的做法 | 不这么做会怎样 |
|---|---|---|
| **批 + 尾批** | `executeBatch()` 分批提交（3 条、2 条），**`close()` 里再刷尾批 + `commit()`** | 最后不足一批的数据**静默丢失**（作业还显示成功） |
| **避免文件锁冲突** | source 与 sink **拆成两个作业串行执行**，作业 2 在作业 1 完全结束后才开始 | 同一个 SQLite 文件上并发读写 → `[SQLITE_BUSY]` |
| **JDK 17 反射限制** | 运行需 `--add-opens java.base/java.util=ALL-UNNAMED` | `InaccessibleObjectException` |
| **有界 vs 无界** | source 读完 `SELECT` 结果就 `run()` 返回 → **有界** | 写成死循环的无界 source → 在 **BATCH 执行模式下直接被拒绝** |

> ⚠️ **`env.execute()` 返回 ⇒ `open`/`invoke`/`close` 已全部走完**：所以"作业返回后立刻查库对账"是安全的，不需要 sleep 或额外等待。这一点在写 Sink 的集成测试时非常有用。

---

## 8.3 `source/KafkaConnectorDemo` —— ✅ 编译通过 / ⚠️ **未连 broker 运行**

demo 被**故意设计成"默认不执行"**：不传参数时只打印前置条件并退出（`exit 0`），传任意参数（如 `-Dexec.args="run"`）才真正 `env.execute()`。这样它在 CI / 本机都能安全跑一遍"依赖与 builder 链是否解析成功"。

```
========================================================
 KafkaConnectorDemo —— 只做编译验证，需要 Kafka broker 才能运行
========================================================
 期望的 broker      : localhost:9092
 输入 topic         : learn-flink-input
 输出 topic         : learn-flink-output
 consumer group     : learn-flink-kafka-demo
 事务前缀           : learn-flink-kafka-sink-
 前置条件：
   1) 一个可达的 Kafka 集群（bootstrap.servers=localhost:9092）
   2) 上面两个 topic 已创建，且当前账号有读写权限
   3) broker 版本 >= 2.8 且开启事务（EXACTLY_ONCE 走两阶段提交）
   4) broker 的 transaction.max.timeout.ms > 本 demo 的 transaction.timeout.ms(900000)
   5) 作业必须 enableCheckpointing(...)，否则 KafkaSink 的 EXACTLY_ONCE 不可用
--------------------------------------------------------
[main] 未传入任何参数 -> 跳过真实执行（不调用 env.execute()），exit 0
[main] 想真跑：给 main 传任意参数，例如 -Dexec.args="run"
[main] 编译验证通过：KafkaSource / KafkaSink 的 builder 链已全部解析到本地 jar 中的方法
```

**它验证到了什么、没验证到什么**（必须说清楚）：

| ✅ 已验证 | ❌ 未验证 |
|---|---|
| `flink-connector-kafka:3.3.0-1.20` 的 jar 在本地仓库中存在且能被编译期解析 | 与真实 broker 的连接、topic 自动发现 |
| `KafkaSource.builder()` / `KafkaSink.builder()` 的**整条 builder 链方法签名**与 1.20.4 API 一致 | `EXACTLY_ONCE` 的两阶段提交在真实 broker 上是否生效 |
| `flink-connector-base` 显式依赖已生效（`DeliveryGuarantee` 等类可解析） | **第 5 条前置条件**：`EXACTLY_ONCE` 必须配合 `enableCheckpointing(...)`，未开 checkpoint 时它在运行期才会报错 |
| 反序列化器 / 序列化器类型能通过编译 | 水位线、分区发现、checkpoint 恢复语义 |

> ⚠️ **最容易踩的一条**：第 5 条前置条件。`KafkaSink` 配了 `DeliveryGuarantee.EXACTLY_ONCE` 但作业**没有开 checkpoint**时，**编译、提交都不会报错**，问题只在运行期暴露——所以这个 demo 特意把"必须 `enableCheckpointing(...)`"打成了显式前置条件。

---

## 8.4 HBase / MySQL 为什么在这里"跑不了"

| 系统 | 具体障碍 | 建议 |
|---|---|---|
| **HBase** | Flink 1.20 **没有官方 HBase 连接器**。`flink-connector-hbase` 最后发布到 `4.0.0-1.19`（面向 Flink 1.18.1）；网上大量"Flink 1.20 + HBase"的文章实际是抄的旧版本或 SQL 连接器写法，**照抄跑不通** | 走 HBase 原生 Java Client（`org.apache.hadoop.hbase.client`）包在 `RichSinkFunction` / `RichSourceFunction` 里；或降级 Flink 到有对应连接器的版本 |
| **MySQL** | 本机 Maven **离线**，`flink-connector-jdbc:3.3.0-1.20` 取不到；也无 MySQL 实例 | 本文档给出完整代码与依赖坐标（§5），**联网环境**下可直接启用根 pom 里注释掉的那块依赖；本地想验证 JDBC 路径就用 §8.2 的 SQLite 闭环 |
| **MySQL CDC** | 除依赖外还需实例开启 `binlog_format=ROW` + `binlog_row_image=FULL` + 唯一 `server-id` | 见本文档 §6 的 `my.cnf` 片段 |

> **总结论**：本主题的**外部系统部分以"文档级 + 依赖坐标 + 完整代码"交付**，只有 SQLite/JDBC 一条是**真跑闭环**。凡是标注"未验证"的，都不应被当作"已验证可用"来使用——尤其是 HBase 的"1.20 支持"这个常见误传。
