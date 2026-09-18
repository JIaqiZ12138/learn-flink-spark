# RichFunction 富函数集与 RuntimeContext

> **代码基线**：Apache Flink 1.20.4　**源码位置**：仓库根目录 `flink-1.20-source/`
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/rich/`
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)

---

# 一、富函数到底"富"在哪

`RichFunction` 在 `Function` 之上只补三件事：**生命周期**（`open`/`close`）、**运行时上下文**（`getRuntimeContext`）、**迭代上下文**（`getIterationRuntimeContext`）。所有富能力都是"实现 `RichFunction`"换来的，算子侧靠 `instanceof` 分派。

```java
// flink-core/src/main/java/org/apache/flink/api/common/functions/RichFunction.java#L31
public interface RichFunction extends Function {
```

| 方法 | 行号 | 说明 |
|---|---|---|
| `open(Configuration)` | `#L75` | **已 `@Deprecated`**（since 1.19） |
| `open(OpenContext)` | `#L117` | 新入口，默认实现回落：`open(new Configuration())` |
| `close()` | `#L133` | 释放资源 |
| `getRuntimeContext()` | `#L150` | 上下文门面 |
| `getIterationRuntimeContext()` | `#L162` | 迭代作业专用 |
| `setRuntimeContext(RuntimeContext)` | `#L170` | 由框架调用 |

`OpenContext` 目前是**空接口**（`OpenContext.java#L29`，javadoc 原文：*"currently empty because it can be…"*），FLIP-344 先把参数掏空，未来再挂东西。

## 1.1 家族清单（逐个核实过存在性）

| 类型 | 路径 |
|---|---|
| `AbstractRichFunction` | `flink-core/.../api/common/functions/AbstractRichFunction.java#L32`（`implements RichFunction, Serializable`，`#L40` 持 `transient RuntimeContext`） |
| `RichMapFunction` / `RichFlatMapFunction` / `RichFilterFunction` | 同目录 `#L32` / `#L33` / `#L31` |
| `RichReduceFunction` / `RichAggregateFunction` | `#L31` / `#L34` |
| `RichCoGroupFunction` / `RichJoinFunction` / `RichFlatJoinFunction` / `RichMapPartitionFunction` / `RichGroupReduceFunction` … | 同目录 |
| `RichSourceFunction` / `RichParallelSourceFunction` | `flink-streaming-java/.../api/functions/source/#L51` / `#L40` |
| `RichSinkFunction` | `flink-streaming-java/.../api/functions/sink/RichSinkFunction.java#L31` |
| `RichAsyncFunction` | `flink-streaming-java/.../api/functions/async/RichAsyncFunction.java#L75` |
| `ProcessFunction` / `KeyedProcessFunction` 等 | `extends AbstractRichFunction` —— **ProcessFunction 家族本身就是富函数** |

> ⚠️ **`RichProcessFunction` 不存在**（`find -name "RichProcessFunction.java"` 零命中）。`ProcessFunction` 直接继承 `AbstractRichFunction`，所以它已经"富"了，不需要再加 `Rich` 前缀。
>
> ⚠️ **命名空间陷阱**：`flink-datastream-api/.../datastream/api/context/RuntimeContext.java#L31` 是 FLIP-410 **V2 API 的同名不同接口**，不要与 `flink-core` 的 `api.common.functions.RuntimeContext` 混引。

---

# 二、生命周期：谁在什么时候调 `open` / `close`

## 2.1 分派靠 `FunctionUtils`

```java
// FunctionUtils.java#L31
    public static void openFunction(Function function, OpenContext openContext) throws Exception {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.open(openContext);
        }
    }

    public static void closeFunction(Function function) throws Exception {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.close();
        }
    }

    public static void setFunctionRuntimeContext(Function function, RuntimeContext context) {
        if (function instanceof RichFunction) {
            RichFunction richFunction = (RichFunction) function;
            richFunction.setRuntimeContext(context);
        }
    }
```

## 2.2 算子转发给 UDF

```java
// AbstractUdfStreamOperator.java#L99
    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.openFunction(userFunction, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        if (userFunction instanceof SinkFunction) {
            ((SinkFunction<?>) userFunction).finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(userFunction);
    }
```

## 2.3 任务级时序：`initializeState` 严格早于 `open`

```java
// RegularOperatorChain.java#L102
    public void initializeStateAndOpenOperators(
            StreamTaskStateInitializer streamTaskStateInitializer) throws Exception {
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            StreamOperator<?> operator = operatorWrapper.getStreamOperator();
            operator.initializeState(streamTaskStateInitializer);
            operator.open();
        }
    }
```

调用链：`StreamTask.restoreStateAndGates`（`StreamTask.java#L858`）→ `operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer(...))`。

| 事件 | 时机 | 能做什么 |
|---|---|---|
| `setRuntimeContext` | `AbstractUdfStreamOperator#L83`（`setup` 阶段） | 之后即可安全 `getRuntimeContext()` |
| `initializeState` | `open` **之前**、同一算子内紧邻 | 恢复富函数自己的状态（`AbstractUdfStreamOperator#L94` 调 `StreamingFunctionUtils.restoreFunctionState`） |
| `open(OpenContext)` | `initializeState` 之后 | **可安全读 state**；但**读不到"当前 key 的 state"**（此时无 key context） |
| `finish()` | 有界流结束（仅对 `SinkFunction` 额外触发） | 收尾 |
| `close()` | 作业结束/失败清理时由 `RegularOperatorChain.closeAllOperators()` 遍历调用 | 释放资源；注意算子 `close()` 先 `stateHandler.dispose()` 再关 UDF → **`close()` 里 state 已不可用** |

> ⚠️ **`open(Configuration)` 与 `open(OpenContext)` 只覆写一个**：覆写了新的就不会再调旧的；没覆写才由默认实现回落到 `open(new Configuration())`。**两个都覆写就会都跑一次** —— 这是最常见的自己坑自己。
>
> ⚠️ **不要在字段初始化或构造函数里调 `getRuntimeContext()`**：那时还是 `null`，会抛 `IllegalStateException("The runtime context has not been initialized.")`（`AbstractRichFunction.java#L48`）。

---

# 三、`RuntimeContext`：能力清单

实现类是 `StreamingRuntimeContext extends AbstractRuntimeUDFContext`（`flink-streaming-java/.../api/operators/StreamingRuntimeContext.java#L65`）。

| 能力 | 行号 | 备注 |
|---|---|---|
| `getJobInfo()` / `getTaskInfo()` | `#L542` / `#L550` | 1.19+ 统一入口（FLIP-382） |
| `getJobId()` / `getTaskName()` | `#L76` / `#L91` | **已弃用**，默认方法转发到上面两个 |
| `getNumberOfParallelSubtasks()` / `getMaxNumberOfParallelSubtasks()` | `#L115` / `#L131` | |
| **`getIndexOfThisSubtask()`** | `#L147` | 0 基；demo 里看分区就靠它 |
| `getAttemptNumber()` / `getTaskNameWithSubtasks()` | `#L162` / `#L179` | |
| `getMetricGroup()` | `#L101` | 返回 `OperatorMetricGroup` |
| `getExecutionConfig()` / `createSerializer(...)` | `#L191` / `#L200` | |
| `getGlobalJobParameters()` / `isObjectReuseEnabled()` / `getUserCodeClassLoader()` | `#L208` / `#L216` / `#L224` | |
| `addAccumulator(String, Accumulator)` | `#L248` | |
| `getAccumulator` / `getIntCounter` / `getLongCounter` / `getDoubleCounter` / `getHistogram` | `#L257` / `#L261` / `#L265` / `#L269` / `#L273` | 快捷方法会顺带注册 |
| `getBroadcastVariable*` | `#L293` / `#L305` / `#L322` | **流式下必然抛异常**（见 §3.3） |
| `getDistributedCache()` | `#L330` | |
| `getState` / `getListState` / `getReducingState` / `getAggregatingState` / `getMapState` | `#L378` / `#L419` / `#L456` / `#L497` / `#L534` | keyed state，需 `keyBy` 之后 |
| `getExternalResourceInfos(String)` | `#L282` | 外部资源（GPU 等） |
| `getTaskManagerRuntimeInfo()` | ✘ 不在接口上 | 只在 `StreamingRuntimeContext#L165`，需要强转 |

## 3.1 keyed state 的入口与前置校验

```java
// StreamingRuntimeContext.java#L245
    private KeyedStateStore checkPreconditionsAndGetKeyedStateStore(
            StateDescriptor<?, ?> stateDescriptor) {
        checkNotNull(stateDescriptor, "The state properties must not be null");
        checkNotNull(
                keyedStateStore,
                String.format(
                        "Keyed state '%s' with type %s can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.",
                        stateDescriptor.getName(), stateDescriptor.getType()));
        return keyedStateStore;
    }
```

五个 `getXxxState` 是同一模板：`checkPreconditionsAndGetKeyedStateStore` → `stateProperties.initializeSerializerUnlessSet(this::createSerializer)` → 委托 `keyedStateStore.getXxxState(...)`（`#L209-243`）。`keyedStateStore` 由 `setKeyedStateStore`（`#L116`）注入，源头是 `AbstractStreamOperator#L295`。

> ⚠️ 报错信息本身就是最好的提示：**"can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation"** —— 非 keyed 流上用 `getState` 就是这个错。

## 3.2 累加器 vs 指标：别混用

| | 累加器（`Accumulator`） | 指标（`MetricGroup` / `Counter`） |
|---|---|---|
| 可见性 | **作业结束后**在 `JobExecutionResult` / Web UI 汇总 | **实时**，可被 reporter 抓取 |
| 合并语义 | 跨并行子任务**合并**（javadoc：*"Each parallel instance creates and updates its own accumulator object, and the different parallel instances of the accumulator are later merged"*） | 每个 subtask 各自独立上报 |
| 类型 | `IntCounter` / `LongCounter` / `DoubleCounter` / `Histogram` / `AverageAccumulator` / `ListAccumulator` | `Counter`（`inc()` / `inc(long)` / `getCount()`） |
| 注册 | `getRuntimeContext().addAccumulator(name, acc)` | `getRuntimeContext().getMetricGroup().counter("x")` |

> ⚠️ **`DoubleAccumulator` 不存在** —— `flink-core` 提供的是 **`DoubleCounter`**（`flink-runtime` 里那个同名类是 REST 层无关类）。
>
> ⚠️ `addAccumulator` 对**同名**累加器第二次调用直接抛 `UnsupportedOperationException`（`AbstractRuntimeUDFContext#L142`），所以必须放在 `open()` 里，不能放在 `map()` 里逐条调用。
>
> ⚠️ 累加器名必须在**整个 job 内唯一**，否则 JobManager 合并不兼容累加器时会报错。

## 3.3 流式下没有广播变量

```java
// StreamingRuntimeContext.java#L186（三个方法体同构）
    public boolean hasBroadcastVariable(String name) {
        throw new UnsupportedOperationException(
                "Broadcast variables can only be used in DataSet programs");
    }
```

流式里要"广播"请用 `BroadcastStream` / `BroadcastProcessFunction`（见 [`process-functions.md`](./process-functions.md)）。

---

# 四、易错点小结

| # | 易错点 | 正解 |
|---|---|---|
| 1 | 同时覆写 `open(Configuration)` 与 `open(OpenContext)` | 二者会被各调一次；只覆写 `open(OpenContext)` |
| 2 | 在构造函数/字段初始化里 `getRuntimeContext()` | 抛 `IllegalStateException`；改在 `open()` 里用 |
| 3 | 以为 `close()` 里还能读 state | 算子 `close()` 先 dispose state handler |
| 4 | 非 keyed 流上用 `getState` | 抛 "can only be used on a 'keyed stream'"；必须 `keyBy` |
| 5 | 用 `DoubleAccumulator` | 不存在，用 `DoubleCounter` |
| 6 | 在 `map()` 里 `addAccumulator` 同名 | 第二次即抛异常；放 `open()` |
| 7 | 想在流式里用广播变量 | 抛 `UnsupportedOperationException`；用 `BroadcastStream` |
| 8 | `RichAsyncFunction` 里用 state/累加器 | 全部抛异常（见 [`async-io.md`](./async-io.md)） |
| 9 | `RichProcessFunction` | 不存在；`ProcessFunction` 本身就是富函数 |

---

# 五、配套 demo 与真实输出

| demo | 覆盖 | 运行 |
|---|---|---|
| `rich/RichFunctionDemo` | `RichMapFunction`（`open`/`close` + 并行度元信息）、`RichFlatMapFunction`（keyed `ValueState`）、累加器与指标 | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.rich.RichFunctionDemo` |

### 5.1

> 📌 **关于输出中的可变数值**：本文引用的 demo 输出取自**某一次真实运行**。其中并行度 2 下两条分支的输出**交错顺序**（`rich-map:1>` / `rich-map:2>` 谁先谁后）与 `作业耗时 = NNN ms` **每次运行都不同**，因此你重跑时这些数字不会逐字一致 —— 这是并发/时序/墙钟的自然结果，不是文档与代码不一致。**结构性结论（条数、顺序关系、状态名、算子类名）是稳定可复现的。**
 真实运行输出（本地 MiniCluster 实跑通过，退出码 0）

```
=== 提交作业 ===
[open ] subtask 1/2 task=Map -> Sink: Print to Std. Out (2/2)#0 初始化资源 [resource-of-subtask-1]
[open ] subtask 0/2 task=Map -> Sink: Print to Std. Out (1/2)#0 初始化资源 [resource-of-subtask-0]
rich-map:2> FLINK SPARK FLINK
rich-map:1> SPARK FLINK FLINK
rich-map:2> FLINK AND SPARK
rich-map:1> BIG DATA FLINK
keyed-state:1> (spark,1)
keyed-state:2> (flink,1)
keyed-state:1> (spark,2)
keyed-state:2> (flink,2)
keyed-state:2> (flink,3)
keyed-state:1> (spark,3)
keyed-state:2> (and,1)
keyed-state:1> (big,1)
keyed-state:2> (flink,4)
keyed-state:1> (data,1)
keyed-state:2> (flink,5)
keyed-state:2> (flink,6)
[close] subtask 0/2 释放资源 [resource-of-subtask-0]（本 subtask 处理了 2 条）
[close] subtask 1/2 释放资源 [resource-of-subtask-1]（本 subtask 处理了 2 条）
=== 作业结束，累加器汇总 ===
myCounter(RichMapFunction 处理总条数)        = 4（期望 4 条输入 × 1 = 4）
keyedFlatMapCounter(切出的总词数)            = 12
全部累加器 = {myCounter=4, keyedFlatMapCounter=12}
作业耗时 = 137 ms
```

**这张输出把富函数的四个要点全打出来了**：

| 观察 | 证据 | 对应机制 |
|---|---|---|
| `open` 每个 subtask **各调一次** | 并行度 2 → 两行 `[open ]`，且 `task=... (1/2)#0` / `(2/2)#0` 不同 | `AbstractUdfStreamOperator.open()` → `FunctionUtils.openFunction` |
| `open` 里拿得到并行度元信息 | `初始化资源 [resource-of-subtask-1]` —— 资源名带 subtask 下标 | `getRuntimeContext().getIndexOfThisSubtask()` |
| `close` 也在每个 subtask 各调一次，且**在数据全部处理完之后** | 两行 `[close]` 排在所有 `rich-map:` / `keyed-state:` 输出**之后** | 生命周期顺序 `initializeState` → `open` → 处理 → `finish` → `close` |
| keyed state 按 key 隔离、跨记录累积 | `(flink,1) → (flink,2) → … → (flink,6)`，`(spark,1) → (spark,2) → (spark,3)` | `RichFlatMapFunction` + `getRuntimeContext().getState(ValueStateDescriptor)` |
| 累加器在**作业结束后**汇总才能读 | `myCounter=4`、`keyedFlatMapCounter=12`，且 `env.execute()` 之后才打印 | `getRuntimeContext().addAccumulator(...)`；结果随 `JobExecutionResult` 返回 |

> **易错点复现**：`keyed-state:1>` / `keyed-state:2>` 的前缀说明这两个 key 分别落在 subtask 1 和 2 —— 富函数里的 keyed state **不是全局 map**，各 subtask 只看得见自己那段 key group 的数据（详见 [`keyby-and-partitioners.md`](./keyby-and-partitioners.md)）。
>
> **实跑结论（诚实标注）**：`rich/RichFunctionDemo` 在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**；输出已剔除 Flink 的 `WARN` 日志行，业务输出未改动。
