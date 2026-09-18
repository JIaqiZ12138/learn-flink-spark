# 窗口函数与窗口算子（WindowFunction / ProcessWindowFunction / WindowOperator）

> **代码基线**：Apache Flink 1.20.4　**源码位置**：仓库根目录 `flink-1.20-source/`
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/window/`
> **配套图**：[`function-operator-mapping.drawio`](../../assets/function-operator-mapping.drawio)
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)

> **下文路径缩写**：`SJ/` = `flink-streaming-java/src/main/java/org/apache/flink/streaming/`；`RT/` = `flink-runtime/src/main/java/org/apache/flink/runtime/`。
>
> ⚠️ **同名文件陷阱（本文引用多为不带路径的简写，务必对照本节）**：`WindowOperator.java` 与 `WindowOperatorBuilder.java` 在仓库里**各有两个同名文件** —— 一个是流处理版（`SJ/runtime/operators/windowing/`），另一个是 **Table SQL 版**（`flink-table/flink-table-runtime/.../table/runtime/operators/window/groupwindow/operator/`）。**本文（以及本主题所有文档）提到的 `WindowOperator` / `WindowOperatorBuilder` 一律指流处理版**，行号也只对 `SJ/runtime/operators/windowing/` 下的文件有效。

---

# 一、窗口这一层的界限

| | 位置 | 说明 |
|---|---|---|
| **起点** | `KeyedStream.window(...)` / `DataStream.windowAll(...)` | 返回 `WindowedStream` / `AllWindowedStream`，此时**还没有任何物理算子**，只有 builder |
| **终点** | `WindowOperator.processElement(...)` 里对每个元素 `assignWindows → state.add → trigger → emit` | 用户函数在 `emitWindowContents` 里被调用 |
| **不属于本篇** | 水位线怎么产生与传播、状态后端怎么存窗口内容 | 见 `window-watermark/` 与 `state-fault-tolerance/` |

窗口这一层最容易误解的一点：**四个函数接口（`WindowFunction` / `AllWindowFunction` / `ProcessWindowFunction` / `ProcessAllWindowFunction`）与 `reduce`/`aggregate` 最终都落到同一个 `WindowOperator`**，差别只在"状态里存什么"与"用户函数何时被调"。

---

# 二、API 层：三个入口与四个函数接口

## 2.1 关键类

| 类 | 路径 | 职责 |
|---|---|---|
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/WindowedStream.java" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowedStream</code></a> | `streaming/api/datastream/` | keyed 窗口：`(key, window)` 双分组 |
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/AllWindowedStream.java" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">AllWindowedStream</code></a> | 同上 | 非 keyed 窗口：**只按 window 分组，并行度恒为 1** |
| `PartitionWindowedStream` / `NonKeyedPartitionWindowedStream` / `KeyedPartitionWindowedStream` | 同上 | 1.20 新增的"**末窗**"批量入口（FLIP-375 风格），走 `fullWindowPartition()` |
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperator.java#L101" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowOperator</code></a> | `streaming/runtime/operators/windowing/` | 窗口运行时本体 |
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/EvictingWindowOperator.java#L63" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">EvictingWindowOperator</code></a> | 同上 | 配置了 `evictor` 时使用 |
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperatorBuilder.java" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowOperatorBuilder</code></a> | 同上 | 把函数接口翻译成"算子 + 状态描述符" |

## 2.2 三个入口

```java
// KeyedStream.java#L773
    @PublicEvolving
    public <W extends Window> WindowedStream<T, KEY, W> window(
            WindowAssigner<? super T, W> assigner) {
        return new WindowedStream<>(this, assigner);
    }
```

```java
// DataStream.java#L848
    @PublicEvolving
    public <W extends Window> AllWindowedStream<T, W> windowAll(
            WindowAssigner<? super T, W> assigner) {
        return new AllWindowedStream<>(this, assigner);
    }
```

**`windowAll` 并行度恒为 1 的实现根因**（不是配置限制，而是设计）：

```java
// AllWindowedStream.java#L112
    @PublicEvolving
    public AllWindowedStream(DataStream<T> input, WindowAssigner<? super T, W> windowAssigner) {
        this.input = input.keyBy(new NullByteKeySelector<T>());
        this.windowAssigner = windowAssigner;
        this.trigger = windowAssigner.getDefaultTrigger();
    }
```

它把流 `keyBy` 到一个**常量 key**（`NullByteKeySelector`），从而复用同一套 `WindowOperator`；再在每个算子调用上 `.forceNonParallel()`（`AllWindowedStream.java#L1132`）—— 因为只有一个 key，key group 也只能落在一个 subtask 上。

> ⚠️ 因此 `windowAll` 与 `keyBy(常量).window(...)` 在实现上等价；大数据量下 `windowAll` 是单点瓶颈，应改成 `keyBy(业务键)`。

## 2.3 四个函数接口

| 接口 | 方法签名 | 能否拿到 key/window 元信息 |
|---|---|---|
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/windowing/WindowFunction.java#L48" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowFunction</code></a> | `apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out)` | 能拿 key 与 window |
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/windowing/AllWindowFunction.java#L46" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">AllWindowFunction</code></a> | `apply(W window, Iterable<IN> values, Collector<OUT> out)` | 只有 window |
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/windowing/ProcessWindowFunction.java#L52" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">ProcessWindowFunction</code></a> | `process(KEY key, Context context, Iterable<IN> elements, Collector<OUT> out)` | 能拿 key、window、**且是 `RichFunction`**（可用状态/侧输出） |
| <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/windowing/ProcessAllWindowFunction.java#L50" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">ProcessAllWindowFunction</code></a> | `process(Context context, Iterable<IN> elements, Collector<OUT> out)` | 只有 window + RichFunction 能力 |

**关键差别**：只有 `Process*` 版本是 `RichFunction`（能 `open()`、能访问状态与侧输出）；旧的两个 `apply` 版本是普通函数。

---

# 三、四条求值路径：状态里到底存什么

`WindowedStream` 上的每个方法最终都调 `WindowOperatorBuilder` 的对应方法：

```java
// WindowedStream.java#L585
        final String opName = builder.generateOperatorName();
        final String opDescription = builder.generateOperatorDescription(function, null);
        OneInputStreamOperator<T, R> operator = builder.apply(function);

        return input.transform(opName, resultType, operator).setDescription(opDescription);
```

builder 内部按"是否配置 evictor"和"状态类型"分派：

```java
// WindowOperatorBuilder.java#L263
    private <R> WindowOperator<K, T, ?, R, W> apply(
            InternalWindowFunction<Iterable<T>, R, K, W> function) {
        if (evictor != null) {
            return buildEvictingWindowOperator(function);
        } else {
            ListStateDescriptor<T> stateDesc =
                    new ListStateDescriptor<>(
                            WINDOW_STATE_NAME,
                            inputType.createSerializer(config.getSerializerConfig()));

            return buildWindowOperator(stateDesc, function);
        }
    }
```

| API | 状态类型 | 用户函数被调时机 | 说明 |
|---|---|---|---|
| `apply(WindowFunction)` / `apply(AllWindowFunction)` | `ListState<IN>`（`"window-contents"`） | 窗口触发时，拿到**全量元素** | 全量缓存，内存压力最大 |
| `process(ProcessWindowFunction)` | `ListState<IN>` | 同上 | 同上 + RichFunction 能力 |
| `reduce(ReduceFunction)` | `ReducingState<IN>` | 触发时只传**一个聚合值** | 增量聚合，状态 O(1) |
| `aggregate(AggregateFunction)` | `AggregatingState<IN, ACC>` | 同上 | 增量聚合 |
| `reduce(f, ProcessWindowFunction)` / `aggregate(af, ProcessWindowFunction)` | 同增量聚合 | 触发时把**聚合结果**再交给 `ProcessWindowFunction` | 兼顾 O(1) 状态与窗口元信息 |

> ⚠️ **`ReduceFunction` / `AggregateFunction` 不允许是 `RichFunction`**：builder 在 <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperatorBuilder.java#L148" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">#L148 / #L174 / #L203 / #L232</code></a> 直接抛 `UnsupportedOperationException`。窗口里要 `open()` 就用组合重载（第二个参数传 `ProcessWindowFunction`）。
>
> ⚠️ **`aggregate(AggregateFunction, ProcessWindowFunction)` 的泛型是三段**：`AggregateFunction<T, ACC, V>` + `ProcessWindowFunction<V, R, KEY, W>` —— 中间类型 `V` 必须对齐，否则编译期类型推断失败。
>
> ⚠️ **有 `evictor` 时增量聚合会降级**为 `InternalAggregateProcessWindowFunction`（聚合与 evictor 不能增量共存），且 `EvictingWindowOperator` 的 `ACC = Iterable<IN>`，用户函数拿到的是**原始 `StreamRecord` 集合**。

---

# 四、窗口分配器：`WindowAssigner` 与窗口合并

## 4.1 接口

```java
// WindowAssigner.java#L44
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
    public abstract Collection<W> assignWindows(
            T element, long timestamp, WindowAssignerContext context);
    public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);
    public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);
    public abstract boolean isEventTime();
    /** A context for {@link WindowAssigner}, it gives access to the current processing time. */
    public abstract static class WindowAssignerContext {
        public abstract long getCurrentProcessingTime();
    }
```

```java
// MergingWindowAssigner.java#L42
    public abstract void mergeWindows(Collection<W> windows, MergeCallback<W> callback);
```

## 4.2 四种常用分配器

```java
// TumblingEventTimeWindows.java#L70
    @Override
    public Collection<TimeWindow> assignWindows(
            Object element, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            if (staggerOffset == null) {
                staggerOffset =
                        windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
            }
            // Long.MIN_VALUE is currently assigned when no timestamp is present
            long start =
                    TimeWindow.getWindowStartWithOffset(
                            timestamp, (globalOffset + staggerOffset) % size, size);
            return Collections.singletonList(new TimeWindow(start, start + size));
        } else {
            throw new RuntimeException(
                    "Record has Long.MIN_VALUE timestamp (= no timestamp marker). "
                            + "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
                            + "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }
```

```java
// SlidingEventTimeWindows.java#L70
    @Override
    public Collection<TimeWindow> assignWindows(
            Object element, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
            long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
            for (long start = lastStart; start > timestamp - size; start -= slide) {
                windows.add(new TimeWindow(start, start + size));
            }
            return windows;
        } else {
            throw new RuntimeException(...
```

```java
// EventTimeSessionWindows.java#L62 与 #L133
    public Collection<TimeWindow> assignWindows(
            Object element, long timestamp, WindowAssignerContext context) {
        return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
    }
    ...
    @Override
    public void mergeWindows(
            Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {
        TimeWindow.mergeWindows(windows, c);
    }
```

| 分配器 | 一个元素落入几个窗口 | 默认触发器 | 合并 |
|---|---|---|---|
| `TumblingEventTimeWindows` | **1 个** | `EventTimeTrigger` | — |
| `SlidingEventTimeWindows` | **`size/slide` 个** | `EventTimeTrigger` | — |
| `EventTimeSessionWindows` | 先 1 个，后按 gap 合并 | `EventTimeTrigger`（`canMerge=true`） | ✅ |
| `DynamicEventTimeSessionWindows` | 同上，gap 由 `SessionWindowTimeGapExtractor` 逐元素给出 | 同上 | ✅ |
| `GlobalWindows` | 1 个（全局唯一窗口） | **`NeverTrigger`**（必须自己 `trigger(...)`） | — |

**合并算法本体**在 `windows/TimeWindow.java#L208` 的 `mergeWindows(...)`：按 start 排序后线性扫描，`intersects` 则 `cover` 合并，最后对每个合并组回调 `c.merge(group, mergedWindow)`。

> ⚠️ **offset 约束**：`abs(offset) < size`，否则抛 `IllegalArgumentException`（`TumblingEventTimeWindows.java#L60`）。
>
> ⚠️ **滑动窗口内存放大 `size/slide` 倍** —— `SlidingEventTimeWindows.java#L74` 直接预分配 `new ArrayList<>((int)(size/slide))`。
>
> ⚠️ **`getDefaultTrigger(StreamExecutionEnvironment)` 在 1.20 已全部抛 `UnsupportedOperationException`**（如 `TumblingEventTimeWindows.java#L92`），自定义 assigner 不要再覆盖这个旧方法。
>
> ⚠️ **事件时间窗口必须在 `keyBy/window` 之前 `assignTimestampsAndWatermarks(...)`**，否则 assigner 直接抛上面那段 `Long.MIN_VALUE timestamp` 异常。

---

# 五、触发器与驱逐器

## 5.1 `Trigger`

```java
// Trigger.java#L69
    public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx)
            throws Exception;
    public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
    public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx)
    public boolean canMerge() {
        return false;
    }
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        throw new UnsupportedOperationException("This trigger does not support merging.");
    }
    public abstract void clear(W window, TriggerContext ctx) throws Exception;
```

```java
// TriggerResult.java#L30
public enum TriggerResult {
    /** No action is taken on the window. */
    CONTINUE(false, false),
    /** {@code FIRE_AND_PURGE} evaluates the window function and emits the window result. */
    FIRE_AND_PURGE(true, true),
    FIRE(true, false),
    PURGE(false, true);
```

```java
// EventTimeTrigger.java#L36
    @Override
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }
```

```java
// CountTrigger.java#L47
        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
        count.add(1L);
        if (count.get() >= maxCount) {
            count.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
```

| 触发器 | 语义 |
|---|---|
| `EventTimeTrigger` | 水位线越过 `window.maxTimestamp()` 时 FIRE（**`canMerge=true`**） |
| `ProcessingTimeTrigger` | 处理时间到达窗口末尾时 FIRE |
| `CountTrigger.of(n)` | 每 n 条 FIRE |
| `ContinuousEventTimeTrigger.of(interval)` | 水位线推进期间按固定间隔重复 FIRE |
| `PurgingTrigger.of(inner)` | 把内层结果转成 `FIRE_AND_PURGE` |
| `DeltaTrigger.of(deltaFunction, threshold)` | 与上一次触发点的 delta 超阈值时 FIRE |
| `NeverTrigger` | 永不触发（`GlobalWindows` 的默认值） |

> ⚠️ **最常踩的坑**：`canMerge()` 默认 `false`。给 `EventTimeSessionWindows` 这类**合并窗口**配一个不支持合并的 trigger，会在 <a href="../../../../flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperatorBuilder.java#L109" style="color:#0969da"><code style="color:#0969da;background:transparent;border:none">WindowOperatorBuilder.java#L109</code></a> 直接抛 `UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.")`。

## 5.2 `Evictor`

`Evictor`（`evictors/Evictor.java#L44`）两个方法：`evictBefore` / `evictAfter`。三个实现都带 `doEvictAfter` 开关，且**`evict` 在 before 与 after 两处都会被调用**（如 `CountEvictor.java#L50-61`）：

| 实现 | 语义 |
|---|---|
| `CountEvictor.of(maxCount[, doEvictAfter])` | 只保留最后 maxCount 个元素 |
| `TimeEvictor.of(windowSize[, doEvictAfter])` | 删掉 `currentTime - windowSize` 之前的元素（`TimeEvictor#L76`） |
| `DeltaEvictor.of(deltaFunction, threshold[, doEvictAfter])` | 按 delta 函数与阈值裁剪 |

> ⚠️ 配置 evictor 会同时把算子换成 `EvictingWindowOperator`、把状态换成 `ListState<StreamRecord<IN>>`、并**禁用增量聚合**（见 §三）。

---

# 六、运行时：`WindowOperator` 主流程

## 6.1 类声明与关键字段

```java
// WindowOperator.java#L101
@Internal
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
        extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {
```

三个要点：

1. 它继承 `AbstractUdfStreamOperator` —— 所以 window 算子**同样是"UDF 算子"**，`open()`/`close()`/checkpoint 走的是同一套骨架；
2. 它实现 `Triggerable` —— 定时器到期会回调**自己**的 `onEventTime`/`onProcessingTime`；
3. 窗口内容以 `Window` 为 namespace 存在 keyed state 里（逻辑结构 `(key, window) -> 元素/聚合值`）。

```java
// WindowOperator.java#L214
    public void open() throws Exception {
        super.open();
        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
        timestampedCollector = new TimestampedCollector<>(output);
        internalTimerService = getInternalTimerService("window-timers", windowSerializer, this);
```

## 6.2 非合并窗口的主流程

```java
// WindowOperator.java#L397（非 merging 分支节选）
                windowState.setCurrentNamespace(window);
                windowState.add(element.getValue());
                triggerContext.key = key;
                triggerContext.window = window;
                TriggerResult triggerResult = triggerContext.onElement(element);
                if (triggerResult.isFire()) {
                    ACC contents = windowState.get();
                    if (contents != null) {
                        emitWindowContents(window, contents);
                    }
                }
                if (triggerResult.isPurge()) {
                    windowState.clear();
                }
                registerCleanupTimer(window);
```

精确顺序：**`assignWindows` → 迟到判定 → `state.add` → `trigger.onElement` → `isFire()` 则 `emitWindowContents` → `isPurge()` 则 `state.clear()` → `registerCleanupTimer`**；元素迟到且未配 `sideOutputLateData` 时只增加 `numLateRecordsDropped` 指标（`#L428`）。

```java
// WindowOperator.java#L563
    private void emitWindowContents(W window, ACC contents) throws Exception {
        timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
        processContext.window = window;
        userFunction.process(
                triggerContext.key, window, processContext, contents, timestampedCollector);
    }
```

> 注意 `setAbsoluteTimestamp(window.maxTimestamp())`：**窗口输出的时间戳是窗口末尾**，不是元素时间戳。

## 6.3 合并窗口（session）的额外动作

```java
// WindowOperator.java#L341（merge 回调内部节选）
                                        triggerContext.onMerge(mergedWindows);
                                        for (W m : mergedWindows) {
                                            triggerContext.window = m;
                                            triggerContext.clear();
                                            deleteCleanupTimer(m);
                                        }
                                        // merge the merged state windows into the newly resulting
                                        // state window
                                        windowMergingState.mergeNamespaces(
                                                stateWindowResult, mergedStateWindows);
```

合并时要做四件事：**触发器状态合并（`onMerge`）→ 旧窗口的 trigger 状态清理 → 删除旧窗口的清理定时器 → keyed state 的 namespace 合并（`mergeNamespaces`）**。

`MergingWindowSet`（`MergingWindowSet.java#L54`）维护"逻辑窗口 → state 窗口"的映射，`addWindow`（`#L153`）把现存映射与新窗口一起交给 `windowAssigner.mergeWindows`，合并结果**复用被合并窗口的 state window**。

> ⚠️ 合并窗口在事件时间下要求 `mergeResult.maxTimestamp() + allowedLateness > currentWatermark`，否则抛 `UnsupportedOperationException("The end timestamp of an event-time window cannot become earlier than the current watermark by merging...")`（`WindowOperator.java#L309`）。

## 6.4 定时器与清理

```java
// WindowOperator.java#L437
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();
        ...
        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
        if (triggerResult.isFire()) { ACC contents = windowState.get(); ... }
        if (windowAssigner.isEventTime()
                && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
            clearAllState(triggerContext.window, windowState, mergingWindows);
        }
```

```java
// WindowOperator.java#L548
    private void clearAllState(
            W window, AppendingState<IN, ACC> windowState, MergingWindowSet<W> mergingWindows)
            throws Exception {
        windowState.clear();
        triggerContext.clear();
        processContext.window = window;
        processContext.clear();
        if (mergingWindows != null) {
            mergingWindows.retireWindow(window);
            mergingWindows.persist();
        }
    }
```

清理时间是 `window.maxTimestamp() + allowedLateness`（`registerCleanupTimer` `#L646`、`isCleanupTime` `#L675`）。

> ⚠️ **`cleanupTime == Long.MAX_VALUE` 时直接 return，不注册清理定时器** —— `GlobalWindows` + 未配 `allowedLateness` 就会命中这条，**窗口状态永不回收**（长期运行会状态泄漏）。

---

# 七、易错点小结

| # | 易错点 | 正解 |
|---|---|---|
| 1 | `windowAll` 以为能并行 | 它内部 `keyBy(常量)` + `forceNonParallel()`，并行度恒 1 |
| 2 | `GlobalWindows` 不触发 | 默认 `NeverTrigger`，必须 `.trigger(CountTrigger.of(n))` 等 |
| 3 | 会话窗口配了不支持的 trigger | 抛 `A merging window assigner cannot be used with a trigger that does not support merging.` |
| 4 | `reduce`/`aggregate` 里想用 `open()` | 不允许 `RichFunction`，改用 `aggregate(af, ProcessWindowFunction)` |
| 5 | 事件时间窗口没有 watermark | assigner 抛 `Record has Long.MIN_VALUE timestamp ...` |
| 6 | 窗口输出时间戳以为是元素时间 | 是 `window.maxTimestamp()` |
| 7 | 配了 evictor 又想要增量聚合 | 二者不可共存，会降级为全量 + `Iterable<IN>` |
| 8 | 合并窗口在 watermark 之后合并 | 抛 `The end timestamp of an event-time window cannot become earlier than the current watermark by merging...` |
| 9 | 1.20 里找 `WindowState` / `InternalWindowState` / `WindowStore` | **都已不存在**；现役是 `InternalAppendingState` / `InternalMergingState`（`flink-runtime/.../state/internal/`） |

---

# 八、配套 demo 与真实输出

| demo | 覆盖 | 运行 |
|---|---|---|
| `window/WindowAssignersDemo` | 翻滚 / 滑动 / 会话 / `windowAll` + `Trigger` + `Evictor` | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.window.WindowAssignersDemo` |
| `window/WindowFunctionsDemo` | `WindowFunction` / `AllWindowFunction` / `ProcessWindowFunction` 四条路径 + 迟到数据两个出口 | `... -Dmain.class=com.jiaqiz.flink.window.WindowFunctionsDemo` |
| `window/WindowAggregateDemo` | `reduce` / `aggregate` / 内置聚合 / `aggregate + ProcessWindowFunction` / 会话窗口 `merge` | `... -Dmain.class=com.jiaqiz.flink.window.WindowAggregateDemo` |

> **实跑状态**：三个 demo 全部在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**。三个 demo 都设计成"多个独立本地作业串行执行"，每个小节用**有界源**，所以**跑完自动退出**（不会挂住）。输出已剔除 Flink 的 `WARN` 日志行，业务输出未改动。

## ⚠️ 先说一个会影响所有事件时间窗口 demo 的坑：水位线生成策略

三个 demo 都用了一个**自定义的"每条记录发一次水位线"生成器**（`PerRecordWatermarks`），而**不是** `WatermarkStrategy.forMonotonousTimestamps()`。原因值得单独记：

```java
public static class PerRecordWatermarks implements WatermarkGenerator<Sale> {

    private long maxTs = Long.MIN_VALUE;

    private final long outOfOrdernessMillis;   // 默认 1 = "单调递增"语义

    @Override
    public void onEvent(Sale event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - outOfOrdernessMillis));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 每条都发过了，周期发射这里什么都不做
    }
}
```

| 现象 | 原因 |
|---|---|
| `forMonotonousTimestamps()` 在**本地有界源**上"一条水位线都不发" | 它的实现是 `AscendingTimestampsWatermarks`，只在 **`onPeriodicEmit`** 里发水位线，周期由 `ExecutionConfig#getAutoWatermarkInterval()`（**默认 200ms**）控制。而 `env.fromData(...)` 的有界源**瞬间跑完**，还没到第一次周期发射就结束了 |
| 结果：所有窗口都只在**输入结束时**靠 `MAX_WATERMARK` 一次性触发 | 「窗口随水位线推进逐个触发」的过程完全看不见；

`sideOutputLateData` 也永远不触发（没有任何元素"迟到"） |
| 解法 | 用 `WatermarkStrategy.forGenerator(...)` 自定义 `onEvent` 里逐条发；**安全性由框架保证**：`TimestampsAndWatermarksOperator#processElement` 先 `output.collect(element)`、后调 `onEvent(...)`（`TimestampsAndWatermarksOperator.java#L138/L139`），所以水位线不会"追尾"自己的元素 |

> 📌 **这个坑在真实生产里同样存在**：只要是"启动瞬间灌完"的有界补数作业 + 默认的周期水位线，窗口同样会全部堆到最后才触发。要么显式调小 `autoWatermarkInterval`，要么像这里一样改成事件驱动发射。

---

### 8.1 `WindowAssignersDemo` —— Assigner / Trigger / Evictor 三件套

**1) 翻滚窗口 + `windowAll`（非 keyed）**

```
=== 1) 翻滚窗口 TumblingEventTimeWindows.of(Duration.ofSeconds(5)) + windowAll（非 keyed）===
    算子：WindowOperator；windowAll 内部 keyBy(常量) + forceNonParallel()，并行度恒为 1
    [翻滚] 窗口 [+0ms, +5000ms) maxTimestamp=+4999ms 元素数=3 -> [u1@+0ms(v=1), u2@+1000ms(v=2), u1@+3000ms(v=3)]
1-翻滚> [翻滚] [+0ms, +5000ms) 汇总 3 条: [u1@+0ms(v=1), u2@+1000ms(v=2), u1@+3000ms(v=3)]
    [翻滚] 窗口 [+5000ms, +10000ms) maxTimestamp=+9999ms 元素数=3 -> [u2@+5500ms(v=4), u1@+6000ms(v=5), u2@+9000ms(v=6)]
1-翻滚> [翻滚] [+5000ms, +10000ms) 汇总 3 条: [u2@+5500ms(v=4), u1@+6000ms(v=5), u2@+9000ms(v=6)]
```

⭐ **注意 `maxTimestamp=+4999ms`**：窗口 `[0,5000)` 的 `maxTimestamp` 是 **4999**、不是 5000。触发条件是 `watermark >= window.maxTimestamp()`，差这 1ms 就是很多"窗口为什么晚一格才触发"问题的根源（§6.4）。另外 `windowAll` 把两个不同 user 的数据放进了**同一个窗口**（没有 key 的含义），并且**并行度被强制为 1**。

**2) 滑动窗口：一条数据进多个窗口**

```
=== 2) 滑动窗口 SlidingEventTimeWindows.of(10s, 5s)（keyed + WindowFunction）===
    窗口大小 10s、滑动步长 5s ⇒ 每条数据会被分到 2 个窗口（除了边界），这就是滑动窗口的"重复计算"代价
    [滑动] key=u1 窗口 [+-5000ms, +5000ms) 元素数=2 -> [u1@+0ms(v=1), u1@+1000ms(v=2)]
2-滑动> [滑动] key=u1 [+-5000ms, +5000ms) -> [u1@+0ms(v=1), u1@+1000ms(v=2)]
    [滑动] key=u2 窗口 [+-5000ms, +5000ms) 元素数=1 -> [u2@+2000ms(v=9)]
2-滑动> [滑动] key=u2 [+-5000ms, +5000ms) -> [u2@+2000ms(v=9)]
    [滑动] key=u1 窗口 [+0ms, +10000ms) 元素数=3 -> [u1@+0ms(v=1), u1@+1000ms(v=2), u1@+6000ms(v=3)]
2-滑动> [滑动] key=u1 [+0ms, +10000ms) -> [u1@+0ms(v=1), u1@+1000ms(v=2), u1@+6000ms(v=3)]
    [滑动] key=u2 窗口 [+0ms, +10000ms) 元素数=1 -> [u2@+2000ms(v=9)]
2-滑动> [滑动] key=u2 [+0ms, +10000ms) -> [u2@+2000ms(v=9)]
    [滑动] key=u1 窗口 [+5000ms, +15000ms) 元素数=1 -> [u1@+6000ms(v=3)]
2-滑动> [滑动] key=u1 [+5000ms, +15000ms) -> [u1@+6000ms(v=3)]
```

`u2@+2000ms(v=9)` **同时出现在 `[-5000,5000)` 与 `[0,10000)` 两个窗口里** —— 这是"一条数据被算多次"的直接证据。⚠️ 还出现了一个 **起始时间为负** 的窗口 `[+-5000ms, +5000ms)`：`SlidingEventTimeWindows` 会为"起点在时间原点之前"的数据也开窗，生产环境必须靠 watermark 下界 / `allowedLateness` 把它挡掉，否则状态会异常膨胀（§4.2）。

**3) 会话窗口：类名与"动态边界"**

```
=== 3) 会话窗口 EventTimeSessionWindows.withGap(Duration.ofSeconds(2))（合并窗口）===
    1.20.4 里已经没有 SessionWindows 这个类，事件时间会话窗口叫 EventTimeSessionWindows
    它是 MergingWindowAssigner：新元素可能把相邻两个窗口"粘"成一个，窗口边界因此会变宽
    [会话] 窗口 [+0ms, +3000ms) 宽度=3000ms 元素数=2 -> [u1@+0ms(v=1), u1@+1000ms(v=2)]
3-会话> [会话] [+0ms, +3000ms) 宽度=3000ms -> [u1@+0ms(v=1), u1@+1000ms(v=2)]
    [会话] 窗口 [+5000ms, +8000ms) 宽度=3000ms 元素数=2 -> [u2@+5000ms(v=3), u2@+6000ms(v=4)]
3-会话> [会话] [+5000ms, +8000ms) 宽度=3000ms -> [u2@+5000ms(v=3), u2@+6000ms(v=4)]
    [会话] 窗口 [+12000ms, +14000ms) 宽度=2000ms 元素数=1 -> [u1@+12000ms(v=5)]
3-会话> [会话] [+12000ms, +14000ms) 宽度=2000ms -> [u1@+12000ms(v=5)]
```

1. **类名纠错**：Flink 1.20 **没有 `SessionWindows`**。它被拆成 **`EventTimeSessionWindows`** 与 **`ProcessingTimeSessionWindows`**，所以网上 `SessionWindows.withGap(...)` 的写法在 1.20 上**编译不过**。
2. **宽度是会变的**：`[0,3000)` 宽 3000ms = `gap(2000) + 最后一个元素时间(1000)`；`[12000,14000)` 只有 2000ms = `gap(2000) + 0`（单元素）。**会话窗口不能用"固定宽度"理解**。

**4) 自定义 `Trigger`：每满 2 条提前触发**

```
=== 4) 自定义 Trigger：EarlyFireCountTrigger（每满 2 条提前触发一次）===
    窗口：10s 翻滚窗口，5 条数据（+0 ~ +4000ms）全部落在 [  +0ms, +10000ms) 里
    [自定义Trigger] 触发! key=u1 窗口 [+0ms, +10000ms) 本次内容(2 条)=[u1@+0ms(v=1), u1@+1000ms(v=2)]
4-自定义Trigger> [自定义Trigger] [+0ms, +10000ms) 本次触发内容=[u1@+0ms(v=1), u1@+1000ms(v=2)]
    [自定义Trigger] 触发! key=u1 窗口 [+0ms, +10000ms) 本次内容(2 条)=[u1@+2000ms(v=3), u1@+3000ms(v=4)]
4-自定义Trigger> [自定义Trigger] [+0ms, +10000ms) 本次触发内容=[u1@+2000ms(v=3), u1@+3000ms(v=4)]
    [自定义Trigger] 触发! key=u1 窗口 [+0ms, +10000ms) 本次内容(1 条)=[u1@+4000ms(v=5)]
4-自定义Trigger> [自定义Trigger] [+0ms, +10000ms) 本次触发内容=[u1@+4000ms(v=5)]
```

**同一个窗口 `[0,10000)` 被触发了 3 次**（2+2+1 条），第 3 次只有 1 条 —— 说明 `Trigger` 返回 `FIRE` 时**只交出"上次触发之后新增的元素"**，触发是**增量**的，不是重发全窗口。要"每次都看全量"必须配 `Evictor` 或自己维护状态（§5.1）。

**5) `Evictor`：算子变成 `EvictingWindowOperator`**

```
=== 5) 驱逐器 CountEvictor.of(2)（只保留窗口内最后 2 条）===
    只要调用了 .evictor(...)，算子就从 WindowOperator 变成 EvictingWindowOperator：
    窗口内容状态是 ListState<StreamRecord<Event>>（状态名 "window-contents"），
    因此配置 evictor 后窗口函数拿到的是 Iterable<Event>（全量），不能再用增量聚合
    [驱逐器] key=u1 窗口 [+0ms, +10000ms) 驱逐后剩余(2 条)=[u1@+2000ms(v=3), u1@+4000ms(v=5)]
5-驱逐器> [驱逐器] key=u1 [+0ms, +10000ms) -> [u1@+2000ms(v=3), u1@+4000ms(v=5)]
    [驱逐器] key=u2 窗口 [+0ms, +10000ms) 驱逐后剩余(2 条)=[u2@+3000ms(v=4), u2@+5000ms(v=6)]
5-驱逐器> [驱逐器] key=u2 [+0ms, +10000ms) -> [u2@+3000ms(v=4), u2@+5000ms(v=6)]
```

| 观察 | 机制 |
|---|---|
| 6 条数据最后只剩 2 条 | `CountEvictor.of(2)` 在**输出前**丢掉多余元素 |
| 留下的是**最后** 2 条（`v=3, v=5`）而非最前 2 条 | `CountEvictor` 默认从**头部**驱逐（保留最近的） |
| 算子变成 `EvictingWindowOperator` | 只要调 `.evictor(...)`，`WindowOperatorBuilder` 就走 `buildEvictingWindowOperator`（`WindowOperatorBuilder.java#L293`），窗口内容**强制为 `ListState<StreamRecord<T>>`** |

> ⚠️ **`evictor` 与增量聚合互斥**：驱逐器必须"看到每一条元素才能决定丢谁"，所以窗口内容只能是全量 `ListState`，`ReducingState` / `AggregatingState` 用不了 —— 配 `evictor` 就等于**主动放弃增量聚合的性能优势**。

**三个 assigner 的一句话总结（demo 自己的收尾）**

```
  1. Assigner 决定数据进哪些窗口 —— 翻滚/滑动是固定边界，会话窗口会合并（边界动态变化）
  2. Trigger 决定何时触发 —— 默认 EventTimeTrigger 在 watermark 越过 window.maxTimestamp() 时触发
  3. Evictor 决定输出前扔掉什么 —— 配了它算子就变 EvictingWindowOperator，且与增量聚合互斥
```

### 8.2 `WindowFunctionsDemo` —— 四条求值路径 + 迟到数据的两个出口

**1) keyed `.apply(WindowFunction)`：全量 `Iterable`**

```
=== 1) keyed .apply(WindowFunction)：窗口函数拿到全量 Iterable<Event> ===
    状态：ListState<StreamRecord<Event>>，状态名 "window-contents"（WindowOperatorBuilder.java#L71）
    也就是说：窗口里的每一条原始元素都被存下来了，元素越多状态越大
    [WindowFunction] key=u1 窗口 [+0ms, +5000ms) 元素数=3 -> [u1@+0ms(v=1), u1@+1000ms(v=2), u1@+3000ms(v=3)]
1-WindowFunction> [WindowFunction] key=u1 [+0ms, +5000ms) 共 3 条
    [WindowFunction] key=u2 窗口 [+0ms, +5000ms) 元素数=2 -> [u2@+2000ms(v=10), u2@+4000ms(v=20)]
1-WindowFunction> [WindowFunction] key=u2 [+0ms, +5000ms) 共 2 条
```

状态名已核实：`WindowOperatorBuilder.java#L71` 明文 `private static final String WINDOW_STATE_NAME = "window-contents";`。

**2) `.windowAll(...).apply(AllWindowFunction)`：非 keyed**

```
=== 2) .windowAll(...).apply(AllWindowFunction)：非 keyed 版本 ===
    windowAll 没有 key，所有数据进同一个算子实例（并行度被强制为 1），是全局统计的写法
    [AllWindowFunction] 窗口 [+0ms, +5000ms) 元素数=3 -> [u1@+0ms(v=1), u2@+1000ms(v=2), u1@+3000ms(v=3)]
2-AllWindowFunction> [AllWindowFunction] [+0ms, +5000ms) 共 3 条
    [AllWindowFunction] 窗口 [+5000ms, +10000ms) 元素数=1 -> [u2@+6000ms(v=4)]
2-AllWindowFunction> [AllWindowFunction] [+5000ms, +10000ms) 共 1 条
```

**`u1` 和 `u2` 混在同一个窗口**（没有 key 的含义）；`windowAll` **并行度强制为 1**，是全局统计写法，数据量大时是单点瓶颈（§2.2）。

**3) `.process(ProcessWindowFunction)`：能拿到窗口元信息**

```
=== 3) .process(ProcessWindowFunction)：Context 能拿到窗口边界与时间 ===
    [ProcessWindowFunction] key=u1 window=[+0ms, +5000ms) maxTimestamp=+4999ms 元素数=3
        currentProcessingTime=1789727523575（墙钟毫秒，截断显示 23575） currentWatermark=+9223370336854775807ms
        内容 -> [u1@+0ms(v=1), u1@+2000ms(v=2), u1@+4000ms(v=3)]
3-ProcessWindowFunction> [ProcessWindowFunction] key=u1 [+0ms, +5000ms) 元素数=3 watermark=+9223370336854775807ms
```

`ProcessWindowFunction.Context` 一次给出四样：`window()`（起止 + `maxTimestamp`）、`currentProcessingTime()`、`currentWatermark()`、`windowState()`（窗口级 `KeyedStateStore`）。

> ⚠️ **`ProcessAllWindowFunction.Context` 没有 `currentProcessingTime()`**（只有 `window()` / `windowState()` / `globalState()` / `output()`）——all-window 版本的能力比 keyed 版少，别想当然。这里的 `currentWatermark=+9223370336854775807ms` 是**输入结束时**的 `MAX_WATERMARK`（这个小节的数据在窗口触发前没有把水位线推过 `maxTimestamp`，所以窗口是最后统一触发的）。

**4) 增量聚合 + `ProcessWindowFunction`：`Iterable` 里只有 1 个元素**

```
=== 4) 增量聚合 + ProcessWindowFunction：Iterable 里只有 1 个元素 ===
    [reduce+Process] key=u1 窗口 [+0ms, +5000ms) Iterable 元素数=1 -> [u1@+0ms(v=6)]（注意：只有 1 个，是归约结果本身）
4a-reduce+Process> [reduce+Process] key=u1 归约结果=[u1@+0ms(v=6)]
    [aggregate+Process] key=u1 窗口 [+0ms, +5000ms) Iterable 元素数=1 -> [6]（累加器结果） | watermark=+9223370336854775807ms
4b-aggregate+Process> [aggregate+Process] key=u1 求和=[6]
```

⭐ **这是整篇最值得记住的一行：`元素数=1`。** `.reduce(rf, pwf)` / `.aggregate(af, pwf)` 的组合里，`ProcessWindowFunction.process` 收到的 `Iterable` **只含 1 个元素**（那个累加器/归约结果），**不是窗口里的全部原始元素**。于是该写法**既有增量聚合的状态优势，又能拿到窗口元信息**；代价是**必须自己把累加器包成输出类型**。

**5) 迟到数据：两个出口，这次**两个都真的触发了** ⭐**

```
=== 5) 迟到数据的两个出口 ===
    (a) 窗口内乱序到达：把元素放进窗口，同时用 ctx.output(OutputTag, element) 记一份到侧输出
    (b) 超过 allowedLateness：窗口状态已经清理，无法补算，被 sideOutputLateData 分流
    数据（tumbling 5s，allowedLateness=2s，key=u1）：
      +1000 → 窗口[+0,+5000)；  +0 → 同一窗口，但比已见过的 +1000 早 ⇒ 乱序，走 (a)
      +6000 → 水位线推到 +5999，窗口[+0,+5000) 首次触发
      +2000 → 该窗口清理时刻是 +4999+2000=+6999 > 水位线 +5999 ⇒ 仍在 allowedLateness 内，
              元素被补进窗口并触发第二次输出（EventTimeTrigger 在 watermark 已过 maxTimestamp 时直接 FIRE）
      +10000 → 水位线推到 +9999，窗口[+0,+5000) 被清理
      +500   → 窗口已清理 + 超过 allowedLateness ⇒ 走 (b) 侧输出
    注意两点实际现象：
      1. 第二次输出时 Iterable 里是【整个窗口的全部元素】= 3 条（不是只有迟到的那一条），
         也就是同一窗口的结果被重复输出了 —— 下游必须能按窗口去重，这是 allowedLateness 的代价；
      2. 乱序检测会在"补算"时把 +0 再报一次，因为窗口内容被整体重放了（不是 bug，是语义）。
5-侧输出(late-data 窗口内乱序)> u1@+0ms(v=2)
    [迟到演示] key=u1 窗口 [+0ms, +5000ms) 水位线=+5999ms Iterable 元素数=2 -> [u1@+1000ms(v=1), u1@+0ms(v=2)]
5-主输出> [迟到演示] [+0ms, +5000ms) 内容=[u1@+1000ms(v=1), u1@+0ms(v=2)] watermark=+5999ms
5-侧输出(late-data 窗口内乱序)> u1@+0ms(v=2)
    [迟到演示] key=u1 窗口 [+0ms, +5000ms) 水位线=+5999ms Iterable 元素数=3 -> [u1@+1000ms(v=1), u1@+0ms(v=2), u1@+2000ms(v=4)]  <== 这是窗口被迟到元素"补算"后的再次输出
5-主输出> [迟到演示] [+0ms, +5000ms) 内容=[u1@+1000ms(v=1), u1@+0ms(v=2), u1@+2000ms(v=4)] watermark=+5999ms
    [迟到演示] key=u1 窗口 [+5000ms, +10000ms) 水位线=+9999ms Iterable 元素数=1 -> [u1@+6000ms(v=3)]
5-主输出> [迟到演示] [+5000ms, +10000ms) 内容=[u1@+6000ms(v=3)] watermark=+9999ms
5-侧输出(超过 allowedLateness)> u1@+500ms(v=6)
    [迟到演示] key=u1 窗口 [+10000ms, +15000ms) 水位线=+9223370336854775807ms Iterable 元素数=1 -> [u1@+10000ms(v=5)]
5-主输出> [迟到演示] [+10000ms, +15000ms) 内容=[u1@+10000ms(v=5)] watermark=+9223370336854775807ms
```

**这一节把两个出口都跑出来了**，对照如下：

| 出口 | 触发条件 | 本次的实证 |
|---|---|---|
| **主输出（窗口被"补算"，重复输出）** | 元素仍在 `allowedLateness` 内 → **补进窗口并再次触发** | `[+0ms,+5000ms)` 被输出了**两次**：第一次 2 条（`watermark=+5999ms`），第二次 3 条（含迟到的 `v=4`）。**同一窗口的结果被重复输出** |
| **侧输出 · 窗口内乱序** `5-侧输出(late-data 窗口内乱序)` | 元素比"本窗口已见过的最大时间戳"更早，但**窗口还没触发** → 正常入窗，同时用 `ctx.output(...)` 记一份 | `u1@+0ms(v=2)` 出现 **2 次**（比已见的 `+1000` 早） |
| **侧输出 · 超过 allowedLateness** `5-侧输出(超过 allowedLateness)` | 窗口状态**已被清理**且超出 `allowedLateness` → 无法补算，只能分流 | `u1@+500ms(v=6)` —— 此时 `[0,5000)` 已被清理（水位线 `+9999 > 4999+2000`） |

> ⚠️ **迟到数据的三个坑**：
> ① **配了 `allowedLateness` 就必须接受"同一个窗口被输出多次"**。本例 `[0,5000)` 输出了两次，且**第二次是整个窗口的全量内容（3 条），不是只补那一条**。下游必须幂等或按窗口去重。
> ② **`sideOutputLateData` 只在"窗口状态已清理"之后才生效**。只要还在 `allowedLateness` 内，数据会**进窗口并被补算**（走主输出），而不是进侧输出——两种"迟到"是**不同出口**。
> ③ **窗口内容会被整体重放**，所以"乱序检测"可能对同一条元素报两次（本例 `v=2` 报了 2 次）。这是语义而非 bug。

**demo 自己的选型结论**

```
  只做聚合 → .reduce / .aggregate（增量聚合，状态 = 1 个累加器/键/窗口）
  需要窗口元信息（起止时间、水位线、侧输出） → .aggregate(af, ProcessWindowFunction) 或 .reduce(rf, ProcessWindowFunction)
  需要遍历窗口内每一条原始元素 → .apply(WindowFunction) / .process(ProcessWindowFunction)，代价是全量元素常驻状态
```

### 8.3 `WindowAggregateDemo` —— 增量聚合三条路径 + `merge` 真触发

**1) `.reduce(ReduceFunction)`：就地归约，状态是 `ReducingState`**

```
=== 1) .reduce(ReduceFunction)：就地归约（输入、输出都是 Sale）===
    状态：ReducingState（每个 key+窗口只存"归约后的那一条"），不是 ListState
1-reduce>     [reduce] user=u1 归约结果 amount=60（输出时间戳 = 窗口 maxTimestamp，不是元素时间）
1-reduce>     [reduce] user=u2 归约结果 amount=100（输出时间戳 = 窗口 maxTimestamp，不是元素时间）
```

⭐ **输出时间戳 = 窗口 `maxTimestamp`**，不是元素时间。这与 §6.4 里"侧输出用 `window.maxTimestamp()`"一致 —— 窗口输出的时间戳语义是"**窗口结束时刻**"，下游做二次开窗要靠它。

**2) `.aggregate(AggregateFunction)`：`IN` / `ACC` / `OUT` 三类型可不同**

```
=== 2) .aggregate(AggregateFunction)：自己实现累加器（输入 Sale / 累加器 Stats / 输出 String 三类型可不同）===
    四个方法：createAccumulator / add（每来一条调一次）/ getResult（触发时调）/ merge（合并窗口时调）
2-aggregate>     [aggregate] count=3, sum=60, min=10, max=30, avg=20.00
2-aggregate>     [aggregate] count=2, sum=105, min=5, max=100, avg=52.50
```

这是 `AggregateFunction<IN, ACC, OUT>` 相对 `ReduceFunction` 的核心优势：**三者互相独立**。`ReduceFunction` 强制同类型（`reduce(T,T) -> T`），做不了"输入 `Sale` 明细 → 输出统计 `String`"。

**3) 内置 `.sum` / `.min` / `.max`：本质是内置 `ReduceFunction`**

```
=== 3) 内置聚合 .sum / .min / .max（字段表达式与位置表达式的两种写法）===
    POJO 用字段名 .sum("amount")；Tuple 用位置 .sum(1)
3a-sum>     [sum("amount")] user=u1 求和=60
3a-sum>     [sum("amount")] user=u2 求和=100
3b-min>     [min("amount")] 最小那条=u1@+0ms(10)
3b-min>     [min("amount")] 最小那条=u2@+2000ms(100)
3c-max>     [max("amount")] 最大那条=u1@+0ms(30)
3c-max>     [max("amount")] 最大那条=u2@+2000ms(100)
3d-sum-position>     [sum(1) 位置写法] (u1,30)（时间戳固定成 BASE，所以都落在第一个窗口）
3d-sum-position>     [sum(1) 位置写法] (u2,7)（时间戳固定成 BASE，所以都落在第一个窗口）
```

`min` / `max` **输出"取得最值的那一条完整记录"**（`u1@+0ms(10)`），不是最值本身 —— 这和 `sum`（输出数值）不同，容易误用。另外 **POJO 用字段名 `.sum("amount")`、Tuple 用位置 `.sum(1)`**，位置索引写错会静默算错列。

**4) `.aggregate(af, ProcessWindowFunction)`：推荐写法（`元素数=1`）**

```
=== 4) .aggregate(AggregateFunction, ProcessWindowFunction)：增量聚合 + 窗口元信息（推荐写法）===
    [aggregate+Process] key=u1 窗口 [+0ms, +5000ms) 元素数=1 -> [count=3, sum=60, min=10, max=30, avg=20.00]
4-aggregate+Process>     [aggregate+Process] key=u1 [+0ms, +5000ms) -> [count=3, sum=60, min=10, max=30, avg=20.00]
```

与 §8.2 第 4 点完全一致：**`元素数=1`**，同时拿到了窗口元信息。

**5) 会话窗口里的 `merge`：**这次真的被调用了** ⭐⭐**

```
=== 5) 会话窗口（合并窗口）里的 AggregateFunction.merge ===
    数据（gap=2s，key=u1，水位线压后 4s 以免窗口提前触发并清理）：
      +0    → 会话 A [0,2000)   累加器 {count=1, sum=10}
      +2500 → 会话 B [2500,4500) 累加器 {count=1, sum=20}
      +1500 → 【乱序】它同时落在 A 的 gap 和 B 的 gap 内 ⇒ 把两个【已存在】的窗口架桥合并
              ⇒ 这一步才会真正调用 AggregateFunction.merge（下面会打印 [merge] 行）
        [merge] AggregateFunction.merge 被调用: a=Stats{count=1, sum=10, min=10, max=10} + b=Stats{count=1, sum=20, min=20, max=20} -> Stats{count=2, sum=30, min=10, max=20}   （两个窗口的累加器在这里合并成一个）
    [会话+aggregate] key=u2 窗口 [+0ms, +2000ms) -> count=1, sum=100, min=100, max=100, avg=100.00
5-session-merge>     [会话+aggregate] key=u2 [+0ms, +2000ms) -> count=1, sum=100, min=100, max=100, avg=100.00
    [会话+aggregate] key=u1 窗口 [+0ms, +4500ms) -> count=3, sum=35, min=5, max=20, avg=11.67
5-session-merge>     [会话+aggregate] key=u1 [+0ms, +4500ms) -> count=3, sum=35, min=5, max=20, avg=11.67
    [会话+aggregate] key=u1 窗口 [+9000ms, +13000ms) -> count=2, sum=70, min=30, max=40, avg=35.00
5-session-merge>     [会话+aggregate] key=u1 [+9000ms, +13000ms) -> count=2, sum=70, min=30, max=40, avg=35.00
```

**`[merge] AggregateFunction.merge 被调用: a=Stats{count=1, sum=10, ...} + b=Stats{count=1, sum=20, ...} -> Stats{count=2, sum=30, ...}`** —— 这是 `merge` 真的被调用的现场证据。合并后 `key=u1` 的会话变成 **`[+0ms, +4500ms)`、`count=3, sum=35`**（`10 + 20 + 5`，第三条 `+1500` 元素也进来了），而 `min=5` 正是那条乱序元素的值。

调用链已在源码核实：

```java
// flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/HeapAggregatingState.java#L109-111
    @Override
    protected ACC mergeState(ACC a, ACC b) {
        return aggregateTransformation.aggFunction.merge(a, b);
    }
```

> ⚠️⚠️ **`merge` 有一个"骗人"的特性，本 demo 专门演示了它**：
>
> **会话窗口"吸收/延长"一个已有窗口时，`merge` 并不会被调用！** 只有"**一条元素同时桥接两个已经存在的窗口**"时才会调。原因是 `MergingWindowSet.addWindow` 会把"被吸收的那个已有窗口"选作合并目标并从 `mergedStateWindows` 里**排除掉**，于是 `AbstractHeapMergingState.mergeNamespaces(...)` 在 `sources` 为空时**直接 return**。
>
> 实践后果：**如果你只用"时间戳单调递增"的数据测会话窗口，`merge` 永远不会被调用**，写错了也测不出来（这正是本 demo 之前那一版的真实 bug —— 当时输出里一条 `[merge]` 都没有）。要真正测到 `merge`，必须构造"**乱序元素桥接两个已有窗口**"的数据，并且**把水位线压后**（本 demo 用 `perRecordWatermarks(4000L)`），否则窗口早就在水位线越过 `maxTimestamp` 时触发并清理，那条乱序元素会被当成迟到数据丢掉。
>
> **任何给会话窗口用的 `AggregateFunction`，都必须为 `merge` 写单测，且测试数据必须包含"桥接两个已有窗口"的乱序元素。**

**6) 状态占用对照：本节最有价值的部分**

| | 增量聚合 `.reduce` / `.aggregate` | 全量遍历 `.apply(WindowFunction)` / `.process(ProcessWindowFunction)` |
|---|---|---|
| 状态名 | `"window-contents"`（`WindowOperatorBuilder.java#L71`） | 同名 |
| 状态类型 | `ReducingState` / `AggregatingState` | `ListState<StreamRecord<IN>>` |
| 每个 key+窗口存 | **恰好 1 个累加器**（本例 4 个 long/int 字段） | 窗口内**全部**元素（含 `StreamRecord` 包装与时间戳开销） |
| 元素到达时 | **O(1) 就地合并**，元素本身立刻可丢 | append 到 `ListState`，触发时才读出遍历 |
| 输出时间戳 | 窗口 `maxTimestamp` | 窗口 `maxTimestamp` |

**量级直觉**：5 分钟窗口 × 每秒 1 万条 = 单窗口 **300 万条**。全量路径要扛 300 万条元素的状态，增量路径只扛 1 个累加器 —— 这就是"**优先用 `reduce` / `aggregate`**"的全部理由。

> ⚠️ **一旦调用 `.evictor(...)`**，算子变成 `EvictingWindowOperator`，窗口内容**强制**为 `ListState<StreamRecord<T>>`，**增量聚合直接失效**（`WindowOperatorBuilder.java#L293` 的 `buildEvictingWindowOperator`）。

> **实跑结论（诚实标注）**：`window/WindowAssignersDemo`、`window/WindowFunctionsDemo`、`window/WindowAggregateDemo` 三个 demo 均在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**。本文所有输出均取自**最后一次干净运行**（在 demo 源码定稿之后采集，不存在"文档与代码不同步"）。`currentProcessingTime` / `currentWatermark` 的具体数值每次运行不同，文中数值取自该次运行。
