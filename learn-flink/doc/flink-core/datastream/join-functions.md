# 可用于 Join 的函数与算子（JoinFunction / CoGroupFunction / ProcessJoinFunction）

> **代码基线**：Apache Flink 1.20.4　**源码位置**：仓库根目录 `flink-1.20-source/`
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/join/`
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)
> 下文缩写：`SJ` = `flink-streaming-java/src/main/java/org/apache/flink/streaming/api`；`CORE` = `flink-core/src/main/java/org/apache/flink/api/common/functions`

---

# 一、三类 Join 与四个用户函数

| Join 方式 | 用户函数 | 位置 | 语义 | 底层算子 |
|---|---|---|---|---|
| **窗口 Join** | `JoinFunction` / `FlatJoinFunction` | `CORE/JoinFunction.java#L28`、`CORE/FlatJoinFunction.java#L32` | 同 key 同窗口内配对（**inner join**） | `WindowOperator`（由 coGroup 改写而来） |
| **CoGroup** | `CoGroupFunction` | `CORE/CoGroupFunction.java#L33` | 同 key 同窗口内**成对 / 仅左 / 仅右**都能输出（可做 outer join） | `WindowOperator` |
| **Interval Join** | `ProcessJoinFunction` | `SJ/functions/co/ProcessJoinFunction.java#L38` | 同 key、**时间差在 ±Δ 内**即配对（可跨窗口边界） | `IntervalJoinOperator`（`SJ/operators/co/`） |

> ⚠️ **三处路径易错**（已核实）：
> ① `JoinFunction` / `FlatJoinFunction` / `CoGroupFunction` **不在** `SJ/api/functions/co/`，而在 **`flink-core` 的 `api/common/functions`**（依据：`JoinedStreams.java#L23` 的 import 就是 `org.apache.flink.api.common.functions.*`）。
> ② `IntervalJoinOperator` **不在** `runtime/operators/`，而在 **`SJ/operators/co/IntervalJoinOperator.java#L84`**。
> ③ `IntervalJoin` / `IntervalJoined` **不是独立文件**，是 `KeyedStream` 的静态内部类（`KeyedStream.java#L451` / `#L547`）。

---

# 二、窗口 Join：`JoinedStreams`

## 2.1 API 链

`DataStream.join(other)`（`DataStream.java#L749`）→ `.where(KeySelector)`（`JoinedStreams.java#L94`）→ `.equalTo(KeySelector)`（`#L139`）→ `.window(WindowAssigner)`（`#L184`，返回 `WithWindow` `#L212`）→ `.apply(JoinFunction)`（`#L363`）/ `.apply(FlatJoinFunction)`（`#L451`）。

还可以链 `.trigger(...)`（`#L298`）、`.evictor(...)`、`.allowedLateness(...)`（`#L342`）。

## 2.2 ★ 核心结论：`join` 是用 `coGroup` 实现的

```java
// JoinedStreams.java#L494
        public <T> DataStream<T> apply(
                JoinFunction<T1, T2, T> function, TypeInformation<T> resultType) {
            // clean the closure
            function = input1.getExecutionEnvironment().clean(function);

            coGroupedWindowedStream =
                    input1.coGroup(input2)
                            .where(keySelector1)
                            .equalTo(keySelector2)
                            .window(windowAssigner)
                            .trigger(trigger)
                            .evictor(evictor)
                            .allowedLateness(allowedLateness);

            return coGroupedWindowedStream.apply(new JoinCoGroupFunction<>(function), resultType);
        }
```

```java
// JoinedStreams.java#L551
    /** CoGroup function that does a nested-loop join to get the join result. */
    private static class JoinCoGroupFunction<T1, T2, T>
            extends WrappingFunction<JoinFunction<T1, T2, T>>
            implements CoGroupFunction<T1, T2, T> {
        private static final long serialVersionUID = 1L;

        public JoinCoGroupFunction(JoinFunction<T1, T2, T> wrappedFunction) {
            super(wrappedFunction);
        }

        @Override
        public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out)
                throws Exception {
            for (T1 val1 : first) {
                for (T2 val2 : second) {
                    out.collect(wrappedFunction.join(val1, val2));
                }
            }
        }
    }
```

**结论（有源码依据，不是推测）**：`join` **不是独立算子**。它把 `JoinedStreams` 改写成 `CoGroupedStreams.WithWindow`（字段 `coGroupedWindowedStream`，`#L229`），并把 `JoinFunction` 包成 `CoGroupFunction`，函数体是**双重循环**。

**inner join 语义的来源**：不是显式 filter 算子，而是"**某一侧集合为空 → 双重循环体一次都不执行**"的副作用。`JoinFunction` 的 javadoc（`CORE/JoinFunction.java#L31`）原文也说明了这一点：*"joins follows strictly the semantics of an 'inner join' … elements are filtered out"*，并提示 *"You can use a CoGroupFunction to perform an outer join"*。

## 2.3 两个函数接口的差别

| | `JoinFunction<IN1,IN2,OUT>` | `FlatJoinFunction<IN1,IN2,OUT>` |
|---|---|---|
| 方法 | `OUT join(IN1 first, IN2 second)` | `void join(IN1 first, IN2 second, Collector<OUT> out)` |
| 输出条数 | **恰好 1 条** | **0 / 1 / N 条** |
| 类型信息 | 可推断 | 需自己给 `resultType`（`apply(f, TypeInformation)` `#L494`），否则 `TypeExtractor` 推不出 |

> ⚠️ 两侧 key 的 `TypeInformation` 必须相等，否则 `equalTo` 抛 `IllegalArgumentException`（`#L158`）。
>
> ⚠️ `apply(...)` 返回 `DataStream<T>`，**无法设算子级并行度**；`with(...)`（`#L393`）返回 `SingleOutputStreamOperator` 但已 `@Deprecated`。
>
> ⚠️ 只支持 keyed join（内部 `input1.coGroup`），**非 keyed join 在 1.20 不可用**。

---

# 三、CoGroup：`CoGroupedStreams`

## 3.1 API 链

`DataStream.coGroup(other)`（`DataStream.java#L741`）→ `.where`（`CoGroupedStreams.java#L107`）→ `.equalTo`（`#L136`）→ `.window`（`#L198`）→ `.apply(CoGroupFunction)`（`#L373`）/ `.apply(f, resultType)`（`#L408`）。

## 3.2 实现：union + tag + 单窗口算子

```java
// CoGroupedStreams.java#L428
            DataStream<TaggedUnion<T1, T2>> unionStream = taggedInput1.union(taggedInput2);

            // we explicitly create the keyed stream to manually pass the key type information in
            windowedStream =
                    new KeyedStream<TaggedUnion<T1, T2>, KEY>(
                                    unionStream, unionKeySelector, keyType)
                            .window(windowAssigner);

            if (trigger != null) {
                windowedStream.trigger(trigger);
            }
            if (evictor != null) {
                windowedStream.evictor(evictor);
            }
            if (allowedLateness != null) {
                windowedStream.allowedLateness(allowedLateness);
            }

            return windowedStream.apply(
                    new CoGroupWindowFunction<T1, T2, T, KEY, W>(function), resultType);
```

```java
// CoGroupedStreams.java#L846
        @Override
        public void apply(KEY key, W window, Iterable<TaggedUnion<T1, T2>> values, Collector<T> out)
                throws Exception {

            List<T1> oneValues = new ArrayList<>();
            List<T2> twoValues = new ArrayList<>();

            for (TaggedUnion<T1, T2> val : values) {
                if (val.isOne()) {
                    oneValues.add(val.getOne());
                } else {
                    twoValues.add(val.getTwo());
                }
            }
            wrappedFunction.coGroup(oneValues, twoValues, out);
        }
```

**关键点**：

| 要点 | 说明 |
|---|---|
| 作业图上多出 3 个节点 | **2 个 `map`（`Input1Tagger` / `Input2Tagger`）+ 1 个 `union`**（`#L418`） |
| 为什么用 union | `WindowOperator` 是 `OneInputStreamOperator`，只能有一个输入 |
| `TaggedUnion` 的判定 | `isOne()` 靠 `one != null`（`#L493`）→ **用户元素本身不能是 null** |
| 状态 | 缓冲原始元素（`ListStateDescriptor<T>`），内存/状态压力比增量聚合大 |

## 3.3 语义差别（javadoc 原文）

`CoGroupFunction` javadoc（`CORE/CoGroupFunction.java#L28`）：*"If a key is present in only one of the two inputs, it may be that one of the groups is empty"*，且 *"the CoGroup function is invoked with in empty input for the side … that did not contain elements"*（`#L48`）。

**一句话**：`coGroup` 是更底层的原语，能表达 left / right / full outer join；`join` 只是它的 inner-join 特化。

---

# 四、Interval Join：`IntervalJoinOperator`

## 4.1 API 链

`KeyedStream.intervalJoin(otherKeyed)`（`KeyedStream.java#L440`）→ `.inEventTime()`（`#L477`）/ `.inProcessingTime()`（`#L483`）→ `.between(Duration, Duration)`（`#L524`）→ `.lowerBoundExclusive()`（`#L594`）/ `.upperBoundExclusive()`（`#L587`）/ `.sideOutputLeftLateData(...)`（`#L604`）/ `.sideOutputRightLateData(...)`（`#L615`）→ `.process(ProcessJoinFunction)`（`#L630`）。

## 4.2 装配：走 ConnectedStreams

```java
// KeyedStream.java#L669
            final IntervalJoinOperator<KEY, IN1, IN2, OUT> operator =
                    new IntervalJoinOperator<>(
                            lowerBound,
                            upperBound,
                            lowerBoundInclusive,
                            upperBoundInclusive,
                            leftLateDataOutputTag,
                            rightLateDataOutputTag,
                            left.getType()
                                    .createSerializer(
                                            left.getExecutionConfig().getSerializerConfig()),
                            right.getType()
                                    .createSerializer(
                                            right.getExecutionConfig().getSerializerConfig()),
                            cleanedUdf);

            return left.connect(right)
                    .keyBy(keySelector1, keySelector2)
                    .transform("Interval Join", outputType, operator);
```

## 4.3 状态结构：两个 `MapState`，无 TTL

```java
// IntervalJoinOperator.java#L167
        this.leftBuffer =
                context.getKeyedStateStore()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        LEFT_BUFFER,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(
                                                new BufferEntrySerializer<>(leftTypeSerializer))));

        this.rightBuffer =
                context.getKeyedStateStore()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        RIGHT_BUFFER,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(
                                                new BufferEntrySerializer<>(rightTypeSerializer))));
```

字段声明在 `#L105`，常量 `LEFT_BUFFER` / `RIGHT_BUFFER` 在 `#L92`。**注意：没有 `StateTtlConfig`**（全文无该 import）—— 过期完全靠**定时器**。

## 4.4 配对扫描 + 注册清理定时器

```java
// IntervalJoinOperator.java#L243
        for (Map.Entry<Long, List<BufferEntry<OTHER>>> bucket : otherBuffer.entries()) {
            final long timestamp = bucket.getKey();

            if (timestamp < ourTimestamp + relativeLowerBound
                    || timestamp > ourTimestamp + relativeUpperBound) {
                continue;
            }

            for (BufferEntry<OTHER> entry : bucket.getValue()) {
                if (isLeft) {
                    collect((T1) ourValue, (T2) entry.element, ourTimestamp, timestamp);
                } else {
                    collect((T1) entry.element, (T2) ourValue, timestamp, ourTimestamp);
                }
            }
        }

        long cleanupTime =
                (relativeUpperBound > 0L) ? ourTimestamp + relativeUpperBound : ourTimestamp;
        if (isLeft) {
            internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_LEFT, cleanupTime);
        } else {
            internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_RIGHT, cleanupTime);
        }
```

```java
// IntervalJoinOperator.java#L307
    public void onEventTime(InternalTimer<K, String> timer) throws Exception {
        long timerTimestamp = timer.getTimestamp();
        String namespace = timer.getNamespace();
        switch (namespace) {
            case CLEANUP_NAMESPACE_LEFT:
                {
                    long timestamp =
                            (upperBound <= 0L) ? timerTimestamp : timerTimestamp - upperBound;
                    leftBuffer.remove(timestamp);
                    break;
                }
            case CLEANUP_NAMESPACE_RIGHT:
                {
                    long timestamp =
                            (lowerBound <= 0L) ? timerTimestamp + lowerBound : timerTimestamp;
                    rightBuffer.remove(timestamp);
                    break;
                }
```

## 4.5 输出

```java
// IntervalJoinOperator.java#L283
    private void collect(T1 left, T2 right, long leftTimestamp, long rightTimestamp)
            throws Exception {
        final long resultTimestamp = Math.max(leftTimestamp, rightTimestamp);

        collector.setAbsoluteTimestamp(resultTimestamp);
        context.updateTimestamps(leftTimestamp, rightTimestamp, resultTimestamp);

        userFunction.processElement(left, right, context, collector);
    }
```

`ProcessJoinFunction.Context` 提供 `getLeftTimestamp()`（`#L67`）/ `getRightTimestamp()`（`#L70`）/ `getTimestamp()`（`#L73`）。

## 4.6 约束与易错点（逐条有源码依据）

| # | 约束 | 依据 |
|---|---|---|
| 1 | **只支持事件时间** | `between()` 里 `if (timeBehaviour != TimeBehaviour.EventTime) throw new UnsupportedTimeCharacteristicException("Time-bounded stream joins are only supported in event time")`（`KeyedStream.java#L525`）；`inProcessingTime()` 之后**无法** `between`；`onProcessingTime()`（`IntervalJoinOperator.java#L337`）是**空实现** |
| 2 | **没有 `allowedLateness`** | `grep` 在 `KeyedStream.java` 与 `IntervalJoinOperator.java` 均无命中；迟到只由水位线决定：`isLate(ts) = ts < currentWatermark`（`#L265`），迟到元素走侧输出后 `return`，**不入 buffer**（`#L270`） |
| 3 | **区间闭合性靠构造器整体偏移边界** | `this.lowerBound = lowerBoundInclusive ? lowerBound : lowerBound + 1L; this.upperBound = upperBoundInclusive ? upperBound : upperBound - 1L;`（`#L146`）—— `lowerBoundExclusive()` + 同值边界时区间可能变空，表现为"莫名其妙没结果" |
| 4 | 构造器强制 `lowerBound <= upperBound` | `#L141` |
| 5 | 时间戳为 `Long.MIN_VALUE` 直接抛 `FlinkException` | `#L228` —— event-time interval join 必须有真实时间戳 |
| 6 | 状态按**时间戳**分桶 | bucket key = 事件时间，清理是精确 `remove(timestamp)` 而非 TTL 扫描 |

---

# 五、窗口 join 的运行时：落到哪个算子

`JoinedStreams.WithWindow.apply` → `CoGroupedStreams.WithWindow.apply` → `WindowedStream.apply(WindowFunction)`（`WindowedStream.java#L563` / `#L581`），后者只负责造算子：

```java
// WindowedStream.java#L585
        final String opName = builder.generateOperatorName();
        final String opDescription = builder.generateOperatorDescription(function, null);
        OneInputStreamOperator<T, R> operator = builder.apply(function);

        return input.transform(opName, resultType, operator).setDescription(opDescription);
```

`builder.apply(WindowFunction)` 先把用户函数包成 `InternalIterableWindowFunction`（`WindowOperatorBuilder.java#L253`），再决定算子与状态形态：

```java
// WindowOperatorBuilder.java#L277
    private <ACC, R> WindowOperator<K, T, ACC, R, W> buildWindowOperator(
            StateDescriptor<? extends AppendingState<T, ACC>, ?> stateDesc,
            InternalWindowFunction<ACC, R, K, W> function) {

        return new WindowOperator<>(
                windowAssigner,
                windowAssigner.getWindowSerializer(config),
                keySelector,
                keyType.createSerializer(config.getSerializerConfig()),
                stateDesc,
                function,
                trigger,
                allowedLateness,
                lateDataOutputTag);
    }
```

| 条件 | 算子 | 状态 |
|---|---|---|
| 无 `evictor` | `WindowOperator` | `ListStateDescriptor<T>`（名 `"window-contents"`，`#L71`） |
| 有 `evictor` | `EvictingWindowOperator`（`EvictingWindowOperator.java#L63`） | `ListStateDescriptor<StreamRecord<T>>` |

**keyed state 的逻辑结构是 `(key, window) -> 元素列表`**：`WindowOperator` 的窗口内容以 **`Window` 为 namespace**（`WindowOperator.java#L150` 注释 *"Each window is a namespace"*）；merging assigner（session window）额外用 `ListState` 存合并集（`"merging-window-set"`，`#L259`）。

---

# 六、和 `ConnectedStreams` 手写 join 的区别

```java
// ConnectedStreams.java#L411
        if ((inputStream1 instanceof KeyedStream) && (inputStream2 instanceof KeyedStream)) {
            operator = new KeyedCoProcessOperator<>(inputStream1.clean(keyedCoProcessFunction));
        } else {
            throw new UnsupportedOperationException(
                    "KeyedCoProcessFunction can only be used "
                            + "when both input streams are of type KeyedStream.");
        }
```

`ConnectedStreams.keyBy(...).process(KeyedCoProcessFunction)` **没有窗口、没有区间、没有自动配对**：你拿到 `processElement1/2` 两条回调，必须**自己用 state 缓存对方侧元素并自己决定何时清理**。漏了清理就状态无限增长 —— interval join 帮你做了定时清理（见 §4.4），这是它不可替代的价值。

## 选型表

| 场景 | 选择 |
|---|---|
| 同 key、同一窗口内配对，只要匹配成功的行 | `join` |
| 同 key、同一窗口内要输出"仅左 / 仅右 / 成对"（outer join） | `coGroup` |
| 同 key、时间上有 ±Δ 区间约束，且希望**跨窗口边界**匹配 | `intervalJoin`（仅事件时间） |
| 匹配规则不是时间/窗口（"最近 N 条""按业务 id 查表""自定义定时触发"） | `ConnectedStreams.keyBy().process()` 手写 |
| 一条流广播规则、另一条流按规则处理 | `BroadcastStream`（非配对语义） |

---

# 七、易错点小结

| # | 易错点 | 正解 |
|---|---|---|
| 1 | 以为 `join` 有独立算子 | 它就是 `coGroup` + 双重循环，作业图上是 `map→map→union→WindowOperator` |
| 2 | `JoinFunction` 想输出多条 | 用 `FlatJoinFunction`（并显式给 `resultType`） |
| 3 | `coGroup` 里元素为 null | `TaggedUnion.isOne()` 靠 `one != null` 判定，**null 元素会串到另一侧** |
| 4 | interval join 用了处理时间 | 直接抛 `UnsupportedTimeCharacteristicException` |
| 5 | 依赖 interval join 的 `allowedLateness` | 不存在该参数；迟到走侧输出 |
| 6 | `lowerBoundExclusive()` + 同值边界 | 构造器把边界整体偏移 1，区间可能为空 |
| 7 | interval join 数据没有真实时间戳 | 抛 `FlinkException` |
| 8 | 手写 `ConnectedStreams` join 不清状态 | 状态无限增长；或直接用 `intervalJoin` |

---

# 八、配套 demo 与真实输出

| demo | 覆盖 | 运行 |
|---|---|---|
| `join/WindowJoinDemo` | `JoinFunction` + `FlatJoinFunction`（窗口 join，inner 语义） | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.join.WindowJoinDemo` |
| `join/CoGroupDemo` | `CoGroupFunction` 输出"成对 / 仅左 / 仅右" | `... -Dmain.class=com.jiaqiz.flink.join.CoGroupDemo` |
| `join/IntervalJoinDemo` | `intervalJoin + ProcessJoinFunction`（含跨窗口边界匹配） | `... -Dmain.class=com.jiaqiz.flink.join.IntervalJoinDemo` |

**demo 数据构造要点**（只有这样才能同时看到"匹配 / 不匹配"）：

| 数据 | 现象 |
|---|---|
| 两侧同 key、时间差 500ms | 三种 join 全部匹配 |
| 只有左侧 | 窗口 join 丢弃；coGroup 输出 `LEFT`；interval join 无输出 |
| 只有右侧 | 窗口 join 丢弃；coGroup 输出 `RIGHT` |
| 左 4900（窗口 `[0,5000)`）、右 5100（窗口 `[5000,10000)`） | **窗口 join 因窗口边界失配而不匹配**，但 **interval join 因 dt=200ms 仍匹配** —— 这是 interval join 唯一不可替代之处 |

### 8.1 `WindowJoinDemo`：`JoinFunction` vs `FlatJoinFunction`（本地 MiniCluster 实跑通过，退出码 0）

输入（双向都在同一 5s 滚动窗口体系下）：

```
用户流: id=1(ts=1000), id=2(ts=1500), id=3(ts=2600,无订单), id=1(ts=3000,资料更新), id=4(ts=6500)
订单流: id=1(ts=1200), id=2(ts=1800), id=1(ts=3000), id=9(ts=4200,无用户), id=4(ts=6500), id=9(ts=8800,无用户)
```

```
======== Window Join 演示 ========
窗口: TumblingEventTimeWindows.of(Time.seconds(5)) → [0,5000) 与 [5000,10000)
预期: JoinFunction 共 6 对（每对 1 条）；FlatJoinFunction 共 5 条（金额<50 的 2 对被过滤，金额>=200 的那对多输出 1 条风控）；
      id=3(用户无订单) 与 id=9(订单无用户) 永远不出现 → 窗口 join 是 inner join。
[FlatJoin-明细] win[0,5000) 订单O-1001(金额99.5) ← 用户1-v1
[JoinFunction] win[0,5000) 订单O-1001(金额99.5, ts=1200) × 用户1-v1
[FlatJoin-明细] win[0,5000) 订单O-1001(金额99.5) ← 用户1-v2
[JoinFunction] win[0,5000) 订单O-1003(金额30.0, ts=3000) × 用户1-v1
[JoinFunction] win[0,5000) 订单O-1001(金额99.5, ts=1200) × 用户1-v2
[JoinFunction] win[0,5000) 订单O-1003(金额30.0, ts=3000) × 用户1-v2
[FlatJoin-明细] win[0,5000) 订单O-1002(金额200.0) ← 用户2
[JoinFunction] win[0,5000) 订单O-1002(金额200.0, ts=1800) × 用户2
[FlatJoin-风控] 订单O-1002 金额200.0 超过 200，人工复核（用户2）
[JoinFunction] win[5000,10000) 订单O-1005(金额66.6, ts=6500) × 用户4
[FlatJoin-明细] win[5000,10000) 订单O-1005(金额66.6) ← 用户4
```

**三个从输出里直接可读出的结论**：

1. **窗口 join 是 inner join**：`id=3`（用户无订单）与 `id=9`（订单无用户，两个窗口各一次）**在输出里完全不出现**。这是因为 `JoinedStreams` 最终被改写成 `CoGroupedStreams` + `JoinCoGroupFunction`（见 §三），而后者只在**两侧都非空**时才调用户函数——即"inner 语义 = 对空集合那一侧不做事"。
2. **笛卡尔积会放大条数**：`id=1` 在 `[0,5000)` 内左 2 条（`v1`、`v2`）× 右 2 条（`O-1001`、`O-1003`）= **4 对**，`JoinFunction` 就输出 4 条。**窗口内同 key 的记录是两两组合的**，这是窗口 join 最容易在生产上爆量的地方。
3. **`FlatJoinFunction` 用"发 0 条 / 多发"实现了过滤与分流**：金额 < 50 的 2 对被过滤（**一条都不 collect**，效果等价于内层过滤）；金额 ≥ 200 的那对除了明细还**多输出一条 `[FlatJoin-风控]`**。对比 `JoinFunction` 的 SAM 是 `void join(T1, T2, Collector<OUT>)`——它**也有 Collector**，所以同样能发 0..N 条，两者差别只在"是否需要读全窗口"（`FlatJoinFunction` 名字里的 Flat 指的是**这个**，不是 flatMap 那层的含义）。

### 8.2 `CoGroupDemo`：coGroup 才能做 outer join（本地 MiniCluster 实跑通过，退出码 0）

```
======== CoGroup 演示 ========
窗口: TumblingEventTimeWindows.of(Time.seconds(5))
用户流: id=1(ts=1000), id=2(ts=1500), id=3(ts=2600,无订单), id=4(ts=6500)
订单流: id=1(ts=1200), id=2(ts=1800), id=1(ts=3000), id=9(ts=4200,无用户), id=4(ts=6500), id=9(ts=8800,无用户)
预期: 双侧匹配 3 组(key 1/2/4)；仅左 1 组(key 3)；仅右 2 组(key 9 在两个窗口各一次)。
[双侧  ] win[0,5000) key=1 左1条 × 右2条，订单=O-1001, O-1003，合计金额=129.5 → coGroup 里可以自己实现 left/right/full outer
[双侧  ] win[0,5000) key=2 左1条 × 右1条，订单=O-1002，合计金额=200.0 → coGroup 里可以自己实现 left/right/full outer
[仅右/右外] win[0,5000) 没有用户记录的订单 1 条，订单号=O-1004(无用户)
[仅左/左外] win[0,5000) 用户3(无订单)(userId=3) 窗口内没有订单
[双侧  ] win[5000,10000) key=4 左1条 × 右1条，订单=O-1005，合计金额=66.6 → coGroup 里可以自己实现 left/right/full outer
[仅右/右外] win[5000,10000) 没有用户记录的订单 1 条，订单号=O-1006(无用户)
```

**与 8.1 的对照就是"inner vs outer"的全部差别**：同一份数据，窗口 join 只输出 3 组（`key=1/2/4`），coGroup 输出 **6 条**——多出来的正是"仅左 1 组 + 仅右 2 组"。`CoGroupFunction.coGroup(Iterable<T1>, Iterable<T2>, Collector<O>)` **两侧都非空 / 只有一侧非空时都会被调用**，所以：

```
只判 iterable.iterator().hasNext() 的组合 → 自己实现 left / right / full outer
```

代价：**每一侧都要在 `Iterable` 里判定"是否为空 + 是否需要遍历"**，比 `JoinFunction` 多一层手工逻辑。生产经验是——**要 inner join 就用 `.join()`，要 outer join 才用 `.coGroup()`**。

### 8.3 `IntervalJoinDemo`：跨窗口边界仍能匹配（本地 MiniCluster 实跑通过，退出码 0）

同一份数据，只改 `between` 的下界/上界跑两遍，就能看出区间是**在匹配时实时计算**的：

```
======== Interval Join 演示: between(-2s, +2s) ========
下单流(左, key=订单号): A@1000ms, B@5000ms, C@9000ms, D@13000ms
支付流(右, key=订单号): A@2000(+1000), A@5000(+4000), B@3000(-2000), B@8000(+3000), D@12500(-500), E@20000(左流无E)
[匹配] 订单A(金额100.0, ts=1000) ← 支付渠道ALIPAY(ts=2000)，右-左 = +1000ms ∈ [-2000ms, +2000ms]
[匹配] 订单B(金额200.0, ts=5000) ← 支付渠道ALIPAY(ts=3000)，右-左 = -2000ms ∈ [-2000ms, +2000ms]
[匹配] 订单D(金额400.0, ts=13000) ← 支付渠道WECHAT(ts=12500)，右-左 = -500ms ∈ [-2000ms, +2000ms]

======== Interval Join 演示: between(-2s, +5s) ========
[匹配] 订单A(金额100.0, ts=1000) ← 支付渠道ALIPAY(ts=2000)，右-左 = +1000ms ∈ [-2000ms, +5000ms]
[匹配] 订单B(金额200.0, ts=5000) ← 支付渠道ALIPAY(ts=3000)，右-左 = -2000ms ∈ [-2000ms, +5000ms]
[匹配] 订单A(金额100.0, ts=1000) ← 支付渠道WECHAT(ts=5000)，右-左 = +4000ms ∈ [-2000ms, +5000ms]
[匹配] 订单B(金额200.0, ts=5000) ← 支付渠道WECHAT(ts=8000)，右-左 = +3000ms ∈ [-2000ms, +5000ms]
[匹配] 订单D(金额400.0, ts=13000) ← 支付渠道WECHAT(ts=12500)，右-左 = -500ms ∈ [-2000ms, +5000ms]
```

**四个关键点，每条都能对上 `IntervalJoinOperator` 的实现**：

| 观察 | 证据 | 机制 |
|---|---|---|
| **边界是闭区间** | `B@5000 ← ALIPAY@3000` 的 `右-左 = -2000ms` **正好落在下界上，仍然匹配** | `IntervalJoinOperator` 里 `between` 的下/上界带 `lowerBoundaryInclusive` / `upperBoundaryInclusive` 开关，默认包含端点 |
| **放宽上界会多出匹配** | 从 `+2s` 改到 `+5s`，多出 **2 对**（`A@1000 ← WECHAT@5000` 的 +4000ms、`B@5000 ← WECHAT@8000` 的 +3000ms） | 匹配在**元素到达时实时判定**，不在窗口触发时批量算 |
| **`E@20000` 从不匹配** | 左流（下单流）没有 key=E，输出里完全没有 E | interval join **仍然是 inner join**：必须两侧都有同 key 的记录 |
| **跨"窗口边界"照样匹配** | `D@13000 ← WECHAT@12500` 落在 `[10000,15000)` 窗口内，而 `A@1000 ← WECHAT@5000` 横跨了 `[0,5000)` 与 `[5000,10000)` 的边界 | ⭐ **这是 interval join 唯一不可替代之处**：它**不按窗口切分**，只按时间差判定，所以"左在窗口 N、右在窗口 N+1"照样能配上；而 8.1 的窗口 join 会把它们分到不同窗口从而**永远配不上** |

> **实现层面的代价（对应 §七）**：`IntervalJoinOperator` 内部是**两个 `MapState`**（左、右各一份，按 key 存 `timestamp → 记录列表`），并且**只支持事件时间**（处理时间区间 join 语义上无意义）。清理靠**定时器**：每来一条记录就为"过期时间点"注册 timer，到点把该 key 两侧的过期条目删掉——**所以它是有状态的、需要 checkpoint 的**，长时间不用 `between` 收窄区间会导致状态无限增长。

> **实跑结论（诚实标注）**：`join/WindowJoinDemo`、`join/CoGroupDemo`、`join/IntervalJoinDemo` 三个 demo 均在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**；输出已剔除 Flink 的 `WARN` 日志行，业务输出未改动。
