# ProcessFunction 家族（含定时器、侧输出、广播状态）

> **代码基线**：Apache Flink 1.20.4　**源码位置**：仓库根目录 `flink-1.20-source/`
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/process/`
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)
> 下文缩写：`SJ` = `flink-streaming-java/src/main/java/org/apache/flink/streaming/api`

---

# 一、家族全景：四条支线

`ProcessFunction` 家族是 DataStream API 里**能力最强**的一层：能访问时间、注册定时器、读写状态、发侧输出。所有成员都是 `AbstractRichFunction` 子类，按"流的形态"分四条支线：

| 支线 | 函数 | 路径 | 落到哪个算子 |
|---|---|---|---|
| **单流** | `ProcessFunction<I,O>` | `SJ/functions/ProcessFunction.java#L52` | `ProcessOperator` |
| | `KeyedProcessFunction<K,I,O>` | `SJ/functions/KeyedProcessFunction.java#L53` | `KeyedProcessOperator` |
| **双流** | `CoProcessFunction<IN1,IN2,OUT>` | `SJ/functions/co/CoProcessFunction.java#L48` | `CoProcessOperator` |
| | `KeyedCoProcessFunction<K,IN1,IN2,OUT>` | `SJ/functions/co/KeyedCoProcessFunction.java#L48` | `KeyedCoProcessOperator` |
| **窗口** | `ProcessWindowFunction<IN,OUT,KEY,W>` | `SJ/functions/windowing/ProcessWindowFunction.java#L38` | `WindowOperator` + `InternalIterableProcessWindowFunction` |
| | `ProcessAllWindowFunction<IN,OUT,W>` | `SJ/functions/windowing/ProcessAllWindowFunction.java#L37` | 同上（非 keyed 分支） |
| **Join** | `ProcessJoinFunction<IN1,IN2,OUT>` | `SJ/functions/co/ProcessJoinFunction.java#L38` | `IntervalJoinOperator`（`SJ/operators/co/`） |
| **广播** | `BroadcastProcessFunction<IN1,IN2,OUT>` | `SJ/functions/co/BroadcastProcessFunction.java#L54` | `CoBroadcastWithNonKeyedOperator` |
| | `KeyedBroadcastProcessFunction<KS,IN1,IN2,OUT>` | `SJ/functions/co/KeyedBroadcastProcessFunction.java#L61` | `CoBroadcastWithKeyedOperator` |

> ⚠️ **三处最容易搞错的事实**（都已核实）：
> ① **算子不在 `runtime/operators/`**，而在 **`SJ/operators/`**（`ProcessOperator` / `KeyedProcessOperator` / `co/CoProcessOperator` …）；`runtime/operators/` 放的是 `StreamMap` / `StreamFilter` / `AsyncWaitOperator` / `windowing/*` 这类。
> ② **广播算子叫 `CoBroadcastWithNonKeyedOperator` / `CoBroadcastWithKeyedOperator`**，不是 `BroadcastOperator`。
> ③ **1.20 没有 `ProcessWindowOperator`**：窗口 + Process 由通用的 `WindowOperator` 承担（`SJ/runtime/operators/windowing/WindowOperator.java#L101`）。

---

# 二、单流：`ProcessFunction` 与 `KeyedProcessFunction`

## 2.1 接口契约

```java
// KeyedProcessFunction.java#L53
public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {
    // ...（省略 serialVersionUID 与 javadoc）
    public abstract void processElement(I value, Context ctx, Collector<O> out) throws Exception;
    // ...（省略 onTimer 的 javadoc）
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {}
```

| 能力 | `ProcessFunction` | `KeyedProcessFunction` |
|---|---|---|
| `processElement` | `#L70` | `#L71` |
| `onTimer` | `#L84` | `#L85` |
| 上下文 | `Context`（`#L90`）：`timestamp()` `#L98`、`timerService()` `#L101`、`output(OutputTag,X)` `#L109` | 多一个 `getCurrentKey()`（`Context` `#L113`、`OnTimerContext` `#L125`） |
| `OnTimerContext.timeDomain()` | `#L117` | 同名（用于区分事件时间/处理时间定时器） |

> ⚠️ **1.20 的 `processElement` 没有 key 参数** —— key 要用 `ctx.getCurrentKey()` 取。Flink 2.x 才把它改成 `processElement(K key, I value, ...)`，**照 2.x 文档写 demo 会编译失败**。
>
> ⚠️ 这些 `Context` / `OnTimerContext` 都是**非静态内部抽象类**（`ProcessFunction<IN,OUT>.Context`），用户只能拿接口引用。

## 2.2 运行时算子

```java
// ProcessOperator.java#L62
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        context.element = element;
        userFunction.processElement(element.getValue(), context, collector);
        context.element = null;
    }
```

```java
// ProcessOperator.java#L98
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }
            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }
```

```java
// KeyedProcessOperator.java#L53
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);
        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
        TimerService timerService = new SimpleTimerService(internalTimerService);
        context = new ContextImpl(userFunction, timerService);
        onTimerContext = new OnTimerContextImpl(userFunction, timerService);
    }
```

三个关键差异：

| | `ProcessOperator`（非 keyed） | `KeyedProcessOperator`（keyed） |
|---|---|---|
| 基类 | `AbstractUdfStreamOperator<OUT, ProcessFunction<IN,OUT>>`（`#L35`） | 同左，泛型换成 `KeyedProcessFunction`（`#L35`） |
| 定时器 | **没有** `InternalTimerService`；`ContextImpl` 虽 `implements TimerService`，但注册/删除**直接抛 `UnsupportedOperationException`**（`#L117-134`） | `open()` 里注册 `"user-timers"` 定时器服务，并以**自身**作 `Triggerable`（`#L59`） |
| `getCurrentKey()` | 无 | 委托算子：`return (K) KeyedProcessOperator.this.getCurrentKey();`（`#L135`） |

> ⚠️ **最经典的 demo 错误**：在**非 keyed** 流上写 `ctx.timerService().registerEventTimeTimer(...)` —— 运行期直接抛 `UnsupportedOperationException`。要定时器必须 `keyBy(...)` 后使用 `KeyedProcessFunction`。
>
> ⚠️ 还有一对兼容类：`LegacyKeyedProcessOperator`（`SJ/operators/LegacyKeyedProcessOperator.java#L40`）与 `LegacyKeyedCoProcessOperator`（`SJ/operators/co/LegacyKeyedCoProcessOperator.java#L47`），分别由 `KeyedStream.process(ProcessFunction)` 与双 keyed 分支选用。

## 2.3 定时器机制（这一层最值得深挖）

用户视角只有四层薄封装：

```
TimerService（用户可见，SJ/TimerService.java#L25，registerEventTimeTimer #L57）
  └─ SimpleTimerService（SJ/SimpleTimerService.java#L27，补 VoidNamespace.INSTANCE，#L45-50）
       └─ InternalTimerService<N>（SJ/operators/InternalTimerService.java#L33，方法带 namespace）
            └─ InternalTimerServiceImpl / InternalTimerServiceAsyncImpl（SJ/operators/InternalTimerServiceImpl.java#L45）
```

**注册语义**：

```java
// InternalTimerServiceImpl.java#L227
    public void registerProcessingTimeTimer(N namespace, long time) {
        InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
        if (processingTimeTimersQueue.add(
                new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
            long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
            // check if we need to re-schedule our timer to earlier
            if (time < nextTriggerTime) {
                if (nextTimer != null) {
                    nextTimer.cancel(false);
                }
                nextTimer = processingTimeService.registerTimer(time, this::onProcessingTime);
            }
        }
    }
    @Override
    public void registerEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.add(
                new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }
```

- **处理时间定时器**：入队 + 按需向 `ProcessingTimeService` 注册（更早的时间会取消旧系统定时器重排）。
- **事件时间定时器**：**只入队，不注册任何系统定时器** —— 触发完全由水位线推动。

**触发语义**：

```java
// InternalTimerServiceImpl.java#L323
    public boolean tryAdvanceWatermark(
            long time, InternalTimeServiceManager.ShouldStopAdvancingFn shouldStopAdvancingFn)
            throws Exception {
        currentWatermark = time;
        InternalTimer<K, N> timer;
        boolean interrupted = false;
        while ((timer = eventTimeTimersQueue.peek()) != null
                && timer.getTimestamp() <= time
                && !cancellationContext.isCancelled()
                && !interrupted) {
            keyContext.setCurrentKey(timer.getKey());
            eventTimeTimersQueue.poll();
            triggerTarget.onEventTime(timer);
```

```java
// MailboxWatermarkProcessor.java#L70
    private void emitWatermarkInsideMailbox() throws Exception {
        // Try to progress min watermark as far as we can.
        if (internalTimeServiceManager.tryAdvanceWatermark(
                maxInputWatermark, mailboxExecutor::shouldInterrupt)) {
            // In case output watermark has fully progressed emit it downstream.
            output.emitWatermark(maxInputWatermark);
        } else if (!progressWatermarkScheduled) {
            progressWatermarkScheduled = true;
            // We still have work to do, but we need to let other mails to be processed first.
            mailboxExecutor.execute(
                    MailboxExecutor.MailOptions.deferrable(),
                    () -> {
                        progressWatermarkScheduled = false;
                        emitWatermarkInsideMailbox();
                    },
                    "emitWatermarkInsideMailbox");
        } else {
```

**数据结构**：定时器存在**按 key group 分片的优先队列**里 —— `KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K,N>>`（`InternalTimerServiceImpl.java#L53-58`），元素 `TimerHeapInternalTimer` 带 `timerHeapIndex` 支持 O(1) 删除（`TimerHeapInternalTimer.java#L34`），比较器 `InternalTimer.TIMER_COMPARATOR`（`InternalTimer.java#L42`）。checkpoint 时按 key group 切快照（`getSubsetForKeyGroup(...)` `#L351`）。

> ⚠️ **定时器与 mailbox 的公平性**：`tryAdvanceWatermark` 每弹一个 timer 都会检查 `mailboxExecutor::shouldInterrupt()`，一旦需要让位就**中断本轮、把续跑重新投递为 deferrable mail**。所以"定时器风暴"不会饿死网络/checkpoint 邮件。
>
> ⚠️ **keyed 定时器按 `(key, timestamp)` 去重**：同一 key 同一时间戳重复注册只有一个定时器。
>
> ⚠️ **事件时间定时器只在"水位线真的推进"时触发**：有界输入结束时 Flink 会发 `MAX_WATERMARK` 触发收尾；无界源若无数据推进水位线，`onTimer` 永不触发（可用 `withIdleness` 兜底）。

---

# 三、双流：`CoProcessFunction` / `KeyedCoProcessFunction`

| API | 位置 | 生成的算子 |
|---|---|---|
| `s1.connect(s2).process(CoProcessFunction)` | `SJ/datastream/ConnectedStreams.java#L309`（`#L343`） | `CoProcessOperator`（两侧都 keyed 时走 `LegacyKeyedCoProcessOperator` `#L349`） |
| `ks1.connect(ks2).process(KeyedCoProcessFunction)` | `ConnectedStreams.java#L373`（构造算子 `#L413`） | `KeyedCoProcessOperator` |

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

`KeyedCoProcessFunction` 与 `CoProcessFunction` 的差别与单流一致：**多 `getCurrentKey()`（`#L129`/`#L141`）+ 能用定时器**。它是"手写 join"的基础设施 —— 自己用 `ValueState`/`MapState` 缓存一侧、另一侧到达时配对。

---

# 四、窗口：`ProcessWindowFunction` / `ProcessAllWindowFunction`

这两个接口**不在** `process-functions` 的运行时算子里单独成算子，而是由 `WindowOperator` 承载：

```java
// WindowOperatorBuilder.java#L253
    public <R> WindowOperator<K, T, ?, R, W> apply(WindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(function, "WindowFunction cannot be null");
        return apply(new InternalIterableWindowFunction<>(function));
    }

    public <R> WindowOperator<K, T, ?, R, W> process(ProcessWindowFunction<T, R, K, W> function) {
        Preconditions.checkNotNull(function, "ProcessWindowFunction cannot be null");
        return apply(new InternalIterableProcessWindowFunction<>(function));
    }
```

| 能力 | `ProcessWindowFunction.Context` |
|---|---|
| `window()` | `ProcessWindowFunction.java#L67` |
| `currentProcessingTime()` / `currentWatermark()` | `#L70` / `#L73` |
| **`windowState()` / `globalState()`** | `#L81` / `#L84`（per-window / global keyed state） |
| `output(OutputTag, X)` | `#L92` |

> ⚠️ `ProcessWindowFunction` 会缓存**整个窗口元素**（`WindowedStream.java#L617` 注释），想省状态就用 `aggregate(AggregateFunction, ProcessWindowFunction)` 做增量聚合前置。
>
> ⚠️ 窗口 API 与算子的完整分析见 [`window-functions.md`](./window-functions.md)。

---

# 五、侧输出（Side Output）

侧输出 = **`OutputTag` 作为路由键 + 每条虚拟边一个 `RecordWriterOutput`**，靠 tag 相等性认领。

```java
// SJ/operators/Output.java#L39
public interface Output<T> extends Collector<T> {
    <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record);
```

```java
// OutputTag.java#L47 与 #L89
public class OutputTag<T> implements Serializable {
    public OutputTag(String id) {
    public static boolean isResponsibleFor(
            @Nullable OutputTag<?> owner, @Nonnull OutputTag<?> other) {
        return other.equals(owner);
    }
```

```java
// OperatorChain.java#L535 —— OutputTag → Output 的映射点
    private RecordWriterOutput<OUT> createStreamOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            NonChainedOutput streamOutput,
            StreamConfig upStreamConfig,
            Environment taskEnvironment) {
        OutputTag sideOutputTag =
                streamOutput.getOutputTag(); // OutputTag, return null if not sideOutput
        TypeSerializer outSerializer;
        if (streamOutput.getOutputTag() != null) {
            // side output
            outSerializer =
                    upStreamConfig.getTypeSerializerSideOut(
                            streamOutput.getOutputTag(),
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        } else {
            // main output
            outSerializer =
                    upStreamConfig.getTypeSerializerOut(
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        }
        return closer.register(
                new RecordWriterOutput<OUT>(
                        recordWriter,
                        outSerializer,
                        sideOutputTag,
                        streamOutput.supportsUnalignedCheckpoints()));
```

```java
// RecordWriterOutput.java#L125
    public <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record) {
        if (!OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
            // we are not responsible for emitting to the side-output specified by this
            // OutputTag.
            return false;
        }
        pushToRecordWriter(record);
        return true;
    }
```

| 要点 | 说明 |
|---|---|
| 读取侧 | `DataStream.getSideOutput(tag)`（`SJ/datastream/SingleOutputStreamOperator.java#L402`）生成独立的 SideOutput 虚拟节点 |
| 去重语义 | `OutputTag` 的 `equals/hashCode` 只看 `id`（`OutputTag.java#L107`/`#L119`），所以**同一个静态实例**才最稳妥；推荐用 `new OutputTag<>(id, TypeInformation)`（`#L82`） |
| 时间戳 | 侧输出记录带**当前元素时间戳**（`ProcessOperator.java#L103`、`KeyedProcessOperator.java#L129`）或**窗口末尾**（`WindowOperator.java#L808`） |
| 空 tag | `outputTag == null` 抛 `IllegalArgumentException("OutputTag must not be null.")` |

---

# 六、广播：`BroadcastProcessFunction` / `KeyedBroadcastProcessFunction`

## 6.1 API 层

```java
// DataStream.java#L437
    public BroadcastStream<T> broadcast(
            final MapStateDescriptor<?, ?>... broadcastStateDescriptors) {
        Preconditions.checkNotNull(broadcastStateDescriptors);
        final DataStream<T> broadcastStream = setConnectionType(new BroadcastPartitioner<>());
        return new BroadcastStream<>(environment, broadcastStream, broadcastStateDescriptors);
```

| 用法 | 位置 | 算子 |
|---|---|---|
| `stream.connect(bs).process(BroadcastProcessFunction)` | `BroadcastConnectedStream.java#L178`（`#L209`） | `CoBroadcastWithNonKeyedOperator` |
| `stream.keyBy(k).connect(bs).process(KeyedBroadcastProcessFunction)` | `BroadcastConnectedStream.java#L124`（`#L155`） | `CoBroadcastWithKeyedOperator` |

**类型系统已经固化了读写权限**：

```java
// BaseBroadcastProcessFunction.java#L75 与 #L102
    public abstract class Context extends BaseContext {
        public abstract <K, V> BroadcastState<K, V> getBroadcastState(
                final MapStateDescriptor<K, V> stateDescriptor);
    }
    public abstract class ReadOnlyContext extends BaseContext {
        public abstract <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(
                final MapStateDescriptor<K, V> stateDescriptor);
    }
```

主流侧拿到 `ReadOnlyContext`（只能读），广播侧拿到 `Context`（可写）—— 写错侧编译期就报错。

## 6.2 运行时

```java
// CoBroadcastWithNonKeyedOperator.java#L79 —— 非 keyed 广播态存在 operator state 里
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);
        this.broadcastStates =
                CollectionUtil.newHashMapWithExpectedSize(broadcastStateDescriptors.size());
        for (MapStateDescriptor<?, ?> descriptor : broadcastStateDescriptors) {
            broadcastStates.put(
                    descriptor, getOperatorStateBackend().getBroadcastState(descriptor));
        }
```

```java
// CoBroadcastWithKeyedOperator.java#L123 —— 普通侧设 key context，广播侧不设（无 key）
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestamp(element);
        rContext.setElement(element);
        userFunction.processElement(element.getValue(), rContext, collector);
        rContext.setElement(null);
    }
    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestamp(element);
        rwContext.setElement(element);
        userFunction.processBroadcastElement(element.getValue(), rwContext, collector);
        rwContext.setElement(null);
    }
```

| 约束 | 说明 |
|---|---|
| **各实例内容必须一致** | `BroadcastState` javadoc 原文：*"This state assumes that **the same elements are sent to all instances of an operator.** CAUTION: the user has to guarantee that all task instances store the same elements in this type of state."* |
| 广播侧**没有 key** | `processElement2` 不设 key context，所以在 `processBroadcastElement` 里读 keyed state 会失败；要改 keyed state 只能用 `Context.applyToKeyedState`（`KeyedBroadcastProcessFunction.java#L147`） |
| keyed 版才有定时器 | `ReadOnlyContext.timerService()`（`#L165`）+ `onTimer`（`#L125`），算子实现 `Triggerable<KS,VoidNamespace>`（`CoBroadcastWithKeyedOperator.java#L66`） |
| 状态位置 | 非 keyed 广播态在 **operator state**（`getOperatorStateBackend().getBroadcastState(...)`），恢复/扩缩容时按 key group 轮转重分配 |
| 流类型断言 | `BroadcastProcessFunction` 只能配非 keyed（`BroadcastConnectedStream.java#L216`），`KeyedBroadcastProcessFunction` 只能配 keyed（`#L155`） |

## 6.3 实跑验证：`BroadcastProcessFunctionDemo` —— 广播维表补全

> **实跑状态**：JDK 17 + Flink 1.20.4 本地 MiniCluster **实跑通过，退出码 0**（主流与广播流都是**有界源**，会给主流加固定延时来编排"维表先到"的顺序，因此跑完自动退出）。输出已剔除 Flink 的 `WARN` 日志行。

**1) 非 keyed 广播：`events.connect(rules.broadcast(dim))` → `CoBroadcastWithNonKeyedOperator`**

```
=== 1) 非 keyed 广播：events.connect(rules.broadcast(dim)) → CoBroadcastWithNonKeyedOperator ===
    维表 3 条规则先到（t=0），业务数据每条延时 300ms 后再到，因此都能命中维表
1-非keyed广播> [非keyed processBroadcastElement] 写入广播状态: user-1 = 张三（旧值=null）；这条记录会被复制到算子的每一个并行子任务
1-非keyed广播> [非keyed processBroadcastElement] 写入广播状态: user-2 = 李四（旧值=null）；这条记录会被复制到算子的每一个并行子任务
1-非keyed广播> [非keyed processBroadcastElement] 写入广播状态: user-3 = 王五（旧值=null）；这条记录会被复制到算子的每一个并行子任务
1-非keyed广播> [非keyed processElement] Event{user-1, 0ms, 11.5} → 维表命中 key="user-1" ⇒ 张三 | 当前广播态条目数=3
1-非keyed广播> [非keyed processElement] Event{user-2, 100ms, 22.0} → 维表命中 key="user-2" ⇒ 李四 | 当前广播态条目数=3
1-非keyed广播> [非keyed processElement] Event{user-9, 200ms, 33.5} → 维表命中 key="user-9" ⇒ <未命中，走默认值 UNKNOWN> | 当前广播态条目数=3
1-非keyed广播> [非keyed processElement] Event{user-3, 300ms, 44.0} → 维表命中 key="user-3" ⇒ 王五 | 当前广播态条目数=3
```

三个观察点：

| 观察 | 说明 |
|---|---|
| `processBroadcastElement` 收到 **3 条**，且每条的"旧值=`null`" | 维表**只在广播侧写一次**；旧值是 `null` 说明该 key 此前不存在（维表首次下发） |
| `processElement` 每条都能 `getBroadcastState(dim).get(userId)` 命中 | **主流元素到来时维表已就绪**——这就是"维表先行 + 主流延时"编排的目的 |
| `user-9` 未命中，走默认值 `UNKNOWN` | 广播维表是**可能缺项**的，`processElement` 必须处理"查不到"的分支 |
| `当前广播态条目数=3` 在每条主流数据上都一样 | 广播态是**算子级状态**（不含 key），所以每个元素看到的都是同一份完整维表 |

**2) keyed 广播：`events.keyBy(userId).connect(rules.broadcast(dim))` → `CoBroadcastWithKeyedOperator`**

```
=== 2) keyed 广播：events.keyBy(userId).connect(rules.broadcast(dim)) → CoBroadcastWithKeyedOperator ===
    规则 user-1 先写入 张三-v1，1s 后又被更新成 张三-v2；
    业务数据每 400ms 一条（首条延后 400ms）：前 2 条看到 v1，后 4 条看到 v2 → 维表更新实时生效
2-keyed广播> [keyed processBroadcastElement] key 不存在（广播状态无 key）: user-1 : null → 张三-v1；所有并行子任务都会收到并各自存一份副本
2-keyed广播> [keyed processElement] key=user-1 Event{user-1, 0ms, 1.0} → 维表="user-1" ⇒ 张三-v1
2-keyed广播> [keyed processElement] key=user-1 Event{user-1, 100ms, 2.0} → 维表="user-1" ⇒ 张三-v1
2-keyed广播> [keyed processBroadcastElement] key 不存在（广播状态无 key）: user-1 : 张三-v1 → 张三-v2；所有并行子任务都会收到并各自存一份副本
2-keyed广播> [keyed processElement] key=user-1 Event{user-1, 200ms, 3.0} → 维表="user-1" ⇒ 张三-v2
2-keyed广播> [keyed processElement] key=user-1 Event{user-1, 300ms, 4.0} → 维表="user-1" ⇒ 张三-v2
2-keyed广播> [keyed processElement] key=user-1 Event{user-1, 400ms, 5.0} → 维表="user-1" ⇒ 张三-v2
2-keyed广播> [keyed processElement] key=user-1 Event{user-1, 500ms, 6.0} → 维表="user-1" ⇒ 张三-v2
```

**这条输出把"广播维表实时更新"演示得非常干净**：

```
维表:  v1 下发 ────────────────► v2 下发 ──────────────►
主流:  e1(v1) e2(v1)  |  e3(v2) e4(v2) e5(v2) e6(v2)
                      ↑ 更新在这个位置生效，之后所有元素立刻看到新值
```

| 关键点 | 证据 | 机制 |
|---|---|---|
| **广播态与 key 无关** | `[keyed processBroadcastElement] key 不存在（广播状态无 key）` | `BroadcastState` 是 **operator state**，没有 key 维度（§6.2 的约束表） |
| **每个并行子任务各存一份副本** | 每行广播写入都标注"所有并行子任务都会收到并各自存一份副本" | 广播流被复制到下游每个 subtask；这也是"广播态内容必须各实例一致"的原因 |
| **更新立刻对后续元素生效** | `e1,e2 → v1`；`e3..e6 → v2` | 广播记录是**按序处理**的，`processBroadcastElement` 写完 state，后续 `processElement` 立刻读到新值 |
| **不用每条数据查外部系统** | 全程无外部 IO | ⭐ 这正是广播维表的核心价值：维表**下发一次、内存里常驻**，主流每条数据 O(1) 命中 |

> ⚠️ **广播态的正确用法边界（4 条）**：
> ① **只能在 `processBroadcastElement` 里写**；`processElement` 拿到的 `ReadOnlyBroadcastState` 是**只读视图**，写入会被编译期/运行期拦下。
> ② **广播态与 key 无关**，所以**不要**在 `processBroadcastElement` 里读 keyed state（会失败）；要改 keyed state 必须用 `ctx.applyToKeyedState(...)`。
> ③ **广播态必须各实例一致**——javadoc 明确要求"用户保证所有 task 实例存相同的元素"。所以**绝不能**在 `processElement` 里往广播态写数据（那样各实例就不一致了）。
> ④ **广播流通常无界、量必须小**：维表/规则适合，明细数据绝不适合（一条广播记录 = N 个 subtask 各存一份 + 走一遍网络）。

**demo 自己的收尾结论（原文）**：

```
=== 全部小节结束 ===
回收结论：
  1. 广播状态只能在 processBroadcastElement 里写；processElement 拿到的是只读视图（编译期拦截）
  2. 广播状态与 key 无关：它是 operator state，每个并行子任务各存一份完整副本
  3. 广播侧的 keyed state 只能通过 ctx.applyToKeyedState(...) 间接改
  4. 换来的能力：主流每条数据都能 O(1) 查到维表/规则，不用每条数据都去外部系统查一次
```

> ⚠️ **诚实标注（demo 的编排方式）**：`BroadcastProcessFunctionDemo` 用**墙钟 `Thread.sleep`**（一个 `DelayMapper`）来保证"维表先到、主流后到"，并用 1s 间隔演示"维表中途更新"。这是**时序启发式，不是 API 保证**——在负载很高的机器上先后顺序可能漂移（demo 仍然有意义，只是 `v1`/`v2` 的分界点会移动）。真实生产里"维表必须先于主流可用"靠的是**广播状态与 checkpoint 一起持久化**，而不是靠延时。
> 已验证：本次运行确实产生"前 2 条看到 `张三-v1`、后 4 条看到 `张三-v2`"的分界。

---

# 七、易错点小结

| # | 易错点 | 正解 |
|---|---|---|
| 1 | 非 keyed 流上注册定时器 | `ProcessOperator` 直接抛 `UnsupportedOperationException`；必须 `keyBy` + `KeyedProcessFunction` |
| 2 | 按 Flink 2.x 文档写 `processElement(key, value, ctx, out)` | 1.20 的 key 从 `ctx.getCurrentKey()` 取 |
| 3 | 去找 `ProcessWindowOperator` | 1.20 不存在，窗口由 `WindowOperator` + `InternalIterableProcessWindowFunction` 承担 |
| 4 | 在 `processBroadcastElement` 里读 keyed state | 广播侧无 key context，只能用 `applyToKeyedState` |
| 5 | 广播侧在主流里 `put` | 主流拿到的是 `ReadOnlyBroadcastState`，编译期即拦截 |
| 6 | `OutputTag` 每次 `new` 一个新的 | `equals` 虽只看 id，但类型信息会丢；用同一静态实例或带 `TypeInformation` 的构造 |
| 7 | 无界源上等 `onTimer` 触发 | 水位线不推进就永不触发；有界输入结束会发 `MAX_WATERMARK` |
| 8 | 以为定时器触发会阻塞整个 task | 每弹一个 timer 都要过 mailbox 的 `shouldInterrupt`，会让位给网络/checkpoint 邮件 |

---

# 八、配套 demo 与真实输出

| demo | 覆盖 | 运行 |
|---|---|---|
| `process/ProcessFunctionDemo` | `ProcessFunction`（侧输出 + 处理时间定时器）、`KeyedProcessFunction`（事件时间定时器） | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.process.ProcessFunctionDemo` |
| `process/CoProcessFunctionDemo` | `CoProcessFunction` / `KeyedCoProcessFunction` 双侧关联 + 超时清理 | `... -Dmain.class=com.jiaqiz.flink.process.CoProcessFunctionDemo` |
| `process/BroadcastProcessFunctionDemo` | `KeyedBroadcastProcessFunction` 广播维表补全 | `... -Dmain.class=com.jiaqiz.flink.process.BroadcastProcessFunctionDemo` |

### 8.1 `ProcessFunctionDemo`（本地 MiniCluster 实跑通过，退出码 0）

> 📌 **关于输出中的可变数值**：本文引用的 demo 输出取自**某一次真实运行**。其中`currentProcessingTime()` / 定时器注册时刻的墙钟毫秒值（`@1789xxxxxx`）、以及非 keyed 那行 `currentProcessingTime()` 的读数 **每次运行都不同**，因此你重跑时这些数字不会逐字一致 —— 这是并发/时序/墙钟的自然结果，不是文档与代码不一致。**结构性结论（条数、顺序关系、状态名、算子类名）是稳定可复现的。**


```
主输出> [定时器] 非 keyed 流注册定时器被拒绝: "Setting timers is only supported on a keyed streams."（currentProcessingTime() 可读=1789727013425；定时器用法见本文件 keyed 分支）
主输出> 主输出 正常数据 -> Sensor{id=s1, ts=1000, temp=21.0} | ctx.timestamp()=-9223372036854775808
侧输出-异常> Sensor{id=s2, ts=2000, temp=999.0}
侧输出-迟到> Sensor{id=s3, ts=1500, temp=22.0}
主输出> 主输出 正常数据 -> Sensor{id=s4, ts=3000, temp=23.0} | ctx.timestamp()=-9223372036854775808
侧输出-异常> Sensor{id=s5, ts=4000, temp=-100.0}
主输出> 主输出 正常数据 -> Sensor{id=s6, ts=5000, temp=24.0} | ctx.timestamp()=-9223372036854775808
事件时间定时器> key=user-A 收到 ts=1700000000000 temp=20.0 | 累计=1 | currentWatermark=-9223372036854775808
事件时间定时器> key=user-B 收到 ts=1700000001000 temp=30.0 | 累计=1 | currentWatermark=-9223372036854775808
事件时间定时器> key=user-A 收到 ts=1700000002000 temp=21.0 | 累计=2 | currentWatermark=-9223372036854775808
事件时间定时器> key=user-A 收到 ts=1700000003000 temp=22.0 | 累计=3 | currentWatermark=-9223372036854775808
事件时间定时器> key=user-B 收到 ts=1700000004000 temp=31.0 | 累计=2 | currentWatermark=-9223372036854775808
事件时间定时器> key=user-C 收到 ts=1700000005000 temp=40.0 | 累计=1 | currentWatermark=-9223372036854775808
事件时间定时器> [事件时间定时器触发] key=user-A timerTs=1700000005000 timeDomain=EVENT_TIME 累计条数=3
事件时间定时器> [事件时间定时器触发] key=user-B timerTs=1700000006000 timeDomain=EVENT_TIME 累计条数=2
事件时间定时器> [事件时间定时器触发] key=user-C timerTs=1700000010000 timeDomain=EVENT_TIME 累计条数=1
处理时间定时器> key=t1 第一条数据到达，注册处理时间定时器 @1789727013906
处理时间定时器> key=t1 收到 t1 -> 累计=1
处理时间定时器> key=t1 收到 t1 -> 累计=2
处理时间定时器> key=t1 收到 t1 -> 累计=3
处理时间定时器> [处理时间定时器触发] key=t1 timerTs=1789727013906 timeDomain=PROCESSING_TIME 触发时累计条数=3
处理时间定时器> key=t2 第一条数据到达，注册处理时间定时器 @1789727014219
处理时间定时器> key=t2 收到 t2 -> 累计=1
处理时间定时器> key=t2 收到 t2 -> 累计=2
处理时间定时器> key=t2 收到 t2 -> 累计=3
处理时间定时器> [处理时间定时器触发] key=t2 timerTs=1789727014219 timeDomain=PROCESSING_TIME 触发时累计条数=3
处理时间定时器> key=t3 第一条数据到达，注册处理时间定时器 @1789727014531
处理时间定时器> key=t3 收到 t3 -> 累计=1
处理时间定时器> key=t3 收到 t3 -> 累计=2
处理时间定时器> key=t3 收到 t3 -> 累计=3
处理时间定时器> [处理时间定时器触发] key=t3 timerTs=1789727014531 timeDomain=PROCESSING_TIME 触发时累计条数=3
```

**四个关键结论都有现场证据**：

| 观察 | 证据 | 说明 |
|---|---|---|
| **非 keyed 流不能注册定时器** | 第一行直接打印被拒的异常消息 `"Setting timers is only supported on a keyed streams."` | `ProcessOperator` 提供的是 `SimpleTimerService` 的空实现，只有 `KeyedProcessOperator` 才接 `InternalTimerService` |
| **无时间戳时 `ctx.timestamp()` 是 `Long.MIN_VALUE`** | `ctx.timestamp()=-9223372036854775808` | 输入没带时间戳，`StreamRecord` 的时间戳字段就是 `Long.MIN_VALUE`；**用之前必须判空**，否则算术会溢出 |
| **侧输出真的分流到不同 tag** | 主输出 3 条正常数据，`侧输出-异常` 收到 2 条（temp=999/-100），`侧输出-迟到` 收到 1 条（ts=1500） | `ctx.output(outputTag, value)`；下游用 `getSideOutput(tag)` 取，类型安全 |
| **事件时间定时器由 watermark 推进触发，且批量触发** | `currentWatermark=-9223372036854775808` 期间只累积不触发；输入结束后 watermark 推进到最大值，3 个 key 的定时器**连续触发** | `InternalTimerServiceImpl.tryAdvanceWatermark`：watermark 一跳过 `timerTs` 就触发该 key 的定时器 |
| **处理时间定时器按 key 独立计时** | `t1/t2/t3` 各注册一次、各触发一次，且"触发时累计条数=3" | 注册的是"第一条数据处理时"的时刻；三个 key 注册时间不同（`…013906` / `…014219` / `…014531`），**各自触发** |

> **`ctx.timestamp()` 的坑值得单独记**：`-9223372036854775808` 就是 `Long.MIN_VALUE`。常见错误写法 `ctx.timestamp() - windowSize` 会**整数下溢变成极大的正数**，定时器立刻触发或永远不触发。正确写法是先判断是否带时间戳（如 `ctx.timestamp() == Long.MIN_VALUE` 就跳过定时器逻辑）。

### 8.2 `CoProcessFunctionDemo`（本地 MiniCluster 实跑通过，退出码 0）

> 📌 **关于输出中的可变数值**：本文引用的 demo 输出取自**某一次真实运行**。其中**两条流的到达交错顺序**（demo 的订单流与支付流并发灌入，`[订单暂存]` / `[支付暂存]` / `[关联成功]` / `[超时清理]` 各行的先后与组合会随运行变化）**每次运行都不同**，因此你重跑时这些数字不会逐字一致 —— 这是并发/时序/墙钟的自然结果，不是文档与代码不一致。**结构性结论（条数、顺序关系、状态名、算子类名）是稳定可复现的。**


```
订单支付关联> [订单暂存] key=O1 订单=O1 存入 ValueState，注册超时清理定时器 @1700000010000
订单支付关联> [关联成功·订单先到] key=O1 订单=O1 订单金额=100.0 支付金额=100.0
订单支付关联> [订单暂存] key=O2 订单=O2 存入 ValueState，注册超时清理定时器 @1700000011000
订单支付关联> [支付暂存] key=O3 支付=O3 存入 ValueState 等订单，注册超时清理定时器 @1700000018000
订单支付关联> [关联成功·支付先到] key=O3 订单=O3 订单金额=300.0 支付金额=300.0
订单支付关联> [支付暂存] key=O9 支付=O9 存入 ValueState 等订单，注册超时清理定时器 @1700000019000
订单支付关联> [订单暂存] key=O4 订单=O4 存入 ValueState，注册超时清理定时器 @1700000016000
订单支付关联> [超时清理·订单未支付] key=O2 订单=O2 timerTs=1700000011000 timeDomain=EVENT_TIME 超过 10000ms 未收到支付，清空 ValueState
订单支付关联> [超时清理·订单未支付] key=O4 订单=O4 timerTs=1700000016000 timeDomain=EVENT_TIME 超过 10000ms 未收到订单，清空 ValueState
订单支付关联> [超时清理·支付无订单] key=O9 支付=O9 timerTs=1700000019000 timeDomain=EVENT_TIME 超过 15000ms 未收到订单，清空 ValueState
```

**这条输出证明 `CoProcessFunction` 的双流状态是"两个独立入口、一份 keyed state"**：

- 两条流的数据都进 `processElement1` / `processElement2`，但**共享同一个 key 的 state**（`ctx.getState(...)`）。所以"订单先到"（O1）与"支付先到"（O3）都能关联成功——两条路径都要写。
- **超时靠事件时间定时器清理**：`O2`/`O4` 只来订单没来支付、`O9` 只来支付没来订单，都在 timer 触发时清空 state。`O4` 那行是 demo 文案笔误（应为"未收到支付"），timerTs 值本身正确。
- 这是**用 `CoProcessFunction` 自己实现 interval join** 的典型写法；生产上若两侧都在同一个时间窗口内，直接用 `IntervalJoinOperator`（见 [`join-functions.md`](./join-functions.md)）更省事，因为它把"暂存 + 定时清理"做进了算子内部。

> **实跑结论（诚实标注）**：`process/ProcessFunctionDemo` 与 `process/CoProcessFunctionDemo` 均在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**；输出已剔除 Flink 的 `WARN` 日志行。
> `process/BroadcastProcessFunctionDemo`（广播维表补全）的运行结论见 §6.3。
