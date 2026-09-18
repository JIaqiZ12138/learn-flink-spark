# AsyncFunction 异步 I/O 与 AsyncWaitOperator

> **代码基线**：Apache Flink 1.20.4　**源码位置**：仓库根目录 `flink-1.20-source/`
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/async/`
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)

---

# 一、异步 I/O 解决什么问题

同步访问外部系统时，一条记录要等一个 RTT 才处理下一条；加并行度只能靠"多开 task"换吞吐。异步 I/O 让**同一个 subtask 同时有 N 个请求在飞**（N = `capacity`），从而在不增加并行度的前提下提升吞吐。

| 关键类 | 路径 | 职责 |
|---|---|---|
| `AsyncFunction<IN,OUT>` | `flink-streaming-java/.../api/functions/async/AsyncFunction.java#L77` | 用户实现 `asyncInvoke(IN, ResultFuture<OUT>)`（`#L87`） |
| `ResultFuture<OUT>` | 同目录 `ResultFuture.java#L31` | 只有两个方法：`complete(Collection<OUT>)`（`#L42`）、`completeExceptionally(Throwable)`（`#L49`） |
| `RichAsyncFunction<IN,OUT>` | `RichAsyncFunction.java#L75` | 富版本：`open()` 里建连接池 |
| `AsyncRetryStrategy<OUT>` | `AsyncRetryStrategy.java#L27` | `canRetry`（`#L30`）/`getBackoffTimeMillis`（`#L33`）/`getRetryPredicate`（`#L36`） |
| `AsyncDataStream` | `flink-streaming-java/.../api/datastream/AsyncDataStream.java` | 门面：8 个入口 + `OutputMode` |
| `AsyncWaitOperator` | `flink-streaming-java/.../api/operators/async/AsyncWaitOperator.java#L92` | 运行时算子 |

> ⚠️ **`AsyncWaitOperator` 在 `api/operators/async/`，不在 `runtime/operators/`**（全库只有这一份，配套 `AsyncWaitOperatorFactory`）。
>
> ⚠️ **1.20 已没有 `AsyncCollector` 这个类型**（`grep` 零命中），对应角色就是 `ResultFuture` 接口 + 内部类 `ResultHandler` / `RetryableResultHandlerDelegator`。

---

# 二、`AsyncDataStream` 的 8 个入口

```java
// AsyncDataStream.java#L48
    public enum OutputMode {          // #L48
        ORDERED,                      // #L49
        UNORDERED                     // #L50
    }

    private static final int DEFAULT_QUEUE_CAPACITY = 100;   // #L53
```

| 方法 | 签名 | 行号 |
|---|---|---|
| `unorderedWait` | `(in, func, timeout, timeUnit, capacity)` | `#L115` |
| `unorderedWait` | `(in, func, timeout, timeUnit)` → capacity=100 | `#L141` |
| `orderedWait` | `(in, func, timeout, timeUnit, capacity)` | `#L165` |
| `orderedWait` | `(in, func, timeout, timeUnit)` → capacity=100 | `#L192` |
| `unorderedWaitWithRetry` | `(in, func, timeout, timeUnit, strategy)` | `#L217` |
| `unorderedWaitWithRetry` | `(in, func, timeout, timeUnit, capacity, strategy)` | `#L247` |
| `orderedWaitWithRetry` | `(in, func, timeout, timeUnit, strategy)` | `#L277` |
| `orderedWaitWithRetry` | `(in, func, timeout, timeUnit, capacity, strategy)` | `#L307` |

八个方法都汇入私有 `addOperator(...)`（`#L75`）：校验"带重试则 `timeout > 0`"（`#L75-78`）→ 类型推断（`#L80-89`）→ `new AsyncWaitOperatorFactory<>(...)`（`#L92-98`）→ `in.transform("async wait operator", outTypeInfo, operatorFactory)`（`#L100`）。

| 参数 | 语义 |
|---|---|
| `timeout`（无重试） | **单次** `asyncInvoke` 到完成的时限；`<= 0` 表示**永不超时**（`AsyncWaitOperator#L269` 只在 `timeout > 0` 时注册定时器） |
| `timeout`（带重试） | **从首次 invoke 到最终完成、含多次重试**的总时限（`AsyncDataStream.java#L298` 原文说明） |
| `capacity` | 同时在飞的请求上限（默认 100） |
| `OutputMode.ORDERED` | 输出顺序 = 输入顺序（被慢记录阻塞） |
| `OutputMode.UNORDERED` | 完成即发（但**不越过 watermark**） |

> ⚠️ **`AsyncRetryStrategy` 与 `NO_RETRY_STRATEGY` 是引用比较**（`#L75` 用 `!=`）。自己 `new` 一个"空策略"不算无重试，会走进重试分支并强制要求 `timeout > 0`。

---

# 三、`AsyncWaitOperator` 运行时

```java
// AsyncWaitOperator.java#L92
class AsyncWaitOperator<IN, OUT> extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
```

`setup()` 里按模式建队列（`#L187`）：`ORDERED → new OrderedStreamElementQueue<>(capacity)`、`UNORDERED → new UnorderedStreamElementQueue<>(capacity)`。

## 3.1 `processElement`：先入队，再发起异步调用

```java
// AsyncWaitOperator.java#L253
        // add element first to the queue
        final ResultFuture<OUT> entry = addToWorkQueue(element);

        if (retryEnabled) {
            final RetryableResultHandlerDelegator resultHandler =
                    new RetryableResultHandlerDelegator(element, entry, getProcessingTimeService());

            // register a timeout for the entry
            assert timeout > 0L;
            resultHandler.registerTimeout(timeout);

            userFunction.asyncInvoke(element.getValue(), resultHandler);

        } else {
            final ResultHandler resultHandler = new ResultHandler(element, entry);

            // register a timeout for the entry if timeout is configured
            if (timeout > 0L) {
                resultHandler.registerTimeout(getProcessingTimeService(), timeout);
            }

            userFunction.asyncInvoke(element.getValue(), resultHandler);
        }
```

## 3.2 `capacity` 的实现方式：入队不成功就 `yield`

```java
// AsyncWaitOperator.java#L344
    private ResultFuture<OUT> addToWorkQueue(StreamElement streamElement)
            throws InterruptedException {

        Optional<ResultFuture<OUT>> queueEntry;
        while (!(queueEntry = queue.tryPut(streamElement)).isPresent()) {
            mailboxExecutor.yield();
        }

        return queueEntry.get();
    }
```

队列满了就 `mailboxExecutor.yield()` 让出 mailbox 线程，等下游完成后腾出位置 —— **这就是 `capacity` 的强制手段**，也是"异步算子不会把 TM 内存吃爆"的原因。

## 3.3 有序 vs 无序：队列语义

```java
// OrderedStreamElementQueue.java#L38（javadoc 原文）
/**
 * Ordered {@link StreamElementQueue} implementation. The ordered stream element queue provides
 * asynchronous results in the order in which the {@link StreamElementQueueEntry} have been added to
 * the queue. Thus, even if the completion order can be arbitrary, the output order strictly follows
 * the insertion order (element cannot overtake each other).
 */
```

```java
// OrderedStreamElementQueue.java#L62
    public boolean hasCompletedElements() {
        return !queue.isEmpty() && queue.peek().isDone();
    }

    public void emitCompletedElement(TimestampedCollector<OUT> output) {
        if (hasCompletedElements()) {
            final StreamElementQueueEntry<OUT> head = queue.poll();
            head.emitResult(output);
        }
    }
```

```java
// UnorderedStreamElementQueue.java#L42（javadoc 原文）
/**
 * Unordered implementation of the {@link StreamElementQueue}. The unordered stream element queue
 * provides asynchronous results as soon as they are completed. Additionally, it maintains the
 * watermark-stream record order.
 *
 * <p>Elements can be logically grouped into different segments separated by watermarks. A segment
 * needs to be completely emitted before entries from a following segment are emitted. Thus, no
 * stream record can be overtaken by a watermark and no watermark can overtake a stream record.
 * However, stream records falling in the same segment between two watermarks can overtake each
 * other (their emission order is not guaranteed).
 */
```

| | `ORDERED` | `UNORDERED` |
|---|---|---|
| 放行条件 | **只有队头 done 才发**（`queue.peek().isDone()`） | 完成即发（**同一 segment 内**） |
| watermark | 前面的记录不完成，watermark 就发不出去 | 按 segment 分段：前一段清空后才允许后一段 |
| 副作用 | 慢记录会拖住整个 subtask（包括窗口/定时器所依赖的水位线） | 吞吐更高，但同段内乱序 |

## 3.4 checkpoint 不等待在飞请求 —— 而是"重放"

```java
// AsyncWaitOperator.java#L289
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        ListState<StreamElement> partitionableState =
                getOperatorStateBackend()
                        .getListState(
                                new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));

        try {
            partitionableState.update(queue.values());
        } catch (Exception e) {
            partitionableState.clear();
            ...
```

```java
// AsyncWaitOperator.java#L301
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        recoveredStreamElements =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
    }
```

> ⚠️ **全库核实：`async/` 包下没有覆写 `prepareSnapshotPreBarrier`**。也就是说 checkpoint **不会等待**在飞的异步请求完成，而是把**在飞的输入元素本身**（`queue.values()`）存进 operator state，恢复时在 `open()`（`#L223`）**重新执行一遍** `processElement`/`processWatermark`/`processLatencyMarker` —— 即**重新发起异步请求**。
>
> **推论（很重要）**：异步 I/O 的语义是 **at-least-once**，故障恢复时在飞记录会被**重复处理**，下游必须能容忍重复（或用幂等写）。恢复后 `timeout` 计时也重新开始。

## 3.5 结束与结果回填

```java
// AsyncWaitOperator.java#L320（endInput 语义）
    public void endInput() throws Exception {
        finishInFlightDelayedRetry();
        waitInFlightInputsFinished();
    }
```

`finishInFlightDelayedRetry()`（`#L355`）先置 `retryDisabledOnFinish=true` 并对每个在飞重试 handler 立即放弃重试；`waitInFlightInputsFinished()`（`#L375`）`while (!queue.isEmpty()) { mailboxExecutor.yield(); }`。

```java
// AsyncWaitOperator.java#L598
        private void processInMailbox(Collection<OUT> results) {
            // move further processing into the mailbox thread
            mailboxExecutor.execute(
                    () -> processResults(results),
                    "Result in AsyncWaitOperator of input %s",
                    results);
        }

        private void processResults(Collection<OUT> results) {
            // Cancel the timer once we've completed the stream record buffer entry. ...
            if (timeoutTimer != null) {
                // canceling in mailbox thread avoids
                // https://issues.apache.org/jira/browse/FLINK-13635
                timeoutTimer.cancel(true);
            }

            // update the queue entry with the result
            resultFuture.complete(results);
            // now output all elements from the queue that have been completed (in the correct
            // order)
            outputCompletedElement();
        }
```

`outputCompletedElement()`（`#L389`）**一次只发一个**元素，若还有完成元素就再排一个 mail，避免长时间霸占 mailbox 线程。

## 3.6 超时与失败

`ResultHandler.timerTriggered()`（`#L648`）→ `userFunction.timeout(inputRecord.getValue(), this)`，默认实现是 `completeExceptionally(new TimeoutException("Async function call has timed out."))`（`AsyncFunction.java#L96`）。`completeExceptionally`（`#L624`）会先 `getContainingTask().getEnvironment().failExternally(...)` **让任务失败**（触发 failover），再把队列推进一格以解除 `addToWorkQueue` 的阻塞。

> ⚠️ `ResultHandler` 里有 `AtomicBoolean completed`（`#L578`）防重复完成：既 `complete` 又 `completeExceptionally` 只会生效第一个。
>
> ⚠️ 想让超时"跳过该条"而不是失败重启，就在自定义 `AsyncFunction` 里覆写 `timeout(...)` 给一个占位结果。

## 3.7 链式约束：链上算子必须"从尾到头" open

类 javadoc（`AsyncWaitOperator.java#L84`）原文：*"In case of chaining of this operator, it has to be made sure that the operators in the chain are opened tail to head. The reason for this is that an opened `AsyncWaitOperator` starts already emitting recovered `StreamElement` to downstream operators."* —— 因为它 `open()` 时就往下游发恢复数据。

---

# 四、`RichAsyncFunction` 的 RuntimeContext 被"阉割"了

```java
// RichAsyncFunction.java#L100（javadoc 原文）
    /**
     * A wrapper class for async function's {@link RuntimeContext}. The async function runtime
     * context only supports basic operations which are thread safe. Consequently, state access,
     * accumulators, broadcast variables and the distributed cache are disabled.
     */
    private static class RichAsyncFunctionRuntimeContext implements RuntimeContext {
```

```java
// RichAsyncFunction.java#L166
        @Override
        public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException(
                    "State is not supported in rich async functions.");
        }
```

`setRuntimeContext`（`#L81`）会把真正的 context **包一层**再交给父类。类 javadoc 给的理由（`#L65`）：*"State related apis in RuntimeContext are not supported yet because the key may get changed while accessing states in the working thread."*

| 放行 | 禁用（抛 `UnsupportedOperationException`） |
|---|---|
| `getMetricGroup`（`#L113`）、`getExecutionConfig`（`#L119`）、`createSerializer`（`#L124`）、`getGlobalJobParameters`（`#L129`）、`isObjectReuseEnabled`（`#L135`）、`getUserCodeClassLoader`（`#L140`）、`getExternalResourceInfos`（`#L151`） | `getState`（`#L166`）、`getListState`（`#L172`）、`getReducingState`、`getAggregatingState`、`getMapState`（`#L191`）、`addAccumulator`（`#L197`）、`getAccumulator`（`#L204`）、`getIntCounter`（`#L210`）、`getLongCounter`（`#L216`）、`getDistributedCache`（`#L160`） |

**实践含义**：异步函数里**只能用指标**（`getRuntimeContext().getMetricGroup().counter("x").inc()`），不能用累加器、不能用状态。

---

# 五、常见误传澄清

| 说法 | 事实 |
|---|---|
| "异步 I/O 必须先 `keyBy`" | **错**。`AsyncDataStream` 的 8 个入口参数都是 `DataStream<IN>`（`#L115`、`#L165` …），没有 `KeyedStream` 要求；`AsyncWaitOperator` 是 `OneInputStreamOperator`（`#L94`），不设 key context。`keyBy` 的唯一作用是"在异步算子前插一次 hash 重分区" |
| "`AsyncWaitOperator` 在 `runtime/operators/`" | 错，在 `api/operators/async/` |
| "有 `AsyncCollector`" | 1.20 已移除 |
| "异步算子会等 checkpoint 完成在飞请求" | **不会**。它保存"在飞输入元素"并在恢复时重放 → at-least-once |
| "`capacity` = 并发请求数" | 近似但不精确：ordered 模式下"已完成但未发出"的条目**也占 capacity**；watermark 条目**同样占一个 capacity** |

| 想让"每个 key 内保序"怎么办 | 保序口径是**每个 subtask 的输入 channel**；`keyBy` 可使同 key 落同一 subtask，从而得到"key 内有序" |
| 想全局保序 | 只能把异步算子并行度设为 1 |

---

# 六、配套 demo 与真实输出

| demo | 覆盖 | 运行 |
|---|---|---|
| `async/AsyncFunctionDemo` | 同一份输入分别走 `orderedWait` 与 `unorderedWait`，对比发射顺序（延迟与输入序号成反比） | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.async.AsyncFunctionDemo` |
| `async/RichAsyncFunctionDemo` | `RichAsyncFunction`：`open()` 建线程池、`close()` 关池、`getIndexOfThisSubtask()` | `... -Dmain.class=com.jiaqiz.flink.async.RichAsyncFunctionDemo` |

### 6.1

> 📌 **关于输出中的可变数值**：本文引用的 demo 输出取自**某一次真实运行**。其中`@+NNNN ms` 形式的耗时（异步完成时刻 / 发射时刻）与 `capacity` 段的排队时刻 **每次运行都不同**，因此你重跑时这些数字不会逐字一致 —— 这是并发/时序/墙钟的自然结果，不是文档与代码不一致。**结构性结论（条数、顺序关系、状态名、算子类名）是稳定可复现的。**
 `AsyncFunctionDemo`：ordered vs unordered（本地 MiniCluster 实跑通过，退出码 0）

demo 的输入设计：**首条 `r1` 故意慢（600ms），`r2..r6` 快（60ms）**，随后一个 watermark，再来 `r7..r9`（快）。这样一跑就能同时看出"无序段内按完成序"和"watermark 会顶住后续元素"两件事。

**`orderedWait` —— 严格按输入序发射，一条慢的全卡住**

```
================ orderedWait   ================
输入: [r1 A 慢600ms, r2..r6 快60ms] --WM--> [r7, r8, r9 快60ms]，并行度 1
预期: ordered 严格按输入序发射(r1 先卡 600ms，后面全被顶住)；
      unordered 段内按完成序发射(r2..r6 先出来)，但 WM 之后要等 r1 完成。
[OK     ] orderedWait   r1   asyncDone@+1584 ms  emit@+1584 ms  (异步耗时 600ms)
[OK     ] orderedWait   r2   asyncDone@+1039 ms  emit@+1585 ms  (异步耗时 60ms)
[OK     ] orderedWait   r3   asyncDone@+1039 ms  emit@+1585 ms  (异步耗时 60ms)
[OK     ] orderedWait   r4   asyncDone@+1039 ms  emit@+1586 ms  (异步耗时 60ms)
[OK     ] orderedWait   r5   asyncDone@+1039 ms  emit@+1586 ms  (异步耗时 60ms)
[OK     ] orderedWait   r6   asyncDone@+1039 ms  emit@+1586 ms  (异步耗时 60ms)
[OK     ] orderedWait   r7   asyncDone@+1039 ms  emit@+1586 ms  (异步耗时 60ms)
[OK     ] orderedWait   r8   asyncDone@+1039 ms  emit@+1586 ms  (异步耗时 60ms)
[OK     ] orderedWait   r9   asyncDone@+1102 ms  emit@+1586 ms  (异步耗时 60ms)
```

**看两列时间差**：`r2..r9` 的 `asyncDone` 都在 **+1039ms** 左右就完成了（异步请求本身很快），但 `emit` 全被推迟到 **+1585ms**（等 `r1` 的 600ms 走完）。**"异步完成"与"发射下游"是两件事**——这就是有序队列的代价：一个有界队列 + 按输入序出队。

**`unorderedWait` —— 段内按完成序发射，但 watermark 仍然是屏障**

```
================ unorderedWait ================
[OK     ] unorderedWait r2   asyncDone@+2049 ms  emit@+2049 ms  (异步耗时 60ms)
[OK     ] unorderedWait r5   asyncDone@+2049 ms  emit@+2049 ms  (异步耗时 60ms)
[OK     ] unorderedWait r6   asyncDone@+2049 ms  emit@+2049 ms  (异步耗时 60ms)
[OK     ] unorderedWait r4   asyncDone@+2049 ms  emit@+2049 ms  (异步耗时 60ms)
[OK     ] unorderedWait r3   asyncDone@+2049 ms  emit@+2049 ms  (异步耗时 60ms)
[OK     ] unorderedWait r1   asyncDone@+2591 ms  emit@+2592 ms  (异步耗时 600ms)
[OK     ] unorderedWait r7   asyncDone@+2051 ms  emit@+2594 ms  (异步耗时 60ms)
[OK     ] unorderedWait r8   asyncDone@+2051 ms  emit@+2594 ms  (异步耗时 60ms)
[OK     ] unorderedWait r9   asyncDone@+2110 ms  emit@+2594 ms  (异步耗时 60ms)
```

**两处必须看出来的细节**：

1. `r2..r6` 的发射顺序是 **r2, r5, r6, r4, r3** —— **不是输入序**，而是"谁先完成谁先发"（unordered 队列就是 `completed` 池，谁先放进去谁先出）。这正是 unordered 的吞吐优势来源。
2. **`r7..r9` 的 `asyncDone` 是 +2051ms（早就完成了），但 `emit` 却等到 +2594ms** —— 因为它们排在 watermark 之后，而 watermark 是**输入序屏障**：`r1` 未完成，watermark 不能越过，连带后面所有元素都被"顶住"。**结论：unordered 消除的是"数据元素之间"的顺序约束，不消除 watermark 的顺序约束。**

**timeout / capacity 段**

```
================ timeout / capacity ================
输入: t1 K 100ms, t2 K 900ms, t3 K 100ms；timeout=400ms，capacity=2
预期: t2 在 400ms 处超时降级输出；capacity=2 使 t3 必须等前面腾出名额。
[OK     ] timeout/cap2  t1   asyncDone@+3046 ms  emit@+3046 ms  (异步耗时 100ms)
[TIMEOUT] timeout/cap2  t2   asyncDone@+3348 ms  emit@+3348 ms  (异步耗时 900ms)
[OK     ] timeout/cap2  t3   asyncDone@+3151 ms  emit@+3349 ms  (异步耗时 100ms)
[main] 异步客户端线程池已 shutdown()
```

`t2` 的异步耗时 900ms > timeout 400ms，被标记为 `[TIMEOUT]`：**框架不等它，直接用 `AsyncFunction.timeout()` 的降级结果发下游**。注意 `t1` 的 `emit@+3046` 到 `t2` 的 `emit@+3348` 相差约 300ms，而 `t3` 的 `asyncDone@+3151` 早于此却要排队 —— **ordered 模式下已完成但未发射的条目也占 `capacity`**（§五 的最后一条澄清）。

### 6.2 `RichAsyncFunctionDemo`：`open` 建池 / `close` 关池（本地 MiniCluster 实跑通过，退出码 0）

```
[open ] subtask 0/2 task=async wait operator -> Sink: Print to Std. Out -> 创建线程池 java.util.concurrent.ThreadPoolExecutor@674b2cff[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
[open ] subtask 1/2 task=async wait operator -> Sink: Print to Std. Out -> 创建线程池 java.util.concurrent.ThreadPoolExecutor@76539573[Running, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
1> [subtask-0] p4 -> 客户端返回(p4 on rich-async-subtask-0-worker)
1> [subtask-0] p2 -> 客户端返回(p2 on rich-async-subtask-0-worker)
2> [subtask-1] p1 -> 客户端返回(p1 on rich-async-subtask-1-worker)
2> [subtask-1] p3 -> 客户端返回(p3 on rich-async-subtask-1-worker)
1> [subtask-0] p6 -> 客户端返回(p6 on rich-async-subtask-0-worker)
[close] subtask 0 -> 线程池已关闭
2> [subtask-1] p5 -> 客户端返回(p5 on rich-async-subtask-1-worker)
[close] subtask 1 -> 线程池已关闭
```

**三个观察点**：

1. **每个 subtask 各建一个线程池**（两个不同的 `ThreadPoolExecutor` 对象 hash），不是全局共享 —— `open()` 是 per-subtask 的，这正是"线程池必须建在 `open` 而不是 `main`"的原因。
2. **`pool size = 0`** 说明 `Executors.newFixedThreadPool` 是**懒创建**的：建池时不立刻起线程，第一次 submit 才起。
3. `1>` / `2>` 前缀与 `[subtask-0]` / `[subtask-1]` 标注**一一对应**，且 `p1..p6` 的分布是 keyBy 之后的重分区结果。`close()` 里 `shutdown()` 必须做——否则 MiniCluster 退出时线程池线程会让 JVM **挂住不退出**（这是 `RichAsyncFunction` 最经典的"作业不结束"故障）。

> **实跑结论（诚实标注）**：`async/AsyncFunctionDemo`、`async/RichAsyncFunctionDemo` 均在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**；输出已剔除 Flink 的 `WARN` 日志行。这两个 demo 是**有界输入**（`fromData` + 显式 watermark），所以会自己结束——真实生产里异步算子面对的通常是无界流。
