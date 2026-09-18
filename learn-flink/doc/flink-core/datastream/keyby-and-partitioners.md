# keyBy、KeyGroup 与 Partitioner 家族

> **代码基线**：Apache Flink 1.20.4　**源码位置**：仓库根目录 `flink-1.20-source/`
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/keyed/`、`.../partitioner/`
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)

---

# 一、keyBy 链路：只插"虚拟节点"，不建物理算子

```java
// DataStream.java#L293
    public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key) {
        Preconditions.checkNotNull(key);
        return new KeyedStream<>(this, clean(key));
    }
```

```java
// KeyedStream.java#L128
    public KeyedStream(
            DataStream<T> dataStream,
            KeySelector<T, KEY> keySelector,
            TypeInformation<KEY> keyType) {
        this(
                dataStream,
                new PartitionTransformation<>(
                        dataStream.getTransformation(),
                        new KeyGroupStreamPartitioner<>(
                                keySelector,
                                StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)),
                keySelector,
                keyType);
    }
```

`keyBy(int...)`（`DataStream.java#L320`）、`keyBy(String...)`（`#L341`）都收敛到：

```java
// DataStream.java#L345
    private KeyedStream<T, Tuple> keyBy(Keys<T> keys) {
        return new KeyedStream<>(
                this,
                clean(KeySelectorUtil.getSelectorForKeys(keys, getType(), getExecutionConfig())));
    }
```

| 类 | 位置 | 说明 |
|---|---|---|
| `KeySelector<IN,KEY>` | `flink-core-api/.../api/java/functions/KeySelector.java#L36` | `@FunctionalInterface`，`getKey(IN)` 声明 `throws Exception` |
| `KeySelectorUtil` | `flink-streaming-java/.../util/keys/KeySelectorUtil.java#L44` | `getSelectorForKeys` / `getSelectorForArray` / `getSelectorForOneKey`，把字段位置编译成复用 `TypeComparator` 的 `ComparableKeySelector`（比反射逐字段取值快） |

> ⚠️ **KeyedStream 锁死了分区**：
> ```java
> // KeyedStream.java#L275
>     public DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
>         throw new UnsupportedOperationException("Cannot override partitioning for KeyedStream.");
> ```
> 所以 `keyBy(...).rebalance()` **编译能过、构图即炸**。
>
> ⚠️ **`KeySelector` 必须确定性 + 可序列化**：同一对象多次调用必须返回相同 key，否则同一逻辑 key 会散到不同 subtask，keyed state 被切碎。
>
> ⚠️ 泛型擦除下 `keyBy(lambda)` 的 keyType 常退化成 `GenericType`；生产代码建议用 `keyBy(key, TypeInformation<K>)` 重载（`DataStream.java#L306`）。

---

# 二、KeyGroup：key 空间与并行度之间的"可缩放中间层"

```java
// KeyGroupRangeAssignment.java#L50
    public static int assignKeyToParallelOperator(Object key, int maxParallelism, int parallelism) {
        Preconditions.checkNotNull(key, "Assigned key must not be null!");
        return computeOperatorIndexForKeyGroup(
                maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
    }

    public static int assignToKeyGroup(Object key, int maxParallelism) {
        Preconditions.checkNotNull(key, "Assigned key must not be null!");
        return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
    }

    public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
        return MathUtils.murmurHash(keyHash) % maxParallelism;
    }
```

```java
// KeyGroupRangeAssignment.java#L93 与 #L124
    public static KeyGroupRange computeKeyGroupRangeForOperatorIndex(
            int maxParallelism, int parallelism, int operatorIndex) {
        checkParallelismPreconditions(parallelism);
        checkParallelismPreconditions(maxParallelism);
        Preconditions.checkArgument(
                maxParallelism >= parallelism,
                "Maximum parallelism must not be smaller than parallelism.");
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        return new KeyGroupRange(start, end);
    }

    public static int computeOperatorIndexForKeyGroup(
            int maxParallelism, int parallelism, int keyGroupId) {
        return keyGroupId * parallelism / maxParallelism;
    }
```

**完整三跳**（`KeyGroupStreamPartitioner.selectChannel` 用的正是前两跳）：

```
key → murmurHash(key.hashCode()) % maxParallelism = keyGroup
    → keyGroup * parallelism / maxParallelism     = subtask 下标
```

且某 subtask 覆盖的 keyGroup 恰好是 `computeKeyGroupRangeForOperatorIndex` 给出的闭区间（`KeyGroupRange.java#L53`，`contains` = `>= start && <= end`，`#L67`）。

| 常量 | 值 | 位置 |
|---|---|---|
| `DEFAULT_LOWER_BOUND_MAX_PARALLELISM` | `1 << 7` = 128 | `KeyGroupRangeAssignment.java#L32` |
| `UPPER_BOUND_MAX_PARALLELISM` | `1 << 15` = 32768 | `flink-core/.../api/dag/Transformation.java#L114` |
| `computeDefaultMaxParallelism` | `clamp(roundUpToPowerOfTwo(p + p/2), 128, 32768)` | `KeyGroupRangeAssignment.java#L137` |

> ⚠️ **为什么 keyGroup 与 parallelism 解耦**：`maxParallelism` 一旦定下**不可更改**（改了 key→keyGroup 全变，state 无法恢复）；`parallelism` 可任意调（只要 ≤ maxParallelism），因为只改了第二层的整数除法。这正是 `SubtaskStateMapper.RANGE` 与 rescale 恢复能成立的前提。
>
> ⚠️ `parallelism > maxParallelism` 直接抛 `IllegalArgumentException`（`checkParallelismPreconditions` `#L149`）。
>
> ⚠️ **`keyBy` 时写死的 128 不是终值**：运行时会被下游 key group 数覆盖 ——
> ```java
> // StreamTask.java#L1803
>         if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
>             int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
>             if (0 < numKeyGroups) {
>                 ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
>             }
>         }
> ```
> 这就是 `KeyGroupStreamPartitioner` 必须实现 `ConfigurableStreamPartitioner` 的原因。

---

# 三、Partitioner 家族

`StreamPartitioner` 是 `ChannelSelector` 的流式专化：

```java
// StreamPartitioner.java#L29
/** A special {@link ChannelSelector} for use in streaming programs. */
@Internal
public abstract class StreamPartitioner<T>
        implements ChannelSelector<SerializationDelegate<StreamRecord<T>>>, Serializable {
    protected int numberOfChannels;

    @Override
    public void setup(int numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
    }

    public abstract StreamPartitioner<T> copy();

    public abstract SubtaskStateMapper getDownstreamSubtaskStateMapper();

    public abstract boolean isPointwise();

    public boolean isSupportsUnalignedCheckpoint() {
        return supportsUnalignedCheckpoint && !isPointwise() && !isBroadcast();
    }
```

```java
// ChannelSelector.java#L28
public interface ChannelSelector<T extends IOReadableWritable> {
    void setup(int numberOfChannels);
    int selectChannel(T record);
    boolean isBroadcast();
}
```

> ⚠️ **1.20 没有 `selectChannels`（复数）**：广播只靠 `isBroadcast()` 标志，由 writer 侧循环铺开。

| 类（`streaming/runtime/partitioner/`） | `selectChannel` 行为 | `isPointwise` | `toString` | downstream mapper |
|---|---|---|---|---|
| `ForwardPartitioner` | 恒 `0` | **true** | `FORWARD` | `UNSUPPORTED` |
| `RebalancePartitioner` | `setup` 时随机起点，之后 `+1 % n` | false | `REBALANCE` | `ROUND_ROBIN` |
| `RescalePartitioner` | `++next % n` | **true** | `RESCALE` | `UNSUPPORTED` |
| `ShufflePartitioner` | `random.nextInt(n)` | false | `SHUFFLE` | `ROUND_ROBIN` |
| `BroadcastPartitioner` | **抛异常** | false | `BROADCAST` | `UNSUPPORTED` |
| `GlobalPartitioner` | 恒 `0` | false | `GLOBAL` | `FIRST` |
| `KeyGroupStreamPartitioner` | keyGroup → subtask | false | `HASH` | **`RANGE`** |
| `CustomPartitionerWrapper` | `partitioner.partition(key, n)` | false | `CUSTOM` | `FULL` |

```java
// ForwardPartitioner.java#L34
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        return 0;
    }

    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public boolean isPointwise() {
        return true;
    }
```

```java
// BroadcastPartitioner.java#L34
    /**
     * Note: Broadcast mode could be handled directly for all the output channels in record writer,
     * so it is no need to select channels via this method.
     */
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        throw new UnsupportedOperationException(
                "Broadcast partitioner does not support select channels.");
    }
    ...
    @Override
    public boolean isBroadcast() {
        return true;
    }
```

```java
// KeyGroupStreamPartitioner.java#L54
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        K key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not extract key from " + record.getInstance().getValue(), e);
        }
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(
                key, maxParallelism, numberOfChannels);
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.RANGE;
    }
```

```java
// CustomPartitionerWrapper.java#L49
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        K key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException("Could not extract key from " + record.getInstance(), e);
        }

        return partitioner.partition(key, numberOfChannels);
    }
```

`Partitioner` 本体在 `flink-core/.../api/common/functions/Partitioner.java#L28`：`@Public @FunctionalInterface`，`int partition(K key, int numPartitions)`。

## 3.1 两个"过渡态 forward"（容易看不懂的类）

| 类 | 用途 |
|---|---|
| `ForwardForConsecutiveHashPartitioner`（`#L25`） | SQL planner 把连续多个相同 hash shuffle 改写成 forward 以便链化；一旦因多输入断链，这个 forward 边就锁死下游并行度推断（FLINK-25046）。故引入占位：**链内转 `ForwardPartitioner`、链间转回其持有的 hashPartitioner** |
| `ForwardForUnspecifiedPartitioner`（`#L27`） | 为 `partitioner == null` 的边占位：**链内转 FORWARD、链间转 RESCALE**，避免默认 FORWARD 边强迫下游并行度对齐上游 |

> ⚠️ 这两个类的 `selectChannel` / `copy()` / `getDownstreamSubtaskStateMapper` **一律抛 `RuntimeException`** —— 它们只允许存在于链化之前；`isPointwise()` 是唯一不抛的方法（因为 `StreamGraphGenerator#shouldDisableUnalignedCheckpointing` 会问它）。

> ⚠️ `copy()` 的两种语义：无状态分区器返回 `this`；`ShufflePartitioner` 返回新实例、`CustomPartitionerWrapper` 用 `InstantiationUtil.clone(partitioner)` 克隆（`#L69`），保证 task 间不共享 `Random` / 用户状态。
>
> ⚠️ `equals/hashCode` 参与 `StreamEdge` 去重判定：基类只比 `numberOfChannels`；`KeyGroupStreamPartitioner` 额外比 `keySelector` 与 `maxParallelism`（`#L93`）。

---

# 四、DataStream API 上的用户入口

所有入口共用一个私有出口：

```java
// DataStream.java#L1242
    protected DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
        return new DataStream<>(
                this.getExecutionEnvironment(),
                new PartitionTransformation<>(this.getTransformation(), partitioner));
    }
```

| 方法（均在 `DataStream.java`） | 行号 | 生成的 partitioner |
|---|---|---|
| `broadcast()` | `#L422` | `BroadcastPartitioner` |
| `shuffle()` | `#L451` | `ShufflePartitioner` |
| `forward()` | `#L461` | `ForwardPartitioner` |
| `rebalance()` | `#L471` | `RebalancePartitioner` |
| `rescale()` | `#L494` | `RescalePartitioner` |
| `global()` | `#L506` | `GlobalPartitioner` |
| `partitionCustom(Partitioner<K>, KeySelector<T,K>)` | `#L400` | `CustomPartitionerWrapper` |
| `partitionCustom(Partitioner<K>, int field)`（`@Deprecated`） | `#L363` | 同上 |
| `partitionCustom(Partitioner<K>, String field)`（`@Deprecated`） | `#L381` | 同上 |

`RescalePartitioner.java#L26` 的类注释点明了 rebalance vs rescale 的**图结构差异**：

```java
/**
 * Partitioner that distributes the data equally by cycling through the output channels. This
 * distributes only to a subset of downstream nodes because {@link
 * org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator} instantiates a {@link
 * DistributionPattern#POINTWISE} distribution pattern when encountering {@code RescalePartitioner}.
 * ...
```

| 语义速记 | |
|---|---|
| `forward` | 一对一（**要求并行度相等**） |
| `rebalance` | 全局 round-robin（`ALL_TO_ALL`） |
| `rescale` | **局部** round-robin（`POINTWISE`）：上下游 2→4 时，上游每个只发 2 个下游 |
| `shuffle` | 随机 |
| `global` | 全给 subtask 0（并行度 >1 时是瓶颈） |
| `broadcast` | 每条发给所有 subtask |

> ⚠️ **`isPointwise()` 是"局部/全局"的唯一开关**：它直接决定 JobGraph 里是 `DistributionPattern.POINTWISE` 还是 `ALL_TO_ALL`。

---

# 五、分区器如何进入运行时

## 5.1 Transformation → StreamGraph：虚拟分区节点

```java
// StreamGraph.java#L570
    public void addVirtualPartitionNode(
            Integer originalId,
            Integer virtualId,
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode) {
        if (virtualPartitionNodes.containsKey(virtualId)) {
            throw new IllegalStateException(
                    "Already has virtual partition node with id " + virtualId);
        }
        virtualPartitionNodes.put(virtualId, new Tuple3<>(originalId, partitioner, exchangeMode));
    }
```

未指定分区器时的**默认规则**：

```java
// StreamGraph.java#L684
        if (partitioner == null
                && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
            partitioner =
                    dynamic ? new ForwardForUnspecifiedPartitioner<>() : new ForwardPartitioner<>();
        } else if (partitioner == null) {
            partitioner = new RebalancePartitioner<Object>();
        }
```

紧随其后（`#L696`）若 `partitioner instanceof ForwardPartitioner` 而上下游并行度不等，抛 `UnsupportedOperationException`（*"Forward partitioning does not allow change of parallelism…"*）。

## 5.2 StreamGraph → JobGraph：`isPointwise` 决定 DistributionPattern

```java
// StreamingJobGraphGenerator.java#L1577
        JobEdge jobEdge;
        if (partitioner.isPointwise()) {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.POINTWISE,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        } else {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.ALL_TO_ALL,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        }

        // set strategy name so that web interface can show it.
        jobEdge.setShipStrategyName(partitioner.toString());
```

## 5.3 为什么只有 forward 能链化

```java
// StreamingJobGraphGenerator.java#L1758
    static boolean arePartitionerAndExchangeModeChainable(
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode,
            boolean isDynamicGraph) {
        // 条件 3（partitioner 层面）：只有 ForwardPartitioner 能链。
        // 其余 partitioner —— hash / rebalance / rescale / broadcast / custom ——
        // 都意味着"按某种规则把数据分发到下游不同 subtask"，
        // 这必须经由 ResultPartition/InputChannel 的网络栈，链内直传无法表达。
        if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
            checkState(isDynamicGraph);
            return true;
        } else if ((partitioner instanceof ForwardPartitioner)
                && exchangeMode != StreamExchangeMode.BATCH) {
            return true;
        } else {
            return false;
        }
    }
```

**链内边根本不生成 JobEdge** —— 能链 = 无网络开销；断链 = 必有 `ResultPartition`。

## 5.4 运行时：变成 `ChannelSelector`

```java
// StreamTask.java#L1784 与 #L1803
        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }

        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        .setChannelSelector(outputPartitioner)
                        .setTimeout(bufferTimeout)
                        .setTaskName(taskNameWithSubtask)
                        .build(bufferWriter);
```

```java
// ChannelSelectorRecordWriter.java#L42
    @Override
    public void emit(T record) throws IOException {
        emit(record, channelSelector.selectChannel(record));
    }
```

```java
// RecordWriter.java#L105
    public void emit(T record, int targetSubpartition) throws IOException {
        checkErroneous();

        targetPartition.emitRecord(serializeRecord(serializer, record), targetSubpartition);

        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }
```

广播**不走** `selectChannel`：`ChannelSelectorRecordWriter.java#L58` 的 `broadcastEmit` 自行 `for (subpartitionIndex = 0; ...; ...)` 逐个 `emit`。

> ⚠️ 分区器在 task 内是**每个 output 一份克隆**（`StreamTask.java#L1786` 用 `InstantiationUtil.clone`），所以用户 `Partitioner` 里的可变状态不会跨 subtask 串。
>
> ⚠️ **别指望在 JobGraph 上看到 `HASH` 分区器的"分发逻辑"**：hash 边靠运行时 `KeyGroupStreamPartitioner` 算 channel；`jobEdge.setShipStrategyName(partitioner.toString())` 打的只是显示名 `HASH`。

---

# 六、易错点小结

| # | 易错点 | 正解 |
|---|---|---|
| 1 | `keyBy(...)` 之后调 `rebalance()` | 抛 `Cannot override partitioning for KeyedStream.` |
| 2 | `forward()` 但上下游并行度不等 | 抛 `Forward partitioning does not allow change of parallelism…` |
| 3 | 以为 `maxParallelism` 可以随便改 | 改了 key→keyGroup 映射全变，state 无法恢复 |
| 4 | 以为 `keyBy` 用的是 `key.hashCode() % parallelism` | 是 `murmurHash(hashCode) % maxParallelism` 再除 parallelism |
| 5 | `KeySelector` 返回不确定的 key | keyed state 被切碎，结果不稳定 |
| 6 | 依赖 `selectChannels`（复数） | 1.20 没有该方法；广播看 `isBroadcast()` |
| 7 | 在 `partitionCustom` 的 `Partitioner` 里放可变状态 | 运行时每 output 一份克隆，但同一 output 内共享，注意线程安全 |
| 8 | 以为 `union` 会重分区 | `union` 是"多输入合一"，不重分布 |

---

# 七、配套 demo 与真实输出

| demo | 覆盖 | 运行 |
|---|---|---|
| `keyed/KeyedStreamDemo` | `keyBy(KeySelector)` / `keyBy(int...)` / `keyBy(String...)`、`reduce` / `aggregate`、keyGroup 计算 | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.keyed.KeyedStreamDemo` |
| `partitioner/PartitionerDemo` | forward / rebalance / rescale / shuffle / broadcast / global / custom 的分布对照 | `... -Dmain.class=com.jiaqiz.flink.partitioner.PartitionerDemo` |

### 7.1 `KeyedStreamDemo`：KeyGroup 三跳公式的现场验证

> **实跑状态**：JDK 17 + Flink 1.20.4 本地 MiniCluster **实跑通过，退出码 0**。

```
1) keyBy(KeySelector lambda)      -> KeyedStream, keyType=String
   keyBy(KeySelector 具名类)      -> KeyedStream
2) keyBy(0)（tuple 位置键）        -> KeyedStream, keyType=Java Tuple1<String>
3) keyBy("category")（POJO 字段名） -> KeyedStream, keyType=Java Tuple1<String>

6) key group 落点：maxParallelism(即 key group 数)=128, parallelism=4
   env.setParallelism 只影响调度并行度；key group 数由 maxParallelism 决定，两者解耦
   key=flink  hashCode=97520992     -> keyGroup= 98
   key=spark  hashCode=109638365    -> keyGroup=  7
   key=hive   hashCode=3202928      -> keyGroup=  9
   key=hello  hashCode=99162322     -> keyGroup= 35
   key=world  hashCode=113318802    -> keyGroup= 66
   assignToKeyGroup("flink", 128) = 98
   subtask 0 负责 keyGroupRange=KeyGroupRange{startKeyGroup=0, endKeyGroup=31}
   subtask 1 负责 keyGroupRange=KeyGroupRange{startKeyGroup=32, endKeyGroup=63}
   subtask 2 负责 keyGroupRange=KeyGroupRange{startKeyGroup=64, endKeyGroup=95}
   subtask 3 负责 keyGroupRange=KeyGroupRange{startKeyGroup=96, endKeyGroup=127}
   computeOperatorIndexForKeyGroup(128, 4, keyGroup=10) = 0
   computeDefaultMaxParallelism(parallelism=4) = 128
```

这张输出把 §二 的三跳公式**逐跳打印了出来**：

| 输出值 | 对应哪一跳 / 什么规则 |
|---|---|
| `key=flink hashCode=97520992 → keyGroup=98` | **第 1 跳** `key → keyGroup`。注意 **`97520992 % 128 = 32`，但真实 keyGroup 是 98** —— 中间必须过 `murmurHash` 混淆，**绝不是 `hashCode % maxParallelism`** |
| `computeOperatorIndexForKeyGroup(128, 4, keyGroup=10) = 0` | **第 2 跳** `keyGroup → subtask`：`keyGroup * parallelism / maxParallelism`，即 `10 * 4 / 128 = 0` |
| `computeDefaultMaxParallelism(parallelism=4) = 128` | `maxParallelism` 的自动计算规则（下面的源码） |

`maxParallelism` 自动值不是"向上取到并行度的下一个 2 的幂"，真实公式是：

```java
// flink-runtime/src/main/java/org/apache/flink/runtime/state/KeyGroupRangeAssignment.java#L137-147
    public static int computeDefaultMaxParallelism(int operatorParallelism) {

        checkParallelismPreconditions(operatorParallelism);

        return Math.min(
                Math.max(
                        MathUtils.roundUpToPowerOfTwo(
                                operatorParallelism + (operatorParallelism / 2)),
                        DEFAULT_LOWER_BOUND_MAX_PARALLELISM),
                UPPER_BOUND_MAX_PARALLELISM);
    }
```

代入 `parallelism=4`：`4 + 4/2 = 6` → `roundUpToPowerOfTwo(6) = 8` → `max(8, DEFAULT_LOWER_BOUND_MAX_PARALLELISM=128) = 128` → `min(128, 32768) = 128`。**对小并行度而言，真正起作用的是下限 128**，不是那个 2 的幂。

> ⭐ **`maxParallelism` 与 `parallelism` 解耦的硬证据**：keyGroup 总数固定 128，`env.setParallelism(4)` 只是把 128 个 keyGroup **平均切成 4 段、每段 32 个**。因此**改并行度不改变 `key → keyGroup` 的映射，只改变 `keyGroup → subtask` 的映射**——这正是 Flink 能在不丢 key 归属的前提下调整并行度（rescale）的根本原因。
>
> ⚠️ **不要自己用 `hashCode % 并行度` 算分区**：`flink.hashCode()=97520992` 取模 128 得 32，而真实 keyGroup 是 98。任何"手算分区"的代码都必须改用 `KeyGroupRangeAssignment.assignToKeyGroup(...)`。

**keyBy 的四种重载全部跑通**（输出前三行）：`KeySelector` lambda、`KeySelector` 具名类、`keyBy(0)` 位置键、`keyBy("category")` POJO 字段名。后两种的 `keyType` 都是 `Java Tuple1<String>` —— **位置键 / 字段名键在内部被包装成元组选择器**；这也是它们后来不太被推荐的原因：字符串字段名无法在编译期校验，改字段名只会在运行时炸。

**keyed 聚合算子的真实输出**（并行度 4，前缀是 subtask 下标）：

```
4a-reduce(sum):1> (spark,2)
4b-sum(1):1> (spark,2)
4a-reduce(sum):1> (spark,6)
4a-reduce(sum):1> (hive,6)
4c-max(1):1> (spark,2)
4c-max(1):2> (flink,1)
4c-max(1):1> (spark,4)
4b-sum(1):1> (spark,6)
4b-sum(1):1> (hive,6)
4a-reduce(sum):2> (flink,1)
4a-reduce(sum):2> (flink,4)
4a-reduce(sum):2> (flink,9)
5-aggregate(countWindow(2) 求和):1> (spark,6)
4b-sum(1):2> (flink,1)
4b-sum(1):2> (flink,4)
4b-sum(1):2> (flink,9)
5-aggregate(countWindow(2) 求和):2> (flink,4)
4d-Sale.sum(amount):1> Sale(book, 10)
4d-Sale.sum(amount):1> Sale(book, 40)
4d-Sale.sum(amount):2> Sale(food, 20)
4c-max(1):1> (hive,6)
4d-Sale.sum(amount):2> Sale(food, 70)
4c-max(1):2> (flink,3)
4c-max(1):2> (flink,5)
4d-Sale.sum(amount):1> Sale(toy, 40)
```

**`spark`（keyGroup 7）全程只出现在 `1>`，`flink`（keyGroup 98）全程只出现在 `2>`** —— 这是"同 key 必落同一 subtask"的直接证据：`(spark,2) → (spark,6)` 在同一 subtask 内累加，`(flink,1) → (flink,4) → (flink,9)` 同理。

四种聚合写法对照：

| 分支 | 写法 | 输出特征 |
|---|---|---|
| `4a-reduce(sum)` | `reduce(ReduceFunction)` | `(spark,2) → (spark,6)`，与 `sum` 结果一致 |
| `4b-sum(1)` | `sum(1)` | 与 `reduce` **逐值相同**（`sum` 就是内置的 reduce） |
| `4c-max(1)` | `max(1)` | 只保留当前最大值（`(flink,1)→(flink,3)→(flink,5)`），**不回退** |
| `4d-Sale.sum(amount)` | POJO 字段 sum | 输出整个 POJO 对象（`Sale(book, 40)`），不是单个数字 |
| `5-aggregate(countWindow(2))` | `countWindow(2).sum(1)` | 每 2 条触发一次，所以是"两两合并"的值 |

---

### 7.2 `PartitionerDemo`：八个分区器的实测分布（**同一 demo 跑四遍**对照）

> **实跑状态**：JDK 17 + Flink 1.20.4 本地 MiniCluster **实跑通过，退出码 0**（共跑 4 遍用于区分"确定性"与"随机性"）。输入固定为 `1..8`，下游并行度 4；`print` 前缀 `N>` 与 demo 自己打印的 `-> subtask N/4` 互为印证。

#### 先给结论：哪些是"确定"的，哪些不是

**跑 4 遍，逐值比对后的结果**（这是本文最重要的一张表）：

| 分区器 | 条数守恒 | 4 遍映射是否完全一致 | 稳定可验证的特征 |
|---|---|---|---|
| `global()` | ✅ 8 | ✅ **完全一致** | 8 条**全部** → subtask 0 |
| `partitionCustom((k,n)->k%2)` | ✅ 8 | ✅ **完全一致** | **偶 → 0、奇 → 1** |
| `broadcast()` | ❌ **32**（8×4） | ✅ 结构一致 | 每个值 → **全部 4 个** subtask |
| `forward()` | ✅ 8 | ❌ 变 | 值被切成**连续两元组** `{1,2}{3,4}{5,6}{7,8}`，每组一个 subtask；**哪组落哪个 subtask 每次变** |
| `rescale()`(4→4) | ✅ 8 | ❌ 变（但**每次都与同一次的 `forward()` 逐值相同**） | 等并行度下**退化为 forward** |
| `rescale()`(2→4) | ✅ 8 | ❌ 变（4 遍中 3 遍相同） | 在**相邻两个** subtask 之间交替 |
| `rebalance()` | ✅ 8 | ❌ 变 | 不是干净的全局面轮询 |
| `shuffle()` | ✅ 8 | ❌ 变 | 在**两个** subtask 之间成组交替 |

> ⭐ **第一条铁律：除 `broadcast()` 外，所有分区器都严格保持总条数**（8 进 8 出，每值恰好一次，无重复无丢失）。`broadcast()` 是唯一放大条数的（8 × 4 = 32）。**验证分区器正确性只能看这类不变量**，不能看单次分布是否"均匀"。

#### （1）`global()` 与 `partitionCustom` —— 只有这两个是"值 → subtask"完全确定的

```
global()               4 遍全部 = {1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0}
partitionCustom(%2)    4 遍全部 = {1:1, 2:0, 3:1, 4:0, 5:1, 6:0, 7:1, 8:0}
```

- `global()`：**8 条全进 subtask 0**，subtask 1/2/3 **一条都没有**。
  > ⚠️ 这意味着 `global()` 之后下游并行度名义上是 4，**实际只有 1 个 subtask 在干活**。用于全局排序 / 全局去重可以，但必须清楚这是"主动放弃并行度"。
- `partitionCustom(%2)`：**完全确定**，4 遍逐值相同。demo 的 `ModTwoPartitioner` 刻意只返回 0/1（**忽略 `numPartitions=4`**），所以 subtask 2/3 收不到数据 —— 证明**自定义分区器的返回值直接决定落点，框架不做任何二次分配**：
  ```java
  public int partition(Integer key, int numPartitions) {
      int target = Math.abs(key) % 2; // 只映射到 0/1，无视 numPartitions=4
      // 防御性校验：返回越界下标会在运行时抛 IndexOutOfBoundsException
      return Math.min(target, numPartitions - 1);
  }
  ```
  > ⚠️ **返回值必须落在 `[0, numPartitions)`**，否则运行时抛 `IndexOutOfBoundsException`。这是自定义分区器最常见的翻车点。

#### （2）`broadcast()` —— 唯一放大条数，且每条去**所有** subtask

4 遍**都是 32 条**，且逐值检查后每个值的目标集合都是 `[0, 1, 2, 3]`：

```
broadcast()   4 遍均为 32 条；per-value destinations = {1:[0,1,2,3], 2:[0,1,2,3], ..., 8:[0,1,2,3]}
```

> ⚠️ **代价**：条数 × N、网络流量 × N。只该用在"小维表下发"（配合广播状态，见 [`process-functions.md`](./process-functions.md) 的广播小节），**绝不能**用来"提高并行度"。

#### （3）`forward()` —— 值→subtask 映射**每次运行都变**

4 遍的完整映射：

| 运行 | 值 → subtask |
|---|---|
| run1 | `1→3, 2→3, 3→2, 4→2, 5→1, 6→1, 7→0, 8→0` |
| run2 | `1→1, 2→1, 3→3, 4→3, 5→2, 6→2, 7→0, 8→0` |
| run3 | `1→1, 2→1, 3→2, 4→2, 5→3, 6→3, 7→0, 8→0` |
| run4 | `1→1, 2→1, 3→3, 4→3, 5→0, 6→0, 7→2, 8→2` |

**4 遍的共同点（这才是可依赖的部分）**：8 个值被切成 **4 个连续两元组** `{1,2} {3,4} {5,6} {7,8}`，**每个 subtask 恰好拿一组**（8 ÷ 4 = 2 条/subtask），条数守恒。**不同点**：哪一组落到哪个 subtask **每次都在变**。

**为什么？** 关键在 `forward` 自己不做任何选择：

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/partitioner/ForwardPartitioner.java#L35-37
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        return 0;
    }
```

`selectChannel` **恒返回 0**、`isPointwise()` 为 `true` —— 所以 **`forward()` 的目标 subtask 完全由"上游是哪个 subtask"决定**，分区器本身不掺和。既然映射会变，变化只能来自**上游**：本 demo 的上游是 `env.fromData(...).setParallelism(4)`（`FromElementsFunction` 集合源，`StreamExecutionEnvironment.java#L1501-1503`；注意 `fromCollection` 内部还会 `.setParallelism(1)`，demo 再显式改成 4），它把 8 个元素按**连续分片**交给 4 个 subtask，而**"分片 ↔ subtask 下标"的对应关系在不同运行间不稳定**。

> ⭐ **结论（比"forward 是轮询"重要得多）**：`forward()` 只保证 **"上游 subtask i → 下游 subtask i"**，**不保证任何"值 → subtask"映射**。要让某个值稳定落到某个 subtask，必须用 `keyBy` 或 `partitionCustom`。
>
> 另外 `forward()` **要求上下游并行度完全相等**，否则作业提交时报：
> ```
> java.lang.UnsupportedOperationException: Forward partitioning does not allow change of parallelism.
> Upstream operation: Source: Collection Source-1 parallelism: 1, downstream operation: Map-3 parallelism: 4
> You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.
> ```

#### （4）`rescale()` —— 等并行度时**退化为 forward**；2→4 时"就近分段"可见但段归属会变

**⭐ 一个 4 遍都成立的强结论**：`rescale()(4→4)` 的映射**与同一次运行的 `forward()` 逐值完全相同**（4/4 遍验证通过）：

| 运行 | `forward()` | `rescale()(4→4)` | 相同？ |
|---|---|---|---|
| run1 | `1→3,2→3,3→2,4→2,5→1,6→1,7→0,8→0` | 同左 | ✅ |
| run2 | `1→1,2→1,3→3,4→3,5→2,6→2,7→0,8→0` | 同左 | ✅ |
| run3 | `1→1,2→1,3→2,4→2,5→3,6→3,7→0,8→0` | 同左 | ✅ |
| run4 | `1→1,2→1,3→3,4→3,5→0,6→0,7→2,8→2` | 同左 | ✅ |

原因在源码里很直白：`RescalePartitioner` 的计数器**从 -1 开始、无随机起点、纯自增取模**：

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/partitioner/RescalePartitioner.java#L50-58
    private int nextChannelToSendTo = -1;

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        if (++nextChannelToSendTo >= numberOfChannels) {
            nextChannelToSendTo = 0;
        }
        return nextChannelToSendTo;
    }
```

上下游并行度相等时，每个上游 subtask 的 POINTWISE 下游子集**只有它自己**（`numberOfChannels == 1`），计数器恒为 0 → **与 `forward` 完全等价**（同线程、无网络）。

> ⚠️ 所以"`rescale` 在并行度相等时与 `rebalance` 相同"这个常见说法是**错的**：等并行度时 `rescale ≈ forward`，而 `rebalance` 是**跨所有下游 subtask 轮询**（可能跨网络）。要看出 `rescale` 的"就近分段"，**必须让下游并行度大于上游**。

**`rescale()(2→4)`：段内轮询可见，但"哪个上游拿哪一段"会变**

| 运行 | 值 → subtask |
|---|---|
| run1 | `1→2, 2→3, 3→2, 4→3, 5→0, 6→1, 7→0, 8→1` |
| run2 | `1→0, 2→1, 3→0, 4→1, 5→2, 6→3, 7→2, 8→3` |
| run3 | `1→0, 2→1, 3→0, 4→1, 5→2, 6→3, 7→2, 8→3` |
| run4 | `1→0, 2→1, 3→0, 4→1, 5→2, 6→3, 7→2, 8→3` |

**稳定可见的特征**：每一组 4 个值都在**相邻的两个** subtask 之间交替（run2/3/4 是 `{1,2,3,4}` 在 `{0,1}` 交替、`{5,6,7,8}` 在 `{2,3}` 交替；run1 则是 `{0,1}` 与 `{2,3}` 对调）。**这就是"就近分段"**：上游 subtask 0 只发往下游 `{0,1}`、上游 1 只发往下游 `{2,3}`，**跨段绝不发生**。这正是 `rescale` 与 `rebalance`（全局轮询）的本质区别。

> ⚠️ **但"哪个上游 subtask 拥有哪一段"会变**（run1 与其他三遍相反）——因为段归属由上游的分片↔subtask 对应关系决定，而那正是（3）里已经证明**不稳定**的东西。
> **所以：`rescale` 的"段内轮询"是确定性的；"值→subtask"的完整映射不是。** 4 遍里 3 遍相同只能说明"多数情况下稳定"，不能当作保证。

#### （5）`rebalance()` —— 每个上游通道各自轮询，**起点随机**

| 运行 | 值 → subtask |
|---|---|
| run1 | `1→0, 2→1, 3→1, 4→2, 5→0, 6→1, 7→0, 8→1` |
| run2 | `1→2, 2→3, 3→3, 4→0, 5→1, 6→2, 7→2, 8→3` |
| run3 | `1→2, 2→3, 3→0, 4→1, 5→1, 6→2, 7→3, 8→0` |
| run4 | `1→2, 2→3, 3→2, 4→3, 5→0, 6→1, 7→2, 8→3` |

4 遍都**不是** `0,1,2,3,0,1,2,3` 那样的全局轮询，而是"**每个上游通道内部连续**"。机制：

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/partitioner/RebalancePartitioner.java#L39-49
    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);

        nextChannelToSendTo = ThreadLocalRandom.current().nextInt(numberOfChannels);
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
        return nextChannelToSendTo;
    }
```

**起点是 `ThreadLocalRandom` 随机选的**，之后每来一条 `+1 取模`。而**每个上游通道的 partitioner 实例各有一份独立计数器**，所以：

- 单次运行看起来"不均匀"是**正常现象**；
- 短数据量下**看不到**漂亮的全局面轮询；
- 数据量足够大 + 通道足够多时，整体才趋于均匀。

#### （6）`shuffle()` —— **不是"每条独立均匀随机"那么理想**

| 运行 | 值 → subtask |
|---|---|
| run1 | `1→3, 2→1, 3→3, 4→1, 5→3, 6→1, 7→3, 8→1` |
| run2 | `1→2, 2→0, 3→2, 4→0, 5→2, 6→0, 7→2, 8→0` |
| run3 | `1→3, 2→0, 3→3, 4→0, 5→3, 6→0, 7→3, 8→0` |
| run4 | `1→1, 2→3, 3→1, 4→3, 5→1, 6→3, 7→1, 8→3` |

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/partitioner/ShufflePartitioner.java#L39-41
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        return random.nextInt(numberOfChannels);
    }
```

`selectChannel` **每条记录独立 `nextInt`**、无状态。但**实测 4 遍都只落在 2 个 subtask 上**（run1/4 是 `{1,3}`、run2/3 是 `{0,2}`），并呈**成组交替**，没有一次落在全部 4 个 subtask 上。

> ⭐ **这是本节最反直觉的一条**：`shuffle()` **并不保证一次运行里"均匀铺满所有 subtask"**。
> - **8 条数据太少**：4 个通道下，"具体落在哪两个通道"本身就带很大方差；
> - 真正的 `random.nextInt` 调用发生在**每个上游通道各自的 partitioner 实例**里，随机序列相互独立；
> - 所以**绝不能**用"跑一次看分布均匀不均匀"来判断 `shuffle` 是否正常 —— **只能看条数守恒**。
> 生产上想在短时间窗口内获得稳定的均匀分布，应该用 **`rebalance()`**（有计数器，长期严格轮询）而不是 `shuffle()`。

#### （7）怎么正确验证分区器

因为 `forward` / `rebalance` / `shuffle` / `rescale` 的**具体落点本质上是非确定的**，验证分区器只能看**不变量**：

| 该看什么 | 不该看什么 |
|---|---|
| 总条数是否守恒（**`broadcast` 例外，应为 ×N**） | 单次运行各 subtask 是否"正好一半" |
| 每个值是否恰好出现预期次数（`broadcast` 为 N 次；其余为 1 次） | `rebalance` / `shuffle` / `forward` 的某一次具体映射 |
| **确定**的：`global()`（全 → 0）、`partitionCustom`（按你的函数）、`keyBy`（同 key 同 subtask） | 把 `shuffle` 的偏斜当成 bug |
| `rescale` 的**段内相邻轮询**是否出现（必须下游并行度 > 上游） | 用 `hashCode % 并行度` 手算去比对 `keyBy` |
| **`broadcast`**：每个值的目标集合是否 == 全部 subtask | 用 `forward` 期望某个值固定落某个 subtask |

> **实跑结论（诚实标注）**：`partitioner/PartitionerDemo` 在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**，本文的稳定性结论来自**同一 demo 连续跑 4 遍**的逐值比对（4 遍映射全部列在正文里，未做任何修饰）。`global` / `partitionCustom` 4 遍完全一致；`forward` / `rebalance` / `shuffle` / `rescale`(2→4) 的**值→subtask 映射会变**；`rescale`(4→4) 在 4 遍中都**等于同一次的 `forward`**；`broadcast` 4 遍都是 32 条且每值到全部 4 个 subtask。
