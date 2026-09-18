# 基础算子：MapFunction / FlatMapFunction / FilterFunction

> **代码基线**：Apache Flink 1.20.4　**源码位置**：仓库根目录 `flink-1.20-source/`
> **配套 demo**：`learn-flink/src/main/java/com/jiaqiz/flink/operators/`
> **上级文档**：[`datastream-api-architecture.md`](./datastream-api-architecture.md)　**导航**：[`README.md`](./README.md)

---

# 一、先纠正三处「位置」——它们和直觉不一样

写这一节的原因：这三个算子的类不在同一个包里，且**函数接口根本不在 `flink-core-api`**。记错位置会在读源码时浪费大量时间。

| 你要找的东西 | 真实位置 | 说明 |
|---|---|---|
| `MapFunction` / `FlatMapFunction` / `FilterFunction` | `flink-core/src/main/java/org/apache/flink/api/common/functions/` | ⚠️ **不在** `flink-core-api`。后者 `api/common/functions/` 只有 `Function`、`ReduceFunction`、`AggregateFunction` 三个 |
| `StreamMap` / `StreamFlatMap` / `StreamFilter` | `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/` | ⚠️ **不在** `streaming/runtime/operators/`。后者只有 `TimestampsAndWatermarksOperator`、`asyncprocessing/`、`sink/`、`windowing/`、`util/` |
| `OneInputTransformation` | `flink-streaming-java/.../streaming/api/transformations/` | ⚠️ `flink-core/.../api/dag/` 只有 `Transformation` 与 `Pipeline` |
| `TypeExtractor` | `flink-core/.../api/java/typeutils/TypeExtractor.java` | 类型推断的全部逻辑在这里 |

另外，**Flink 1.20 已经没有 `StreamGraphGenerator.transformOneInputTransform()`**（grep 零命中）。它被 FLIP-134 的 `TransformationTranslator` + `translatorMap` 精确类查表机制取代，详见 §2.2。

---

# 二、三个函数接口的契约

三个接口都是 `@Public @FunctionalInterface ... extends Function, Serializable`，都只有**一个**抽象方法。区别全在签名上：

```java
// flink-core/src/main/java/org/apache/flink/api/common/functions/MapFunction.java#L56
O map(T value) throws Exception;

// flink-core/src/main/java/org/apache/flink/api/common/functions/FlatMapFunction.java#L56
void flatMap(T value, Collector<O> out) throws Exception;

// flink-core/src/main/java/org/apache/flink/api/common/functions/FilterFunction.java#L58
boolean filter(T value) throws Exception;
```

javadoc 里的契约原文（这就是"算子语义"的官方定义）：

| 接口 | 行号 | javadoc 原文 |
|---|---|---|
| `MapFunction` | `#L27` | *"A Map function always produces a single result element for each input element."* |
| `FlatMapFunction` | `#L27-28` | *"FlatMap functions take elements and transform them, into zero, one, or more elements."* |
| `FilterFunction` | `#L37-38` | *"IMPORTANT: The system assumes that the function does not modify the elements on which the predicate is applied."* |

**一句话抓住差异**：

```
map      1 : 1   返回值 = 唯一的输出；框架无条件 collect 一次
flatMap  1 : N   返回 void；输出完全由 Collector 决定，可以一次都不调 collect
filter   0 : 1   返回 boolean；true 才把「原始对象」发下游，用户拿不到 Collector
```

> `filter` 的 javadoc 那句 "does not modify the elements" **不是风格建议**，而是运行时约束：`StreamFilter` 把**原始 `StreamRecord` 原样发下游**（§3.3），你在 `filter` 里改对象，等于改了已经在下游流里传播的同一个对象。

---

# 三、从 API 到算子：一次 `map` 调用发生了什么

## 3.1 三跳链路

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L593-600
    public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {

        TypeInformation<R> outType =
                TypeExtractor.getMapReturnTypes(
                        clean(mapper), getType(), Utils.getCallLocationName(), true);

        return map(mapper, outType);
    }

// DataStream.java#L613-616
    public <R> SingleOutputStreamOperator<R> map(
            MapFunction<T, R> mapper, TypeInformation<R> outputType) {
        return transform("Map", outputType, new StreamMap<>(clean(mapper)));
    }
```

注意 `map` 双参版最后那个 `true` 是 `allowMissing`（§4.1）。`flatMap` 同型：

```java
// DataStream.java#L650-653
    public <R> SingleOutputStreamOperator<R> flatMap(
            FlatMapFunction<T, R> flatMapper, TypeInformation<R> outputType) {
        return transform("Flat Map", outputType, new StreamFlatMap<>(clean(flatMapper)));
    }
```

`flatMap` 的**单参版**（`#L629-636`）会用 `TypeExtractor.getFlatMapReturnTypes(...)` 做推断。而 `filter` **完全不推断**：

```java
// DataStream.java#L716-718
    public SingleOutputStreamOperator<T> filter(FilterFunction<T> filter) {
        return transform("Filter", getType(), new StreamFilter<>(clean(filter)));
    }
```

> **为什么 `filter` 不推断**：`FilterFunction` 的 SAM 是 `IN → boolean`，输出类型**恒等于**输入类型，直接 `getType()` 拿上游类型即可。所以 **`filter` 永远不需要 `.returns(...)`**，只有 `map` / `flatMap` 可能被类型擦除坑到。

## 3.2 `transform()` → `doTransform()`：登记进 environment

```java
// DataStream.java#L1226-1233
        @SuppressWarnings({"unchecked", "rawtypes"})
        SingleOutputStreamOperator<R> returnStream =
                new SingleOutputStreamOperator(environment, resultTransform);

        getExecutionEnvironment().addOperator(resultTransform);

        return returnStream;
    }
```

`resultTransform` 在 `#L1218-1225` 构造，上游 `transformation` 直接作为输入边：

```java
new OneInputTransformation<>(
        this.transformation, operatorName, operatorFactory, outTypeInfo,
        environment.getParallelism(), false)
```

其中 `operatorFactory` 是 `SimpleOperatorFactory.of(operator)`（见 `#L1180-1186`）——**运行时算子在这一刻就被 new 出来了**，只是还没被调度。

登记落点只有 4 行：

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/api/StreamExecutionEnvironment.java#L2618-2621
    protected void addOperator(Transformation<?> transformation) {
        checkNotNull(transformation, "Transformation must not be null.");
        this.transformations.add(transformation);
    }
```

字段是 `#L195` 的 `protected final List<Transformation<?>> transformations`。

**三个易错点**：

1. 同一个 `DataStream` 上调用两次 `map`，得到**两个独立的 `OneInputTransformation`**（同源 DAG 分支），不是覆盖。demo 里 `lengthsByLambda` / `lengthsByAnon` / `lengthsByName` 就是这么来的。
2. `doTransform()` 第一行就主动读 **上游** `transformation.getOutputType()`（`#L1216`，读的是 `this.transformation`），**故意提前引爆上游的类型错误**。⚠️ 但它**读的不是新建的那个 transformation**，所以**不会**替下游引爆「输出类型没推断出来」的错误——那个要等第一次有人查输出类型才抛（§8.2 有真实堆栈）。
3. **"算子调用次数" ≠ "`JobVertex` 数量"**：算子链会把连续的 forward 算子合并成一个 `JobVertex`（§6）。

## 3.3 到运行时：`Transformation` → `StreamGraph` → `JobVertex`

| 跳 | 入口 | 关键行 |
|---|---|---|
| ① 触发建图 | `StreamExecutionEnvironment.getStreamGraph()` → `getStreamGraphGenerator(transformations).generate()` | `StreamExecutionEnvironment.java#L2503` / `#L2527` |
| ② 造空壳图 | `StreamGraphGenerator.generate()` | `StreamGraphGenerator.java#L310` |
| ③ 递归翻译（记忆化去重） | `private Collection<Integer> transform(...)` | `StreamGraphGenerator.java#L584` |
| ④ 翻译器注册表 | `static { tmp.put(OneInputTransformation.class, new OneInputTransformationTranslator<>()); }` | `StreamGraphGenerator.java#L183` |
| ⑤ 查表分派 | `translatorMap.get(transform.getClass())` | `StreamGraphGenerator.java#L665` |
| ⑥ 建 `StreamNode` + 加边 | 翻译器调 `streamGraph.addOperator(...)` / `addEdge(...)` | `runtime/translators/AbstractOneInputTransformationTranslator.java#L64` / `#L94` |
| ⑦ 选 `TaskInvokable` | `operatorFactory.isStreamSource() ? SourceStreamTask.class : OneInputStreamTask.class` | `StreamGraph.java#L380-381` |
| ⑧ 链化 + 出 `JobGraph` | `StreamingJobGraphGenerator.createJobGraph()` → `setChaining()` | `StreamingJobGraphGenerator.java#L245` / `#L718` |
| ⑨ 运行时驱动 | `StreamTask` → `OneInputStreamTask.StreamTaskNetworkOutput.emitRecord` → `recordProcessor.accept(record)` | `OneInputStreamTask.java#L218-239` |
| ⑩ 链内直传 | `StreamMap.output.collect(...)` → `ChainingOutput.pushToOperator` → 下游 `processElement` | `ChainingOutput.java#L100-109` |

③ 的**记忆化去重闸门**值得单独记：它返回的是 `StreamNode` **id 集合**（不是单个 id），因为 union / 迭代的一个逻辑节点对应多个 `StreamNode`；图可能有环，没有这道闸门递归停不下来。

④ 的翻译器按**精确类**注册，所以 `OneInputTransformation` → `OneInputTransformationTranslator`，`PartitionTransformation` → 另一个翻译器。1.20 的分派是查表命中即 `translate(...)`，只在 feedback 等三种兜底情况才走 `legacyTransform(...)`。

## 3.4 `RecordProcessorUtils` 的 per-record 优化

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/tasks/RecordProcessorUtils.java#L56-57
        if (canOmitSetKeyContext == true) {
            return input::processElement;
        }
```

无 key 上下文时**直接返回 `input::processElement`，跳过 `setKeyContextElement`**。这是热路径上实打实的省事：`map`/`flatMap`/`filter` 这类无状态算子，每条记录少一次 key 上下文设置。有 keyed state 的场景才走 `record -> { input.setKeyContextElement(record); input.processElement(record); }`。

---

# 四、三个算子外壳的实现

三个算子的实现短到可以完整贴出来，但它们把 §2 的三条契约精确落成了代码。

## 4.1 `StreamMap`：构造时指定 `ALWAYS` 链化，`processElement` 无条件 collect 一次

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/StreamMap.java#L26-39
public class StreamMap<IN, OUT> extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    public StreamMap(MapFunction<IN, OUT> mapper) {
        super(mapper);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        output.collect(element.replace(userFunction.map(element.getValue())));
    }
}
```

**三个要点**：

- `element.replace(...)` **复用同一个 `StreamRecord` 对象**、只换 payload 并**保留时间戳**——这是一对一算子的标准做法，避免每条记录 new 一个 `StreamRecord`。
- `output.collect(...)` 在 `map` 返回后**无条件**执行一次。`map` 抛异常则一次都不 collect（异常向上冒泡，由 `StreamTask` 决定失败/重启）。
- `chainingStrategy = ChainingStrategy.ALWAYS` 是**默认值就够**的，这里显式写出来是为了"读代码时不用去猜"。

## 4.2 `StreamFilter`：直接透传原始 `StreamRecord`，连 `replace` 都不做

```java
// StreamFilter.java#L26-41
public class StreamFilter<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>>
        implements OneInputStreamOperator<IN, IN> {

    private static final long serialVersionUID = 1L;

    public StreamFilter(FilterFunction<IN> filterFunction) {
        super(filterFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (userFunction.filter(element.getValue())) {
            output.collect(element);
        }
    }
}
```

差异全部在 `#L39` 这一行：**`output.collect(element)` 而不是 `output.collect(element.replace(...))`**。

- 泛型是 `OneInputStreamOperator<IN, IN>`——输入输出**同一个类型**，这是编译期就把"filter 不改变类型"钉死了。
- 时间戳天然保留（对象都没换），且**下游拿到的是同一个对象引用**。这正是 `FilterFunction` javadoc 警告"不要修改元素"的原因。
- `false` 分支什么都不做 = **静默丢弃**，不算异常、不算脏数据，只是少一条。所以 filter 输出条数**严格 ≤** 输入条数。

> **澄清一个常见误解**：`filter` **没有** `Collector` 参数。它的 SAM 只返回 `boolean`，所以它**不可能**一对多——只能 0 或 1。想"按条件动态决定发射几条（含 0 条）"必须用 `flatMap`。demo 的 `BasicFunctionsDemo` 第 4 段专门做了这个对照：用一条"命中才 `out.collect`"的 `flatMap` 实现 filter 效果。

## 4.3 `StreamFlatMap`：唯一的"有状态外壳"（持 `TimestampedCollector`）

```java
// StreamFlatMap.java#L26-48
public class StreamFlatMap<IN, OUT> extends AbstractUdfStreamOperator<OUT, FlatMapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private transient TimestampedCollector<OUT> collector;

    public StreamFlatMap(FlatMapFunction<IN, OUT> flatMapper) {
        super(flatMapper);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        userFunction.flatMap(element.getValue(), collector);
    }
}
```

**为什么要 `open()` 里建 `collector`**：`output` 是 `AbstractStreamOperator.output` 字段，要等运行时 `setup(...)` 注入后才可用（`AbstractStreamOperator.java#L120` / `#L170-175`）。`open()` 是保证 `setup` 已完成的第一个回调，所以 `TimestampedCollector` 只能在这里、且必须标 `transient`（它包着非序列化的 `Output`）。

**`TimestampedCollector` 的语义**（`TimestampedCollector.java#L38-61`）：

```java
public final class TimestampedCollector<T> implements Output<T> {

    private final Output<StreamRecord<T>> output;

    private final StreamRecord<T> reuse;          // ← 注意：只有一个实例

    public TimestampedCollector(Output<StreamRecord<T>> output) {
        this.output = output;
        this.reuse = new StreamRecord<T>(null);
    }

    @Override
    public void collect(T record) {
        output.collect(reuse.replace(record));     // ← 所有 collect 复用同一个 StreamRecord
    }

    public void setTimestamp(StreamRecord<?> timestampBase) {
        if (timestampBase.hasTimestamp()) {
            reuse.setTimestamp(timestampBase.getTimestamp());
        } else {
            reuse.eraseTimestamp();
        }
    }
```

`StreamFlatMap.processElement` 先 `setTimestamp(element)` 把**输入记录的时间戳**灌进 reuse，再交给用户。用户后续每次 `out.collect(...)` 都打上这同一个时间戳——这就是 flatMap 一对多时"所有输出继承输入时间戳"的机制来源。

> ⚠️ **`reuse` 只有一个实例**，这是最容易踩的坑：
> - 不要把 `out.collect(x)` 的返回值/引用缓存起来跨记录使用；
> - 不要把 `Collector` 存成字段跨 `processElement` 使用（下一个元素会改写同一个 `StreamRecord`）；
> - 开了对象重用（`enableObjectReuse`）时下游会看到同一对象被反复改写，必须立刻消费。
>
> ⚠️ `flatMap` **不做条数校验**：用户一次都不 collect 就输出 0 条，这是合法且常用的（用 flatMap 实现 filter 或"展开后无结果"的场景）。框架只负责把 `collect` 调用转成 `Output.collect`。

## 4.4 三算子对照总表

| 维度 | `StreamMap` | `StreamFlatMap` | `StreamFilter` |
|---|---|---|---|
| 类声明泛型 | `AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>` `implements OneInputStreamOperator<IN, OUT>` | `AbstractUdfStreamOperator<OUT, FlatMapFunction<IN, OUT>>` `implements OneInputStreamOperator<IN, OUT>` | `AbstractUdfStreamOperator<IN, FilterFunction<IN>>` `implements OneInputStreamOperator<IN, IN>` |
| 链化策略 | `ChainingStrategy.ALWAYS`（`#L33`） | `ChainingStrategy.ALWAYS`（`#L35`） | `ChainingStrategy.ALWAYS`（`#L33`） |
| 额外字段 | 无 | `transient TimestampedCollector<OUT> collector` | 无 |
| 重写 `open()` | 否 | **是**（建 collector） | 否 |
| 发下游的对象 | `element.replace(newValue)` | 用户 `collect` 的每个值（经 `reuse` 包装） | `element` **原样** |
| 输出条数 | 恰好 1（异常则 0） | 0..N（用户决定） | 0 或 1 |
| 时间戳 | 保留（`replace` 不擦） | 继承输入（`setTimestamp`） | 保留（对象未变） |
| 类型推断 | `TypeExtractor.getMapReturnTypes` | `TypeExtractor.getFlatMapReturnTypes` | **不推断**，直接用上游 `getType()` |

---

# 五、类型推断：什么情况下必须写 `.returns(...)`

`TypeExtractor`（`flink-core/.../api/java/typeutils/TypeExtractor.java`）靠**反射**从 UDF 的类/接口泛型参数里"猜"输出类型。作者自己在类注释里就承认了这套机制不可靠：

> `TypeExtractor.java#L88-92`
> *"Automatic type extraction is a hacky business ... The type extraction fails regularly with either MissingTypeInfo or hard exceptions."*

它要求调用方"务必准备手动传类型的兜底"——这就是 `.returns(...)` 存在的理由。失败有**两条路径，后果完全不同**：

## 5.1 路径 A：lambda + 泛型 → 抛异常 → 包成 `MissingTypeInfo` → 作业建不起来

```java
// flink-core/src/main/java/org/apache/flink/api/java/typeutils/TypeExtractionUtils.java#L358-374
    public static void validateLambdaType(Class<?> baseClass, Type t) {
        // ...
        if (clazz.getTypeParameters().length > 0) {
            throw new InvalidTypesException(
                    "The generic type parameters of '"
                            + clazz.getSimpleName()
                            + "' are missing. "
                            + "In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. "
                            + "An easy workaround is to use an (anonymous) class instead that implements the '"
                            + baseClass.getName()
                            + "' interface. "
                            + "Otherwise the type has to be specified explicitly using type information.");
        }
    }
```

调用点 `TypeExtractor.java#L599-601`（lambda 分支由 `#L571` 的 `checkAndExtractLambda(function)` 识别出来）：`output = exec.getReturnType(); TypeExtractionUtils.validateLambdaType(baseClass, output);`。

异常被捕获并**降级**成 `MissingTypeInfo`：

```java
// TypeExtractor.java#L617-625
        if (allowMissing) {
            return (TypeInformation<OUT>)
                    new MissingTypeInfo(functionName != null ? functionName : function.toString(), e);
        } else {
            throw e;
        }
```

`DataStream.map` 传的最后一个实参就是 `true`（`DataStream.java#L597`），所以**不立刻抛**。但 `MissingTypeInfo` **不会静默跑到 Kryo**，它在 `Transformation.getOutputType()` 里硬失败：

```java
// flink-core/src/main/java/org/apache/flink/api/dag/Transformation.java#L549-560
        if (outputType instanceof MissingTypeInfo) {
            MissingTypeInfo typeInfo = (MissingTypeInfo) this.outputType;
            // ...
                    throw new InvalidTypesException(
                            "The return type of function '"
                                    + typeInfo.getFunctionName()
                                    + "' could not be determined automatically, due to type erasure. "
                                    + "You can give type information hints by using the returns(...) method on the result of the transformation call, "
                                    + "or by letting your function implement the 'ResultTypeQueryable' interface.", ...);
```

**触发时机被刻意提前**——`StreamGraphGenerator.java#L656-657` 主动读一次输出类型，注释写明"唯一目的是提前触发异常"（`// call at least once to trigger exceptions about MissingTypeInfo`）。附带效果：`StreamGraph.createSerializer`（`StreamGraph.java#L1021-1025`）对 `MissingTypeInfo` 直接返回 `null` 序列化器。

> **`map`/`flatMap` 的 `allowMissing=true` 还有个必记的连带后果**：`SingleOutputStreamOperator.getType()` 拿到的是 `MissingTypeInfo`，但**整个作业提交时会抛 `InvalidTypesException`**。所以它绝不是"能跑但慢"，而是"根本交不上去"。

## 5.2 路径 B：类型可取但非 POJO → **静默**退化成 `GenericTypeInfo`（Kryo）

```java
// TypeExtractor.java#L1990-2000
        } catch (InvalidTypesException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unable to handle type " + clazz + " as POJO. Message: " + e.getMessage(), e);
            }
            // ignore and create generic type info
        }
        return new GenericTypeInfo<>(clazz);
```

POJO 规则很苛刻：**public 无参构造 + 所有字段 public，或所有字段有标准 getter/setter**。任一条不满足就落到 `#L2000`。

**最阴的坑**：元组里**某个字段**退化只打一条 `LOG.info`、**不报错**：

```java
// TypeExtractor.java#L933-939
                        LOG.info(
                                "Tuple field #{} of type '{}' will be processed as GenericType. {}",
                                ...);
```

后果：作业能跑，但**静默用 Kryo**——序列化体积大、吞吐低，且兼容性差（序列化异常往往推迟到运行时才炸）。查这个坑的正确方式是看日志里的 `will be processed as GenericType`。

## 5.3 两条路径对照与兜底手段

| | 路径 A：`MissingTypeInfo` | 路径 B：`GenericTypeInfo` |
|---|---|---|
| 触发条件 | lambda 的返回类型带泛型参数（`Tuple2<K,V>`、`List<T>`…） | 字段/类不满足 POJO 规则 |
| 是否报错 | **报错**：第一次查输出类型即抛 `InvalidTypesException`（§8.2 实测，可能远早于建图） | **不报错**，只 `LOG.info` / `LOG.debug` |
| 后果 | 建图失败，跑不起来 | 能跑，但静默走 Kryo（性能 + 兼容性双输） |
| 可见性 | 异常消息明确，好排查 | 极易被忽略 |

**兜底手段（优先级从高到低）**：

1. **`ResultTypeQueryable`** —— 让 UDF 实现它，显式给出类型。`TypeExtractor.java#L562-565` 注释：`// explicit result type has highest precedence`。
2. **`.returns(...)`** —— `SingleOutputStreamOperator.returns(TypeInformation<T>)`（`SingleOutputStreamOperator.java#L348-353`）实现为 `transformation.setOutputType(typeInfo)`，**直接覆盖推断结果**，完全绕过 `TypeExtractor`。`returns(Class)`（`#L294`）与 `returns(TypeHint)`（`#L324`）最终都走它。
3. **改用（匿名/具名）类而不是 lambda** —— 把 `Tuple2<String, Integer>` 写在 `implements` 子句上，`TypeExtractor` 就能反射读到。demo 里三种写法输出**完全相同的 `TupleTypeInfo`**。
4. `Types.STRING` / `Types.INT` / `Types.LONG` 就是 `BasicTypeInfo` 常量（`flink-core/.../api/common/typeinfo/Types.java#L78` / `#L102` / `#L108`）；`Types.TUPLE(...)` / `Types.POJO(...)`（`#L283`）构造复合类型。

**经验法则**：

```
lambda 返回值是非泛型的（String / Integer / Long …）        → 不用写 returns
lambda 返回值带泛型参数（Tuple2 / List / Map …）           → 必须写 returns
匿名类 / 具名类把泛型写在 implements 子句                    → 可以不写，但显式写更安全
想用 POJO 序列化（FieldAccessSerializer）而不是 Kryo         → 类必须严格合规，否则静默降级
```

---

# 六、算子的公共骨架

三个算子都继承同一条链：`StreamOperator` ← `AbstractStreamOperator` ← `AbstractUdfStreamOperator` ← 具体算子。

```java
// flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/AbstractUdfStreamOperator.java#L50-57
public abstract class AbstractUdfStreamOperator<OUT, F extends Function>
        extends AbstractStreamOperator<OUT>
        implements OutputTypeConfigurable<OUT>, UserFunctionProvider<F> {

    private static final long serialVersionUID = 1L;

    protected final F userFunction;
```

它的生命周期方法就是**把 UDF 的 `open`/`close` 挂进算子生命周期**：

```java
// AbstractUdfStreamOperator.java#L99-117
    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.openFunction(userFunction, DefaultOpenContext.INSTANCE);
    }
    // ...（省略 finish：对 SinkFunction 额外调 userFunction.finish()）
    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(userFunction);
    }
```

`setup`（`#L77-84`）注入 `RuntimeContext`：`FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());`（细节见 [`rich-functions.md`](./rich-functions.md)）。**`setKeyContextElement` 不在这个类里**，在 `AbstractStreamOperator.java#L536-543`（`setKeyContextElement1(record)` → `setKeyContextElement(record, stateKeySelector1)`），`OneInputStreamOperator.java#L34-37` 把单输入版本默认转发过去。

## 6.1 职责分层：谁提供什么

| 层 | 提供 |
|---|---|
| `AbstractStreamOperator` | 状态后端、`output` 字段、metrics、`timerService`、key 上下文 |
| `AbstractUdfStreamOperator` | `userFunction` 字段 + 生命周期转发 + OutputType 注入 |
| 具体算子（`StreamMap`…） | 只写 `processElement`：**怎么把用户逻辑接到 `output` 上** |

**"算子外壳的职责"就是这个**：把"用户逻辑"接到"运行时输出 / 时间戳 / key 上下文"上。三个算子的 `processElement` 加起来不到 10 行，但正是这几行决定了 §2 里那三条契约。

## 6.2 `Output` / `Collector`：用户永远只看窄接口

```java
// flink-core/src/main/java/org/apache/flink/util/Collector.java#L28-38
public interface Collector<T> {
    void collect(T record);
    void close();
}
```

```java
// flink-core/src/main/java/org/apache/flink/streaming/api/operators/Output.java#L39-66
public interface Output<T> extends Collector<T> {
    void emitWatermark(Watermark mark);
    void emitWatermarkStatus(WatermarkStatus watermarkStatus);
    <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record);
    void emitLatencyMarker(LatencyMarker latencyMarker);
    void emitRecordAttributes(RecordAttributes recordAttributes);
}
```

- **`Collector<T>` 是给用户看的窄接口**：只有 `collect` + `close`。
- **`Output<T>` 是算子内部宽接口**：泛型是 `StreamRecord<T>`，另有 watermark / latency marker / 侧输出等能力。
- `AbstractStreamOperator.output` 在 `#L120`，由 `setup(...)`（`#L170-175`）注入。**用户函数永远只看到 `Collector` 视角**——这正是三个算子内部 API 能统一的原因，也是用户无法从 `flatMap` 里直接发 watermark / 侧输出的原因（侧输出要走 `ProcessFunction`）。

`StreamOperator` 接口本身（`StreamOperator.java#L47`）`extends CheckpointListener, KeyContext, Serializable`，并在 `#L40-42` 明确保证：*"Methods of StreamOperator are guaranteed not to be called concurrently."* ——单线程语义由 `StreamTask` 的 mailbox 线程模型保证（详见 [`datastream-api-architecture.md`](./datastream-api-architecture.md)）。

---

# 七、易错点小结

| # | 坑 | 后果 / 正确做法 |
|---|---|---|
| 1 | lambda 不写 `.returns(...)`（泛型信息被擦除） | **第一次查输出类型时**即抛 `InvalidTypesException`（不必等到提交，见 §8.2）；补 `.returns(Types.TUPLE(...))` 或改用（具名 / 匿名）类写法 |
| 2 | 自定义类不满足 POJO 规则还写 `.returns(Types.POJO(X.class))` | **静默**退化成 `GenericTypeInfo`（Kryo）；元组字段退化只打 `LOG.info` |
| 3 | 缓存 `out.collect(x)` 的引用，或把 `Collector` 存成字段 | `TimestampedCollector` 只有一个 `reuse`，下一个元素会改写同一个 `StreamRecord` |
| 4 | 在 `filter` 里修改被判定元素 | 原对象直接发下游，下游看到的是你改过的对象；javadoc 明令禁止 |
| 5 | 以为 `filter` 能一对多 | `FilterFunction` 没有 `Collector`，只能 0/1；要 N 条用 `flatMap` |
| 6 | 以为 `map` 能过滤/拆多条 | `StreamMap#L38` 无条件 collect 一次，恒定一对一 |
| 7 | 用"算子调用次数"估 `JobVertex` 数 | forward + 同并行度会链化；demo 里 `fromData → map → flatMap → filter → print` 只有 **1 个** `JobVertex`，插入 `keyBy` 变 2 个 |
| 8 | 以为异常时 `map` 会发一条 null | `map` 抛异常则**一次都不 collect**，异常冒泡给 `StreamTask` |
| 9 | 以为 `StreamGraphGenerator.transformOneInputTransform()` 存在 | 1.20 已删，改用 `translationMap` / `translatorMap` 精确类查表（FLIP-134） |

---

# 八、配套 demo 与真实输出

| demo | 覆盖内容 | 运行命令 |
|---|---|---|
| `operators/BasicFunctionsDemo` | 三个函数 × 三种写法（lambda / 匿名类 / 具名类）；flatMap 当 filter；类型推断实测 | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.operators.BasicFunctionsDemo` |
| `operators/DataStreamStructureDemo` | 打印 `DataStream → SingleOutputStreamOperator → KeyedStream → WindowedStream → DataStreamSink` 对象链，及 `name`/`uid`/`setParallelism`/`slotSharingGroup`/`disableChaining` | `mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.operators.DataStreamStructureDemo` |

## 8.1 必看的三个观察点

1. **map 恒定一对一**：4 条进 → 4 条出，条数不变（`)`依据 `StreamMap.java#L38` 只有一次 `collect`）。
2. **flatMap 零/多输出**：4 行文本 → 多个词；专门喂一行 `""` 或全空白，**输出 0 条**——证明"零输出"合法（`StreamFlatMap.java#L47`，用户不调 `collect` 即可）。
3. **filter 只减不增**：输出条数严格 ≤ 输入，元素值不变（`StreamFilter.java#L39` 原样 `output.collect(element)`，不做 `replace`）。

## 8.2 实测：lambda 不写 `.returns(...)` 的真实报错与**真实抛出时机**

这一节的内容全部来自**真跑一个故意漏写 `.returns(...)` 的作业**得到的堆栈，不是推测。

### 关键纠正：不是"建图时"才抛，而是"第一次查输出类型"时就抛

很多说法（含网上不少文章）称"提交作业时才报错"。**实测不是。** 下面这个程序在 `flatMap(...)` 那一行**构建成功、没有抛异常**，异常是在**下一行查类型时**抛出的：

```java
// 故意漏写 .returns(...)
DataStream<Tuple2<String, Integer>> words =
        env.fromData("a b c")
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    for (String w : line.split("\\s+")) {
                        if (!w.isEmpty()) { out.collect(Tuple2.of(w, 1)); }
                    }
                });

System.out.println(">>> flatMap 构建完成（没有当场抛异常），类型 = " + words.getType()); // ← 就是这一行抛的
```

**真实输出（原样复制，仅删去日志行）**：

```
>>> 开始构建 flatMap（漏写 returns）
Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException: The return type of function 'main(NoReturnsDemo.java:16)' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
	at org.apache.flink.api.dag.Transformation.getOutputType(Transformation.java:557)
	at org.apache.flink.streaming.api.datastream.DataStream.getType(DataStream.java:195)
	at com.jiaqiz.scratch.NoReturnsDemo.main(NoReturnsDemo.java:23)
Caused by: org.apache.flink.api.common.functions.InvalidTypesException: The generic type parameters of 'Collector' are missing. In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface. Otherwise the type has to be specified explicitly using type information.
	at org.apache.flink.api.java.typeutils.TypeExtractionUtils.validateLambdaType(TypeExtractionUtils.java:371)
	at org.apache.flink.api.java.typeutils.TypeExtractionUtils.extractTypeFromLambda(TypeExtractionUtils.java:188)
	at org.apache.flink.api.java.typeutils.TypeExtractor.getUnaryOperatorReturnType(TypeExtractor.java:592)
	at org.apache.flink.api.java.typeutils.TypeExtractor.getFlatMapReturnTypes(TypeExtractor.java:209)
	at org.apache.flink.streaming.api.datastream.DataStream.flatMap(DataStream.java:632)
	at com.jiaqiz.scratch.NoReturnsDemo.main(NoReturnsDemo.java:16)
```

**从堆栈里能读出三件常被说错的事**：

| # | 常见说法 | 实测事实 |
|---|---|---|
| 1 | "`flatMap(...)` 一调用就抛" | ❌ **不抛**。`flatMap(...)`（栈里 `DataStream.java:632`）只是把 `MissingTypeInfo` **装进** transformation，然后正常返回 |
| 2 | "提交 / 建图时才抛" | ❌ **不必等到建图**。栈顶是 `Transformation.getOutputType(Transformation.java:557)` ← `DataStream.getType()(DataStream.java:195)`，即**任何第一次查输出类型的地方**都会抛：可能是 `getType()`、可能是下游算子要读输入类型、也可能是 `StreamGraphGenerator` |
| 3 | "因为 lambda 的**返回类型**带泛型" | ⚠️ 不准确。真实消息是 **`The generic type parameters of 'Collector' are missing`** —— 卡住的是 **`flatMap` 的 `Collector<O>` 参数**（`void` 返回值本身没有泛型）。"返回类型带泛型"这个说法对 `map` 才成立 |

> ⭐ **实践含义**：既然触发点是"第一次 `getOutputType()`"，错误就可能出现在**离出错算子很远的地方**。真正的出错算子在 `Caused by` 段的**最深处**（这里是 `DataStream.flatMap(DataStream.java:632)`）——排查时看 `Caused by`，不要只看栈顶的 `getType()` 调用点。
>
> 另外：`doTransform()` 第一行读的是**上游** transformation 的类型（`DataStream.java#L1216`，读的是 `this.transformation`）而不是新建的那个，所以 **`doTransform` 不会替你引爆这个错误**——这正好解释了"为什么构建能成功"。

### 对照实验：换成具名类就不需要 `.returns(...)`

`BasicFunctionsDemo` 里的 `Tokenizer` 是具名类：

```java
public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> { ... }
```

泛型 `Tuple2<String, Integer>` 写在 `implements` 子句上，属于**运行时可见的 `Signature` 信息**，`TypeExtractor` 反射就能读到。所以它**不写 `.returns(...)` 也拿得到完全一样的 `TupleTypeInfo`**（这正是 §8.3.2 ⑤ 里两行输出完全相同的原因）。

**结论**：lambda 退化、类不退化。用 lambda 就必须 `.returns(...)`，或让函数实现 `ResultTypeQueryable`。

## 8.3 真实运行输出

输入固定为三行文本（并行度 1）：

```java
env.fromData("hello flink world", "flink spark flink", "big data");
```

### 8.3.1 `DataStreamStructureDemo`：包装链是真的

```
[1] env.fromData(...)                -> DataStreamSource
[2] .map(...).returns(...)           -> SingleOutputStreamOperator
[3] .keyBy(...)                      -> KeyedStream
[4] .countWindow(2)                  -> WindowedStream
[4b] .sum(1)（回到 DataStream）      -> SingleOutputStreamOperator
[5] .print("structure")              -> DataStreamSink

=== 每个算子的 uid / name / 并行度 ===
source      : Collection Source, parallelism=1
classify    : classify-parity, uid=classify-parity-uid, parallelism=2, parallelismConfigured=true, maxParallelism=-1, slotSharingGroup=default
window-sum  : GlobalWindows, parallelism=2
sink        : Print to Std. Out, parallelism=2

=== 打印完结构后真正提交作业（有界，会自己退出） ===
structure:2> (even,6)
structure:2> (odd,4)
```

> 每个"链式调用"返回的**确实是另一个 `DataStream` 家族子类**——这就是 §3.2 里 `doTransform()` 每次 `new SingleOutputStreamOperator(...)` 的直接证据。
>
> 注意 `maxParallelism=-1` 表示 `AUTO`：**提交时才计算**，不是"没有值"。

### 8.3.2 `BasicFunctionsDemo`：三个算子 × 三种写法

`print()` 前缀 `flatMap-lambda>` / `flatMap-anon>` / `flatMap-named>` 是三条独立分支各自的 Sink，**它们并行输出所以行序会交错**——这是"每个 `print()` 是一个独立 Sink 算子"的正常现象，不是数据错乱。按分支归类后如下（数字为真实输出，未改动）：

**① map —— 1:1，条数不变（3 行进 → 3 行出）**

```
lambda  -> 17
匿名类  -> 17
具名类  -> 17
lambda  -> 17
匿名类  -> 17
具名类  -> 17
lambda  -> 8
匿名类  -> 8
具名类  -> 8
```

三行输入长度分别为 `"hello flink world"=17`、`"flink spark flink"=17`、`"big data"=8`。**三种写法（lambda / 匿名类 / 具名类）输出完全一致**。

**② flatMap —— 1:N，8 条输出（3 行进 → 8 条出）**

以 `flatMap-named` 分支为例（`Tokenizer` 切词 + `keyBy(word).sum(1)` 的流式 WordCount）：

```
flatMap-named> 具名类  -> (hello,1)
flatMap-named> 具名类  -> (flink,1)
flatMap-named> 具名类  -> (world,1)
flatMap-named> 具名类  -> (flink,2)
flatMap-named> 具名类  -> (spark,1)
flatMap-named> 具名类  -> (flink,3)
flatMap-named> 具名类  -> (big,1)
flatMap-named> 具名类  -> (data,1)
```

8 个词（`hello / flink / world / flink / spark / flink / big / data`），`flink` 出现 3 次所以累加到 `(flink,3)`。**注意这里插入了 `keyBy`，所以链已被切断**——正好印证 §3.3 的链化规则。

**③ filter —— 0/1，条数严格 ≤ 输入，元素值不变**

`filter(word -> word.length() > 4)` 只留下 6 个长度 > 4 的单词：

```
lambda  -> hello
匿名类  -> hello
具名类  -> hello
lambda  -> flink
匿名类  -> flink
具名类  -> flink
lambda  -> world
匿名类  -> world
具名类  -> world
lambda  -> flink
匿名类  -> flink
具名类  -> flink
lambda  -> spark
匿名类  -> spark
具名类  -> spark
lambda  -> flink
匿名类  -> flink
具名类  -> flink
```

`big`(3) 与 `data`(4) 被 **静默丢弃**（`filter` 返回 `false`，`StreamFilter#L38-40` 什么都不做）。留下来的是 `hello / flink / world / flink / spark / flink` 共 6 条，单词本身**一个字都没变**。

**④ 用 `flatMap` 实现 filter —— `Collector` 可以一次都不发射**

```
flatMap-as-filter -> hello
flatMap-as-filter -> flink
flatMap-as-filter -> world
flatMap-as-filter -> flink
flatMap-as-filter -> spark
flatMap-as-filter -> flink
```

输出与 ③ 完全一致，但机制不同：这里是 `if (word.length() > 4) out.collect(word);`——**控制权在 `Collector` 手里**，所以它还能做"一条输入拆成 0 / 1 / 多条"的动态决定，而 `filter` 永远只能是 0 或 1（§4.2）。

**⑤ 类型推断对照**

```
=== 5) 类型推断 ===
wordsByLambda(显式 returns TUPLE) 的 TypeInformation = Java Tuple2<String, Integer>
wordsByName(未写 returns)      的 TypeInformation = Java Tuple2<String, Integer>
环境实际并行度 = 1
```

**两行完全一样**——lambda 补 `.returns(...)` 与具名类"靠反射拿到泛型"最终产出同一个 `TupleTypeInfo`。唯一的差别是**前者是必须的，后者是白拿的**：把 `wordsByLambda` 的 `.returns(...)` 删掉就会得到 §8.2 的异常。

> **实跑结论（诚实标注）**：`BasicFunctionsDemo` 与 `DataStreamStructureDemo` 均在 **JDK 17 + Flink 1.20.4 本地 MiniCluster 实跑通过，退出码 0**，输出为 `print()` 的 stdout（已剔除 Flink 的 `WARN` 日志行，未改动任何业务输出）。

---

## 九、延伸阅读

- [`datastream-api-architecture.md`](./datastream-api-architecture.md) —— 总体架构、五大分层、算子链、类型推断全景
- [`rich-functions.md`](./rich-functions.md) —— `RichMapFunction` / `RichFlatMapFunction` / `RichFilterFunction` 与 `RuntimeContext`
- [`process-functions.md`](./process-functions.md) —— 需要侧输出 / 定时器 / 状态时的升级路径
- [`keyby-and-partitioners.md`](./keyby-and-partitioners.md) —— `keyBy` 之后算子如何被切开、`JobVertex` 为什么变多
