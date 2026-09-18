package com.jiaqiz.flink.keyed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示 keyBy 的三种写法与 KeyedStream 上的聚合算子，并额外打印 key group 的落点，
 * 说明「key group 数 = maxParallelism」以及它如何与运行时并行度解耦。
 *
 * <p>运行命令：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.keyed.KeyedStreamDemo
 * </pre>
 *
 * <p>keyBy 的本质：按 key 的 hash 做一次重分区（PartitionTransformation + KeyGroupStreamPartitioner），
 * 保证「同一个 key 一定被路由到同一个 subtask」。它返回的不是新数据，而是把流"标记成有 key"的
 * {@link KeyedStream}，从而在编译期允许你使用 keyed state。
 */
public class KeyedStreamDemo {

    /**
     * POJO：{@code keyBy(String...)} 要求按字段名取 key，因此字段必须是 public
     * （或提供符合 JavaBean 规范的 getter/setter），并且必须有无参构造 —— 这是 Flink POJO 规则。
     */
    public static class Sale {

        public String category; // public 字段 → keyBy("category") 才能按名字索引到它
        public int amount;

        /** Flink POJO 要求 public 无参构造（用于反序列化 / 类型提取）。 */
        public Sale() {
        }

        public Sale(String category, int amount) {
            this.category = category;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Sale(" + category + ", " + amount + ")";
        }
    }

    /** {@code keyBy(KeySelector)} 的具名写法：从元素里取出 key。 */
    public static class WordKeySelector implements KeySelector<Tuple2<String, Integer>, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    /**
     * AggregateFunction：增量聚合（来一条算一条），比把整个窗口元素攒起来再算要省内存。
     * 泛型是 IN, ACC, OUT —— 这里的 ACC 和 OUT 都用 Tuple2&lt;key, runningSum&gt;。
     */
    public static class SumAggregate
            implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>,
                    Tuple2<String, Integer>> {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return Tuple2.of("", 0);
        }

        @Override
        public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> acc) {
            return Tuple2.of(value.f0, acc.f1 + value.f1);
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> acc) {
            return acc;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            // 只有 SessionWindow 这类"窗口需要合并"的 assigner 才会调用 merge
            return Tuple2.of(a.f0, a.f1 + b.f1);
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        final DataStream<Tuple2<String, Integer>> pairs =
                env.fromData(
                        Tuple2.of("flink", 1),
                        Tuple2.of("spark", 2),
                        Tuple2.of("flink", 3),
                        Tuple2.of("spark", 4),
                        Tuple2.of("flink", 5),
                        Tuple2.of("hive", 6));

        // ============================================================
        // 1) keyBy(KeySelector)
        // ============================================================
        // lambda 写法：key 类型由 Tuple2<String, Integer> 的 f0 推断为 String。
        // 如果元素类型本身是泛型擦除后的结果（例如来自没有 returns 的 lambda），
        // Flink 推断不出 KEY 时会抛 InvalidTypesException，那时要改用
        // keyBy(keySelector, Types.STRING) 这个带 TypeInformation 的重载。
        final KeyedStream<Tuple2<String, Integer>, String> bySelector = pairs.keyBy(tuple -> tuple.f0);
        System.out.println("1) keyBy(KeySelector lambda)      -> " + bySelector.getClass().getSimpleName()
                + ", keyType=" + bySelector.getKeyType());

        // 具名 KeySelector 写法，语义完全等价（生产代码里便于复用与单测）
        final KeyedStream<Tuple2<String, Integer>, String> byNamedSelector =
                pairs.keyBy(new WordKeySelector());
        System.out.println("   keyBy(KeySelector 具名类)      -> "
                + byNamedSelector.getClass().getSimpleName());

        // ============================================================
        // 2) keyBy(int...) —— Tuple 位置键（从 0 开始数）
        // ============================================================
        // 注意：Flink 1.20 里 keyBy(int...) / keyBy(String...) 已被标记 @Deprecated
        // （官方推荐一律用 KeySelector 写法，因为位置键/字段名是"魔法字符串"，重构时容易漏改）。
        // 这里按学习目的照常演示，编译只会有 deprecation 警告，运行完全正常。
        final KeyedStream<Tuple2<String, Integer>, Tuple> byPosition = pairs.keyBy(0);
        System.out.println("2) keyBy(0)（tuple 位置键）        -> " + byPosition.getClass().getSimpleName()
                + ", keyType=" + byPosition.getKeyType());

        // ============================================================
        // 3) keyBy(String...) —— POJO 字段名
        // ============================================================
        final DataStream<Sale> sales =
                env.fromData(new Sale("book", 10), new Sale("food", 20), new Sale("book", 30),
                        new Sale("toy", 40), new Sale("food", 50));
        final KeyedStream<Sale, Tuple> byFieldName = sales.keyBy("category");
        System.out.println("3) keyBy(\"category\")（POJO 字段名） -> " + byFieldName.getClass().getSimpleName()
                + ", keyType=" + byFieldName.getKeyType());

        // ============================================================
        // 4) keyed 聚合：reduce / sum / max
        // ============================================================
        // reduce：滚动聚合，每来一条就输出"到目前为止的合并结果"（流式，不缓存历史元素）
        final DataStream<Tuple2<String, Integer>> reduced =
                bySelector
                        .reduce(
                                new ReduceFunction<Tuple2<String, Integer>>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Tuple2<String, Integer> reduce(
                                            Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                                        return Tuple2.of(a.f0, a.f1 + b.f1);
                                    }
                                });
        reduced.print("4a-reduce(sum)");

        // 内置聚合 sum(1)：等价于上面把 f1 相加的 reduce，是 Flink 预置的 AggregationFunction
        bySelector.sum(1).print("4b-sum(1)");
        // 内置聚合 max(1)：取 f1 的最大值（其余字段保持 Flink 的内置语义，见注释）
        bySelector.max(1).print("4c-max(1)");
        // 按 POJO 字段名聚合
        byFieldName.sum("amount").print("4d-Sale.sum(amount)");

        // ============================================================
        // 5) aggregate(AggregateFunction)
        // ============================================================
        // 注意：KeyedStream 上唯一带 AggregateFunction 的 aggregate 不是 public API
        // （javap 显示只有 protected aggregate(AggregationFunction)），
        // 公开的 aggregate(AggregateFunction) 在 WindowedStream 上。
        // 所以这里先开一个 countWindow：每 2 条「同 key」数据触发一次窗口。
        // 用 countWindow 而不是时间窗口，是为了保证有界作业一定能触发（时间窗口在本地小数据下可能不触发）。
        final DataStream<Tuple2<String, Integer>> aggregated =
                bySelector.countWindow(2).aggregate(new SumAggregate());
        aggregated.print("5-aggregate(countWindow(2) 求和)");

        // ============================================================
        // 6) key group 与并行度解耦
        // ============================================================
        // 结论先写在这里：
        //   * key group 的数量 == maxParallelism（不是当前并行度）；
        //   * key 先被映射到某个 key group（keyGroup = murmurHash(key.hashCode()) % maxParallelism），
        //     再由「key group → subtask」的区间映射决定它落到哪个 subtask；
        //   * 所以运行中改并行度只会重算第二层映射（key group 区间重新划分），
        //     第一层 key→keyGroup 完全不变 —— 这就是 Flink 能在线 rescale 而不丢状态的原因。
        final int maxParallelism = 128; // 128 是 Flink 的默认 maxParallelism，也就是 key group 数
        final int parallelism = 4; // 当前（或将来）的算子并行度，可以随时改
        System.out.println();
        System.out.println("6) key group 落点：maxParallelism(即 key group 数)=" + maxParallelism
                + ", parallelism=" + parallelism);
        System.out.println("   env.setParallelism 只影响调度并行度；key group 数由 maxParallelism 决定，"
                + "两者解耦");

        final String[] keys = {"flink", "spark", "hive", "hello", "world"};
        for (String key : keys) {
            final int keyHash = key.hashCode();
            // 公开 API：computeKeyGroupForKeyHash(int keyHash, int maxParallelism)
            // 源码实现即 MathUtils.murmurHash(keyHash) % maxParallelism
            final int keyGroup = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(keyHash, maxParallelism);
            System.out.printf("   key=%-6s hashCode=%-12d -> keyGroup=%3d%n", key, keyHash, keyGroup);
        }

        // assignToKeyGroup(Object key, int maxParallelism) 是同一件事的"对象版"：
        // 内部就是 computeKeyGroupForKeyHash(key.hashCode(), maxParallelism)
        System.out.println("   assignToKeyGroup(\"flink\", 128) = "
                + KeyGroupRangeAssignment.assignToKeyGroup("flink", maxParallelism));

        // 每个 subtask 负责哪一段 key group（并行度变化时，只有这里会变）
        for (int index = 0; index < parallelism; index++) {
            System.out.printf("   subtask %d 负责 keyGroupRange=%s%n", index,
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            maxParallelism, parallelism, index));
        }
        // 反向：某个 key group 由哪个 subtask 负责
        System.out.println("   computeOperatorIndexForKeyGroup(128, 4, keyGroup=10) = "
                + KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, 10));
        System.out.println("   computeDefaultMaxParallelism(parallelism=4) = "
                + KeyGroupRangeAssignment.computeDefaultMaxParallelism(4));

        env.execute("KeyedStreamDemo");
    }
}
