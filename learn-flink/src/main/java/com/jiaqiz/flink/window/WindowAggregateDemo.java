package com.jiaqiz.flink.window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口<b>增量聚合</b>四条写法（{@code reduce} / {@code aggregate} / {@code sum-min-max} /
 * {@code aggregate + ProcessWindowFunction}）的演示，并对照说明状态占用的差别。
 *
 * <p>本 demo 覆盖：
 *
 * <ol>
 *   <li>{@code .reduce(ReduceFunction)} —— 用 {@link ReduceFunction} 就地归约，输入输出同类型
 *   <li>{@code .aggregate(AggregateFunction)} —— 自己实现 {@link AggregateFunction}
 *       （{@code createAccumulator} / {@code add} / {@code getResult} / {@code merge}），输入、累加器、输出三者类型可以不同
 *   <li>{@code .sum(int)} / {@code .sum(String)} / {@code .min(String)} / {@code .max(String)} —— 内置聚合的语法糖
 *   <li>{@code .aggregate(AggregateFunction, ProcessWindowFunction)} —— <b>增量聚合 + 窗口元信息</b>，
 *       窗口函数拿到的 {@code Iterable} 里只有 1 个元素（累加器结果）
 *   <li>{@link AggregateFunction#merge} 对<b>会话窗口（合并窗口）</b>为什么是必需的
 * </ol>
 *
 * <p>运行命令（有界源 + 事件时间，跑完自动退出）：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.window.WindowAggregateDemo
 * </pre>
 *
 * <p>涉及的运行时算子：全部是 {@code WindowOperator}
 * （{@code flink-1.20-source/.../runtime/operators/windowing/WindowOperator.java#L101}）。
 * 差别只在窗口内容状态的类型（由 {@code WindowOperatorBuilder} 决定）：
 *
 * <pre>
 *   .reduce(...)            → ReducingStateDescriptor，状态名 "window-contents"，每个 key+窗口 1 个累加器
 *   .aggregate(...)         → AggregatingStateDescriptor，每个 key+窗口 1 个累加器
 *   .apply(WindowFunction)  → ListStateDescriptor&lt;StreamRecord&lt;IN&gt;&gt;，每个 key+窗口存【全部原始元素】
 * </pre>
 *
 * <p>一句话结论：<b>窗口越大、元素越多，全量路径的状态就越危险</b>；能用增量聚合就别用全量遍历。
 */
public class WindowAggregateDemo {

    /** 固定基准时间（能被 5000 整除）。 */
    private static final long BASE = 1700000000000L;

    /** 销售记录 POJO。字段名会在 {@code .sum("amount")} 这类字段表达式里用到。 */
    public static class Sale {
        public String user;
        public long ts;
        public int amount;

        public Sale() {}

        public Sale(String user, long ts, int amount) {
            this.user = user;
            this.ts = ts;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return user + "@" + rel(ts) + "(" + amount + ")";
        }
    }

    /** 累加器（POJO：public 字段 + 无参构造，Flink 才能用 POJO 序列化器）。 */
    public static class Stats {
        public long count;
        public long sum;
        public int min = Integer.MAX_VALUE;
        public int max = Integer.MIN_VALUE;

        public Stats() {}

        @Override
        public String toString() {
            return "Stats{count=" + count + ", sum=" + sum + ", min=" + min + ", max=" + max + "}";
        }
    }

    /**
     * 自己实现的 {@link AggregateFunction}：累加器只存 4 个标量 → 每个 key+窗口 1 个 {@code Stats}。
     *
     * <p>{@code reportMerge} 为 true 时会把每次 {@code merge} 调用打印出来，
     * 用来证明「合并窗口（会话窗口）合并时，累加器是靠 {@code merge} 合并的」。
     */
    public static class StatsAggregate implements AggregateFunction<Sale, Stats, String> {

        private final boolean reportMerge;

        public StatsAggregate() {
            this(false);
        }

        public StatsAggregate(boolean reportMerge) {
            this.reportMerge = reportMerge;
        }

        @Override
        public Stats createAccumulator() {
            // 空累加器：注意 min/max 的哨兵值，别写成 0
            return new Stats();
        }

        @Override
        public Stats add(Sale value, Stats accumulator) {
            // 每来一条数据就地更新累加器：这行代码是 O(1)，不需要把元素存下来
            accumulator.count += 1;
            accumulator.sum += value.amount;
            accumulator.min = Math.min(accumulator.min, value.amount);
            accumulator.max = Math.max(accumulator.max, value.amount);
            return accumulator;
        }

        @Override
        public String getResult(Stats accumulator) {
            double avg = accumulator.count == 0 ? 0.0 : (double) accumulator.sum / accumulator.count;
            return "count=" + accumulator.count + ", sum=" + accumulator.sum + ", min=" + accumulator.min
                    + ", max=" + accumulator.max + ", avg=" + String.format("%.2f", avg);
        }

        @Override
        public Stats merge(Stats a, Stats b) {
            // 只有合并窗口（会话窗口）会调用；翻滚/滑动窗口永远不会走到这里
            Stats merged = new Stats();
            merged.count = a.count + b.count;
            merged.sum = a.sum + b.sum;
            merged.min = Math.min(a.min, b.min);
            merged.max = Math.max(a.max, b.max);
            if (reportMerge) {
                System.out.println("        [merge] AggregateFunction.merge 被调用: a=" + a + " + b=" + b
                        + " -> " + merged + "   （两个窗口的累加器在这里合并成一个）");
            }
            return merged;
        }
    }

    private static String rel(long ts) {
        return "+" + (ts - BASE) + "ms";
    }

    private static String win(TimeWindow w) {
        return "[" + rel(w.getStart()) + ", " + rel(w.getEnd()) + ")";
    }

    /**
     * 每条记录都发一条 watermark 的事件时间生成器。
     *
     * <p>不用 {@code WatermarkStrategy.forMonotonousTimestamps()} 的原因：它只在
     * {@code onPeriodicEmit} 里发水位线，周期由 {@code ExecutionConfig#getAutoWatermarkInterval()}（默认 200ms）控制，
     * 而本 demo 的有界源瞬间跑完，中间一条都发不出来 —— 所有窗口只能等输入结束的
     * {@code MAX_WATERMARK} 才触发，「窗口随水位线推进逐个触发」的过程就看不见了。
     *
     * <p>发射顺序由 {@code TimestampsAndWatermarksOperator#processElement} 保证：
     * 先 {@code output.collect(element)}、后 {@code watermarkGenerator.onEvent(...)}
     * （{@code TimestampsAndWatermarksOperator.java#L138/L139}），所以水位线不会「追尾」自己的元素。
     */
    public static class PerRecordWatermarks implements WatermarkGenerator<Sale> {

        private long maxTs = Long.MIN_VALUE;

        /**
         * 乱序容忍度：watermark = maxTs - outOfOrdernessMillis。
         *
         * <p>默认 1（= 「单调递增」语义，只让出 1ms 表示"小于等于 maxTs 的都已经到齐"）。
         * 第 5 节要故意插入一条<b>乱序</b>元素去"架桥"合并两个会话窗口，就必须把水位线压后，
         * 否则那个窗口早就在水位线越过 maxTimestamp 时触发并清理掉了，乱序元素会被当成迟到数据丢弃。
         */
        private final long outOfOrdernessMillis;

        public PerRecordWatermarks() {
            this(1L);
        }

        public PerRecordWatermarks(long outOfOrdernessMillis) {
            this.outOfOrdernessMillis = outOfOrdernessMillis;
        }

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

    private static WatermarkStrategy<Sale> perRecordWatermarks() {
        return perRecordWatermarks(1L);
    }

    private static WatermarkStrategy<Sale> perRecordWatermarks(long outOfOrdernessMillis) {
        return WatermarkStrategy.<Sale>forGenerator(
                        ctx -> new PerRecordWatermarks(outOfOrdernessMillis))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<Sale>) (element, recordTimestamp) -> element.ts);
    }

    private static StreamExecutionEnvironment newEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return env;
    }

    // ==================================================================================
    // 各小节
    // ==================================================================================

    /** 1) reduce(ReduceFunction)：输入输出同类型的就地归约。 */
    private static void demoReduce() throws Exception {
        System.out.println();
        System.out.println("=== 1) .reduce(ReduceFunction)：就地归约（输入、输出都是 Sale）===");
        System.out.println("    状态：ReducingState（每个 key+窗口只存\"归约后的那一条\"），不是 ListState");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Sale> sales = env.fromData(
                        new Sale("u1", BASE + 0L, 10),
                        new Sale("u1", BASE + 1000L, 20),
                        new Sale("u1", BASE + 3000L, 30),
                        new Sale("u2", BASE + 2000L, 100))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Sale, String> keyed = sales.keyBy(s -> s.user);

        // reduce 的结果类型 == 输入类型；这里把 amount 累加，ts 取第一条
        SingleOutputStreamOperator<Sale> reduced =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        .reduce((ReduceFunction<Sale>) (a, b) -> new Sale(a.user, a.ts, a.amount + b.amount));

        reduced.map(s -> "    [reduce] user=" + s.user + " 归约结果 amount=" + s.amount
                        + "（输出时间戳 = 窗口 maxTimestamp，不是元素时间）")
                .print("1-reduce");

        env.execute("WindowAggregateDemo-1-reduce");
    }

    /** 2) aggregate(AggregateFunction)：自定义累加器。 */
    private static void demoAggregate() throws Exception {
        System.out.println();
        System.out.println("=== 2) .aggregate(AggregateFunction)：自己实现累加器（输入 Sale / 累加器 Stats / 输出 String 三类型可不同）===");
        System.out.println("    四个方法：createAccumulator / add（每来一条调一次）/ getResult（触发时调）/ merge（合并窗口时调）");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Sale> sales = env.fromData(
                        new Sale("u1", BASE + 0L, 10),
                        new Sale("u1", BASE + 1000L, 20),
                        new Sale("u1", BASE + 3000L, 30),
                        new Sale("u2", BASE + 2000L, 100),
                        new Sale("u2", BASE + 4000L, 5))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Sale, String> keyed = sales.keyBy(s -> s.user);

        SingleOutputStreamOperator<String> aggregated =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        .aggregate(new StatsAggregate());

        aggregated.map(s -> "    [aggregate] " + s).print("2-aggregate");

        env.execute("WindowAggregateDemo-2-aggregate");
    }

    /** 3) 内置聚合：sum / min / max。 */
    private static void demoBuiltInAggregations() throws Exception {
        System.out.println();
        System.out.println("=== 3) 内置聚合 .sum / .min / .max（字段表达式与位置表达式的两种写法）===");
        System.out.println("    POJO 用字段名 .sum(\"amount\")；Tuple 用位置 .sum(1)");
        System.out.println("    它们本质上是 Flink 自带的 ReduceFunction（AggregationFunction），同样走增量聚合");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Sale> sales = env.fromData(
                        new Sale("u1", BASE + 0L, 10),
                        new Sale("u1", BASE + 1000L, 20),
                        new Sale("u1", BASE + 3000L, 30),
                        new Sale("u2", BASE + 2000L, 100))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Sale, String> keyed = sales.keyBy(s -> s.user);
        WindowedStream<Sale, String, TimeWindow> window =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)));

        // 3.1 POJO 字段名写法
        window.sum("amount").map(s -> "    [sum(\"amount\")] user=" + s.user + " 求和=" + s.amount)
                .print("3a-sum");
        // 3.2 min / max 返回的是「最小值/最大值那一条元素」本身（其他字段跟着那条元素走）
        window.min("amount").map(s -> "    [min(\"amount\")] 最小那条=" + s).print("3b-min");
        window.max("amount").map(s -> "    [max(\"amount\")] 最大那条=" + s).print("3c-max");

        // 3.3 Tuple 位置写法
        DataStream<Tuple2<String, Integer>> tuples = env.fromData(
                        Tuple2.of("u1", 10), Tuple2.of("u1", 20), Tuple2.of("u2", 7))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Tuple2<String, Integer>>)
                                                (element, recordTimestamp) -> BASE));

        tuples.keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .sum(1) // 位置 1 = f1
                .map(t -> "    [sum(1) 位置写法] " + t + "（时间戳固定成 BASE，所以都落在第一个窗口）")
                .print("3d-sum-position");

        env.execute("WindowAggregateDemo-3-builtin");
    }

    /** 4) aggregate + ProcessWindowFunction：增量聚合拿到窗口元信息。 */
    private static void demoAggregateWithWindowFunction() throws Exception {
        System.out.println();
        System.out.println("=== 4) .aggregate(AggregateFunction, ProcessWindowFunction)：增量聚合 + 窗口元信息（推荐写法）===");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Sale> sales = env.fromData(
                        new Sale("u1", BASE + 0L, 10),
                        new Sale("u1", BASE + 1000L, 20),
                        new Sale("u1", BASE + 3000L, 30))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        SingleOutputStreamOperator<String> result =
                sales.keyBy(s -> s.user)
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        .aggregate(
                                new StatsAggregate(true),
                                new ProcessWindowFunction<String, String, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                            String key,
                                            Context context,
                                            Iterable<String> elements,
                                            Collector<String> out) {
                                        List<String> list = new ArrayList<>();
                                        for (String s : elements) {
                                            list.add(s);
                                        }
                                        // 增量聚合路径下 elements 恒为 singleton：里面就是 getResult 的产物
                                        System.out.println("    [aggregate+Process] key=" + key + " 窗口 "
                                                + win(context.window()) + " 元素数=" + list.size() + " -> " + list);
                                        out.collect("    [aggregate+Process] key=" + key + " " + win(context.window())
                                                + " -> " + list);
                                    }
                                });

        result.print("4-aggregate+Process");
        env.execute("WindowAggregateDemo-4-aggregate-process");
    }

    /** 5) 会话窗口 + merge：合并窗口为什么必须有 merge。 */
    private static void demoMergeForSessionWindows() throws Exception {
        System.out.println();
        System.out.println("=== 5) 会话窗口（合并窗口）里的 AggregateFunction.merge ===");
        System.out.println("    会话窗口是 MergingWindowAssigner：新元素可能把两个已存在的窗口合并成一个，");
        System.out.println("    WindowOperator 于是调用 mergeNamespaces(target, sources)，落到 keyed state 上就是");
        System.out.println("    HeapAggregatingState.merge -> AggregateFunction.merge（HeapAggregatingState.java#L110）");
        System.out.println("    所以：给会话窗口用的 AggregateFunction 如果 merge 乱写（或没实现），结果就是错的");
        System.out.println("    数据（gap=2s，key=u1，水位线压后 4s 以免窗口提前触发并清理）：");
        System.out.println("      +0    → 会话 A [0,2000)   累加器 {count=1, sum=10}");
        System.out.println("      +2500 → 会话 B [2500,4500) 累加器 {count=1, sum=20}");
        System.out.println("      +1500 → 【乱序】它同时落在 A 的 gap 和 B 的 gap 内 ⇒ 把两个【已存在】的窗口架桥合并");
        System.out.println("              ⇒ 这一步才会真正调用 AggregateFunction.merge（下面会打印 [merge] 行）");
        System.out.println("    注意：单个会话窗口只是被'吸收/延长'时（sources 为空）merge 不会被调用，");
        System.out.println("    必须是这种'一条乱序数据桥接两个已有窗口'的情形才会触发（见 mergeNamespaces 的空 sources 短路）");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Sale> sales = env.fromData(
                        new Sale("u2", BASE + 0L, 100),
                        new Sale("u1", BASE + 0L, 10),
                        new Sale("u1", BASE + 2500L, 20),
                        // ⭐ 乱序元素：+1500 比上一条 +2500 早，且同时与 +0、+2500 的间隔都不超过 gap(2s)
                        //    ⇒ 把会话 A 与会话 B 合并，WindowOperator 调 mergeNamespaces → AggregateFunction.merge
                        new Sale("u1", BASE + 1500L, 5),
                        new Sale("u1", BASE + 9000L, 30),
                        new Sale("u1", BASE + 11000L, 40))
                // 水位线压后 4s：保证插入 +1500 时两个会话窗口都还没触发、状态还在
                .assignTimestampsAndWatermarks(perRecordWatermarks(4000L));

        SingleOutputStreamOperator<String> result =
                sales.keyBy(s -> s.user)
                        .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(2)))
                        .aggregate(
                                new StatsAggregate(true),
                                new ProcessWindowFunction<String, String, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                            String key,
                                            Context context,
                                            Iterable<String> elements,
                                            Collector<String> out) {
                                        for (String s : elements) {
                                            System.out.println("    [会话+aggregate] key=" + key + " 窗口 "
                                                    + win(context.window()) + " -> " + s);
                                            out.collect("    [会话+aggregate] key=" + key + " "
                                                    + win(context.window()) + " -> " + s);
                                        }
                                    }
                                });

        result.print("5-session-merge");
        env.execute("WindowAggregateDemo-5-session-merge");
    }

    /** 6) 状态占用对照说明（纯打印，不建作业）。 */
    private static void explainStateSize() {
        System.out.println();
        System.out.println("=== 6) 状态占用对照：增量聚合 vs 全量遍历 ===");
        System.out.println("  增量聚合 .reduce / .aggregate");
        System.out.println("      状态名            : \"window-contents\"（WindowOperatorBuilder.java#L71）");
        System.out.println("      状态类型          : ReducingState / AggregatingState");
        System.out.println("      每个 key+窗口存   : 恰好 1 个累加器（本例 4 个 long/int 字段）");
        System.out.println("      元素到达时的动作  : O(1) 就地合并，元素本身立刻可以被丢弃");
        System.out.println("  全量遍历 .apply(WindowFunction) / .process(ProcessWindowFunction)");
        System.out.println("      状态类型          : ListState<StreamRecord<IN>>");
        System.out.println("      每个 key+窗口存   : 窗口内【全部】元素（含时间戳等 StreamRecord 包装开销）");
        System.out.println("      元素到达时的动作  : append 到 ListState，触发时才一次性读出遍历");
        System.out.println("  结论：5 分钟窗口 × 每秒 1 万条 = 单窗口 300 万条元素。全量路径要扛 300 万条的状态，");
        System.out.println("        增量路径只扛 1 个累加器 —— 这就是\"优先用 reduce/aggregate\"的原因。");
        System.out.println("        需要窗口起止时间等元信息时，用 aggregate(af, ProcessWindowFunction) 两全其美。");
        System.out.println("  另外：一旦调用 .evictor(...)，算子变成 EvictingWindowOperator，窗口内容强制为 ListState<StreamRecord>，");
        System.out.println("        增量聚合就用不了了（见 WindowOperatorBuilder#L293）。");
    }

    // ==================================================================================

    public static void main(String[] args) throws Exception {
        System.out.println("### WindowAggregateDemo：窗口增量聚合（reduce / aggregate / 内置聚合 / aggregate+Process / merge）###");

        demoReduce();
        demoAggregate();
        demoBuiltInAggregations();
        demoAggregateWithWindowFunction();
        demoMergeForSessionWindows();
        explainStateSize();

        System.out.println();
        System.out.println("=== 全部小节结束 ===");
    }
}
